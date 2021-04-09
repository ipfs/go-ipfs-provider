package batched

import (
	"context"
	"fmt"
	"github.com/ipfs/go-ipfs-provider/queue"
	"strconv"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	provider "github.com/ipfs/go-ipfs-provider"
	"github.com/ipfs/go-ipfs-provider/simple"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-verifcid"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("provider.batched")

type BatchProvidingSystem struct {
	ctx   context.Context
	close context.CancelFunc

	reprovideInterval time.Duration
	rsys              provideMany
	keyProvider       simple.KeyChanFunc

	q                     *queue.Queue
	ds, managedDS, timeDS datastore.Batching

	provch, managedCh, dynamicCh chan cid.Cid

	totalProvides  int
	avgProvideTime time.Duration
}

var _ provider.System = (*BatchProvidingSystem)(nil)

type provideMany interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

// Option defines the functional option type that can be used to configure
// BatchProvidingSystem instances
type Option func(system *BatchProvidingSystem) error

var managedKey = datastore.NewKey("/provider/reprovide/managed")
var timeKey = datastore.NewKey("/provider/reprovide/time")

func New(provider provideMany, q *queue.Queue, opts ...Option) (*BatchProvidingSystem, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &BatchProvidingSystem{
		ctx:   ctx,
		close: cancel,

		reprovideInterval: time.Hour * 24,
		rsys:              provider,
		keyProvider:       nil,
		q:                 q,
		ds:                datastore.NewMapDatastore(),
		provch:            make(chan cid.Cid, 1),
		managedCh:         make(chan cid.Cid, 1),
		dynamicCh:         make(chan cid.Cid, 1),
	}

	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}

	s.managedDS = namespace.Wrap(s.ds, managedKey)
	s.timeDS = namespace.Wrap(s.ds, timeKey)

	return s, nil
}

func Datastore(batching datastore.Batching) Option {
	return func(system *BatchProvidingSystem) error {
		system.ds = batching
		return nil
	}
}

func ReproviderInterval(duration time.Duration) Option {
	return func(system *BatchProvidingSystem) error {
		system.reprovideInterval = duration
		return nil
	}
}

func KeyProvider(fn simple.KeyChanFunc) Option {
	return func(system *BatchProvidingSystem) error {
		system.keyProvider = fn
		return nil
	}
}

func (s *BatchProvidingSystem) Run() {
	go func() {
		m := make(map[cid.Cid]struct{})
		for {
			pauseDetectTimer := time.NewTimer(time.Hour)
			maxDurationCollectionTimer := time.NewTimer(time.Minute * 10)
		loop:
			for {
				select {
				case c := <-s.provch:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
					continue
				default:
				}

				select {
				case c := <-s.provch:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
					continue
				case c := <-s.managedCh:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
					continue
				default:
				}

				select {
				case c := <-s.provch:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
				case c := <-s.managedCh:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
				case c := <-s.dynamicCh:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
				case <-pauseDetectTimer.C:
					break loop
				case <-maxDurationCollectionTimer.C:
					break loop
				case <-s.ctx.Done():
					return
				}
			}
			keys := make([]multihash.Multihash, 0, len(m))
			for c := range m {
				// hash security
				if err := verifcid.ValidateCid(c); err != nil {
					log.Errorf("insecure hash in reprovider, %s (%s)", c, err)
					continue
				}

				keys = append(keys, c.Hash())
			}

			start := time.Now()
			err := s.rsys.ProvideMany(s.ctx, keys)
			if err != nil {
				log.Debugf("providing failed %v", err)
				continue
			}
			dur := time.Since(start)

			totalProvideTime := int64(s.totalProvides) * int64(s.avgProvideTime)
			s.avgProvideTime = time.Duration((totalProvideTime + int64(dur)) / int64(s.totalProvides+len(keys)))
			s.totalProvides += len(keys)

			for c := range m {
				s.timeDS.Put(dshelp.CidToDsKey(c), storeTime(time.Now()))
				delete(m, c)
			}
			s.timeDS.Sync(datastore.NewKey(""))
		}
	}()

	go func() {
		ch := s.q.Dequeue()
		for {
			select {
			case c := <-ch:
				s.provch <- c
			case <-s.ctx.Done():
				return
			}
		}
	}()

	go func() {
		var initialReprovideCh, reprovideCh <-chan time.Time

		// If reproviding is enabled (non-zero)
		if s.reprovideInterval > 0 {
			reprovideTicker := time.NewTicker(s.reprovideInterval)
			defer reprovideTicker.Stop()
			reprovideCh = reprovideTicker.C

			// If the reprovide ticker is larger than a minute (likely),
			// provide once after we've been up a minute.
			//
			// Don't provide _immediately_ as we might be just about to stop.
			if s.reprovideInterval > time.Minute {
				initialReprovideTimer := time.NewTimer(time.Minute)
				defer initialReprovideTimer.Stop()

				initialReprovideCh = initialReprovideTimer.C
			}
		}

		for s.ctx.Err() == nil {
			select {
			case <-initialReprovideCh:
			case <-reprovideCh:
			case <-s.ctx.Done():
				return
			}

			err := s.reprovide(s.ctx, false)

			// only log if we've hit an actual error, otherwise just tell the client we're shutting down
			if s.ctx.Err() == nil && err != nil {
				log.Errorf("failed to reprovide: %s", err)
			}
		}
	}()
}

func storeTime(t time.Time) []byte {
	val := []byte(fmt.Sprintf("%d", t.UnixNano()))
	return val
}

func getTime(b []byte) (time.Time, error) {
	tns, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, tns), nil
}

func (s *BatchProvidingSystem) Close() error {
	s.close()
	return s.q.Close()
}

func (s *BatchProvidingSystem) Provide(cid cid.Cid) error {
	return s.q.Enqueue(cid)
}

func (s *BatchProvidingSystem) Reprovide(ctx context.Context) error {
	return s.reprovide(ctx, true)
}

func (s *BatchProvidingSystem) reprovide(ctx context.Context, force bool) error {
	qres, err := s.managedDS.Query(query.Query{})
	if err != nil {
		return err
	}

	nextCh := qres.Next()
managedCidLoop:
	for {
		select {
		case r, ok := <-nextCh:
			if !ok {
				break managedCidLoop
			}
			c, err := dshelp.DsKeyToCid(datastore.NewKey(r.Key))
			if err != nil {
				log.Debugf("could not decode key %v as CID", r.Key)
				continue
			}

			if !s.shouldReprovide(c) && !force {
				continue
			}

			select {
			case s.managedCh <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	kch, err := s.keyProvider(ctx)
	if err != nil {
		return err
	}

dynamicCidLoop:
	for {
		select {
		case c, ok := <-kch:
			if !ok {
				break dynamicCidLoop
			}
			if !s.shouldReprovide(c) && !force {
				continue
			}

			select {
			case s.dynamicCh <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *BatchProvidingSystem) getLastReprovideTime(c cid.Cid) (time.Time, error) {
	k := dshelp.CidToDsKey(c)
	val, err := s.timeDS.Get(k)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get time for %v", k)
	}

	t, err := getTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not decode time for %v, got %q", k, string(val))
	}

	return t, nil
}

func (s *BatchProvidingSystem) shouldReprovide(c cid.Cid) bool {
	t, err := s.getLastReprovideTime(c)
	if err != nil {
		log.Debugf(err.Error())
		return false
	}

	if time.Since(t) < time.Duration(float64(s.reprovideInterval)*0.5) {
		return false
	}
	return true
}

// Stat returns the total number of provides we are responsible for,
// the number that have been recently provided, the total number of provides we have done
// since starting the system and the average time per provide
func (s *BatchProvidingSystem) Stat(ctx context.Context) (int, int, int, time.Duration, error) {
	// TODO: Overlap between managed + dynamic lists
	total := 0
	recentlyProvided := 0

	qres, err := s.managedDS.Query(query.Query{})
	if err != nil {
		return 0, 0, 0, 0, err
	}

	nextCh := qres.Next()
managedCidLoop:
	for {
		select {
		case r, ok := <-nextCh:
			if !ok {
				break managedCidLoop
			}
			total++
			c, err := dshelp.DsKeyToCid(datastore.NewKey(r.Key))
			if err != nil {
				log.Debugf("could not decode key %v as CID", r.Key)
				continue
			}

			t, err := s.getLastReprovideTime(c)
			if err != nil {
				log.Debugf(err.Error())
				continue
			}

			if time.Since(t) < s.reprovideInterval {
				recentlyProvided++
			}
		case <-ctx.Done():
			return 0, 0, 0, 0, ctx.Err()
		}
	}

	kch, err := s.keyProvider(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}

dynamicCidLoop:
	for {
		select {
		case c, ok := <-kch:
			if !ok {
				break dynamicCidLoop
			}
			total++
			t, err := s.getLastReprovideTime(c)
			if err != nil {
				log.Debugf(err.Error())
				continue
			}

			if time.Since(t) < s.reprovideInterval {
				recentlyProvided++
			}
		case <-ctx.Done():
			return 0, 0, 0, 0, ctx.Err()
		}
	}

	// TODO: Does it matter that there is no locking around the total+average values?
	return total, recentlyProvided, s.totalProvides, s.avgProvideTime, nil
}

func (s *BatchProvidingSystem) ProvideLongterm(cids ...cid.Cid) error {
	for _, c := range cids {
		k := dshelp.CidToDsKey(c)
		if err := s.managedDS.Put(k, []byte{}); err != nil {
			return err
		}
	}
	if err := s.managedDS.Sync(datastore.NewKey("")); err != nil {
		return err
	}
	return nil
}

func (s *BatchProvidingSystem) RemoveLongterm(cids ...cid.Cid) error {
	for _, c := range cids {
		k := dshelp.CidToDsKey(c)
		if err := s.managedDS.Delete(k); err != nil {
			return err
		}
	}
	if err := s.managedDS.Sync(datastore.NewKey("")); err != nil {
		return err
	}
	return nil
}

func (s *BatchProvidingSystem) GetLongtermProvides(ctx context.Context) (<-chan cid.Cid, error) {
	qres, err := s.managedDS.Query(query.Query{
		KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}

	ch := make(chan cid.Cid, 1)

	go func() {
		for r := range qres.Next() {
			c, err := dshelp.DsKeyToCid(datastore.NewKey(r.Key))
			if err != nil {
				log.Debugf("could not decode key %v as CID", r.Key)
			}
			select {
			case ch <- c:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
