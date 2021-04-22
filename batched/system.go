package batched

import (
	"context"
	"fmt"
	"github.com/ipfs/go-ipfs-provider/queue"
	"strconv"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
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

	q  *queue.Queue
	ds datastore.Batching

	provch, dynamicCh chan cid.Cid

	totalProvides, lastReprovideBatchSize     int
	avgProvideDuration, lastReprovideDuration time.Duration
}

var _ provider.System = (*BatchProvidingSystem)(nil)

type provideMany interface {
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

// Option defines the functional option type that can be used to configure
// BatchProvidingSystem instances
type Option func(system *BatchProvidingSystem) error

var lastReprovideKey = datastore.NewKey("/provider/reprovide/lastreprovide")

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
		dynamicCh:         make(chan cid.Cid, 1),
	}

	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}

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

			performedReprovide := false
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
				case c := <-s.dynamicCh:
					m[c] = struct{}{}
					pauseDetectTimer.Reset(time.Millisecond * 500)
					performedReprovide = true
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
				delete(m, c)
			}

			start := time.Now()
			err := s.rsys.ProvideMany(s.ctx, keys)
			if err != nil {
				log.Debugf("providing failed %v", err)
				continue
			}
			dur := time.Since(start)

			totalProvideTime := int64(s.totalProvides) * int64(s.avgProvideDuration)
			s.avgProvideDuration = time.Duration((totalProvideTime + int64(dur)) / int64(s.totalProvides+len(keys)))
			s.totalProvides += len(keys)

			if performedReprovide {
				s.lastReprovideBatchSize = len(keys)
				s.lastReprovideDuration = dur

				if err := s.ds.Put(lastReprovideKey, storeTime(time.Now())); err != nil {
					log.Errorf("could not store last reprovide time: %v", err)
				}
				if err := s.ds.Sync(lastReprovideKey); err != nil {
					log.Errorf("could not perform sync of last reprovide time: %v", err)
				}
			}
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
	if !s.shouldReprovide() && !force {
		return nil
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

func (s *BatchProvidingSystem) getLastReprovideTime() (time.Time, error) {
	val, err := s.ds.Get(lastReprovideKey)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not get last reprovide time")
	}

	t, err := getTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not decode last reprovide time, got %q", string(val))
	}

	return t, nil
}

func (s *BatchProvidingSystem) shouldReprovide() bool {
	t, err := s.getLastReprovideTime()
	if err != nil {
		log.Debugf(err.Error())
		return false
	}

	if time.Since(t) < time.Duration(float64(s.reprovideInterval)*0.5) {
		return false
	}
	return true
}

type BatchedProviderStats struct {
	TotalProvides, LastReprovideBatchSize     int
	AvgProvideDuration, LastReprovideDuration time.Duration
}

// Stat returns various stats about this provider system
func (s *BatchProvidingSystem) Stat(ctx context.Context) (BatchedProviderStats, error) {
	// TODO: Does it matter that there is no locking around the total+average values?
	return BatchedProviderStats{
		TotalProvides:          s.totalProvides,
		LastReprovideBatchSize: s.lastReprovideBatchSize,
		AvgProvideDuration:     s.avgProvideDuration,
		LastReprovideDuration:  s.lastReprovideDuration,
	}, nil
}
