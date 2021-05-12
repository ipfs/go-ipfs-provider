package batched

import (
	"context"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"strconv"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	q "github.com/ipfs/go-ipfs-provider/queue"
)

type mockProvideMany struct {
	keys []mh.Multihash
}

func (m *mockProvideMany) ProvideMany(ctx context.Context, keys []mh.Multihash) error {
	m.keys = keys
	return nil
}

func (m *mockProvideMany) Ready() bool {
	return true
}

var _ provideMany = (*mockProvideMany)(nil)

func TestBatched(t *testing.T) {
	ctx := context.Background()
	defer ctx.Done()

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	queue, err := q.NewQueue(ctx, "test", ds)
	if err != nil {
		t.Fatal(err)
	}

	provider := &mockProvideMany{}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	const numProvides = 100
	keysToProvide := make(map[cid.Cid]int)
	for i := 0; i < numProvides; i++ {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		c := cid.NewCidV1(cid.Raw, h)
		keysToProvide[c] = i
	}

	batchSystem, err := New(provider, queue, KeyProvider(func(ctx context.Context) (<-chan cid.Cid, error) {
		ch := make(chan cid.Cid)
		go func() {
			for k := range keysToProvide {
				select {
				case ch <- k:
				case <-ctx.Done():
					return
				}
			}
		}()
		return ch, nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	batchSystem.Run()

	for {
		if ctx.Err() != nil {
			t.Fatal("test hung")
		}
		if len(provider.keys) != 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	if len(provider.keys) != numProvides {
		t.Fatalf("expected %d provider keys, got %d", numProvides, len(provider.keys))
	}

	provMap := make(map[string]struct{})
	for _, k := range provider.keys {
		provMap[string(k)] = struct{}{}
	}

	for i := 0; i < numProvides; i++ {
		h, err := mh.Sum([]byte(strconv.Itoa(i)), mh.SHA2_256, -1)
		if err != nil {
			panic(err)
		}
		if _, found := provMap[string(h)]; !found {
			t.Fatalf("could not find provider with value %d", i)
		}
	}
}
