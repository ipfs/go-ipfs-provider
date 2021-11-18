module github.com/ipfs/go-ipfs-provider

go 1.16

retract [v1.0.0, v1.0.1]

require (
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.2.1
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-cidutil v0.0.2
	github.com/ipfs/go-datastore v0.5.0
	github.com/ipfs/go-fetcher v1.5.0
	github.com/ipfs/go-ipfs-blockstore v0.2.1
	github.com/ipfs/go-ipfs-blocksutil v0.0.1
	github.com/ipfs/go-ipfs-exchange-offline v0.1.1
	github.com/ipfs/go-ipfs-routing v0.2.1
	github.com/ipfs/go-log v1.0.5
	github.com/ipfs/go-verifcid v0.0.1
	github.com/ipld/go-ipld-prime v0.11.0
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-testing v0.4.0
	github.com/multiformats/go-multihash v0.0.16
)
