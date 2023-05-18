package provider

import (
	"context"

	"github.com/ipfs/go-cid"
)

type offlineProvider struct{}

// NewOfflineProvider creates a ProviderSystem that does nothing
//
// Deprecated: use github.com/ipfs/boxo/provider.NewOfflineProvider
func NewOfflineProvider() System {
	return &offlineProvider{}
}

func (op *offlineProvider) Run() {
}

func (op *offlineProvider) Close() error {
	return nil
}

func (op *offlineProvider) Provide(cid.Cid) error {
	return nil
}

func (op *offlineProvider) Reprovide(context.Context) error {
	return nil
}
