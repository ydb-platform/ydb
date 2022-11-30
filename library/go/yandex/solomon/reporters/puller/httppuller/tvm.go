package httppuller

import "a.yandex-team.ru/library/go/yandex/tvm"

const (
	FetcherPreTVMID  = 2012024
	FetcherTestTVMID = 2012026
	FetcherProdTVMID = 2012028
)

var (
	AllFetchers = []tvm.ClientID{
		FetcherPreTVMID,
		FetcherTestTVMID,
		FetcherProdTVMID,
	}
)

type tvmOption struct {
	client tvm.Client
}

func (*tvmOption) isOption() {}

func WithTVM(tvm tvm.Client) Option {
	return &tvmOption{client: tvm}
}
