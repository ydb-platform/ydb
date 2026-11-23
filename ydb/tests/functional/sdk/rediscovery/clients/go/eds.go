//go:build eds

package main

import "github.com/ydb-platform/ydb-go-sdk/v3"

func discoveryOptions() []ydb.Option { return []ydb.Option{ydb.WithEndpointDiscoveryStream()} }
