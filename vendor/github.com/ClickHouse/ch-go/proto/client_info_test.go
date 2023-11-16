package proto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/ClickHouse/ch-go/internal/gold"
)

func TestClientInfo_EncodeAware(t *testing.T) {
	b := new(Buffer)
	v := queryCreateDatabase.Info
	v.EncodeAware(b, queryProtoVersion)
	gold.Bytes(t, b.Buf, "client_info")

	t.Run("DecodeAware", func(t *testing.T) {
		var i ClientInfo
		r := NewReader(bytes.NewReader(b.Buf))
		assert.NoError(t, i.DecodeAware(r, queryProtoVersion))
		assert.Equal(t, v, i)

		requireNoShortRead(t, b.Buf, aware(&i))
	})
}

func TestClientInfo_OpenTelemetry(t *testing.T) {
	b := new(Buffer)
	v := ClientInfo{
		ProtocolVersion: 54429,
		Major:           21,
		Minor:           11,
		Patch:           4,
		Interface:       InterfaceTCP,
		Query:           ClientQueryInitial,

		InitialUser:    "",
		InitialQueryID: "40c268ad-de50-434d-a391-800da9aa70c3",
		InitialAddress: "0.0.0.0:0",
		OSUser:         "user",
		ClientHostname: "hostname",
		ClientName:     "Name",
		Span: trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		}),
		QuotaKey:         "",
		DistributedDepth: 0,
	}
	v.EncodeAware(b, Version)
	gold.Bytes(t, b.Buf, "client_info_otel")

	t.Run("DecodeAware", func(t *testing.T) {
		var i ClientInfo
		r := NewReader(bytes.NewReader(b.Buf))
		assert.NoError(t, i.DecodeAware(r, Version))
		assert.Equal(t, v, i)
		requireNoShortRead(t, b.Buf, aware(&i))
	})
}
