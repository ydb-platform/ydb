package server

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/nop"
	"github.com/ydb-platform/ydb/library/go/core/metrics/solomon"
	"github.com/ydb-platform/ydb/library/go/httputil/headers"
)

type MetricsStreamer interface {
	StreamJSON(context.Context, io.Writer) (int, error)
	StreamSpack(context.Context, io.Writer, solomon.CompressionType) (int, error)
}

type handler struct {
	registry     MetricsStreamer
	streamFormat headers.ContentType
	logger       log.Logger
}

type spackOption struct {
}

func (*spackOption) isOption() {}

func WithSpack() Option {
	return &spackOption{}
}

type Option interface {
	isOption()
}

// NewHTTPPullerHandler returns new HTTP handler to expose gathered metrics using metrics dumper
func NewHTTPPullerHandler(r MetricsStreamer, opts ...Option) http.Handler {
	h := handler{
		registry:     r,
		streamFormat: headers.TypeApplicationJSON,
		logger:       &nop.Logger{},
	}

	for _, opt := range opts {
		switch opt.(type) {
		case *spackOption:
			h.streamFormat = headers.TypeApplicationXSolomonSpack
		default:
			panic(fmt.Sprintf("unsupported option %T", opt))
		}
	}

	return h
}

func (h handler) okSpack(header http.Header) bool {
	if h.streamFormat != headers.TypeApplicationXSolomonSpack {
		return false
	}

	for _, header := range header[headers.AcceptKey] {
		types, err := headers.ParseAccept(header)
		if err != nil {
			h.logger.Warn("Can't parse accept header", log.Error(err), log.String("header", header))

			continue
		}

		for _, acceptableType := range types {
			if acceptableType.Type == headers.TypeApplicationXSolomonSpack {
				return true
			}
		}
	}

	return false
}

func (h handler) okLZ4Compression(header http.Header) bool {
	for _, header := range header[headers.AcceptEncodingKey] {
		encodings, err := headers.ParseAcceptEncoding(header)

		if err != nil {
			h.logger.Warn("Can't parse accept-encoding header", log.Error(err), log.String("header", header))

			continue
		}

		for _, acceptableEncoding := range encodings {
			if acceptableEncoding.Encoding == headers.EncodingLZ4 {
				return true
			}
		}
	}

	return false
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.okSpack(r.Header) {
		compression := solomon.CompressionNone

		if h.okLZ4Compression(r.Header) {
			compression = solomon.CompressionLz4
		}

		w.Header().Set(headers.ContentTypeKey, headers.TypeApplicationXSolomonSpack.String())
		_, err := h.registry.StreamSpack(r.Context(), w, compression)

		if err != nil {
			h.logger.Error("Failed to write compressed spack", log.Error(err))
		}

		return
	}

	w.Header().Set(headers.ContentTypeKey, headers.TypeApplicationJSON.String())
	_, err := h.registry.StreamJSON(r.Context(), w)

	if err != nil {
		h.logger.Error("Failed to write json", log.Error(err))
	}
}
