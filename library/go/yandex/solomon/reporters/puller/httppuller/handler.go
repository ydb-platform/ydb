package httppuller

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/core/metrics/solomon"
	"a.yandex-team.ru/library/go/httputil/headers"
	"a.yandex-team.ru/library/go/httputil/middleware/tvm"
)

const nilRegistryPanicMsg = "nil registry given"

type MetricsStreamer interface {
	StreamJSON(context.Context, io.Writer) (int, error)
	StreamSpack(context.Context, io.Writer, solomon.CompressionType) (int, error)
}

type handler struct {
	registry     MetricsStreamer
	streamFormat headers.ContentType
	checkTicket  func(h http.Handler) http.Handler
	logger       log.Logger
}

type Option interface {
	isOption()
}

// NewHandler returns new HTTP handler to expose gathered metrics using metrics dumper
func NewHandler(r MetricsStreamer, opts ...Option) http.Handler {
	if v := reflect.ValueOf(r); !v.IsValid() || v.Kind() == reflect.Ptr && v.IsNil() {
		panic(nilRegistryPanicMsg)
	}

	h := handler{
		registry:     r,
		streamFormat: headers.TypeApplicationJSON,
		checkTicket: func(h http.Handler) http.Handler {
			return h
		},
		logger: &nop.Logger{},
	}

	for _, opt := range opts {
		switch o := opt.(type) {
		case *tvmOption:
			h.checkTicket = tvm.CheckServiceTicket(o.client, tvm.WithAllowedClients(AllFetchers))
		case *spackOption:
			h.streamFormat = headers.TypeApplicationXSolomonSpack
		case *loggerOption:
			h.logger = o.logger
		default:
			panic(fmt.Sprintf("unsupported option %T", opt))
		}
	}

	return h.checkTicket(h)
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
