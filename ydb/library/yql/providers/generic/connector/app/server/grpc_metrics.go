package server

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
	"github.com/ydb-platform/ydb/library/go/core/metrics/solomon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func UnaryServerMetrics(registry metrics.Registry) grpc.UnaryServerInterceptor {
	requestCount := registry.CounterVec("requests_total", []string{"protocol", "endpoint"})
	requestDuration := registry.DurationHistogramVec("request_duration_seconds", metrics.MakeExponentialDurationBuckets(250*time.Microsecond, 1.5, 35), []string{"protocol", "endpoint"})
	panicsCount := registry.CounterVec("panics_total", []string{"protocol", "endpoint"})
	inflightRequests := registry.GaugeVec("inflight_requests", []string{"protocol", "endpoint"})
	statusCount := registry.CounterVec("status_total", []string{"protocol", "endpoint", "status"})
	requestBytes := registry.CounterVec("request_bytes", []string{"protocol", "endpoint"})
	responseBytes := registry.CounterVec("response_bytes", []string{"protocol", "endpoint"})

	solomon.Rated(requestCount)
	solomon.Rated(requestDuration)
	solomon.Rated(panicsCount)
	solomon.Rated(statusCount)

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ any, err error) {
		deferFunc := func(startTime time.Time, opName string) {
			requestDuration.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).RecordDuration(time.Since(startTime))

			inflightRequests.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).Add(-1)

			if p := recover(); p != nil {
				panicsCount.With(map[string]string{
					"protocol": "grpc",
					"endpoint": opName,
				}).Inc()
				panic(p)
			}
		}

		opName := info.FullMethod

		requestBytes.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(int64(proto.Size(req.(proto.Message))))

		requestCount.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Inc()

		inflightRequests.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(1)

		startTime := time.Now()
		defer deferFunc(startTime, opName)

		resp, err := handler(ctx, req)

		code := status.Code(err)
		statusCount.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
			"status":   code.String(),
		}).Inc()

		responseBytes.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(int64(proto.Size(resp.(proto.Message))))

		return resp, err
	}
}

func StreamServerMetrics(registry metrics.Registry) grpc.StreamServerInterceptor {
	streamCount := registry.CounterVec("streams_total", []string{"protocol", "endpoint"})
	streamDuration := registry.DurationHistogramVec("stream_duration_seconds", metrics.MakeExponentialDurationBuckets(250*time.Microsecond, 1.5, 35), []string{"protocol", "endpoint"})
	inflightStreams := registry.GaugeVec("inflight_streams", []string{"protocol", "endpoint"})
	panicsCount := registry.CounterVec("panics_total", []string{"protocol", "endpoint"})
	sentStreamMessages := registry.CounterVec("sent_stream_messages_total", []string{"protocol", "endpoint"})
	receivedBytes := registry.CounterVec("received_bytes", []string{"protocol", "endpoint"})
	sentBytes := registry.CounterVec("sent_bytes", []string{"protocol", "endpoint"})
	statusCount := registry.CounterVec("stream_status_total", []string{"protocol", "endpoint", "status"})
	receivedStreamMessages := registry.CounterVec("received_stream_messages_total", []string{"protocol", "endpoint"})

	solomon.Rated(streamCount)
	solomon.Rated(streamDuration)
	solomon.Rated(panicsCount)
	solomon.Rated(sentStreamMessages)
	solomon.Rated(receivedStreamMessages)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		deferFunc := func(startTime time.Time, opName string) {
			streamDuration.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).RecordDuration(time.Since(startTime))

			inflightStreams.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}).Add(-1)

			if p := recover(); p != nil {
				panicsCount.With(map[string]string{
					"protocol": "grpc",
					"endpoint": opName,
				}).Inc()
				panic(p)
			}
		}

		opName := info.FullMethod

		streamCount.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Inc()

		inflightStreams.With(map[string]string{
			"protocol": "grpc",
			"endpoint": opName,
		}).Add(1)

		startTime := time.Now()
		defer deferFunc(startTime, opName)

		return handler(srv, serverStreamWithMessagesCount{
			ServerStream: ss,
			sentStreamMessages: sentStreamMessages.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			receivedStreamMessages: receivedStreamMessages.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			sentBytes: sentBytes.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			receivedBytes: receivedBytes.With(map[string]string{
				"protocol": "grpc",
				"endpoint": opName,
			}),
			getStatusCounter: func(code string) metrics.Counter {
				return statusCount.With(map[string]string{
					"protocol": "grpc",
					"endpoint": opName,
					"status":   code,
				})
			},
		})
	}
}

type serverStreamWithMessagesCount struct {
	grpc.ServerStream
	sentStreamMessages     metrics.Counter
	receivedStreamMessages metrics.Counter
	sentBytes              metrics.Counter
	receivedBytes          metrics.Counter
	getStatusCounter       func(string) metrics.Counter
}

func (s serverStreamWithMessagesCount) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)

	if err == nil {
		s.sentStreamMessages.Inc()
		s.sentBytes.Add(int64(proto.Size(m.(proto.Message))))
	}

	code := status.Code(err)
	s.getStatusCounter(code.String()).Inc()

	return err
}

func (s serverStreamWithMessagesCount) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)

	if err == nil {
		s.receivedStreamMessages.Inc()
		s.receivedBytes.Add(int64(proto.Size(m.(proto.Message))))
	}

	code := status.Code(err)
	s.getStatusCounter(code.String()).Inc()

	return err
}
