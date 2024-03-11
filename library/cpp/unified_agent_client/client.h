#pragma once

#include <library/cpp/unified_agent_client/counters.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NUnifiedAgent {
    struct TClientParameters {
        // uri format https://github.com/grpc/grpc/blob/master/doc/naming.md
        // for example: unix:///unified_agent for unix domain sockets or localhost:12345 for tcp
        explicit TClientParameters(const TString& uri);

        // Simple way to protect against writing to unintended/invalid Unified Agent endpoint.
        // Must correspond to 'shared_secret_key' grpc input parameter
        // (https://a.yandex-team.ru/arc/trunk/arcadia/logbroker/unified_agent/examples/all.yml?rev=6333542#L219),
        // session would end with error otherwise.
        //
        // Default: not set
        TClientParameters& SetSharedSecretKey(const TString& sharedSecretKey) {
            SharedSecretKey = sharedSecretKey;
            return *this;
        }

        // Max bytes count that have been received by client session but not acknowledged yet.
        // When exceeded, new messages will be discarded, an error message
        // will be written to the TLog instance and drop counter will be incremented.
        //
        // Default: 10 mb
        TClientParameters& SetMaxInflightBytes(size_t maxInflightBytes) {
            MaxInflightBytes = maxInflightBytes;
            return *this;
        }

        // TLog instance for client library's own logs.
        //
        // Default: TLoggerOperator<TGlobalLog>::Log()
        TClientParameters& SetLog(TLog log) {
            Log = std::move(log);
            return *this;
        }

        // Throttle client library log by rate limit in bytes, excess will be discarded.
        //
        // Default: not set
        TClientParameters& SetLogRateLimit(size_t bytesPerSec) {
            LogRateLimitBytes = bytesPerSec;
            return *this;
        }

        // Try to establish new grpc session if the current one become broken.
        // Session may break either due to agent unavailability, or the agent itself may
        // reject new session creation if it does not satisfy certain
        // conditions - shared_secret_key does not match, the session creation rate has been
        // exceeded, invalid session metadata has been used and so on.
        // Attempts to establish a grpc session will continue indefinitely.
        //
        // Default: 50 millis
        TClientParameters& SetGrpcReconnectDelay(TDuration delay) {
            GrpcReconnectDelay = delay;
            return *this;
        }

        // Grpc usually writes data to the socket faster than it comes from the client.
        // This means that it's possible that each TClientMessage would be sent in it's own grpc message.
        // This is expensive in terms of cpu, since grpc makes at least one syscall
        // for each message on the sender and receiver sides.
        // To avoid a large number of syscalls, the client holds incoming messages
        // in internal buffer in hope of being able to assemble bigger grpc batch.
        // This parameter sets the timeout for this delay - from IClientSession::Send
        // call to the actual sending of the corresponding grpc message.
        //
        // Default: 10 millis.
        TClientParameters& SetGrpcSendDelay(TDuration delay) {
            GrpcSendDelay = delay;
            return *this;
        }

        // Client library sends messages to grpc in batches, this parameter
        // establishes upper limit on the size of single batch in bytes.
        // If you increase this value, don't forget to adjust max_receive_message_size (https://a.yandex-team.ru/arc/trunk/arcadia/logbroker/unified_agent/examples/all.yml?rev=6661788#L185)
        // in grpc input config, it must be grater than GrpcMaxMessageSize.
        //
        // Default: 1 mb
        TClientParameters& SetGrpcMaxMessageSize(size_t size) {
            GrpcMaxMessageSize = size;
            return *this;
        }

        // Enable forks handling in client library.
        // Multiple threads and concurrent forks are all supported is this regime.
        //
        // Default: false
        TClientParameters& SetEnableForkSupport(bool value) {
            EnableForkSupport = value;
            return *this;
        }

        // Client library counters.
        // App can set this to some leaf of it's TDynamicCounters tree.
        // Actual provided counters are listed in TClientCounters.
        //
        // Default: not set
        TClientParameters& SetCounters(const NMonitoring::TDynamicCounterPtr& counters) {
            return SetCounters(MakeIntrusive<TClientCounters>(counters));
        }

        TClientParameters& SetCounters(const TIntrusivePtr<TClientCounters>& counters) {
            Counters = counters;
            return *this;
        }

    public:
        static const size_t DefaultMaxInflightBytes;
        static const size_t DefaultGrpcMaxMessageSize;
        static const TDuration DefaultGrpcSendDelay;

    public:
        TString Uri;
        TMaybe<TString> SharedSecretKey;
        size_t MaxInflightBytes;
        TLog Log;
        TMaybe<size_t> LogRateLimitBytes;
        TDuration GrpcReconnectDelay;
        TDuration GrpcSendDelay;
        bool EnableForkSupport;
        size_t GrpcMaxMessageSize;
        TIntrusivePtr<TClientCounters> Counters;
    };

    struct TSessionParameters {
        TSessionParameters();

        // Session unique identifier.
        // It's guaranteed that for messages with the same sessionId relative
        // ordering of the messages will be preserved at all processing stages
        // in library, in Unified Agent and in other systems that respect ordering (e.g., Logbroker)
        //
        // Default: generated automatically by Unified Agent.
        TSessionParameters& SetSessionId(const TString& sessionId) {
            SessionId = sessionId;
            return *this;
        }

        // Session metadata as key-value set.
        // Can be used by agent filters and outputs for validation/routing/enrichment/etc.
        //
        // Default: not set
        TSessionParameters& SetMeta(const THashMap<TString, TString>& meta) {
            Meta = meta;
            return *this;
        }

        // Session counters.
        // Actual provided counters are listed in TClientSessionCounters.
        //
        // Default: A single common for all sessions subgroup of client TDynamicCounters instance
        // with label ('session': 'default').
        TSessionParameters& SetCounters(const NMonitoring::TDynamicCounterPtr& counters) {
            return SetCounters(MakeIntrusive<TClientSessionCounters>(counters));
        }

        TSessionParameters& SetCounters(const TIntrusivePtr<TClientSessionCounters>& counters) {
            Counters = counters;
            return *this;
        }

        // Max bytes count that have been received by client session but not acknowledged yet.
        // When exceeded, new messages will be discarded, an error message
        // will be written to the TLog instance and drop counter will be incremented.
        //
        // Default: value from client settings
        TSessionParameters& SetMaxInflightBytes(size_t maxInflightBytes) {
            MaxInflightBytes = maxInflightBytes;
            return *this;
        }

    public:
        TMaybe<TString> SessionId;
        TMaybe<THashMap<TString, TString>> Meta;
        TIntrusivePtr<TClientSessionCounters> Counters;
        TMaybe<size_t> MaxInflightBytes;
    };

    // Message data to be sent to unified agent.
    struct TClientMessage {
        // Opaque message payload.
        TString Payload;

        // Message metadata as key-value set.
        // Can be used by agent filters and outputs for validation/routing/enrichment/etc.
        //
        // Default: not set
        TMaybe<THashMap<TString, TString>> Meta{};

        // Message timestamp.
        //
        // Default: time the client library has received this instance of TClientMessage.
        TMaybe<TInstant> Timestamp{};
    };

    // Message size as it is accounted in byte-related metrics (ReceivedBytes, InflightBytes, etc).
    size_t SizeOf(const TClientMessage& message);

    class IClientSession: public TAtomicRefCount<IClientSession> {
    public:
        virtual ~IClientSession() = default;

        // Places the message into send queue. Actual grpc call may occur later asynchronously,
        // based on settings GrpcSendDelay and GrpcMaxMessageSize.
        // A message can be discarded if the limits defined by the GrpcMaxMessageSize and MaxInflightBytes
        // settings are exceeded, or if the Close method has already been called.
        // In this case an error message will be written to the TLog instance
        // and drop counter will be incremented.
        virtual void Send(TClientMessage&& message) = 0;

        void Send(const TClientMessage& message) {
            Send(TClientMessage(message));
        }

        // Waits until either all current inflight messages are
        // acknowledged or the specified deadline is reached.
        // Upon the deadline grpc connection would be forcefully dropped (via grpc::ClientContext::TryCancel).
        virtual NThreading::TFuture<void> CloseAsync(TInstant deadline) = 0;

        void Close(TInstant deadline) {
            CloseAsync(deadline).Wait();
        }

        void Close(TDuration timeout = TDuration::Seconds(3)) {
            Close(Now() + timeout);
        }
    };
    using TClientSessionPtr = TIntrusivePtr<IClientSession>;

    class IClient: public TAtomicRefCount<IClient> {
    public:
        virtual ~IClient() = default;

        virtual TClientSessionPtr CreateSession(const TSessionParameters& parameters = {}) = 0;

        virtual void StartTracing(ELogPriority) {
        }

        virtual void FinishTracing() {
        }
    };
    using TClientPtr = TIntrusivePtr<IClient>;

    TClientPtr MakeClient(const TClientParameters& parameters);
}
