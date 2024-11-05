#pragma once

#include "aliases.h"
#include "write_events.h"

#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>

#include <util/generic/size_literals.h>

namespace NYdb::NPersQueue {

enum class EClusterDiscoveryMode {
    Auto = 0, // enables cluster discovery only for hostname "logbroker.yandex.net" and "logbroker-prestable.yandex.net"
    On,
    Off
};

//! Settings for write session.
struct TWriteSessionSettings : public TRequestSettings<TWriteSessionSettings> {
    using TSelf = TWriteSessionSettings;

    TWriteSessionSettings() = default;
    TWriteSessionSettings(const TWriteSessionSettings&) = default;
    TWriteSessionSettings(TWriteSessionSettings&&) = default;
    TWriteSessionSettings(const TString& path, const TString& messageGroupId) {
        Path(path);
        MessageGroupId(messageGroupId);
    }

    TWriteSessionSettings& operator=(const TWriteSessionSettings&) = default;
    TWriteSessionSettings& operator=(TWriteSessionSettings&&) = default;

    //! Path of topic to write.
    FLUENT_SETTING(TString, Path);

    //! MessageGroupId (aka SourceId) to use.
    FLUENT_SETTING(TString, MessageGroupId);

    //! Write to an exact partition group. Generally server assigns partition group automatically.
    //! Using this option is not recommended unless you know for sure why you need it.
    FLUENT_SETTING_OPTIONAL(ui32, PartitionGroupId);

    //! Preferred LB cluster. Used for multi-cluster installation.
    //! If specified cluster is unavailable, session will write to other cluster.
    FLUENT_SETTING_OPTIONAL(TString, PreferredCluster);

    //! Write to other clusters if there are problems with connection
    //! to the first one.
    FLUENT_SETTING_DEFAULT(bool, AllowFallbackToOtherClusters, true);

    //! codec and level to use for data compression prior to write.
    FLUENT_SETTING_DEFAULT(ECodec, Codec, ECodec::GZIP);
    FLUENT_SETTING_DEFAULT(i32, CompressionLevel, 4);

    //! Writer will not accept new messages if memory usage exceeds this limit.
    //! Memory usage consists of raw data pending compression and compressed messages being sent.
    FLUENT_SETTING_DEFAULT(ui64, MaxMemoryUsage, 20_MB);

    //! Maximum messages accepted by writer but not written (with confirmation from server).
    //! Writer will not accept new messages after reaching the limit.
    FLUENT_SETTING_DEFAULT(ui32, MaxInflightCount, 100000);

    //! Retry policy enables automatic retries for non-fatal errors.
    //! IRetryPolicy::GetDefaultPolicy() if null (not set).
    FLUENT_SETTING(IRetryPolicy::TPtr, RetryPolicy);

    //! User metadata that may be attached to write session.
    TWriteSessionSettings& AppendSessionMeta(const TString& key, const TString& value) {
        Meta_.Fields[key] = value;
        return *this;
    };

    NYdb::NPersQueue::TWriteSessionMeta Meta_;

    //! Writer will accumulate messages until reaching up to BatchFlushSize bytes
    //! but for no longer than BatchFlushInterval.
    //! Upon reaching FlushInterval or FlushSize limit, all messages will be written with one batch.
    //! Greatly increases performance for small messages.
    //! Setting either value to zero means immediate write with no batching. (Unrecommended, especially for clients
    //! sending small messages at high rate).
    FLUENT_SETTING_OPTIONAL(TDuration, BatchFlushInterval);
    FLUENT_SETTING_OPTIONAL(ui64, BatchFlushSizeBytes);

    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Seconds(30));

    FLUENT_SETTING_OPTIONAL(TWriterCounters::TPtr, Counters);

    //! Executor for compression tasks.
    //! If not set, default executor will be used.
    FLUENT_SETTING(IExecutor::TPtr, CompressionExecutor);

    struct TEventHandlers {
        using TSelf = TEventHandlers;
        using TWriteAckHandler = std::function<void(TWriteSessionEvent::TAcksEvent&)>;
        using TReadyToAcceptHandler = std::function<void(TWriteSessionEvent::TReadyToAcceptEvent&)>;

        //! Function to handle Acks events.
        //! If this handler is set, write ack events will be handled by handler,
        //! otherwise sent to TWriteSession::GetEvent().
        FLUENT_SETTING(TWriteAckHandler, AcksHandler);

        //! Function to handle ReadyToAccept event.
        //! If this handler is set, write these events will be handled by handler,
        //! otherwise sent to TWriteSession::GetEvent().
        FLUENT_SETTING(TReadyToAcceptHandler, ReadyToAcceptHandler);

        //! Function to handle close session events.
        //! If this handler is set, close session events will be handled by handler
        //! and then sent to TWriteSession::GetEvent().
        FLUENT_SETTING(TSessionClosedHandler, SessionClosedHandler);

        //! Function to handle all event types.
        //! If event with current type has no handler for this type of event,
        //! this handler (if specified) will be used.
        //! If this handler is not specified, event can be received with TWriteSession::GetEvent() method.
        FLUENT_SETTING(std::function<void(TWriteSessionEvent::TEvent&)>, CommonHandler);

        //! Executor for handlers.
        //! If not set, default single threaded executor will be used.
        FLUENT_SETTING(IExecutor::TPtr, HandlersExecutor);

        [[deprecated("Typo in name. Use ReadyToAcceptHandler instead.")]]
        TSelf& ReadyToAcceptHander(const TReadyToAcceptHandler& value) {
            return ReadyToAcceptHandler(value);
        }
    };

    //! Event handlers.
    FLUENT_SETTING(TEventHandlers, EventHandlers);

    //! Enables validation of SeqNo. If enabled, then writer will check writing with seqNo and without it and throws exception.
    FLUENT_SETTING_DEFAULT(bool, ValidateSeqNo, true);

    //! Manages cluster discovery mode.
    FLUENT_SETTING_DEFAULT(EClusterDiscoveryMode, ClusterDiscoveryMode, EClusterDiscoveryMode::Auto);
};

//! Simple write session. Does not need event handlers. Does not provide Events, ContinuationTokens, write Acks.
class ISimpleBlockingWriteSession : public TThrRefBase {
public:
    //! Write single message. Blocks for up to blockTimeout if inflight is full or memoryUsage is exceeded;
    //! return - true if write succeeded, false if message was not enqueued for write within blockTimeout.
    //! no Ack is provided.
    virtual bool Write(TStringBuf data, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing(),
                       const TDuration& blockTimeout = TDuration::Max()) = 0;

    //! Blocks till SeqNo is discovered from server. Returns 0 in case of failure on init.
    virtual ui64 GetInitSeqNo() = 0;

    //! Complete all active writes, wait for ack from server and close.
    //! closeTimeout - max time to wait. Empty Maybe means infinity.
    //! return - true if all writes were completed and acked. false if timeout was reached and some writes were aborted.

    virtual bool Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Returns true if write session is alive and acitve. False if session was closed.
    virtual bool IsAlive() const = 0;

    virtual TWriterCounters::TPtr GetCounters() = 0;

    //! Close immediately and destroy, don't wait for anything.
    virtual ~ISimpleBlockingWriteSession() = default;
};

//! Generic write session with all capabilities.
class IWriteSession {
public:
    //! Future that is set when next event is available.
    virtual NThreading::TFuture<void> WaitEvent() = 0;

    //! Wait and return next event. Use WaitEvent() for non-blocking wait.
    virtual TMaybe<TWriteSessionEvent::TEvent> GetEvent(bool block = false) = 0;

    //! Get several events in one call.
    //! If blocking = false, instantly returns up to maxEventsCount available events.
    //! If blocking = true, blocks till maxEventsCount events are available.
    //! If maxEventsCount is unset, write session decides the count to return itself.
    virtual TVector<TWriteSessionEvent::TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing()) = 0;

    //! Future that is set when initial SeqNo is available.
    virtual NThreading::TFuture<ui64> GetInitSeqNo() = 0;

    //! Write single message.
    //! continuationToken - a token earlier provided to client with ReadyToAccept event.
    virtual void Write(TContinuationToken&& continuationToken, TStringBuf data, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) = 0;

    //! Write single message that is already coded by codec. Codec from settings does not apply to this message.
    //! continuationToken - a token earlier provided to client with ReadyToAccept event.
    //! originalSize - size of unpacked message
    virtual void WriteEncoded(TContinuationToken&& continuationToken, TStringBuf data, ECodec codec, ui32 originalSize, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) = 0;


    //! Wait for all writes to complete (no more that closeTimeout()), than close. Empty maybe - means infinite timeout.
    //! return - true if all writes were completed and acked. false if timeout was reached and some writes were aborted.
    virtual bool Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Writer counters with different stats (see TWriterConuters).
    virtual TWriterCounters::TPtr GetCounters() = 0;

    //! Close() with timeout = 0 and destroy everything instantly.
    virtual ~IWriteSession() = default;
};


}  // namespace NYdb::NPersQueue
