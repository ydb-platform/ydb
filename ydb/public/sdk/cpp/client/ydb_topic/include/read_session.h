#pragma once

#include "counters.h"
#include "executor.h"
#include "read_events.h"
#include "retry_policy.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>

#include <library/cpp/logger/log.h>

#include <util/generic/size_literals.h>

namespace NYdb::NTable {
    class TTransaction;
}

namespace NYdb::NTopic {

//! Read settings for single topic.
struct TTopicReadSettings {
    using TSelf = TTopicReadSettings;

    TTopicReadSettings() = default;
    TTopicReadSettings(const TTopicReadSettings&) = default;
    TTopicReadSettings(TTopicReadSettings&&) = default;
    TTopicReadSettings(const TString& path) {
        Path(path);
    }

    TTopicReadSettings& operator=(const TTopicReadSettings&) = default;
    TTopicReadSettings& operator=(TTopicReadSettings&&) = default;

    //! Path of topic to read.
    FLUENT_SETTING(TString, Path);

    //! Start reading from this timestamp.
    FLUENT_SETTING_OPTIONAL(TInstant, ReadFromTimestamp);

    //! Partition ids to read.
    //! 0-based.
    FLUENT_SETTING_VECTOR(ui64, PartitionIds);

    //! Max message time lag. All messages older that now - MaxLag will be ignored.
    FLUENT_SETTING_OPTIONAL(TDuration, MaxLag);
};

//! Settings for read session.
struct TReadSessionSettings: public TRequestSettings<TReadSessionSettings> {
    using TSelf = TReadSessionSettings;

    struct TEventHandlers {
        using TSelf = TEventHandlers;

        //! Set simple handler with data processing and also
        //! set other handlers with default behaviour.
        //! They automatically commit data after processing
        //! and confirm partition session events.
        //!
        //! Sets the following handlers:
        //! DataReceivedHandler: sets DataReceivedHandler to handler that calls dataHandler and (if
        //! commitDataAfterProcessing is set) then calls Commit(). CommitAcknowledgementHandler to handler that does
        //! nothing. StartPartitionSessionHandler to handler that confirms event. StopPartitionSessionHandler to
        //! handler that confirms event. PartitionSessionStatusHandler to handler that does nothing.
        //! PartitionSessionClosedHandler to handler that does nothing.
        //!
        //! dataHandler: handler of data event.
        //! commitDataAfterProcessing: automatically commit data after calling of dataHandler.
        //! gracefulReleaseAfterCommit: wait for commit acknowledgements for all inflight data before confirming
        //! partition session stop.
        TSelf& SimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler,
                                  bool commitDataAfterProcessing = false, bool gracefulStopAfterCommit = true);

        //! Data size limit for the DataReceivedHandler handler.
        //! The data size may exceed this limit.
        FLUENT_SETTING_DEFAULT(size_t, MaxMessagesBytes, Max<size_t>());

        //! Function to handle data events.
        //! If this handler is set, data events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TDataReceivedEvent&)>, DataReceivedHandler);

        //! Function to handle commit ack events.
        //! If this handler is set, commit ack events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TCommitOffsetAcknowledgementEvent&)>,
                       CommitOffsetAcknowledgementHandler);

        //! Function to handle start partition session events.
        //! If this handler is set, create partition session events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TStartPartitionSessionEvent&)>,
                       StartPartitionSessionHandler);

        //! Function to handle stop partition session events.
        //! If this handler is set, destroy partition session events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TStopPartitionSessionEvent&)>,
                       StopPartitionSessionHandler);

        //! Function to handle end partition session events.
        //! If this handler is set, end partition session events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TEndPartitionSessionEvent&)>,
                       EndPartitionSessionHandler);

        //! Function to handle partition session status events.
        //! If this handler is set, partition session status events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TPartitionSessionStatusEvent&)>,
                       PartitionSessionStatusHandler);

        //! Function to handle partition session closed events.
        //! If this handler is set, partition session closed events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TPartitionSessionClosedEvent&)>,
                       PartitionSessionClosedHandler);

        //! Function to handle session closed events.
        //! If this handler is set, close session events will be handled by handler
        //! and then sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(TSessionClosedHandler, SessionClosedHandler);

        //! Function to handle all event types.
        //! If event with current type has no handler for this type of event,
        //! this handler (if specified) will be used.
        //! If this handler is not specified, event can be received with TReadSession::GetEvent() method.
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TEvent&)>, CommonHandler);

        //! Executor for handlers.
        //! If not set, default single threaded executor will be used.
        FLUENT_SETTING(IExecutor::TPtr, HandlersExecutor);
    };


    TString ConsumerName_ = "";
    //! Consumer.
    TSelf& ConsumerName(const TString& name) {
        ConsumerName_ = name;
        WithoutConsumer_ = false;
        return static_cast<TSelf&>(*this);
    }

    bool WithoutConsumer_ = false;
    //! Read without consumer.
    TSelf& WithoutConsumer() {
        WithoutConsumer_ = true;
        ConsumerName_ = "";
        return static_cast<TSelf&>(*this);
    }

    //! Topics.
    FLUENT_SETTING_VECTOR(TTopicReadSettings, Topics);

    //! Maximum memory usage for read session.
    FLUENT_SETTING_DEFAULT(size_t, MaxMemoryUsageBytes, 100_MB);

    //! Max message time lag. All messages older that now - MaxLag will be ignored.
    FLUENT_SETTING_OPTIONAL(TDuration, MaxLag);

    //! Start reading from this timestamp.
    FLUENT_SETTING_OPTIONAL(TInstant, ReadFromTimestamp);

    //! Policy for reconnections.
    //! IRetryPolicy::GetDefaultPolicy() if null (not set).
    FLUENT_SETTING(IRetryPolicy::TPtr, RetryPolicy);

    //! Event handlers.
    //! See description in TEventHandlers class.
    FLUENT_SETTING(TEventHandlers, EventHandlers);

    //! Decompress messages
    FLUENT_SETTING_DEFAULT(bool, Decompress, true);

    //! Executor for decompression tasks.
    //! If not set, default executor will be used.
    FLUENT_SETTING(IExecutor::TPtr, DecompressionExecutor);

    //! Counters.
    //! If counters are not provided explicitly,
    //! they will be created inside session (without link with parent counters).
    FLUENT_SETTING(TReaderCounters::TPtr, Counters);

    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Seconds(30));

    //! AutoscalingSupport.
    FLUENT_SETTING_DEFAULT(bool, AutoscalingSupport, false);

    //! Log.
    FLUENT_SETTING_OPTIONAL(TLog, Log);
};

struct TReadSessionGetEventSettings : public TCommonClientSettingsBase<TReadSessionGetEventSettings> {
    using TSelf = TReadSessionGetEventSettings;

    FLUENT_SETTING_DEFAULT(bool, Block, false);
    FLUENT_SETTING_OPTIONAL(size_t, MaxEventsCount);
    FLUENT_SETTING_DEFAULT(size_t, MaxByteSize, std::numeric_limits<size_t>::max());
    FLUENT_SETTING_OPTIONAL(std::reference_wrapper<NTable::TTransaction>, Tx);
};

class IReadSession {
public:
    //! Main reader loop.
    //! Wait for next reader event.
    virtual NThreading::TFuture<void> WaitEvent() = 0;

    //! Main reader loop.
    //! Get read session events.
    //! Blocks until event occurs if "block" is set.
    //!
    //! maxEventsCount: maximum events count in batch.
    //! maxByteSize: total size limit of data messages in batch.
    //! block: block until event occurs.
    //!
    //! If maxEventsCount is not specified,
    //! read session chooses event batch size automatically.
    virtual TVector<TReadSessionEvent::TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing(),
                                                         size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    virtual TVector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) = 0;

    //! Get single event.
    virtual TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block = false,
                                                       size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    virtual TMaybe<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) = 0;

    //! Close read session.
    //! Waits for all commit acknowledgments to arrive.
    //! Force close after timeout.
    //! This method is blocking.
    //! When session is closed,
    //! TSessionClosedEvent arrives.
    virtual bool Close(TDuration timeout = TDuration::Max()) = 0;

    //! Reader counters with different stats (see TReaderConuters).
    virtual TReaderCounters::TPtr GetCounters() const = 0;

    //! Get unique identifier of read session.
    virtual TString GetSessionId() const = 0;

    virtual ~IReadSession() = default;
};

}
