#pragma once

#include "aliases.h"
#include "read_events.h"

#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>

#include <library/cpp/logger/log.h>

#include <util/generic/size_literals.h>


namespace NYdb::NPersQueue {

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
    FLUENT_SETTING_OPTIONAL(TInstant, StartingMessageTimestamp);

    //! Partition groups to read.
    //! 1-based.
    FLUENT_SETTING_VECTOR(ui64, PartitionGroupIds);
};

//! Settings for read session.
struct TReadSessionSettings : public TRequestSettings<TReadSessionSettings> {
    using TSelf = TReadSessionSettings;

    struct TEventHandlers {
        using TSelf = TEventHandlers;

        //! Set simple handler with data processing and also
        //! set other handlers with default behaviour.
        //! They automatically commit data after processing
        //! and confirm partition stream events.
        //!
        //! Sets the following handlers:
        //! DataReceivedHandler: sets DataReceivedHandler to handler that calls dataHandler and (if commitDataAfterProcessing is set) then calls Commit().
        //! CommitAcknowledgementHandler to handler that does nothing.
        //! CreatePartitionStreamHandler to handler that confirms event.
        //! DestroyPartitionStreamHandler to handler that confirms event.
        //! PartitionStreamStatusHandler to handler that does nothing.
        //! PartitionStreamClosedHandler to handler that does nothing.
        //!
        //! dataHandler: handler of data event.
        //! commitDataAfterProcessing: automatically commit data after calling of dataHandler.
        //! gracefulReleaseAfterCommit: wait for commit acknowledgements for all inflight data before confirming partition stream destroy.
        TSelf& SimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler, bool commitDataAfterProcessing = false, bool gracefulReleaseAfterCommit = true);

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
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TCommitAcknowledgementEvent&)>, CommitAcknowledgementHandler);

        //! Function to handle create partition stream events.
        //! If this handler is set, create partition stream events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TCreatePartitionStreamEvent&)>, CreatePartitionStreamHandler);

        //! Function to handle destroy partition stream events.
        //! If this handler is set, destroy partition stream events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TDestroyPartitionStreamEvent&)>, DestroyPartitionStreamHandler);

        //! Function to handle partition stream status events.
        //! If this handler is set, partition stream status events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TPartitionStreamStatusEvent&)>, PartitionStreamStatusHandler);

        //! Function to handle partition stream closed events.
        //! If this handler is set, partition stream closed events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TPartitionStreamClosedEvent&)>, PartitionStreamClosedHandler);

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

    //! Consumer.
    FLUENT_SETTING(TString, ConsumerName);

    //! Topics.
    FLUENT_SETTING_VECTOR(TTopicReadSettings, Topics);

    //! Default variant.
    //! Read topic instance specified in "Topics" from all clusters.
    TSelf& ReadAll() {
        Clusters_.clear();
        return ReadOnlyOriginal(true);
    }

    //! Read original topic instances specified in "Topics" from several clusters.
    TSelf& ReadOriginal(TVector<TString> clusters) {
        Clusters_ = std::move(clusters);
        return ReadOnlyOriginal(true);
    }

    //! Read mirrored topics specified in "Topics" from one cluster.
    TSelf& ReadMirrored(const TString& cluster) {
        Clusters_ = { cluster };
        return ReadOnlyOriginal(false);
    }

    //! Disable Clusters discovery. ReadMirrored/ReadOriginal/ReadAll will not have any effect
    //! if this option is true.
    FLUENT_SETTING_DEFAULT(bool, DisableClusterDiscovery, false);

    //! Maximum memory usage for read session.
    FLUENT_SETTING_DEFAULT(size_t, MaxMemoryUsageBytes, 100_MB);

    //! Max message time lag. All messages older that now - MaxTimeLag will be ignored.
    FLUENT_SETTING_OPTIONAL(TDuration, MaxTimeLag);

    //! Start reading from this timestamp.
    FLUENT_SETTING_OPTIONAL(TInstant, StartingMessageTimestamp);

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

    //! Read only original topic instance, don't read mirrored.
    //!
    //! It's better to control this setting via ReadAll()/ReadMirrored()/ReadOriginal() helpers.
    FLUENT_SETTING_DEFAULT(bool, ReadOnlyOriginal, true);

    //! Read topics from specified clusters.
    //!
    //! It's better to control this setting via ReadAll()/ReadMirrored()/ReadOriginal() helpers.
    //!
    //! 1. If ReadOnlyOriginal is true and Clusters are empty read will be done from all topic instances from all clusters.
    //! Use ReadAll() function for this variant.
    //! 2. If ReadOnlyOriginal is true and Clusters are not empty read will be done from specified clusters.
    //! Use ReadOriginal() function for this variant.
    //! 3. If ReadOnlyOriginal is false and one cluster is specified read will be done from all topic instances (mirrored and original) in one cluster.
    //! Use ReadMirrored() function for this variant.
    FLUENT_SETTING_VECTOR(TString, Clusters);

    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Seconds(30));

    //! Experimental option
    FLUENT_SETTING_OPTIONAL(bool, RangesMode);

    //! Log.
    FLUENT_SETTING_OPTIONAL(TLog, Log);
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
    virtual TVector<TReadSessionEvent::TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing(), size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    //! Get single event.
    virtual TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block = false, size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    //! Add topic to session, in other words, start reading new topic.
    // virtual void AddTopic(const TTopicReadSettings& topicReadSettings) = 0; // Not implemented yet.

    //! Remove topic from session.
    // virtual void RemoveTopic(const TString& path) = 0; // Not implemented yet.

    //! Remove partition groups of topic from session.
    // virtual void RemoveTopic(const TString& path, const TVector<ui64>& partitionGruops) = 0; // Not implemented yet.

    //! Stop reading data and process only control events.
    //! You might need this function if a receiving side
    //! is not ready to process data.
    virtual void StopReadingData() = 0;

    //! Resume reading data.
    virtual void ResumeReadingData() = 0;

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

}  // namespace NYdb::NPersQueue
