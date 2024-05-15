#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/include/client.h>

#include <ydb/public/api/protos/ydb_federation_discovery.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

#include <unordered_set>

namespace NYdb::NFederatedTopic {

using NTopic::TPrintable;
using TDbInfo = Ydb::FederationDiscovery::DatabaseInfo;

using TSessionClosedEvent = NTopic::TSessionClosedEvent;

//! Federated partition session.
struct TFederatedPartitionSession : public TThrRefBase, public TPrintable<TFederatedPartitionSession> {
    using TPtr = TIntrusivePtr<TFederatedPartitionSession>;

public:
    TFederatedPartitionSession(const NTopic::TPartitionSession::TPtr& partitionSession,
                               std::shared_ptr<TDbInfo> db,
                               std::shared_ptr<TDbInfo> originDb = nullptr,
                               TString originPath = "")
        : PartitionSession(partitionSession)
        , ReadSourceDatabase(std::move(db))
        , TopicOriginDatabase(originDb ? std::move(originDb) : ReadSourceDatabase)
        , TopicOriginPath(originPath ? std::move(originPath) : PartitionSession->GetTopicPath())
        {
            Y_ABORT_UNLESS(ReadSourceDatabase);
        }

    //! Request partition session status.
    //! Result will come to TPartitionSessionStatusEvent.
    void RequestStatus() {
        return PartitionSession->RequestStatus();
    }

    //!
    //! Properties.
    //!

    //! Unique identifier of partition session.
    //! It is unique within one read session.
    ui64 GetPartitionSessionId() const {
        return PartitionSession->GetPartitionSessionId();
    }

    //! Topic path.
    const TString& GetTopicPath() const {
        return TopicOriginPath;
    }

    //! Partition id.
    ui64 GetPartitionId() const {
        return PartitionSession->GetPartitionId();
    }

    const TString& GetDatabaseName() const {
        return GetTopicOriginDatabaseName();
    }

    const TString& GetDatabasePath() const {
        return GetTopicOriginDatabasePath();
    }

    const TString& GetDatabaseId() const {
        return GetTopicOriginDatabaseId();
    }

    const TString& GetReadSourceDatabaseName() const {
        return ReadSourceDatabase->name();
    }

    const TString& GetReadSourceDatabasePath() const {
        return ReadSourceDatabase->path();
    }

    const TString& GetReadSourceDatabaseId() const {
        return ReadSourceDatabase->id();
    }

    const TString& GetTopicOriginDatabaseName() const {
        return TopicOriginDatabase->name();
    }

    const TString& GetTopicOriginDatabasePath() const {
        return TopicOriginDatabase->path();
    }

    const TString& GetTopicOriginDatabaseId() const {
        return TopicOriginDatabase->id();
    }

private:
    NTopic::TPartitionSession::TPtr PartitionSession;
    std::shared_ptr<TDbInfo> ReadSourceDatabase;
    std::shared_ptr<TDbInfo> TopicOriginDatabase;
    TString TopicOriginPath;
};

//! Events for read session.
struct TReadSessionEvent {
    class TFederatedPartitionSessionAccessor {
    public:
        explicit TFederatedPartitionSessionAccessor(TFederatedPartitionSession::TPtr partitionSession)
            : FederatedPartitionSession(std::move(partitionSession))
            {}

        inline const TFederatedPartitionSession::TPtr GetFederatedPartitionSession() const {
            return FederatedPartitionSession;
        }

    protected:
        TFederatedPartitionSession::TPtr FederatedPartitionSession;
    };

    template <typename TEvent>
    struct TFederated : public TFederatedPartitionSessionAccessor, public TEvent, public TPrintable<TFederated<TEvent>> {
        using TPrintable<TFederated<TEvent>>::DebugString;

        TFederated(TEvent event, TFederatedPartitionSession::TPtr federatedPartitionSession)
            : TFederatedPartitionSessionAccessor(std::move(federatedPartitionSession))
            , TEvent(std::move(event))
            {}

        const NTopic::TPartitionSession::TPtr& GetPartitionSession() const override {
            ythrow yexception() << "GetPartitionSession() method unavailable for federated objects, use GetFederatedPartitionSession() instead";
        }
    };

    using TCommitOffsetAcknowledgementEvent = TFederated<NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>;
    using TStartPartitionSessionEvent = TFederated<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>;
    using TStopPartitionSessionEvent = TFederated<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>;
    using TEndPartitionSessionEvent = TFederated<NTopic::TReadSessionEvent::TEndPartitionSessionEvent>;
    using TPartitionSessionStatusEvent = TFederated<NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>;
    using TPartitionSessionClosedEvent = TFederated<NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>;

    struct TDataReceivedEvent : public NTopic::TReadSessionEvent::TPartitionSessionAccessor, public TFederatedPartitionSessionAccessor, public TPrintable<TDataReceivedEvent> {
        using TMessage = TFederated<NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>;
        using TCompressedMessage = TFederated<NTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>;

    public:
        TDataReceivedEvent(NTopic::TReadSessionEvent::TDataReceivedEvent event, TFederatedPartitionSession::TPtr federatedPartitionSession);

        const NTopic::TPartitionSession::TPtr& GetPartitionSession() const override {
            ythrow yexception() << "GetPartitionSession method unavailable for federated objects, use GetFederatedPartitionSession instead";
        }

        bool HasCompressedMessages() const {
            return !CompressedMessages.empty();
        }

        size_t GetMessagesCount() const {
            return Messages.size() + CompressedMessages.size();
        }

        //! Get messages.
        TVector<TMessage>& GetMessages() {
            CheckMessagesFilled(false);
            return Messages;
        }

        const TVector<TMessage>& GetMessages() const {
            CheckMessagesFilled(false);
            return Messages;
        }

        //! Get compressed messages.
        TVector<TCompressedMessage>& GetCompressedMessages() {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        const TVector<TCompressedMessage>& GetCompressedMessages() const {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        //! Commits all messages in batch.
        void Commit();

    private:
        void CheckMessagesFilled(bool compressed) const {
            Y_ABORT_UNLESS(!Messages.empty() || !CompressedMessages.empty());
            if (compressed && CompressedMessages.empty()) {
                ythrow yexception() << "cannot get compressed messages, parameter decompress=true for read session";
            }
            if (!compressed && Messages.empty()) {
                ythrow yexception() << "cannot get decompressed messages, parameter decompress=false for read session";
            }
        }

    private:
        TVector<TMessage> Messages;
        TVector<TCompressedMessage> CompressedMessages;
        std::vector<std::pair<ui64, ui64>> OffsetRanges;
    };

    using TEvent = std::variant<TDataReceivedEvent,
                                TCommitOffsetAcknowledgementEvent,
                                TStartPartitionSessionEvent,
                                TStopPartitionSessionEvent,
                                TEndPartitionSessionEvent,
                                TPartitionSessionStatusEvent,
                                TPartitionSessionClosedEvent,
                                TSessionClosedEvent>;
};

//! Set of offsets to commit.
//! Class that could store offsets in order to commit them later.
//! This class is not thread safe.
class TDeferredCommit {
public:
    //! Add message to set.
    void Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    //! Add all messages from dataReceivedEvent to set.
    void Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent);

    //! Add offsets range to set.
    void Add(const TFederatedPartitionSession& partitionSession, ui64 startOffset, ui64 endOffset);

    //! Add offset to set.
    void Add(const TFederatedPartitionSession& partitionSession, ui64 offset);

    //! Commit all added offsets.
    void Commit();

    TDeferredCommit();
    TDeferredCommit(const TDeferredCommit&) = delete;
    TDeferredCommit(TDeferredCommit&&);
    TDeferredCommit& operator=(const TDeferredCommit&) = delete;
    TDeferredCommit& operator=(TDeferredCommit&&);

    ~TDeferredCommit();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

//! Event debug string.
TString DebugString(const TReadSessionEvent::TEvent& event);


//! Settings for federated write session.
struct TFederatedWriteSessionSettings : public NTopic::TWriteSessionSettings {
    using TSelf = TFederatedWriteSessionSettings;

    //! Preferred database
    //! If specified database is unavailable, session will write to other database.
    FLUENT_SETTING_OPTIONAL(TString, PreferredDatabase);

    //! Write to other databases if there are problems with connection
    //! to the preferred one.
    FLUENT_SETTING_DEFAULT(bool, AllowFallback, true);

    TFederatedWriteSessionSettings() = default;
    TFederatedWriteSessionSettings(const TFederatedWriteSessionSettings&) = default;
    TFederatedWriteSessionSettings(TFederatedWriteSessionSettings&&) = default;
    TFederatedWriteSessionSettings& operator=(const TFederatedWriteSessionSettings&) = default;
    TFederatedWriteSessionSettings& operator=(TFederatedWriteSessionSettings&&) = default;

    TFederatedWriteSessionSettings(const TString& path, const TString& producerId, const TString& messageGroupId)
        : NTopic::TWriteSessionSettings(path, producerId, messageGroupId) {
    }

    TFederatedWriteSessionSettings(const NTopic::TWriteSessionSettings& settings)
        : NTopic::TWriteSessionSettings(settings) {
    }
    TFederatedWriteSessionSettings(NTopic::TWriteSessionSettings&& settings)
        : NTopic::TWriteSessionSettings(std::move(settings)) {
    }
    // TFederatedWriteSessionSettings& operator=(const NTopic::TWriteSessionSettings&);
    // TFederatedWriteSessionSettings& operator=(NTopic::TWriteSessionSettings&&);
};

//! Settings for read session.
struct TFederatedReadSessionSettings: public NTopic::TReadSessionSettings {
    using TSelf = TFederatedReadSessionSettings;

    NTopic::TReadSessionSettings& EventHandlers(const TEventHandlers&) {
        ythrow yexception() << "EventHandlers can not be set for federated session, use FederatedEventHandlers instead";
    }

    // Each handler, if set, is wrapped up and passed down to each subsession
    struct TFederatedEventHandlers {
        using TSelf = TFederatedEventHandlers;

        struct TSimpleDataHandlers {
            std::function<void(TReadSessionEvent::TDataReceivedEvent&)> DataHandler;
            bool CommitDataAfterProcessing;
            bool GracefulStopAfterCommit;
        };

        //! Set simple handler with data processing and also
        //! set other handlers with default behaviour.
        //! They automatically commit data after processing
        //! and confirm partition session events.
        //!
        //! Sets the following handlers:
        //! DataReceivedHandler: sets DataReceivedHandler to handler that calls dataHandler and (if
        //! commitDataAfterProcessing is set) then calls Commit(). CommitAcknowledgementHandler to handler that does
        //! nothing. CreatePartitionSessionHandler to handler that confirms event. StopPartitionSessionHandler to
        //! handler that confirms event. PartitionSessionStatusHandler to handler that does nothing.
        //! PartitionSessionClosedHandler to handler that does nothing.
        //!
        //! dataHandler: handler of data event.
        //! commitDataAfterProcessing: automatically commit data after calling of dataHandler.
        //! gracefulReleaseAfterCommit: wait for commit acknowledgements for all inflight data before confirming
        //! partition session destroy.
        TSimpleDataHandlers SimpleDataHandlers_;

        TSelf& SimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler,
                                  bool commitDataAfterProcessing = false, bool gracefulStopAfterCommit = true) {
            SimpleDataHandlers_.DataHandler = std::move(dataHandler);
            SimpleDataHandlers_.CommitDataAfterProcessing = commitDataAfterProcessing;
            SimpleDataHandlers_.GracefulStopAfterCommit = gracefulStopAfterCommit;
            return static_cast<TSelf&>(*this);
        }

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
        FLUENT_SETTING(NTopic::TSessionClosedHandler, SessionClosedHandler);

        //! Function to handle all event types.
        //! If event with current type has no handler for this type of event,
        //! this handler (if specified) will be used.
        //! If this handler is not specified, event can be received with TReadSession::GetEvent() method.
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TEvent&)>, CommonHandler);

        //! Executor for handlers.
        //! If not set, default single threaded executor will be used.
        //! Shared between subsessions
        FLUENT_SETTING(NTopic::IExecutor::TPtr, HandlersExecutor);
    };

    //! Federated event handlers.
    //! See description in TFederatedEventHandlers class.
    FLUENT_SETTING(TFederatedEventHandlers, FederatedEventHandlers);


    //! Read policy settings

    //! Databases to read from.
    //! Default (empty) value means reading from all available databases.
    //! Adding duplicates or unavailable databases is okay, they will be ignored.
    struct TReadOriginalSettings {
        //! Add reading from specified database if it's available.
        TReadOriginalSettings& AddDatabase(TString database);

        //! Add reading from several specified databases, if available.
        TReadOriginalSettings& AddDatabases(std::vector<TString> databases);

        //! Add reading from database(s) with the same location as client.
        TReadOriginalSettings& AddLocal();

        std::unordered_set<TString> Databases;
    };

    //! Default variant.
    //! Read original topics specified in NTopic::TReadSessionSettings::Topics from databases, specified in settings.
    //! Discards previously set ReadOriginal and ReadMirrored settings.
    TSelf& ReadOriginal(TReadOriginalSettings settings);

    //! Read original and mirrored topics specified in NTopic::TReadSessionSettings::Topics
    //! from one specified database.
    //! Discards previously set ReadOriginal and ReadMirrored settings.
    TSelf& ReadMirrored(TString database);

    bool IsReadMirroredEnabled() {
        return ReadMirroredEnabled;
    }

    auto GetDatabasesToReadFrom() {
        return DatabasesToReadFrom;
    }

private:
    // Read policy settings, set via helpers above
    bool ReadMirroredEnabled = false;
    std::unordered_set<TString> DatabasesToReadFrom;
};



class IFederatedReadSession {
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
    virtual TVector<TReadSessionEvent::TEvent>
    GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing(),
              size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    //! Get single event.
    virtual TMaybe<TReadSessionEvent::TEvent>
    GetEvent(bool block = false, size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    //! Close read session.
    //! Waits for all commit acknowledgments to arrive.
    //! Force close after timeout.
    //! This method is blocking.
    //! When session is closed,
    //! TSessionClosedEvent arrives.
    virtual bool Close(TDuration timeout = TDuration::Max()) = 0;

    //! Reader counters with different stats (see TReaderConuters).
    virtual NTopic::TReaderCounters::TPtr GetCounters() const = 0;

    //! Get unique identifier of read session.
    virtual TString GetSessionId() const = 0;

    virtual ~IFederatedReadSession() = default;
};

struct TFederatedTopicClientSettings : public TCommonClientSettingsBase<TFederatedTopicClientSettings> {
    using TSelf = TFederatedTopicClientSettings;

    //! Default executor for compression tasks.
    FLUENT_SETTING_DEFAULT(NTopic::IExecutor::TPtr, DefaultCompressionExecutor, NTopic::CreateThreadPoolExecutor(2));

    //! Default executor for callbacks.
    FLUENT_SETTING_DEFAULT(NTopic::IExecutor::TPtr, DefaultHandlersExecutor, NTopic::CreateThreadPoolExecutor(1));

    //! Connection timeoout for federation discovery.
    FLUENT_SETTING_DEFAULT(TDuration, ConnectionTimeout, TDuration::Seconds(30));

    //! Retry policy enables automatic retries for non-fatal errors.
    FLUENT_SETTING_DEFAULT(NTopic::IRetryPolicy::TPtr, RetryPolicy, NTopic::IRetryPolicy::GetDefaultPolicy());
};

class TFederatedTopicClient {
public:
    class TImpl;

    // executors from settings are passed to subclients
    TFederatedTopicClient(const TDriver& driver, const TFederatedTopicClientSettings& settings = {});

    void ProvideCodec(NTopic::ECodec codecId, THolder<NTopic::ICodec>&& codecImpl);

    //! Create read session.
    std::shared_ptr<IFederatedReadSession> CreateReadSession(const TFederatedReadSessionSettings& settings);

    //! Create write session.
    // std::shared_ptr<NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TFederatedWriteSessionSettings& settings);
    std::shared_ptr<NTopic::IWriteSession> CreateWriteSession(const TFederatedWriteSessionSettings& settings);

protected:
    void OverrideCodec(NTopic::ECodec codecId, THolder<NTopic::ICodec>&& codecImpl);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NFederatedTopic

namespace NYdb::NTopic {

using namespace NFederatedTopic;

template<>
void TPrintable<TFederatedPartitionSession>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TDataReceivedEvent>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TStartPartitionSessionEvent>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TStopPartitionSessionEvent>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TEndPartitionSessionEvent>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TPartitionSessionStatusEvent>>::DebugString(TStringBuilder& res, bool) const;
template<>
void TPrintable<NFederatedTopic::TReadSessionEvent::TFederated<NFederatedTopic::TReadSessionEvent::TPartitionSessionClosedEvent>>::DebugString(TStringBuilder& res, bool) const;

}
