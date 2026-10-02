#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>
#include <util/string/cast.h>

namespace NYdb::inline Dev::NTopic {

std::pair<uint64_t, uint64_t> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, uint64_t index);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TReadSessionEventHandlers

class TGracefulReleasingSimpleDataHandlers : public TThrRefBase {
public:
    explicit TGracefulReleasingSimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler, bool commitAfterProcessing)
        : DataHandler(std::move(dataHandler))
        , CommitAfterProcessing(commitAfterProcessing)
    {
    }

    void OnDataReceived(TReadSessionEvent::TDataReceivedEvent& event) {
        Y_ASSERT(event.GetMessagesCount());
        TDeferredCommit deferredCommit;
        {
            std::lock_guard guard(Lock);
            const std::string key = GetEventKey(event);
            auto& offsetSet = PartitionStreamToUncommittedOffsets[key];
            // Messages could contain holes in offset, but later commit ack will tell us right border.
            // So we can easily insert the whole interval with holes included.
            // It will be removed from set by specifying proper right border.
            auto firstMessageOffsets = GetMessageOffsetRange(event, 0);
            auto lastMessageOffsets = GetMessageOffsetRange(event, event.GetMessagesCount() - 1);

            offsetSet.InsertInterval(firstMessageOffsets.first, lastMessageOffsets.second);

            if (CommitAfterProcessing) {
                deferredCommit.Add(event);
            }
        }
        DataHandler(event);
        deferredCommit.Commit();
    }

    void OnCommitAcknowledgement(TReadSessionEvent::TCommitOffsetAcknowledgementEvent& event) {
        std::lock_guard guard(Lock);
        const std::string key = GetEventKey(event);
        auto& offsetSet = PartitionStreamToUncommittedOffsets[key];
        if (offsetSet.EraseInterval(0, event.GetCommittedOffset() + 1)) { // Remove some offsets.
            if (offsetSet.Empty()) { // No offsets left.
                auto unconfirmedDestroyIt = UnconfirmedDestroys.find(key);
                if (unconfirmedDestroyIt != UnconfirmedDestroys.end()) {
                    // Confirm and forget about this partition stream.
                    unconfirmedDestroyIt->second.Confirm();
                    UnconfirmedDestroys.erase(unconfirmedDestroyIt);
                    PartitionStreamToUncommittedOffsets.erase(key);
                }
            }
        }
    }

    void OnCreatePartitionStream(TReadSessionEvent::TStartPartitionSessionEvent& event) {
        {
            std::lock_guard guard(Lock);
            const std::string key = GetEventKey(event);
            Y_ABORT_UNLESS(PartitionStreamToUncommittedOffsets[key].Empty());
        }
        event.Confirm();
    }

    void OnDestroyPartitionStream(TReadSessionEvent::TStopPartitionSessionEvent& event) {
        std::lock_guard guard(Lock);
        auto key = GetEventKey(event);
        Y_ABORT_UNLESS(UnconfirmedDestroys.find(key) == UnconfirmedDestroys.end());
        if (PartitionStreamToUncommittedOffsets[key].Empty()) {
            PartitionStreamToUncommittedOffsets.erase(key);
            event.Confirm();
        } else {
            UnconfirmedDestroys.emplace(key, event);
        }
    }

    void OnEndPartitionStream(TReadSessionEvent::TEndPartitionSessionEvent& event) {
        event.Confirm();
    }

    void OnPartitionStreamClosed(TReadSessionEvent::TPartitionSessionClosedEvent& event) {
        std::lock_guard guard(Lock);
        const std::string key = GetEventKey(event);
        PartitionStreamToUncommittedOffsets.erase(key);
        UnconfirmedDestroys.erase(key);
    }

private:
    template<typename TEvent>
    std::string GetEventKey(const TEvent& event) {
        return event.GetPartitionSession()->GetReadSessionId() + "_" + ToString(event.GetPartitionSession()->GetPartitionSessionId());
    }

    TAdaptiveLock Lock; // For the case when user gave us multithreaded executor.
    const std::function<void(TReadSessionEvent::TDataReceivedEvent&)> DataHandler;
    const bool CommitAfterProcessing;
    std::unordered_map<std::string, TDisjointIntervalTree<ui64>> PartitionStreamToUncommittedOffsets; // Session id + Partition stream id -> set of offsets.
    std::unordered_map<std::string, TReadSessionEvent::TStopPartitionSessionEvent> UnconfirmedDestroys; // Session id + Partition stream id -> destroy events.
};

TReadSessionSettings::TEventHandlers& TReadSessionSettings::TEventHandlers::SimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler,
                                                                                               bool commitDataAfterProcessing,
                                                                                               bool gracefulReleaseAfterCommit) {
    Y_ASSERT(dataHandler);

    PartitionSessionStatusHandler([](TReadSessionEvent::TPartitionSessionStatusEvent&){});

    if (gracefulReleaseAfterCommit) {
        auto handlers = MakeIntrusive<TGracefulReleasingSimpleDataHandlers>(std::move(dataHandler), commitDataAfterProcessing);
        DataReceivedHandler([handlers](TReadSessionEvent::TDataReceivedEvent& event) {
            handlers->OnDataReceived(event);
        });
        StartPartitionSessionHandler([handlers](TReadSessionEvent::TStartPartitionSessionEvent& event) {
            handlers->OnCreatePartitionStream(event);
        });
        StopPartitionSessionHandler([handlers](TReadSessionEvent::TStopPartitionSessionEvent& event) {
            handlers->OnDestroyPartitionStream(event);
        });
        EndPartitionSessionHandler([handlers](TReadSessionEvent::TEndPartitionSessionEvent& event) {
            handlers->OnEndPartitionStream(event);
        });
        CommitOffsetAcknowledgementHandler([handlers](TReadSessionEvent::TCommitOffsetAcknowledgementEvent& event) {
            handlers->OnCommitAcknowledgement(event);
        });
        PartitionSessionClosedHandler([handlers](TReadSessionEvent::TPartitionSessionClosedEvent& event) {
            handlers->OnPartitionStreamClosed(event);
        });
    } else {
        if (commitDataAfterProcessing) {
            DataReceivedHandler([dataHandler = std::move(dataHandler)](TReadSessionEvent::TDataReceivedEvent& event) {
                TDeferredCommit deferredCommit;
                deferredCommit.Add(event);
                dataHandler(event);
                deferredCommit.Commit();
            });
        } else {
            DataReceivedHandler(std::move(dataHandler));
        }
        StartPartitionSessionHandler([](TReadSessionEvent::TStartPartitionSessionEvent& event) {
            event.Confirm();
        });
        StopPartitionSessionHandler([](TReadSessionEvent::TStopPartitionSessionEvent& event) {
            event.Confirm();
        });
        EndPartitionSessionHandler([](TReadSessionEvent::TEndPartitionSessionEvent& event) {
            event.Confirm();
        });
        CommitOffsetAcknowledgementHandler([](TReadSessionEvent::TCommitOffsetAcknowledgementEvent&){});
        PartitionSessionClosedHandler([](TReadSessionEvent::TPartitionSessionClosedEvent&){});
    }
    return *this;
}

} // namespace NYdb::NTopic
