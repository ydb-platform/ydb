#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_session.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

namespace NYdb::NTopic {

std::pair<ui64, ui64> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, ui64 index);

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
        with_lock (Lock) {
            auto& offsetSet = PartitionStreamToUncommittedOffsets[event.GetPartitionSession()->GetPartitionSessionId()];
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
        with_lock (Lock) {
            const ui64 partitionStreamId = event.GetPartitionSession()->GetPartitionSessionId();
            auto& offsetSet = PartitionStreamToUncommittedOffsets[partitionStreamId];
            if (offsetSet.EraseInterval(0, event.GetCommittedOffset() + 1)) { // Remove some offsets.
                if (offsetSet.Empty()) { // No offsets left.
                    auto unconfirmedDestroyIt = UnconfirmedDestroys.find(partitionStreamId);
                    if (unconfirmedDestroyIt != UnconfirmedDestroys.end()) {
                        // Confirm and forget about this partition stream.
                        unconfirmedDestroyIt->second.Confirm();
                        UnconfirmedDestroys.erase(unconfirmedDestroyIt);
                        PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
                    }
                }
            }
        }
    }

    void OnCreatePartitionStream(TReadSessionEvent::TStartPartitionSessionEvent& event) {
        with_lock (Lock) {
            Y_ABORT_UNLESS(PartitionStreamToUncommittedOffsets[event.GetPartitionSession()->GetPartitionSessionId()].Empty());
        }
        event.Confirm();
    }

    void OnDestroyPartitionStream(TReadSessionEvent::TStopPartitionSessionEvent& event) {
        with_lock (Lock) {
            const ui64 partitionStreamId = event.GetPartitionSession()->GetPartitionSessionId();
            Y_ABORT_UNLESS(UnconfirmedDestroys.find(partitionStreamId) == UnconfirmedDestroys.end());
            if (PartitionStreamToUncommittedOffsets[partitionStreamId].Empty()) {
                PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
                event.Confirm();
            } else {
                UnconfirmedDestroys.emplace(partitionStreamId, std::move(event));
            }
        }
    }

    void OnEndPartitionStream(TReadSessionEvent::TEndPartitionSessionEvent& event) {
        event.Confirm();
    }

    void OnPartitionStreamClosed(TReadSessionEvent::TPartitionSessionClosedEvent& event) {
        with_lock (Lock) {
            const ui64 partitionStreamId = event.GetPartitionSession()->GetPartitionSessionId();
            PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
            UnconfirmedDestroys.erase(partitionStreamId);
        }
    }

private:
    TAdaptiveLock Lock; // For the case when user gave us multithreaded executor.
    const std::function<void(TReadSessionEvent::TDataReceivedEvent&)> DataHandler;
    const bool CommitAfterProcessing;
    THashMap<ui64, TDisjointIntervalTree<ui64>> PartitionStreamToUncommittedOffsets; // Partition stream id -> set of offsets.
    THashMap<ui64, TReadSessionEvent::TStopPartitionSessionEvent> UnconfirmedDestroys; // Partition stream id -> destroy events.
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

}  // namespace NYdb::NTopic
