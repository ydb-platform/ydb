#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

namespace NYdb::NFederatedTopic {

std::pair<ui64, ui64> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, ui64 index) {
    if (dataReceivedEvent.HasCompressedMessages()) {
        const auto& msg = dataReceivedEvent.GetCompressedMessages()[index];
        return {msg.GetOffset(), msg.GetOffset() + 1};
    }
    const auto& msg = dataReceivedEvent.GetMessages()[index];
    return {msg.GetOffset(), msg.GetOffset() + 1};
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TDeferredCommit

class TDeferredCommit::TImpl {
public:

    void Add(const TFederatedPartitionSession::TPtr& partitionSession, ui64 startOffset, ui64 endOffset);
    void Add(const TFederatedPartitionSession::TPtr& partitionSession, ui64 offset);

    void Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message);
    void Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent);

    void Commit();

private:
    static void Add(const TFederatedPartitionSession::TPtr& partitionSession, TDisjointIntervalTree<ui64>& offsetSet, ui64 startOffset, ui64 endOffset);

private:
    THashMap<TFederatedPartitionSession::TPtr, TDisjointIntervalTree<ui64>> Offsets;
};

#define GET_IMPL()                              \
    if (!Impl) {                                \
        Impl = std::make_shared<TImpl>();       \
    }                                           \
    Impl

void TDeferredCommit::Add(const TFederatedPartitionSession::TPtr& partitionSession, ui64 startOffset, ui64 endOffset) {
    GET_IMPL()->Add(partitionSession, startOffset, endOffset);
}

void TDeferredCommit::Add(const TFederatedPartitionSession::TPtr& partitionSession, ui64 offset) {
    GET_IMPL()->Add(partitionSession, offset);
}

void TDeferredCommit::Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    GET_IMPL()->Add(message);
}

void TDeferredCommit::Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent) {
    GET_IMPL()->Add(dataReceivedEvent);
}

#undef GET_IMPL

void TDeferredCommit::Commit() {
    if (Impl) {
        Impl->Commit();
    }
}

void TDeferredCommit::TImpl::Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    Y_ASSERT(message.GetFederatedPartitionSession());
    Add(message.GetFederatedPartitionSession(), message.GetOffset());
}

void TDeferredCommit::TImpl::Add(const TFederatedPartitionSession::TPtr& partitionSession, TDisjointIntervalTree<ui64>& offsetSet, ui64 startOffset, ui64 endOffset) {
    if (offsetSet.Intersects(startOffset, endOffset)) {
        ThrowFatalError(TStringBuilder() << "Commit set already has some offsets from half-interval ["
                                         << startOffset << "; " << endOffset
                                         << ") for partition session with id " << partitionSession->GetPartitionSessionId());
    } else {
        offsetSet.InsertInterval(startOffset, endOffset);
    }
}

void TDeferredCommit::TImpl::Add(const TFederatedPartitionSession::TPtr& partitionSession, ui64 startOffset, ui64 endOffset) {
    Y_ASSERT(partitionSession);
    Add(partitionSession, Offsets[partitionSession], startOffset, endOffset);
}

void TDeferredCommit::TImpl::Add(const TFederatedPartitionSession::TPtr& partitionSession, ui64 offset) {
    Y_ASSERT(partitionSession);
    auto& offsetSet = Offsets[partitionSession];
    if (offsetSet.Has(offset)) {
        ThrowFatalError(TStringBuilder() << "Commit set already has offset " << offset
                                         << " for partition session with id " << partitionSession->GetPartitionSessionId());
    } else {
        offsetSet.Insert(offset);
    }
}

void TDeferredCommit::TImpl::Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent) {
    const TFederatedPartitionSession::TPtr& partitionSession = dataReceivedEvent.GetFederatedPartitionSession();
    Y_ASSERT(partitionSession);
    auto& offsetSet = Offsets[partitionSession];
    auto [startOffset, endOffset] = GetMessageOffsetRange(dataReceivedEvent, 0);
    for (size_t i = 1; i < dataReceivedEvent.GetMessagesCount(); ++i) {
        auto msgOffsetRange = GetMessageOffsetRange(dataReceivedEvent, i);
        if (msgOffsetRange.first == endOffset) {
            endOffset = msgOffsetRange.second;
        } else {
            Add(partitionSession, offsetSet, startOffset, endOffset);
            startOffset = msgOffsetRange.first;
            endOffset = msgOffsetRange.second;
        }
    }
    Add(partitionSession, offsetSet, startOffset, endOffset);
}

void TDeferredCommit::TImpl::Commit() {
    for (auto&& [partitionSession, offsetRanges] : Offsets) {
        for (auto&& [startOffset, endOffset] : offsetRanges) {
            static_cast<NPersQueue::TPartitionStreamImpl<false>*>(partitionSession->GetPartitionSession())->Commit(startOffset, endOffset);
        }
    }
    Offsets.clear();
}

}
