#include "read_session_impl.ipp"

#include <ydb-cpp-sdk/client/topic/read_events.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

namespace NYdb::inline Dev::NTopic {

std::pair<uint64_t, uint64_t> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, uint64_t index);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NTopic::TDeferredCommit

class TDeferredCommit::TImpl {
public:

    void Add(const TPartitionSession::TPtr& partitionStream, ui64 startOffset, ui64 endOffset);
    void Add(const TPartitionSession::TPtr& partitionStream, ui64 offset);

    void Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message);
    void Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent);

    void Commit();

private:
    static void Add(const TPartitionSession::TPtr& partitionStream, TDisjointIntervalTree<ui64>& offsetSet, ui64 startOffset, ui64 endOffset);

private:
    // Partition stream -> offsets set.
    std::unordered_map<TPartitionSession::TPtr, TDisjointIntervalTree<ui64>, THash<TPartitionSession::TPtr>> Offsets;
};

TDeferredCommit::TDeferredCommit() {
}

TDeferredCommit::TDeferredCommit(TDeferredCommit&&) = default;

TDeferredCommit& TDeferredCommit::operator=(TDeferredCommit&&) = default;

TDeferredCommit::~TDeferredCommit() {
}

#define GET_IMPL()                              \
    if (!Impl) {                                \
        Impl = std::make_unique<TImpl>();       \
    }                                           \
    Impl

void TDeferredCommit::Add(const TPartitionSession::TPtr& partitionStream, uint64_t startOffset, uint64_t endOffset) {
    GET_IMPL()->Add(partitionStream, startOffset, endOffset);
}

void TDeferredCommit::Add(const TPartitionSession::TPtr& partitionStream, uint64_t offset) {
    GET_IMPL()->Add(partitionStream, offset);
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
    Y_ASSERT(message.GetPartitionSession());
    Add(message.GetPartitionSession(), message.GetOffset());
}

void TDeferredCommit::TImpl::Add(const TPartitionSession::TPtr& partitionStream, TDisjointIntervalTree<ui64>& offsetSet, ui64 startOffset, ui64 endOffset) {
    if (offsetSet.Intersects(startOffset, endOffset)) {
        ThrowFatalError(TStringBuilder() << "Commit set already has some offsets from half-interval ["
                                         << startOffset << "; " << endOffset
                                         << ") for partition stream with id " << partitionStream->GetPartitionSessionId());
    } else {
        offsetSet.InsertInterval(startOffset, endOffset);
    }
}

void TDeferredCommit::TImpl::Add(const TPartitionSession::TPtr& partitionStream, ui64 startOffset, ui64 endOffset) {
    Y_ASSERT(partitionStream);
    Add(partitionStream, Offsets[partitionStream], startOffset, endOffset);
}

void TDeferredCommit::TImpl::Add(const TPartitionSession::TPtr& partitionStream, ui64 offset) {
    Y_ASSERT(partitionStream);
    auto& offsetSet = Offsets[partitionStream];
    if (offsetSet.Has(offset)) {
        ThrowFatalError(TStringBuilder() << "Commit set already has offset " << offset
                                         << " for partition stream with id " << partitionStream->GetPartitionSessionId());
    } else {
        offsetSet.Insert(offset);
    }
}

void TDeferredCommit::TImpl::Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent) {
    const TPartitionSession::TPtr& partitionStream = dataReceivedEvent.GetPartitionSession();
    Y_ASSERT(partitionStream);
    auto& offsetSet = Offsets[partitionStream];
    auto [startOffset, endOffset] = GetMessageOffsetRange(dataReceivedEvent, 0);
    for (size_t i = 1; i < dataReceivedEvent.GetMessagesCount(); ++i) {
        auto msgOffsetRange = GetMessageOffsetRange(dataReceivedEvent, i);
        if (msgOffsetRange.first == endOffset) {
            endOffset= msgOffsetRange.second;
        } else {
            Add(partitionStream, offsetSet, startOffset, endOffset);
            startOffset = msgOffsetRange.first;
            endOffset = msgOffsetRange.second;
        }
    }
    Add(partitionStream, offsetSet, startOffset, endOffset);
}

void TDeferredCommit::TImpl::Commit() {
    for (auto&& [partitionStream, offsetRanges] : Offsets) {
        for (auto&& [startOffset, endOffset] : offsetRanges) {
            static_cast<NTopic::TPartitionStreamImpl<false>*>(partitionStream.Get())->Commit(startOffset, endOffset);
        }
    }
    Offsets.clear();
}

}
