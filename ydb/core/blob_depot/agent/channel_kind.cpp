#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    struct TBlobDepotAgent::TChannelKind::TGivenIdRangeHeapComp {
        using TValue = TBlobDepotAgent::TChannelKind::TGivenIdRangePerChannel::value_type*;

        bool operator ()(TValue x, TValue y) const {
            // FIXME: incorrect algorithm with respect to steps; we need to balance using per-channel window of written
            // bytes or items
            return x->second.GetMinimumValue() > y->second.GetMinimumValue();
        }
    };

    void TBlobDepotAgent::TChannelKind::IssueGivenIdRange(const NKikimrBlobDepot::TGivenIdRange& proto) {
        for (const auto& range : proto.GetChannelRanges()) {
            GivenIdRangePerChannel[range.GetChannel()].IssueNewRange(range.GetBegin(), range.GetEnd());
            NumAvailableItems += range.GetEnd() - range.GetBegin();
        }

        // build min-heap for ids
        RebuildHeap();

        ProcessQueriesWaitingForId();
    }

    ui32 TBlobDepotAgent::TChannelKind::GetNumAvailableItems() const {
#ifndef NDEBUG
        ui32 count = 0;
        for (const auto& [_, givenIdRanges] : GivenIdRangePerChannel) {
            count += givenIdRanges.GetNumAvailableItems();
        }
        Y_VERIFY(count == NumAvailableItems);
#endif
        return NumAvailableItems;
    }

    std::optional<TBlobSeqId> TBlobDepotAgent::TChannelKind::Allocate(TBlobDepotAgent& agent) {
        if (GivenIdRangeHeap.empty()) {
            return std::nullopt;
        }

        std::pop_heap(GivenIdRangeHeap.begin(), GivenIdRangeHeap.end(), TGivenIdRangeHeapComp());
        auto& [channel, range] = *GivenIdRangeHeap.back();
        const ui64 value = range.Allocate();
        if (range.IsEmpty()) {
            GivenIdRangeHeap.pop_back();
        } else {
            std::push_heap(GivenIdRangeHeap.begin(), GivenIdRangeHeap.end(), TGivenIdRangeHeapComp());
        }
        --NumAvailableItems;

        agent.IssueAllocateIdsIfNeeded(*this);

        return TBlobSeqId::FromSequentalNumber(channel, agent.BlobDepotGeneration, value);
    }

    std::pair<TLogoBlobID, ui32> TBlobDepotAgent::TChannelKind::MakeBlobId(TBlobDepotAgent& agent,
            const TBlobSeqId& blobSeqId, EBlobType type, ui32 part, ui32 size) const {
        auto id = blobSeqId.MakeBlobId(agent.TabletId, type, part, size);
        const auto [channel, groupId] = ChannelGroups[ChannelToIndex[blobSeqId.Channel]];
        Y_VERIFY_DEBUG(channel == blobSeqId.Channel);
        return {id, groupId};
    }

    void TBlobDepotAgent::TChannelKind::Trim(ui8 channel, ui32 generation, ui32 invalidatedStep) {
        const TBlobSeqId trimmedBlobSeqId{channel, generation, invalidatedStep, TBlobSeqId::MaxIndex};
        const ui64 validSince = trimmedBlobSeqId.ToSequentialNumber() + 1;
        auto& givenIdRanges = GivenIdRangePerChannel[channel];
        NumAvailableItems -= givenIdRanges.GetNumAvailableItems();
        givenIdRanges.Trim(validSince);
        NumAvailableItems += givenIdRanges.GetNumAvailableItems();
        RebuildHeap();
    }

    void TBlobDepotAgent::TChannelKind::RebuildHeap() {
        GivenIdRangeHeap.clear();
        for (auto& kv : GivenIdRangePerChannel) {
            if (!kv.second.IsEmpty()) {
                GivenIdRangeHeap.push_back(&kv);
            }
        }
        std::make_heap(GivenIdRangeHeap.begin(), GivenIdRangeHeap.end(), TGivenIdRangeHeapComp());
    }

    void TBlobDepotAgent::TChannelKind::EnqueueQueryWaitingForId(TQuery *query) {
        QueriesWaitingForId.PushBack(query);
    }

    void TBlobDepotAgent::TChannelKind::ProcessQueriesWaitingForId() {
        TIntrusiveList<TQuery, TPendingId> temp;
        temp.Swap(QueriesWaitingForId);
        temp.ForEach([&](TQuery *query) { query->OnIdAllocated(); });
    }

} // NKikimr::NBlobDepot
