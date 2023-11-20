#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::TChannelKind::IssueGivenIdRange(const NKikimrBlobDepot::TGivenIdRange& proto) {
        for (const auto& range : proto.GetChannelRanges()) {
            GivenIdRangePerChannel[range.GetChannel()].IssueNewRange(range.GetBegin(), range.GetEnd());
            NumAvailableItems += range.GetEnd() - range.GetBegin();
        }

        ProcessQueriesWaitingForId(true);
    }

    ui32 TBlobDepotAgent::TChannelKind::GetNumAvailableItems() const {
#ifndef NDEBUG
        ui32 count = 0;
        for (const auto& [_, givenIdRanges] : GivenIdRangePerChannel) {
            count += givenIdRanges.GetNumAvailableItems();
        }
        Y_ABORT_UNLESS(count == NumAvailableItems);
#endif
        return NumAvailableItems;
    }

    std::optional<TBlobSeqId> TBlobDepotAgent::TChannelKind::Allocate(TBlobDepotAgent& agent) {
        ui64 accum = 0;
        std::vector<std::tuple<ui64, ui8>> options;
        for (const auto& [channel, range] : GivenIdRangePerChannel) {
            if (!range.IsEmpty()) {
                accum += range.GetNumAvailableItems();
                options.emplace_back(accum, channel);
            }
        }
        if (options.empty()) {
            agent.IssueAllocateIdsIfNeeded(*this);
            return std::nullopt;
        }

        const ui64 v = RandomNumber(accum);
        const auto it = std::upper_bound(options.begin(), options.end(), std::make_tuple(v, 0));
        const ui8 channel = std::get<1>(*it);
        TGivenIdRange& range = GivenIdRangePerChannel[channel];
        const ui64 value = range.Allocate();

        --NumAvailableItems;
        agent.IssueAllocateIdsIfNeeded(*this);
        return TBlobSeqId::FromSequentalNumber(channel, agent.BlobDepotGeneration, value);
    }

    std::tuple<TLogoBlobID, ui32> TBlobDepotAgent::TChannelKind::MakeBlobId(TBlobDepotAgent& agent,
            const TBlobSeqId& blobSeqId, EBlobType type, ui32 part, ui32 size) const {
        auto id = blobSeqId.MakeBlobId(agent.TabletId, type, part, size);
        const auto [channel, groupId] = ChannelGroups[ChannelToIndex[blobSeqId.Channel]];
        Y_DEBUG_ABORT_UNLESS(channel == blobSeqId.Channel);
        return {id, groupId};
    }

    void TBlobDepotAgent::TChannelKind::Trim(ui8 channel, ui32 generation, ui32 invalidatedStep) {
        const TBlobSeqId trimmedBlobSeqId{channel, generation, invalidatedStep, TBlobSeqId::MaxIndex};
        const ui64 validSince = trimmedBlobSeqId.ToSequentialNumber() + 1;
        auto& givenIdRanges = GivenIdRangePerChannel[channel];
        NumAvailableItems -= givenIdRanges.GetNumAvailableItems();
        givenIdRanges.Trim(validSince);
        NumAvailableItems += givenIdRanges.GetNumAvailableItems();
    }

    void TBlobDepotAgent::TChannelKind::EnqueueQueryWaitingForId(TQuery *query) {
        QueriesWaitingForId.PushBack(query);
    }

    void TBlobDepotAgent::TChannelKind::ProcessQueriesWaitingForId(bool success) {
        TIntrusiveList<TQuery, TPendingId> temp;
        temp.Swap(QueriesWaitingForId);
        temp.ForEach([&](TQuery *query) { query->OnIdAllocated(success); });
    }

} // NKikimr::NBlobDepot
