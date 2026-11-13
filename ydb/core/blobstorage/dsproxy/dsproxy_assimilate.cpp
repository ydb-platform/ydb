#include "dsproxy.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>

namespace NKikimr {

class TBlobStorageGroupAssimilateRequest : public TBlobStorageGroupRequestActor {
    // cursors of the request as the caller has sent it; restarted request starts from them, as items merged since then
    // have not been returned to the caller
    const std::optional<ui64> OriginalSkipBlocksUpTo;
    const std::optional<std::tuple<ui64, ui8>> OriginalSkipBarriersUpTo;
    const std::optional<TLogoBlobID> OriginalSkipBlobsUpTo;
    std::optional<ui64> SkipBlocksUpTo;
    std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
    std::optional<TLogoBlobID> SkipBlobsUpTo;
    const bool IgnoreDecommitState;
    const bool Reverse;
    const std::optional<ui32> ExpectedGeneration;
    const bool IncludePartPlacement;

    struct TBlock : TEvBlobStorage::TEvAssimilateResult::TBlock {
        TBlock(const TBlock&) = default;

        TBlock(ui64 key, const NKikimrBlobStorage::TEvVAssimilateResult::TBlock& msg, TBlobStorageGroupType /*gtype*/) {
            Y_DEBUG_ABORT_UNLESS(msg.HasBlockedGeneration());
            TabletId = key;
            BlockedGeneration = msg.GetBlockedGeneration();
        }

        bool Merge(const TBlock& other) {
            if (TabletId == other.TabletId) {
                BlockedGeneration = Max(BlockedGeneration, other.BlockedGeneration);
                return true;
            } else {
                return false;
            }
        }

        bool GoesBeforeThan(const TBlock& other, bool reverse) const {
            return reverse
                ? other.TabletId < TabletId
                : TabletId < other.TabletId;
        }
    };

    struct TBarrier : TEvBlobStorage::TEvAssimilateResult::TBarrier {
        TBarrier(const TBarrier&) = default;

        TBarrier(std::tuple<ui64, ui8> key, const NKikimrBlobStorage::TEvVAssimilateResult::TBarrier& msg, TBlobStorageGroupType /*gtype*/) {
            std::tie(TabletId, Channel) = key;

            auto parseValue = [](auto& value, const auto& pb) {
                Y_DEBUG_ABORT_UNLESS(pb.HasRecordGeneration() && pb.HasPerGenerationCounter() && pb.HasCollectGeneration() &&
                    pb.HasCollectStep());
                value = {
                    .RecordGeneration = pb.GetRecordGeneration(),
                    .PerGenerationCounter = pb.GetPerGenerationCounter(),
                    .CollectGeneration = pb.GetCollectGeneration(),
                    .CollectStep = pb.GetCollectStep(),
                };
            };
            if (msg.HasSoft()) {
                parseValue(Soft, msg.GetSoft());
            }
            if (msg.HasHard()) {
                parseValue(Hard, msg.GetHard());
            }
        }

        bool Merge(const TBarrier& other) {
            if (TabletId == other.TabletId && Channel == other.Channel) {
                Merge(Soft, other.Soft);
                Merge(Hard, other.Hard);
                return true;
            } else {
                return false;
            }
        }

        static void Merge(TValue& to, const TValue& from) {
            if (std::tie(to.RecordGeneration, to.PerGenerationCounter) < std::tie(from.RecordGeneration, from.PerGenerationCounter)) {
                to = from;
            }
        }

        bool GoesBeforeThan(const TBarrier& other, bool reverse) const {
            return reverse
                ? std::tie(other.TabletId, other.Channel) < std::tie(TabletId, Channel)
                : std::tie(TabletId, Channel) < std::tie(other.TabletId, other.Channel);
        }
    };

    struct TBlob : TEvBlobStorage::TEvAssimilateResult::TBlob {
        TBlob(const TBlob&) = default;

        TBlob(TLogoBlobID key, const NKikimrBlobStorage::TEvVAssimilateResult::TBlob& msg, TBlobStorageGroupType gtype) {
            Y_DEBUG_ABORT_UNLESS(msg.HasIngress());
            Id = key;
            const TIngress ingress(msg.GetIngress());
            const int collectMode = ingress.GetCollectMode(TIngress::IngressMode(gtype));
            Keep = collectMode & CollectModeKeep;
            DoNotKeep = collectMode & CollectModeDoNotKeep;
            HasData = !ingress.LocalParts(gtype).Empty();
        }

        bool Merge(const TBlob& other) {
            if (Id == other.Id) {
                Keep |= other.Keep;
                DoNotKeep |= other.DoNotKeep;
                HasData |= other.HasData;
                for (ui32 part = 0; part < PartDisks.size(); ++part) {
                    PartDisks[part] |= other.PartDisks[part];
                }
                return true;
            } else {
                return false;
            }
        }

        bool GoesBeforeThan(const TBlob& other, bool reverse) const {
            return reverse
                ? other.Id < Id
                : Id < other.Id;
        }
    };

    using TItemVariant = std::variant<TBlock, TBarrier, TBlob>;

    struct TPerVDiskInfo {
        std::optional<TString> ErrorReason;

        bool Excluded = false;
        std::optional<ui64> LastProcessedBlock;
        std::optional<std::tuple<ui64, ui8>> LastProcessedBarrier;
        std::optional<TLogoBlobID> LastProcessedBlob;

        std::deque<TBlock> Blocks;
        std::deque<TBarrier> Barriers;
        std::deque<TBlob> Blobs;

        struct TFinished {
            friend bool operator <(const TFinished&, const TFinished&) { return false; }
        };

        std::variant<TBlock*, TBarrier*, TBlob*, TFinished> Next;

        void PushDataFromMessage(const NKikimrBlobStorage::TEvVAssimilateResult& msg,
                const TBlobStorageGroupAssimilateRequest& self, TBlobStorageGroupType gtype,
                bool reverse, ui32 orderNumber) {
            Y_ABORT_UNLESS(Blocks.empty() && Barriers.empty() && Blobs.empty());

            std::array<ui64, 3> context = {0, 0, 0};

            auto getKey = [&](const auto& item) {
                using T = std::decay_t<decltype(item)>;
                if constexpr (std::is_same_v<T, NKikimrBlobStorage::TEvVAssimilateResult::TBlock>) {
                    Y_DEBUG_ABORT_UNLESS(item.HasTabletId());
                    return ui64(item.GetTabletId());
                } else if constexpr (std::is_same_v<T, NKikimrBlobStorage::TEvVAssimilateResult::TBarrier>) {
                    Y_DEBUG_ABORT_UNLESS(item.HasTabletId() && item.HasChannel());
                    return std::tuple<ui64, ui8>(item.GetTabletId(), item.GetChannel());
                } else if constexpr (std::is_same_v<T, NKikimrBlobStorage::TEvVAssimilateResult::TBlob>) {
#define UNWRAP(X, INDEX) \
                    if (item.HasRaw##X()) { \
                        context[INDEX] = item.GetRaw##X(); \
                    } else if (item.HasDiff##X()) { \
                        if (reverse) { \
                            context[INDEX] -= item.GetDiff##X(); \
                        } else { \
                            context[INDEX] += item.GetDiff##X(); \
                        } \
                    }
                    UNWRAP(X1, 0)
                    UNWRAP(X2, 1)
                    UNWRAP(X3, 2)
#undef UNWRAP
                    return TLogoBlobID(context.data());
                } else {
                    static_assert(TDependentFalse<T>, "unsupported protobuf");
                }
            };

            auto processItems = [&]<typename TRecord>(const NProtoBuf::RepeatedPtrField<TRecord>& items,
                    auto& lastProcessed, auto& field, const auto& skipUpTo) {
                for (const auto& item : items) {
                    const auto key = getKey(item);
                    const auto expected = [&](const auto& opt) { return !opt || (reverse ? key < *opt : *opt < key); };
                    Y_ABORT_UNLESS(expected(lastProcessed));
                    lastProcessed.emplace(key);
                    if (expected(skipUpTo)) {
                        field.emplace_back(key, item, gtype);
                        if constexpr (std::is_same_v<TRecord, NKikimrBlobStorage::TEvVAssimilateResult::TBlob>) {
                            if (self.IncludePartPlacement) {
                                const TBlobStorageGroupInfo::TTopology& topology = self.Info->GetTopology();
                                TVDiskIdShort disk = topology.GetVDiskId(orderNumber);
                                ui32 index = topology.GetIdxInSubgroup(disk, key.Hash());
                                TBlob& blob = field.back();
                                blob.PartDisks.resize(gtype.TotalPartCount(), 0);
                                if (index < gtype.BlobSubgroupSize()) {
                                    for (ui8 part : TIngress(item.GetIngress()).LocalParts(gtype)) {
                                        blob.PartDisks[part] |= 1u << index;
                                    }
                                }
                            }
                        }
                    }
                }
            };

            processItems(msg.GetBlocks(), LastProcessedBlock, Blocks, self.SkipBlocksUpTo);
            processItems(msg.GetBarriers(), LastProcessedBarrier, Barriers, self.SkipBarriersUpTo);
            processItems(msg.GetBlobs(), LastProcessedBlob, Blobs, self.SkipBlobsUpTo);

            AdjustNext();
        }

        void Consume() {
            if (!Blocks.empty()) {
                Blocks.pop_front();
            } else if (!Barriers.empty()) {
                Barriers.pop_front();
            } else if (!Blobs.empty()) {
                Blobs.pop_front();
            } else {
                Y_UNREACHABLE();
            }
        }

        void AdjustNext() {
            if (!Blocks.empty()) {
                Next.emplace<0>(&Blocks.front());
            } else if (!Barriers.empty()) {
                Next.emplace<1>(&Barriers.front());
            } else if (!Blobs.empty()) {
                Next.emplace<2>(&Blobs.front());
            } else {
                Next.emplace<3>();
            }
        }

        TItemVariant BeginMerge() const {
            return std::visit([](auto value) -> TItemVariant {
                if constexpr (std::is_same_v<decltype(value), TFinished>) {
                    Y_ABORT();
                } else {
                    return TItemVariant(std::in_place_type<std::decay_t<decltype(*value)>>, *value);
                }
            }, Next);
        }

        bool Merge(TItemVariant *to) const {
            return std::visit([to](auto value) -> bool {
                if constexpr (std::is_same_v<decltype(value), TFinished>) {
                    Y_ABORT();
                } else if (auto *toItem = std::get_if<std::decay_t<decltype(*value)>>(to)) {
                    return toItem->Merge(*value);
                } else {
                    return false;
                }
            }, Next);
        }

        bool GoesBeforeThan(const TPerVDiskInfo& other, bool reverse) const {
            return Next.index() != other.Next.index()
                    ? Next.index() < other.Next.index()
                    : std::visit([&other, reverse](auto&& left) {
                using T = std::decay_t<decltype(left)>;
                const auto& right = std::get<T>(other.Next);
                if constexpr (std::is_same_v<T, TFinished>) {
                    return false; // always equal
                } else {
                    return left->GoesBeforeThan(*right, reverse);
                }
            }, Next);
        }

        bool Finished() const {
            return Next.index() == 3;
        }

        bool HasItemsToMerge() const {
            return !Blocks.empty() || !Barriers.empty() || !Blobs.empty();
        }

        struct TCompare {
            const bool Reverse;

            bool operator ()(const TPerVDiskInfo *x, const TPerVDiskInfo *y) const {
                return y->GoesBeforeThan(*x, Reverse);
            }
        };
    };

    std::vector<TPerVDiskInfo> PerVDiskInfo;
    std::vector<TPerVDiskInfo*> Heap;

    std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> Result;
    ui32 RequestsInFlight = 0;

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveAssimilate;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Assimilate;
    }

    TBlobStorageGroupAssimilateRequest(TBlobStorageGroupAssimilateParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , OriginalSkipBlocksUpTo(params.Common.Event->SkipBlocksUpTo)
        , OriginalSkipBarriersUpTo(params.Common.Event->SkipBarriersUpTo)
        , OriginalSkipBlobsUpTo(params.Common.Event->SkipBlobsUpTo)
        , SkipBlocksUpTo(params.Common.Event->SkipBlocksUpTo)
        , SkipBarriersUpTo(params.Common.Event->SkipBarriersUpTo)
        , SkipBlobsUpTo(params.Common.Event->SkipBlobsUpTo)
        , IgnoreDecommitState(params.Common.Event->IgnoreDecommitState)
        , Reverse(params.Common.Event->Reverse)
        , ExpectedGeneration(params.Common.Event->ExpectedGeneration)
        , IncludePartPlacement(params.Common.Event->IncludePartPlacement)
        , PerVDiskInfo(Info->GetTotalVDisksNum())
        , Result(new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK, {}))
    {
        Heap.reserve(PerVDiskInfo.size());
    }

    void Bootstrap() override {
        DSP_LOG_INFO_S("BPA01", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " RestartCounter# " << RestartCounter);

        Become(&TBlobStorageGroupAssimilateRequest::StateWork, TDuration::Seconds(10), new TEvents::TEvWakeup);

        if (ExpectedGeneration && *ExpectedGeneration != Info->GroupGeneration) {
            return ReplyAndDie(NKikimrProto::RACE);
        }
        for (ui32 i = 0; i < PerVDiskInfo.size(); ++i) {
            ui32 realm = Info->GetVDiskId(i).FailRealm;
            TPerVDiskInfo& disk = PerVDiskInfo[i];
            disk.Excluded = realm < Info->GetMirror3dcState().FailRealmStates.size() && Info->IsFailRealmExcluded(realm);
            if (!disk.Excluded) {
                Request(i);
            }
        }
        if (!RequestsInFlight) {
            ErrorReason = "No VDisks available for assimilation";
            ReplyAndDie(NKikimrProto::ERROR);
        }
    }

    void HandleWakeup() {
        DSP_LOG_NOTICE_S("BPA25", "assimilation is way too long");
    }

    STATEFN(StateWork) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVAssimilateResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                Y_DEBUG_ABORT_UNLESS(false);
        }
    }

    void Request(ui32 orderNumber) {
        TPerVDiskInfo& info = PerVDiskInfo[orderNumber];
        Y_ABORT_UNLESS(!info.Finished());

        auto pickCursor = [this](const auto& merged, const auto& disk) {
            if (!merged) {
                return disk;
            }
            if (!disk) {
                return merged;
            }
            bool diskIsAhead = Reverse ? *disk < *merged : *merged < *disk;
            return diskIsAhead ? disk : merged;
        };

        SendToQueue(std::make_unique<TEvBlobStorage::TEvVAssimilate>(Info->GetVDiskId(orderNumber),
            pickCursor(SkipBlocksUpTo, info.LastProcessedBlock),
            pickCursor(SkipBarriersUpTo, info.LastProcessedBarrier),
            pickCursor(SkipBlobsUpTo, info.LastProcessedBlob),
            IgnoreDecommitState, Reverse), 0);

        DSP_LOG_DEBUG_S("BPA03", "Request orderNumber# " << orderNumber << " VDiskId# " << Info->GetVDiskId(orderNumber));

        ++RequestsInFlight;
    }

    void Handle(TEvBlobStorage::TEvVAssimilateResult::TPtr ev) {
        ProcessReplyFromQueue(ev->Get());

        const auto& record = ev->Get()->Record;
        Result->ReplyBytes += record.ByteSizeLong();
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const ui32 orderNumber = Info->GetTopology().GetOrderNumber(vdiskId);
        Y_ABORT_UNLESS(orderNumber < PerVDiskInfo.size());

        DSP_LOG_DEBUG_S("BPA02", "Handle TEvVAssimilateResult"
                << " Status# " << NKikimrProto::EReplyStatus_Name(record.GetStatus())
                << " ErrorReason# '" << record.GetErrorReason() << "'"
                << " VDiskId# " << vdiskId
                << " Blocks# " << record.BlocksSize()
                << " Barriers# " << record.BarriersSize()
                << " Blobs# " << record.BlobsSize()
                << " RequestsInFlight# " << RequestsInFlight);

        Y_ABORT_UNLESS(RequestsInFlight);
        --RequestsInFlight;

        auto& info = PerVDiskInfo[orderNumber];
        Y_ABORT_UNLESS(!info.HasItemsToMerge());
        if (record.GetStatus() == NKikimrProto::OK) {
            info.PushDataFromMessage(record, *this, Info->Type, Reverse, orderNumber);
            if (info.HasItemsToMerge()) {
                Heap.push_back(&info);
                std::push_heap(Heap.begin(), Heap.end(), TPerVDiskInfo::TCompare{Reverse});
            } else if (!info.Finished()) {
                Request(orderNumber);
            }
        } else {
            info.ErrorReason = TStringBuilder() << vdiskId << ": " << NKikimrProto::EReplyStatus_Name(record.GetStatus());
            if (record.GetErrorReason()) {
                *info.ErrorReason += " (" + record.GetErrorReason() + ')';
            }
        }

        if (!RequestsInFlight) {
            Merge();
        }
    }

    void Merge() {
        TStackVec<ui8, 32> requests;

        const TBlobStorageGroupInfo::TTopology *top = &Info->GetTopology();
        TBlobStorageGroupInfo::TGroupVDisks disksWithData(top);
        bool allActiveDisksResponded = true;
        for (ui32 i = 0; i < PerVDiskInfo.size(); ++i) {
            TPerVDiskInfo& info = PerVDiskInfo[i];
            if (info.Excluded) {
                continue;
            }
            if (info.HasItemsToMerge() || info.Finished()) {
                disksWithData += {top, top->GetVDiskId(i)};
            } else {
                allActiveDisksResponded = false;
            }
        }
        bool isMirror3dc = Info->Type.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc;
        const TMirror3dcGroupState& state = Info->GetMirror3dcState();
        std::optional<TMirror3dcGroupLayouts> layouts = GetMirror3dcGroupLayouts(state.State);
        bool requireAllActiveDisks = isMirror3dc && layouts && layouts->Ensured == EMirror3dcLayout::LAYOUT_0
            && top->GetTotalFailRealmsNum() == state.FailRealmStates.size()
            && IsValidMirror3dcGroupConfiguration({state.State, state.FailRealmStates});

        bool hasQuorum;
        if (requireAllActiveDisks) {
            hasQuorum = allActiveDisksResponded;
        } else if (isMirror3dc) {
            hasQuorum = TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(*Info, ~disksWithData);
        } else {
            hasQuorum = Info->GetQuorumChecker().CheckQuorumForGroup(disksWithData);
        }
        if (!hasQuorum) {
            bool emptyResult = Result->Blocks.empty() && Result->Barriers.empty() && Result->Blobs.empty();
            if (requireAllActiveDisks || emptyResult) {
                // For E=0 every active stream is required. An empty successful page would signal end of scan.
                ReplyAndDie(NKikimrProto::ERROR);
            } else {
                // answer with what we have already collected
                DSP_LOG_DEBUG_S("BPA06", "SendResponseAndDie (no items to merge)");
                SendResponseAndDie(std::move(Result));
            }
            return;
        }
        while (requests.empty()) {
            if (Heap.empty()) {
                DSP_LOG_DEBUG_S("BPA07", "SendResponseAndDie (heap empty)");
                SendResponseAndDie(std::move(Result));
                return;
            }

            TPerVDiskInfo *heapItem = Heap.front();
            auto item = heapItem->BeginMerge();

            for (;;) {
                std::pop_heap(Heap.begin(), Heap.end(), TPerVDiskInfo::TCompare{Reverse});
                heapItem->Consume();
                if (heapItem->HasItemsToMerge()) {
                    heapItem->AdjustNext();
                    std::push_heap(Heap.begin(), Heap.end(), TPerVDiskInfo::TCompare{Reverse});
                } else {
                    Heap.pop_back();
                    const ui32 orderNumber = heapItem - PerVDiskInfo.data();
                    requests.push_back(orderNumber);
                }

                heapItem = Heap.empty() ? nullptr : Heap.front();
                if (!heapItem || !heapItem->Merge(&item)) {
                    break;
                }
            }

            std::visit(TOverloaded{
                [&](TBlock& block) {
                    SkipBlocksUpTo.emplace(block.TabletId);
                    Result->Blocks.push_back(block);
                },
                [&](TBarrier& barrier) {
                    SkipBarriersUpTo.emplace(barrier.TabletId, barrier.Channel);
                    Result->Barriers.push_back(barrier);
                },
                [&](TBlob& blob) {
                    SkipBlobsUpTo.emplace(blob.Id);
                    Result->Blobs.push_back(blob);
                }
            }, item);
        }

        if (Result->Blocks.size() + Result->Barriers.size() + Result->Blobs.size() >= 10'000) {
            DSP_LOG_DEBUG_S("BPA05", "SendResponseAndDie (10k)");
            SendResponseAndDie(std::move(Result));
        } else {
            for (const ui32 orderNumber : requests) {
                Request(orderNumber);
            }
        }
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartAssimilate;
        auto ev = std::make_unique<TEvBlobStorage::TEvAssimilate>(OriginalSkipBlocksUpTo, OriginalSkipBarriersUpTo,
            OriginalSkipBlobsUpTo, IgnoreDecommitState, Reverse);
        ev->ExpectedGeneration = ExpectedGeneration;
        ev->IncludePartPlacement = IncludePartPlacement;
        ev->RestartCounter = counter;
        return ev;
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        DSP_LOG_DEBUG_S("BPA04", "ReplyAndDie status# " << NKikimrProto::EReplyStatus_Name(status));
        for (const auto& item : PerVDiskInfo) {
            if (item.ErrorReason) {
                if (ErrorReason) {
                    ErrorReason += ", ";
                }
                ErrorReason += *item.ErrorReason;
            }
        }
        SendResponseAndDie(std::make_unique<TEvBlobStorage::TEvAssimilateResult>(status, ErrorReason));
    }
};

IActor* CreateBlobStorageGroupAssimilateRequest(TBlobStorageGroupAssimilateParameters params) {
    return new TBlobStorageGroupAssimilateRequest(params);
}

} // NKikimr
