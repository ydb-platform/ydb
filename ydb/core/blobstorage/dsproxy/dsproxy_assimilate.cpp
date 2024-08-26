#include "dsproxy.h"

namespace NKikimr {

class TBlobStorageGroupAssimilateRequest : public TBlobStorageGroupRequestActor {
    std::optional<ui64> SkipBlocksUpTo;
    std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
    std::optional<TLogoBlobID> SkipBlobsUpTo;

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

        bool GoesBeforeThan(const TBlock& other) const {
            return TabletId < other.TabletId;
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

        bool GoesBeforeThan(const TBarrier& other) const {
            return std::tie(TabletId, Channel) < std::tie(other.TabletId, other.Channel);
        }
    };

    struct TBlob : TEvBlobStorage::TEvAssimilateResult::TBlob {
        TBlob(const TBlob&) = default;

        TBlob(TLogoBlobID key, const NKikimrBlobStorage::TEvVAssimilateResult::TBlob& msg, TBlobStorageGroupType gtype) {
            Y_DEBUG_ABORT_UNLESS(msg.HasIngress());
            Id = key;
            const int collectMode = TIngress(msg.GetIngress()).GetCollectMode(TIngress::IngressMode(gtype));
            Keep = collectMode & CollectModeKeep;
            DoNotKeep = collectMode & CollectModeDoNotKeep;
        }

        bool Merge(const TBlob& other) {
            if (Id == other.Id) {
                Keep |= other.Keep;
                DoNotKeep |= other.DoNotKeep;
                return true;
            } else {
                return false;
            }
        }

        bool GoesBeforeThan(const TBlob& other) const {
            return Id < other.Id;
        }
    };

    using TItemVariant = std::variant<TBlock, TBarrier, TBlob>;

    struct TPerVDiskInfo {
        std::optional<TString> ErrorReason;

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
                const TBlobStorageGroupAssimilateRequest& self, TBlobStorageGroupType gtype) {
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
                    if (item.HasRawX1()) {
                        context[0] = item.GetRawX1();
                    } else if (item.HasDiffX1()) {
                        context[0] += item.GetDiffX1();
                    }
                    if (item.HasRawX2()) {
                        context[1] = item.GetRawX2();
                    } else if (item.HasDiffX2()) {
                        context[1] += item.GetDiffX2();
                    }
                    if (item.HasRawX3()) {
                        context[2] = item.GetRawX3();
                    } else if (item.HasDiffX3()) {
                        context[2] += item.GetDiffX3();
                    }
                    return TLogoBlobID(context.data());
                } else {
                    static_assert(TDependentFalse<T>, "unsupported protobuf");
                }
            };

            auto processItems = [&](const auto& items, auto& lastProcessed, auto& field, const auto& skipUpTo) {
                for (const auto& item : items) {
                    const auto key = getKey(item);
                    Y_ABORT_UNLESS(lastProcessed < key);
                    lastProcessed.emplace(key);
                    if (skipUpTo < key) {
                        field.emplace_back(key, item, gtype);
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

        bool GoesBeforeThan(const TPerVDiskInfo& other) const {
            return Next.index() != other.Next.index()
                    ? Next.index() < other.Next.index()
                    : std::visit([&other](auto&& left) {
                using T = std::decay_t<decltype(left)>;
                const auto& right = std::get<T>(other.Next);
                if constexpr (std::is_same_v<T, TFinished>) {
                    return false; // always equal
                } else {
                    return left->GoesBeforeThan(*right);
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
            bool operator ()(const TPerVDiskInfo *x, const TPerVDiskInfo *y) const {
                return y->GoesBeforeThan(*x);
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
        , SkipBlocksUpTo(params.Common.Event->SkipBlocksUpTo)
        , SkipBarriersUpTo(params.Common.Event->SkipBarriersUpTo)
        , SkipBlobsUpTo(params.Common.Event->SkipBlobsUpTo)
        , PerVDiskInfo(Info->GetTotalVDisksNum())
        , Result(new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK, {}))
    {
        Heap.reserve(PerVDiskInfo.size());
    }

    void Bootstrap() override {
        R_LOG_INFO_S("BPA01", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " RestartCounter# " << RestartCounter);

        Become(&TBlobStorageGroupAssimilateRequest::StateWork, TDuration::Seconds(10), new TEvents::TEvWakeup);

        for (ui32 i = 0; i < PerVDiskInfo.size(); ++i) {
            Request(i);
        }
    }

    void HandleWakeup() {
        R_LOG_NOTICE_S("BPA25", "assimilation is way too long");
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

        auto maxOpt = [](const auto& x, const auto& y) { return !x ? y : !y ? x : *x < *y ? y : x; };

        SendToQueue(std::make_unique<TEvBlobStorage::TEvVAssimilate>(Info->GetVDiskId(orderNumber),
            maxOpt(SkipBlocksUpTo, info.LastProcessedBlock),
            maxOpt(SkipBarriersUpTo, info.LastProcessedBarrier),
            maxOpt(SkipBlobsUpTo, info.LastProcessedBlob)), 0);

        R_LOG_DEBUG_S("BPA03", "Request orderNumber# " << orderNumber << " VDiskId# " << Info->GetVDiskId(orderNumber));

        ++RequestsInFlight;
    }

    void Handle(TEvBlobStorage::TEvVAssimilateResult::TPtr ev) {
        ProcessReplyFromQueue(ev->Get());

        const auto& record = ev->Get()->Record;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const ui32 orderNumber = Info->GetTopology().GetOrderNumber(vdiskId);
        Y_ABORT_UNLESS(orderNumber < PerVDiskInfo.size());

        R_LOG_DEBUG_S("BPA02", "Handle TEvVAssimilateResult"
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
            info.PushDataFromMessage(record, *this, Info->Type);
            if (info.HasItemsToMerge()) {
                Heap.push_back(&info);
                std::push_heap(Heap.begin(), Heap.end(), TPerVDiskInfo::TCompare());
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
        for (ui32 i = 0; i < PerVDiskInfo.size(); ++i) {
            TPerVDiskInfo& info = PerVDiskInfo[i];
            if (info.HasItemsToMerge() || info.Finished()) {
                disksWithData += {top, top->GetVDiskId(i)};
            }
        }
        if (!Info->GetQuorumChecker().CheckQuorumForGroup(disksWithData)) {
            if (Result->Blocks.empty() && Result->Barriers.empty() && Result->Blobs.empty()) {
                // we didn't get any data, so reply with ERROR to prevent confusing blob depot with 'all finished' situation
                ReplyAndDie(NKikimrProto::ERROR);
            } else {
                // answer with what we have already collected
                R_LOG_DEBUG_S("BPA06", "SendResponseAndDie (no items to merge)");
                SendResponseAndDie(std::move(Result));
            }
            return;
        }
        while (requests.empty()) {
            if (Heap.empty()) {
                R_LOG_DEBUG_S("BPA07", "SendResponseAndDie (heap empty)");
                SendResponseAndDie(std::move(Result));
                return;
            }

            TPerVDiskInfo *heapItem = Heap.front();
            auto item = heapItem->BeginMerge();

            for (;;) {
                std::pop_heap(Heap.begin(), Heap.end(), TPerVDiskInfo::TCompare());
                heapItem->Consume();
                if (heapItem->HasItemsToMerge()) {
                    heapItem->AdjustNext();
                    std::push_heap(Heap.begin(), Heap.end(), TPerVDiskInfo::TCompare());
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
            R_LOG_DEBUG_S("BPA05", "SendResponseAndDie (10k)");
            SendResponseAndDie(std::move(Result));
        } else {
            for (const ui32 orderNumber : requests) {
                Request(orderNumber);
            }
        }
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartAssimilate;
        auto ev = std::make_unique<TEvBlobStorage::TEvAssimilate>(SkipBlocksUpTo, SkipBarriersUpTo, SkipBlobsUpTo);
        ev->RestartCounter = counter;
        return ev;
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        R_LOG_DEBUG_S("BPA04", "ReplyAndDie status# " << NKikimrProto::EReplyStatus_Name(status));
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
