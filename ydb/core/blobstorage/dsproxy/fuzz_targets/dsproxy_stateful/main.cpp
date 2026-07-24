#include <ydb/core/blobstorage/dsproxy/dsproxy_blackboard.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_get_m3of4.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_put_m3of4.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <map>
#include <set>
#include <variant>

namespace {

using namespace NKikimr;

constexpr ui32 MaxOps = 32;
constexpr ui32 MaxStrategySteps = 24;
constexpr ui64 TabletId = 72075186224047637ull;
constexpr ui32 MaxBlobSize = 1024;

enum class EReply {
    Ok,
    Error,
    NoData,
    NotYet,
};

struct TGetOp {
    ui32 OrderNumber;
    TLogoBlobID Id;
    ui32 Shift;
    ui32 Size;

    auto AsTuple() const {
        return std::make_tuple(OrderNumber, Id, Shift, Size);
    }
};

struct TPutOp {
    ui32 OrderNumber;
    TLogoBlobID Id;

    auto AsTuple() const {
        return std::make_tuple(OrderNumber, Id);
    }
};

using TInFlightOp = std::variant<TGetOp, TPutOp>;

TString MakeData(ui32 seed, ui32 size) {
    TString data = TString::Uninitialized(size);
    char* out = data.Detach();
    for (ui32 i = 0; i < size; ++i) {
        out[i] = static_cast<char>(seed + i * 31);
    }
    return data;
}

TLogoBlobID MakeBlobId(ui32 step, ui32 cookie, ui32 size) {
    return TLogoBlobID(TabletId, 1, step + 1, 0, size, cookie & TLogoBlobID::MaxCookie);
}

EReply PickReply(FuzzedDataProvider& provider, bool allowNoData, bool allowNotYet) {
    const ui8 max = allowNotYet ? 3 : allowNoData ? 2 : 1;
    switch (provider.ConsumeIntegralInRange<ui8>(0, max)) {
        case 0:
            return EReply::Ok;
        case 1:
            return EReply::Error;
        case 2:
            return EReply::NoData;
        default:
            return EReply::NotYet;
    }
}

void CheckGroupQueues(const TGroupQueues& queues) {
    Y_ABORT_UNLESS(queues.CostModel);
    for (const auto* disk : queues.DisksByOrderNumber) {
        Y_ABORT_UNLESS(disk);
        Y_ABORT_UNLESS(disk->CostModel);
        disk->Queues.ForEachQueue([](const auto& queue) {
            Y_ABORT_UNLESS(queue.ActorId);
            Y_ABORT_UNLESS(queue.FlowRecord);
            Y_ABORT_UNLESS(queue.CostModel);
            Y_ABORT_UNLESS(queue.IsConnected.load(std::memory_order_acquire));
        });
    }
}

TIntrusivePtr<TGroupQueues> MakeGroupQueues(const TBlobStorageGroupInfo& info) {
    auto queues = MakeIntrusive<TGroupQueues>(info.GetTopology());
    auto costModel = std::make_shared<const TCostModel>(
        1000, 1'000'000'000, 1'000'000'000, 4096, 4096, MaxBlobSize, info.Type);
    queues->CostModel = costModel;
    const NKikimrBlobStorage::EVDiskQueueId queueIds[] = {
        NKikimrBlobStorage::EVDiskQueueId::PutTabletLog,
        NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob,
        NKikimrBlobStorage::EVDiskQueueId::PutUserData,
        NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead,
        NKikimrBlobStorage::EVDiskQueueId::GetFastRead,
        NKikimrBlobStorage::EVDiskQueueId::GetDiscover,
        NKikimrBlobStorage::EVDiskQueueId::GetLowRead,
    };

    ui64 localId = 1;
    for (auto& failDomain : queues->FailDomains) {
        failDomain.CostModel = costModel;
        for (auto& disk : failDomain.VDisks) {
            disk.CostModel = costModel;
            for (const auto queueId : queueIds) {
                auto& queue = disk.Queues.GetQueue(queueId);
                queue.ActorId = TActorId(1, 1, localId++, 0);
                queue.FlowRecord = new NBackpressure::TFlowRecord;
                queue.CostModel = costModel;
                queue.IsConnected.store(true, std::memory_order_release);
            }
        }
    }

    CheckGroupQueues(*queues);
    return queues;
}

struct TModel {
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    TVector<std::map<TLogoBlobID, TRope>> DiskBlobs;
    std::map<TLogoBlobID, TString> FullBlobs;
    TVector<TLogoBlobID> Known;
    TLogContext LogCtx;
    TAccelerationParams Acceleration;

    TModel()
        : Info(new TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureMirror3of4))
        , GroupQueues(MakeGroupQueues(*Info))
        , DiskBlobs(Info->GetTotalVDisksNum())
        , LogCtx(NKikimrServices::BS_PROXY, false)
    {
        LogCtx.SuppressLog = true;
        Acceleration.SlowDiskThreshold = 1'000'000'000;
        Acceleration.MaxNumOfSlowDisks = 0;
    }

    TBlackboard MakeBlackboard() {
        return TBlackboard(Info, GroupQueues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead);
    }

    void CheckKnown() const {
        Y_ABORT_UNLESS(Known.size() == FullBlobs.size());
        for (const TLogoBlobID& id : Known) {
            Y_ABORT_UNLESS(FullBlobs.contains(id));
        }
    }

    void ProcessPending(TBlackboard& blackboard, FuzzedDataProvider& provider) {
        TVector<TInFlightOp> inFlight;
        std::set<std::tuple<ui32, TLogoBlobID, ui32, ui32>> getSet;
        std::set<std::tuple<ui32, TLogoBlobID>> putSet;

        for (const auto& get : blackboard.GroupDiskRequests.GetsPending) {
            TGetOp op{get.OrderNumber, get.Id, get.Shift, get.Size};
            Y_ABORT_UNLESS(getSet.insert(op.AsTuple()).second);
            inFlight.emplace_back(op);
        }
        blackboard.GroupDiskRequests.GetsPending.clear();

        for (const auto& put : blackboard.GroupDiskRequests.PutsPending) {
            TPutOp op{put.OrderNumber, put.Id};
            Y_ABORT_UNLESS(putSet.insert(op.AsTuple()).second);
            inFlight.emplace_back(op);
        }
        blackboard.GroupDiskRequests.PutsPending.clear();

        ui32 replies = 0;
        while (inFlight) {
            const ui32 index = provider.ConsumeIntegralInRange<ui32>(0, inFlight.size() - 1);
            TInFlightOp op = std::move(inFlight[index]);
            inFlight[index] = std::move(inFlight.back());
            inFlight.pop_back();

            std::visit([&](const auto& item) {
                ApplyReply(blackboard, provider, item);
            }, op);
            ++replies;
        }

        Y_ABORT_UNLESS(replies == getSet.size() + putSet.size());
    }

    void ApplyReply(TBlackboard& blackboard, FuzzedDataProvider& provider, const TPutOp& op) {
        Y_ABORT_UNLESS(op.OrderNumber < DiskBlobs.size());
        if (PickReply(provider, false, false) == EReply::Ok) {
            TBlobState& state = blackboard.GetState(op.Id, op.OrderNumber, "fuzz put reply");
            const ui32 partIdx = op.Id.PartId() - 1;
            Y_ABORT_UNLESS(partIdx < state.Parts.size());
            const ui32 partSize = Info->Type.PartSize(op.Id);
            DiskBlobs[op.OrderNumber][op.Id] = partSize
                ? state.Parts[partIdx].Data.Read(0, partSize)
                : TRope(TString());
            blackboard.AddPutOkResponse(op.Id, op.OrderNumber);
        } else {
            blackboard.AddErrorResponse(op.Id, op.OrderNumber, "fuzz put error");
        }
    }

    void ApplyReply(TBlackboard& blackboard, FuzzedDataProvider& provider, const TGetOp& op) {
        Y_ABORT_UNLESS(op.OrderNumber < DiskBlobs.size());
        auto it = DiskBlobs[op.OrderNumber].find(op.Id);
        EReply reply = it == DiskBlobs[op.OrderNumber].end()
            ? PickReply(provider, true, true)
            : PickReply(provider, false, true);
        if (it == DiskBlobs[op.OrderNumber].end() && reply == EReply::Ok) {
            reply = EReply::NoData;
        }

        switch (reply) {
            case EReply::Ok: {
                const TRope& buffer = it->second;
                const ui32 partSize = Info->Type.PartSize(op.Id);
                const ui32 begin = Min<ui32>(op.Shift, buffer.size());
                const ui32 requested = op.Size ? op.Size : buffer.size() - begin;
                const ui32 end = Min<ui32>(buffer.size(), begin + requested);
                if (partSize && begin == end) {
                    blackboard.AddNoDataResponse(op.Id, op.OrderNumber);
                } else {
                    blackboard.AddResponseData(op.Id, op.OrderNumber, begin, TRope(buffer.begin() + begin, buffer.begin() + end));
                }
                break;
            }
            case EReply::Error:
                blackboard.AddErrorResponse(op.Id, op.OrderNumber, "fuzz get error");
                break;
            case EReply::NoData:
                blackboard.AddNoDataResponse(op.Id, op.OrderNumber);
                break;
            case EReply::NotYet:
                blackboard.AddNotYetResponse(op.Id, op.OrderNumber);
                break;
        }
    }

    template <typename TStrategy>
    EStrategyOutcome Drain(TBlackboard& blackboard, TStrategy& strategy, FuzzedDataProvider& provider) {
        for (ui32 step = 0; step < MaxStrategySteps; ++step) {
            const EStrategyOutcome outcome = blackboard.RunStrategy(LogCtx, strategy, Acceleration);
            if (outcome != EStrategyOutcome::IN_PROGRESS) {
                Y_ABORT_UNLESS(blackboard.GroupDiskRequests.GetsPending.empty());
                Y_ABORT_UNLESS(blackboard.GroupDiskRequests.PutsPending.empty());
                return outcome;
            }
            const size_t pending = blackboard.GroupDiskRequests.GetsPending.size()
                + blackboard.GroupDiskRequests.PutsPending.size();
            Y_ABORT_UNLESS(pending);
            ProcessPending(blackboard, provider);
        }
        return EStrategyOutcome::IN_PROGRESS;
    }

    EStrategyOutcome PutBlob(const TLogoBlobID& id, const TString& data, FuzzedDataProvider& provider) {
        TBlackboard blackboard = MakeBlackboard();
        blackboard.RegisterBlobForPut(id, FullBlobs.size());
        blackboard[id].Whole.Data.Write(0, TRope(data));

        std::vector<TRope> parts(Info->Type.TotalPartCount());
        ErasureSplit(TBlobStorageGroupType::CrcModeNone, Info->Type, TRope(data), parts,
            nullptr, GetDefaultRcBufAllocator());
        for (ui32 i = 0; i < parts.size(); ++i) {
            if (Info->Type.PartSize(TLogoBlobID(id, i + 1))) {
                blackboard.AddPartToPut(id, i, TRope(parts[i]));
            }
        }

        TPut3of4Strategy strategy(provider.ConsumeBool()
            ? TEvBlobStorage::TEvPut::TacticMinLatency
            : TEvBlobStorage::TEvPut::TacticDefault);
        const EStrategyOutcome outcome = Drain(blackboard, strategy, provider);
        if (outcome == EStrategyOutcome::DONE) {
            if (!FullBlobs.contains(id)) {
                Known.push_back(id);
            }
            FullBlobs[id] = data;
        }
        CheckKnown();
        return outcome;
    }

    EStrategyOutcome GetBlob(const TLogoBlobID& id, ui32 shift, ui32 size, FuzzedDataProvider& provider) {
        TBlackboard blackboard = MakeBlackboard();
        blackboard.AddNeeded(id, shift, size);
        TMirror3of4GetStrategy strategy;
        return Drain(blackboard, strategy, provider);
    }

    EStrategyOutcome Discover(const TLogoBlobID& id, FuzzedDataProvider& provider) {
        return GetBlob(id, 0, id.BlobSize(), provider);
    }

    EStrategyOutcome PatchBlob(const TLogoBlobID& original, ui32 cookie, FuzzedDataProvider& provider) {
        auto it = FullBlobs.find(original);
        if (it == FullBlobs.end()) {
            return Discover(original, provider);
        }

        TString patched = it->second;
        if (patched) {
            const ui32 offset = provider.ConsumeIntegralInRange<ui32>(0, patched.size() - 1);
            patched.Detach()[offset] ^= static_cast<char>(1 + (cookie & 0x7f));
        }
        const TLogoBlobID patchedId = MakeBlobId(original.Generation() + 4096, cookie, patched.size());
        return PutBlob(patchedId, patched, provider);
    }

    void Collect(ui32 keepStep, bool hard) {
        TVector<TLogoBlobID> nextKnown;
        for (const TLogoBlobID& id : Known) {
            const bool remove = hard ? id.Step() <= keepStep : id.Step() < keepStep;
            if (remove) {
                FullBlobs.erase(id);
                for (auto& disk : DiskBlobs) {
                    for (ui32 partIdx = 0; partIdx < Info->Type.TotalPartCount(); ++partIdx) {
                        disk.erase(TLogoBlobID(id, partIdx + 1));
                    }
                }
            } else {
                nextKnown.push_back(id);
            }
        }
        Known.swap(nextKnown);
        CheckKnown();
    }
};

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TModel model;

    for (ui32 i = 0; i < 3; ++i) {
        const ui32 blobSize = provider.ConsumeIntegralInRange<ui32>(1, MaxBlobSize);
        const TLogoBlobID id = MakeBlobId(i, i, blobSize);
        model.PutBlob(id, MakeData(i, blobSize), provider);
    }

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(1, MaxOps);
    for (ui32 i = 0; i < ops && provider.remaining_bytes(); ++i) {
        const ui8 op = provider.ConsumeIntegralInRange<ui8>(0, 4);
        const ui32 selector = provider.ConsumeIntegral<ui32>();
        const bool hasKnown = !model.Known.empty();
        const TLogoBlobID selected = hasKnown
            ? model.Known[selector % model.Known.size()]
            : MakeBlobId(selector % 64, selector, provider.ConsumeIntegralInRange<ui32>(1, MaxBlobSize));

        switch (op) {
            case 0: {
                const ui32 blobSize = provider.ConsumeIntegralInRange<ui32>(1, MaxBlobSize);
                const TLogoBlobID id = MakeBlobId(model.Known.size() + i + 16, selector, blobSize);
                model.PutBlob(id, MakeData(selector, blobSize), provider);
                break;
            }
            case 1: {
                const ui32 shift = provider.ConsumeIntegralInRange<ui32>(0, selected.BlobSize() - 1);
                const ui32 readSize = provider.ConsumeIntegralInRange<ui32>(1, selected.BlobSize() - shift);
                model.GetBlob(selected, shift, readSize, provider);
                break;
            }
            case 2:
                model.Collect(provider.ConsumeIntegralInRange<ui32>(0, selected.Step()), provider.ConsumeBool());
                break;
            case 3:
                model.Discover(selected, provider);
                break;
            case 4:
                model.PatchBlob(selected, selector, provider);
                break;
        }

        CheckGroupQueues(*model.GroupQueues);
    }

    return 0;
}
