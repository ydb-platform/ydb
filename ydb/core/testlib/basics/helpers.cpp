#include "helpers.h"
#include <ydb/core/base/appdata.h>

namespace NKikimr {

    struct TPDiskReplyChecker : IReplyChecker {
        bool OnRequest(IEventHandle *request) override {
            if (const ui32 type = request->GetTypeRewrite(); type == TEvBlobStorage::EvMultiLog) {
                LastLsn = request->Get<NPDisk::TEvMultiLog>()->LsnSeg.Last;
                return true;
            } else {
                LastLsn = {};
                // PDisk replies with EvConfigureSchedulerResult, so we must wait for it.
                return true;
            }
        }

        bool IsWaitingForMoreResponses(IEventHandle *response) override {
            if (!LastLsn) {
                return false;
            }
            if (response->Type == TEvBlobStorage::EvConfigureSchedulerResult) {
                // Scheduler reconfiguration responses may interleave with MultiLog replies.
                // Keep waiting for EvLogResult that carries requested LSNs.
                return true;
            }
            Y_VERIFY_S(response->Type == TEvBlobStorage::EvLogResult, "expected EvLogResult "
                    << (ui64)TEvBlobStorage::EvLogResult << ", but given " << response->Type);
            NPDisk::TEvLogResult *evResult = response->Get<NPDisk::TEvLogResult>();
            ui64 responseLastLsn = evResult->Results.back().Lsn;
            return *LastLsn > responseLastLsn;
        }

        TMaybe<ui64> LastLsn;
    };

    void TStrandedPDiskSubsystem::Start(const TActorContext &ctx, ui32 pDiskID,
            const TIntrusivePtr<TPDiskConfig> &cfg, const NPDisk::TMainKey &mainKey, ui32 poolId, ui32 nodeId)
    {
        Y_UNUSED(ctx);
        Y_ABORT_UNLESS(!Runtime.IsRealThreads());
        Y_ABORT_UNLESS(nodeId >= Runtime.GetNodeId(0) && nodeId < Runtime.GetNodeId(0) + Runtime.GetNodeCount());
        ui32 nodeIndex = nodeId - Runtime.GetNodeId(0);
        Runtime.BlockOutputForActor(TActorId(nodeId, "actorsystem"));

        TActorId actorId = Runtime.Register(CreatePDisk(cfg, mainKey, Runtime.GetAppData(0).Counters), nodeIndex, poolId, TMailboxType::Revolving);
        TActorId pDiskServiceId = MakeBlobStoragePDiskID(nodeId, pDiskID);

        Runtime.BlockOutputForActor(pDiskServiceId);
        Runtime.BlockOutputForActor(actorId);
        auto factory = CreateStrandingDecoratorFactory(&Runtime, []{ return MakeHolder<TPDiskReplyChecker>(); });
        IActor* wrappedActor = factory->Wrap(actorId, true, TVector<TActorId>());
        TActorId wrappedActorId = Runtime.Register(wrappedActor, nodeIndex, poolId, TMailboxType::Revolving);
        Runtime.RegisterService(pDiskServiceId, wrappedActorId, nodeIndex);
    }

    void SetupPDiskSubsystem(TTestActorRuntime* runtime, bool stranded) {
        auto previous = std::move(runtime->SetupNodeSubSystems);
        runtime->SetupNodeSubSystems = [runtime, stranded, previous = std::move(previous)](
                ui32 nodeIndex, TActorSystemSetup* setup) {
            if (previous) {
                previous(nodeIndex, setup);
            }
            if (stranded && !runtime->IsRealThreads()) {
                setup->RegisterSubSystem<IPDiskSubsystem>(std::make_unique<TStrandedPDiskSubsystem>(runtime));
            } else {
                setup->RegisterSubSystem<IPDiskSubsystem>(CreatePDiskSubsystem());
            }
        };
    }

}
