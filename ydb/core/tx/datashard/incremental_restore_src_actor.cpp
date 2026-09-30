#include "incremental_restore_src_actor.h"

// scheme_tabledefs.h must precede change_exchange_impl.h (provides TPathId/TTableId).
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include "change_exchange_impl.h"
#include "datashard_impl.h"
#include "incr_restore_scan.h"

#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NDataShard {

using namespace NActors;

namespace {

class TIncrementalRestoreSrcActor: public TActorBootstrapped<TIncrementalRestoreSrcActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::INCREMENTAL_RESTORE_SCAN_ACTOR;
    }

    TIncrementalRestoreSrcActor(
            TDataShard* self,
            const NKikimrTxDataShard::TEvIncrementalRestoreSrcCreateRequest& request)
        : Self(self)
        , OperationId(request.GetOperationId())
        , SubOpTxId(request.GetSubOpTxId())
        , ShardIdx(request.GetShardIdx())
        , SchemeShardGeneration(request.GetSchemeShardGeneration())
        , SchemeShardId(request.GetSchemeShardId())
        , SrcPathId(TPathId::FromProto(request.GetSrcPathId()))
        , DstPathId(TPathId::FromProto(request.GetDstPathId()))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);

        if (!SchemeShardId) {
            return ReplyAndDie(ctx, /*success=*/false, NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_FATAL_FAILURE,
                "TEvIncrementalRestoreSrcCreateRequest missing SchemeShardId");
        }
        if (!SrcPathId || !DstPathId) {
            return ReplyAndDie(ctx, /*success=*/false, NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_FATAL_FAILURE,
                "TEvIncrementalRestoreSrcCreateRequest missing Src/Dst PathId");
        }
        if (!SubOpTxId) {
            return ReplyAndDie(ctx, /*success=*/false, NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_FATAL_FAILURE,
                "TEvIncrementalRestoreSrcCreateRequest missing SubOpTxId");
        }

        const ui64 tableId = SrcPathId.LocalPathId;
        const auto& userTables = Self->GetUserTables();
        auto it = userTables.find(tableId);
        if (it == userTables.end()) {
            return ReplyAndDie(ctx, /*success=*/false, NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::END_FATAL_FAILURE,
                TStringBuilder() << "Source table for incremental restore not found in DataShard, tableId: "
                << tableId << ", OwnerId: " << SrcPathId.OwnerId);
        }

        TUserTable::TCPtr table = it->second;
        const ui32 localTableId = table->LocalTid;

        const ui64 tabletID = Self->TabletID();
        const ui64 generation = Self->Generation();
        const TActorId tabletActor = Self->SelfId();
        const TPathId srcPathId = SrcPathId;
        const TPathId dstPathId = DstPathId;

        auto changeSenderFactory = [tabletID, generation, tabletActor, srcPathId, dstPathId]
                (const TActorContext& ctx, TActorId parent) {
            return ctx.Register(
                CreateIncrRestoreChangeSender(
                    parent,
                    TDataShardId{
                        .TabletId = tabletID,
                        .Generation = generation,
                        .ActorId = tabletActor,
                    },
                    srcPathId,
                    dstPathId));
        };

        auto* appData = AppData();
        NStreamScan::TLimits limits;
        limits.BatchMaxBytes = appData->DataShardConfig.GetIncrementalRestoreScanBatchMaxBytes();
        limits.BatchMinRows = appData->DataShardConfig.GetIncrementalRestoreScanBatchMinRows();
        limits.BatchMaxRows = appData->DataShardConfig.GetIncrementalRestoreScanBatchMaxRows();

        THolder<NTable::IScan> scan{CreateIncrementalRestoreScan(
                SelfId(),
                std::move(changeSenderFactory),
                srcPathId,
                table,
                dstPathId,
                SubOpTxId,
                limits)};

        const auto& taskName = appData->DataShardConfig.GetRestoreTaskName();
        const auto taskPrio = appData->DataShardConfig.GetRestoreTaskPriority();

        ui64 readAheadLo = appData->DataShardConfig.GetIncrementalRestoreReadAheadLo();
        if (ui64 readAheadLoOverride = Self->GetIncrementalRestoreReadAheadLoOverride(); readAheadLoOverride > 0) {
            readAheadLo = readAheadLoOverride;
        }
        ui64 readAheadHi = appData->DataShardConfig.GetIncrementalRestoreReadAheadHi();
        if (ui64 readAheadHiOverride = Self->GetIncrementalRestoreReadAheadHiOverride(); readAheadHiOverride > 0) {
            readAheadHi = readAheadHiOverride;
        }

        ScanTaskId = Self->QueueScan(localTableId, scan.Release(), SubOpTxId,
            TScanOptions()
                .SetResourceBroker(taskName, taskPrio)
                .SetReadAhead(readAheadLo, readAheadHi)
                .SetReadPrio(TScanOptions::EReadPrio::Low));

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                   "TIncrementalRestoreSrcActor[" << SelfId() << "] started scan task " << ScanTaskId
                   << " SubOpTxId=" << SubOpTxId
                   << " SrcPathId=" << SrcPathId << " DstPathId=" << DstPathId
                   << " at tablet " << Self->TabletID());
    }

private:
    void Handle(TEvIncrementalRestoreScan::TEvFinished::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        // TxId=0 events are internal flow-control from change_sender; ignore.
        if (msg->TxId == 0) {
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                   "TIncrementalRestoreSrcActor[" << SelfId() << "] scan finished"
                   << " SubOpTxId=" << SubOpTxId << " success=" << msg->Success
                   << " endStatus=" << static_cast<int>(msg->EndStatus));

        ReplyAndDie(ctx, msg->Success, msg->EndStatus, msg->Error);
    }

    void ReplyAndDie(const TActorContext& ctx, bool success,
                     NKikimrTxDataShard::TEvIncrementalRestoreShardProgress::EEndStatus endStatus, const TString& error) {
        if (!SchemeShardId) {
            PassAway();
            return;
        }

        auto progressEv = MakeHolder<TEvDataShard::TEvIncrementalRestoreShardProgress>();
        auto& rec = progressEv->Record;
        rec.SetOperationId(OperationId);
        rec.SetSubOpTxId(SubOpTxId);
        rec.SetShardIdx(ShardIdx);
        rec.SetTabletId(Self->TabletID());
        rec.SetGeneration(SchemeShardGeneration);
        rec.SetEndStatus(endStatus);
        rec.SetSuccess(success);
        if (!error.empty()) {
            rec.SetError(error);
        }

        Self->SendIncrementalRestoreShardProgress(ctx, SchemeShardId, std::move(progressEv));

        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvIncrementalRestoreScan::TEvFinished, Handle);
        }
    }

private:
    TDataShard* const Self;
    const ui64 OperationId;
    const ui64 SubOpTxId;
    const ui64 ShardIdx;
    const ui64 SchemeShardGeneration;
    const ui64 SchemeShardId;
    const TPathId SrcPathId;
    const TPathId DstPathId;
    ui64 ScanTaskId = 0;
};

} // anonymous namespace

TActorId CreateIncrementalRestoreSrcActor(
        TDataShard* self,
        const NKikimrTxDataShard::TEvIncrementalRestoreSrcCreateRequest& request) {
    auto& ctx = TActivationContext::AsActorContext();
    return ctx.RegisterWithSameMailbox(new TIncrementalRestoreSrcActor(self, request));
}

} // namespace NKikimr::NDataShard

namespace NKikimr::NDataShard {

void TDataShard::Handle(TEvDataShard::TEvIncrementalRestoreSrcCreateRequest::TPtr& ev,
                        const TActorContext& ctx) {
    const auto& rec = ev->Get()->Record;
    LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
               "TEvIncrementalRestoreSrcCreateRequest received at tablet " << TabletID()
               << " OperationId=" << rec.GetOperationId()
               << " SubOpTxId=" << rec.GetSubOpTxId()
               << " ShardIdx=" << rec.GetShardIdx()
               << " SchemeShardGeneration=" << rec.GetSchemeShardGeneration());

    CreateIncrementalRestoreSrcActor(this, rec);
}

} // namespace NKikimr::NDataShard
