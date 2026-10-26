#include "columnshard_impl.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/protos/set_column_constraint.pb.h>
#include <ydb/core/tx/columnshard/engines/portions/compacted.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {
namespace {

class TColumnConstraintValidator: public TActorBootstrapped<TColumnConstraintValidator> {
    const TActorId Shard;
    const TActorId ReplyTo;
    const ui64 Cookie;
    const ui64 OperationId;
    const ui64 TabletId;
    std::unique_ptr<TEvDataShard::TEvKqpScan> Request;
    TActorId Scanner;

    void Finish(bool valid, const TString& error = {}) {
        auto response = std::make_unique<TEvDataShard::TEvValidateRowConditionResponse>();
        auto& record = response->Record;
        record.SetId(OperationId);
        record.SetTabletId(TabletId);
        record.SetStatus(error.empty() ? NKikimrSetColumnConstraint::DONE : NKikimrSetColumnConstraint::BAD_REQUEST);
        record.SetIsValid(valid);
        if (error) {
            record.AddIssues()->set_message(error);
        } else if (!valid) {
            record.AddIssues()->set_message("Constraint violation: NULL value found.");
        }
        Send(ReplyTo, response.release(), 0, Cookie);
        PassAway();
    }

    void Ack() {
        Send(Scanner, new NKqp::TEvKqpCompute::TEvScanDataAck(1024 * 1024, 0, 1));
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        Scanner = ActorIdFromProto(ev->Get()->Record.GetScanActorId());
        Ack();
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
        if (const auto& batch = ev->Get()->ArrowBatch) {
            for (const auto& column : batch->columns()) {
                if (column->null_count()) {
                    Finish(false);
                    return;
                }
            }
        }
        if (ev->Get()->Finished) {
            Scanner = {};
            Finish(true);
        } else {
            Ack();
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
        Finish(false, TStringBuilder() << "Column constraint scan failed: " << ev->Get()->Record.ShortDebugString());
    }

    void PassAway() override {
        if (Scanner) {
            Send(Scanner, new TEvents::TEvPoison);
        }
        Send(Shard, new TEvPrivate::TEvColumnConstraintValidationFinished(OperationId));
        TActorBootstrapped::PassAway();
    }

public:
    TColumnConstraintValidator(
        TActorId shard, TActorId replyTo, ui64 cookie, ui64 operationId, ui64 tabletId, std::unique_ptr<TEvDataShard::TEvKqpScan> request)
        : Shard(shard)
        , ReplyTo(replyTo)
        , Cookie(cookie)
        , OperationId(operationId)
        , TabletId(tabletId)
        , Request(std::move(request))
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Shard, Request.release());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, Handle);
            hFunc(NKqp::TEvKqpCompute::TEvScanData, Handle);
            hFunc(NKqp::TEvKqpCompute::TEvScanError, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

}   // namespace

void TColumnShard::Handle(TEvDataShard::TEvValidateRowConditionRequest::TPtr& ev, const TActorContext& ctx) {
    const auto& request = ev->Get()->Record;
    const auto pathId = TSchemeShardLocalPathId::FromRawValue(request.GetPathId());
    const auto internalPathId = TablesManager.ResolveInternalPathId(pathId, false);
    const auto fail = [&](const TString& error) {
        auto response = std::make_unique<TEvDataShard::TEvValidateRowConditionResponse>();
        response->Record.SetId(request.GetId());
        response->Record.SetTabletId(TabletID());
        response->Record.SetStatus(NKikimrSetColumnConstraint::BAD_REQUEST);
        response->Record.AddIssues()->set_message(error);
        ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
    };
    if (request.GetOwnerId() != CurrentSchemeShardId || request.GetTabletId() != TabletID() || !internalPathId ||
        !TablesManager.GetPrimaryIndex() || !request.NotNullColumnsSize()) {
        fail("Invalid column constraint validation request");
        return;
    }
    const auto schema = TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
    auto scan = std::make_unique<TEvDataShard::TEvKqpScan>();
    for (const auto& name : request.GetNotNullColumns()) {
        const auto columnIndex = schema->GetIndexInfo().GetColumnIndexOptional(name);
        if (!columnIndex || !schema->GetIndexInfo().GetColumnFeaturesVerifiedByIndex(*columnIndex).GetIsSetNotNullInProgress()) {
            fail("Column constraint write barrier is not installed");
            return;
        }
        scan->Record.AddColumnTags(schema->GetColumnId(name));
    }
    const auto seqNo = std::make_pair(request.GetSeqNoGeneration(), request.GetSeqNoRound());
    auto [it, inserted] = ColumnConstraintValidators.try_emplace(request.GetId(), TColumnConstraintValidation{ pathId, seqNo, {} });
    auto& validation = it->second;
    if (!inserted) {
        if (seqNo < validation.SeqNo || (seqNo == validation.SeqNo && validation.ScanStarted)) {
            return;
        }
        if (seqNo > validation.SeqNo) {
            if (validation.ActorId) {
                ctx.Send(validation.ActorId, new TEvents::TEvPoisonPill);
            }
            validation = { pathId, seqNo, {} };
        }
    }
    // Fence retries before waiting: a local timer may outlive its SchemeShard sender.
    // Drain pre-barrier writes, including transactions restored after a restart.
    if (OperationsManager->HasWriteOperationsBeforeSchema(*internalPathId, schema->GetVersion())) {
        ctx.Schedule(TDuration::MilliSeconds(100), std::make_unique<IEventHandle>(SelfId(), ev->Sender, ev->Release().Release(), 0, ev->Cookie));
        return;
    }
    // A recovered timecast frontier may lag already committed portions. Derive
    // a covering snapshot from durable metadata, then let scan admission wait
    // until that fixed snapshot is readable.
    auto snapshot = std::max(GetOutdatedSnapshot(), LastCleanupSnapshot);
    const auto& granule = GetIndexAs<NOlap::TColumnEngineForLogs>().GetGranuleVerified(*internalPathId);
    for (const auto& [_, portion] : granule.GetPortions()) {
        if (portion->IsAborted()) {
            continue;
        }
        snapshot = std::max(snapshot, portion->RecordSnapshotMax());
        if (portion->GetPortionType() == NOlap::EPortionType::Compacted) {
            snapshot = std::max(snapshot, static_cast<const NOlap::TCompactedPortionInfo&>(*portion).GetAppeared());
        }
        if (const auto removed = portion->GetRemoveSnapshotOptional()) {
            snapshot = std::max(snapshot, *removed);
        }
    }
    // The scan must apply MVCC and deletions before checking NULL values.
    scan->Record.SetLocalPathId(request.GetPathId());
    scan->Record.SetTxId(request.GetId());
    scan->Record.MutableSnapshot()->SetStep(snapshot.GetPlanStep());
    scan->Record.MutableSnapshot()->SetTxId(snapshot.GetTxId());
    scan->Record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);

    validation.ActorId =
        ctx.Register(new TColumnConstraintValidator(SelfId(), ev->Sender, ev->Cookie, request.GetId(), TabletID(), std::move(scan)));
    validation.ScanStarted = true;
}

void TColumnShard::Handle(TEvPrivate::TEvColumnConstraintValidationFinished::TPtr& ev, const TActorContext&) {
    const auto it = ColumnConstraintValidators.find(ev->Get()->OperationId);
    if (it != ColumnConstraintValidators.end() && it->second.ActorId == ev->Sender) {
        it->second.ActorId = {};
        // Keep the attempt fence until the final ALTER removes the write barrier.
    }
}

}   // namespace NKikimr::NColumnShard
