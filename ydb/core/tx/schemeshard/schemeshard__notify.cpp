#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

TEvSchemeShard::TEvNotifyTxCompletionResult* TSchemeShard::MakeNotifyTxCompletionResult(ui64 txId) const {
    auto result = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletionResult>(txId);
    if (const auto* failure = HnswReplicaFailures.FindPtr(txId)) {
        result->Record.SetStatus(Ydb::StatusIds::UNAVAILABLE);
        result->Record.SetError(failure->Error);
    }
    return result.Release();
}

void TSchemeShard::RecordHnswReplicaFailure(ui64 txId, TString error, NIceDb::TNiceDb& db, const TActorContext& ctx) {
    const auto expiresAt = ctx.Now() + TDuration::Days(1);
    db.Table<Schema::HnswReplicaFailures>().Key(txId).Update(
        NIceDb::TUpdate<Schema::HnswReplicaFailures::Error>(error),
        NIceDb::TUpdate<Schema::HnswReplicaFailures::ExpiresAt>(expiresAt.MicroSeconds()));
    HnswReplicaFailures.emplace(txId, THnswReplicaFailure{std::move(error), expiresAt});
    ScheduleHnswReplicaFailureCleanup(ctx);
}

void TSchemeShard::ScheduleHnswReplicaFailureCleanup(const TActorContext& ctx) {
    if (!HnswReplicaFailureCleanupScheduled && !HnswReplicaFailures.empty()) {
        HnswReplicaFailureCleanupScheduled = true;
        ctx.Schedule(TDuration::Hours(1), new TEvPrivate::TEvCleanupHnswReplicaFailures());
    }
}

struct TSchemeShard::TTxCleanupHnswReplicaFailures : public TSchemeShard::TRwTxBase {
    explicit TTxCleanupHnswReplicaFailures(TSelf* self)
        : TRwTxBase(self)
    {}

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);
        for (auto it = Self->HnswReplicaFailures.begin(); it != Self->HnswReplicaFailures.end();) {
            const auto txId = TTxId(it->first);
            if (it->second.ExpiresAt <= ctx.Now()
                    && !Self->Operations.contains(txId) && !Self->Publications.contains(txId)) {
                db.Table<Schema::HnswReplicaFailures>().Key(it->first).Delete();
                Self->HnswReplicaFailures.erase(it++);
            } else {
                ++it;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        Self->HnswReplicaFailureCleanupScheduled = false;
        Self->ScheduleHnswReplicaFailureCleanup(ctx);
    }
};

void TSchemeShard::Handle(TEvPrivate::TEvCleanupHnswReplicaFailures::TPtr&, const TActorContext& ctx) {
    Execute(new TTxCleanupHnswReplicaFailures(this), ctx);
}

struct TSchemeShard::TTxNotifyCompletion : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvNotifyTxCompletion::TPtr Ev;
    TAutoPtr<IEventBase> Result;

    TTxNotifyCompletion(TSelf *self, TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev)
        : TRwTxBase(self)
        , Ev(ev)
    {}

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        Y_UNUSED(txc);
        Y_UNUSED(ctx);
        auto rawTxId = Ev->Get()->Record.GetTxId();

        if (Self->Operations.contains(TTxId(rawTxId))) {
            auto txId = TTxId(rawTxId);
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: operation in-flight",
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );

            TOperation::TPtr operation = Self->Operations.at(txId);
            if (operation->IsReadyToNotify(ctx)) {
                YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: operation is ready to notify",
                    {"txId", txId},
                    {"schemeshard", Self->TabletID()},
                );
                Result = Self->MakeNotifyTxCompletionResult(ui64(txId));
                return;
            }

            operation->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Publications.contains(TTxId(rawTxId))) {
            auto txId = TTxId(rawTxId);
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: publication in-flight",
                {"count", Self->Publications.at(txId).Paths.size()},
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );

            Self->Publications.at(txId).Subscribers.insert(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->HnswReplicaFailures.contains(rawTxId)) {
            Result = Self->MakeNotifyTxCompletionResult(rawTxId);
            return;
        } else if (Self->Exports.contains(rawTxId)) {
            auto txId = rawTxId;
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: export in-flight",
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );

            TExportInfo::TPtr exportInfo = Self->Exports.at(txId);
            if (exportInfo->IsFinished()) {
                YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: export is ready to notify",
                    {"txId", txId},
                    {"schemeshard", Self->TabletID()},
                );
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            exportInfo->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Imports.contains(rawTxId)) {
            auto txId = rawTxId;
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: import in-flight",
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );

            TImportInfo::TPtr importInfo = Self->Imports.at(txId);
            if (importInfo->IsFinished()) {
                YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: import is ready to notify",
                    {"txId", txId},
                    {"schemeshard", Self->TabletID()},
                );
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            importInfo->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (const auto txId = TIndexBuildId(rawTxId); const auto* indexInfoPtr = Self->IndexBuilds.FindPtr(txId)) {
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: index build in-flight",
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );
            auto& indexInfo = *indexInfoPtr->get();
            if (indexInfo.IsFinished()) {
                YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: index build is ready to notify",
                    {"txId", txId},
                    {"schemeshard", Self->TabletID()},
                );
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId));
                return;
            }

            indexInfo.AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (auto* compactionInfoPtr = Self->ForcedCompactions.FindPtr(rawTxId)) {
            auto& compactionInfo = *compactionInfoPtr->Get();
            auto txId = rawTxId;
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: forced compaction in-flight",
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );
            if (compactionInfo.IsFinished()) {
                YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: forced compaction is ready to notify",
                    {"txId", txId},
                    {"schemeshard", Self->TabletID()},
                );
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            compactionInfo.AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(txId);
        } else if (const auto txId = TIndexBuildId(rawTxId); const auto* operationInfoPtr = Self->SetColumnConstraintOperations.FindPtr(txId)) {
            YDB_LOG_DEBUG_CTX(ctx, "NotifyTxCompletion: set column constraint in-flight",
                {"txId", txId},
                {"schemeshard", Self->TabletID()},
            );
            auto& operationInfo = *operationInfoPtr->get();
            if (operationInfo.IsFinished()) {
                YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: set column constraint is ready to notify",
                    {"txId", txId},
                    {"schemeshard", Self->TabletID()},
                );
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId));
                return;
            }

            operationInfo.AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else {
            YDB_LOG_WARN_CTX(ctx, "NotifyTxCompletion: unknown transaction",
                {"txId", rawTxId},
                {"schemeshard", Self->TabletID()},
            );
            Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(rawTxId);
            return;
        }

        YDB_LOG_INFO_CTX(ctx, "NotifyTxCompletion: transaction is registered",
            {"txId", rawTxId},
            {"schemeshard", Self->TabletID()},
        );
    }

    void DoComplete(const TActorContext &ctx) override {
        if (Result) {
            ctx.Send(Ev->Sender, Result.Release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxNotifyTxCompletion(TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev) {
    return new TTxNotifyCompletion(this, ev);
}

}}

#undef YDB_LOG_THIS_FILE_COMPONENT
