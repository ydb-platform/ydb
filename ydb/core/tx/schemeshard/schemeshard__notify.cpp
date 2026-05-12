#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/schemeshard/index/index_build_info.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

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
            YDBLOG_CTX_DEBUG(ctx, "NotifyTxCompletion operation in-flight",
                {"txId", txId},
                {"at_schemeshard", Self->TabletID()});

            TOperation::TPtr operation = Self->Operations.at(txId);
            if (operation->IsReadyToNotify(ctx)) {
                YDBLOG_CTX_INFO(ctx, "NotifyTxCompletion , operation is ready to notify",
                    {"txId", txId},
                    {"at_schemeshard", Self->TabletID()});
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId));
                return;
            }

            operation->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Publications.contains(TTxId(rawTxId))) {
            auto txId = TTxId(rawTxId);
            YDBLOG_CTX_DEBUG(ctx, "NotifyTxCompletion publication in-flight",
                {"count", Self->Publications.at(txId).Paths.size()},
                {"txId", txId},
                {"at_schemeshard", Self->TabletID()});

            Self->Publications.at(txId).Subscribers.insert(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Exports.contains(rawTxId)) {
            auto txId = rawTxId;
            YDBLOG_CTX_DEBUG(ctx, "NotifyTxCompletion export in-flight",
                {"txId", txId},
                {"at_schemeshard", Self->TabletID()});

            TExportInfo::TPtr exportInfo = Self->Exports.at(txId);
            if (exportInfo->IsFinished()) {
                YDBLOG_CTX_INFO(ctx, "NotifyTxCompletion , export is ready to notify",
                    {"txId", txId},
                    {"at_schemeshard", Self->TabletID()});
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            exportInfo->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Imports.contains(rawTxId)) {
            auto txId = rawTxId;
            YDBLOG_CTX_DEBUG(ctx, "NotifyTxCompletion import in-flight",
                {"txId", txId},
                {"at_schemeshard", Self->TabletID()});

            TImportInfo::TPtr importInfo = Self->Imports.at(txId);
            if (importInfo->IsFinished()) {
                YDBLOG_CTX_INFO(ctx, "NotifyTxCompletion , import is ready to notify",
                    {"txId", txId},
                    {"at_schemeshard", Self->TabletID()});
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            importInfo->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (const auto txId = TIndexBuildId(rawTxId); const auto* indexInfoPtr = Self->IndexBuilds.FindPtr(txId)) {
            YDBLOG_CTX_DEBUG(ctx, "NotifyTxCompletion index build in-flight",
                {"txId", txId},
                {"at_schemeshard", Self->TabletID()});
            auto& indexInfo = *indexInfoPtr->get();
            if (indexInfo.IsFinished()) {
                YDBLOG_CTX_INFO(ctx, "NotifyTxCompletion , index build is ready to notify",
                    {"txId", txId},
                    {"at_schemeshard", Self->TabletID()});
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId));
                return;
            }

            indexInfo.AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (auto* compactionInfoPtr = Self->ForcedCompactions.FindPtr(rawTxId)) {
            auto& compactionInfo = *compactionInfoPtr->Get();
            auto txId = rawTxId;
            YDBLOG_CTX_DEBUG(ctx, "NotifyTxCompletion forced compaction in-flight",
                {"txId", txId},
                {"at_schemeshard", Self->TabletID()});
            if (compactionInfo.IsFinished()) {
                YDBLOG_CTX_INFO(ctx, "NotifyTxCompletion , forced compaction is ready to notify",
                    {"txId", txId},
                    {"at_schemeshard", Self->TabletID()});
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            compactionInfo.AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(txId);
        } else {
            YDBLOG_CTX_WARN(ctx, "NotifyTxCompletion , unknown transaction",
                {"txId", rawTxId},
                {"at_schemeshard", Self->TabletID()});
            Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(rawTxId);
            return;
        }

        YDBLOG_CTX_INFO(ctx, "NotifyTxCompletion transaction is registered",
            {"txId", rawTxId},
            {"at_schemeshard", Self->TabletID()});
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
