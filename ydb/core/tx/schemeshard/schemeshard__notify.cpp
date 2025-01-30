#include "schemeshard_impl.h"

#include <ydb/core/base/appdata.h>

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
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "NotifyTxCompletion"
                            << " operation in-flight"
                            << ", txId: " << txId
                            << ", at schemeshard: " << Self->TabletID());

            TOperation::TPtr operation = Self->Operations.at(txId);
            if (operation->IsReadyToNotify(ctx)) {
                LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "NotifyTxCompletion"
                               << ", operation is ready to notify"
                               << ", txId: " << txId
                               << ", at schemeshard: " << Self->TabletID());
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId));
                return;
            }

            operation->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Publications.contains(TTxId(rawTxId))) {
            auto txId = TTxId(rawTxId);
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "NotifyTxCompletion"
                            << " publication in-flight"
                            << ", count: " << Self->Publications.at(txId).Paths.size()
                            << ", txId: " << txId
                            << ", at schemeshard: " << Self->TabletID());

            Self->Publications.at(txId).Subscribers.insert(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Exports.contains(rawTxId)) {
            auto txId = rawTxId;
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "NotifyTxCompletion"
                            << " export in-flight"
                            << ", txId: " << txId
                            << ", at schemeshard: " << Self->TabletID());

            TExportInfo::TPtr exportInfo = Self->Exports.at(txId);
            if (exportInfo->IsFinished()) {
                LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "NotifyTxCompletion"
                               << ", export is ready to notify"
                               << ", txId: " << txId
                               << ", at schemeshard: " << Self->TabletID());
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            exportInfo->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (Self->Imports.contains(rawTxId)) {
            auto txId = rawTxId;
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "NotifyTxCompletion"
                            << " import in-flight"
                            << ", txId: " << txId
                            << ", at schemeshard: " << Self->TabletID());

            TImportInfo::TPtr importInfo = Self->Imports.at(txId);
            if (importInfo->IsFinished()) {
                LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "NotifyTxCompletion"
                               << ", import is ready to notify"
                               << ", txId: " << txId
                               << ", at schemeshard: " << Self->TabletID());
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(txId);
                return;
            }

            importInfo->AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else if (const auto txId = TIndexBuildId(rawTxId); const auto* indexInfoPtr = Self->IndexBuilds.FindPtr(txId)) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "NotifyTxCompletion"
                            << " index build in-flight"
                            << ", txId: " << txId
                            << ", at schemeshard: " << Self->TabletID());
            auto& indexInfo = *indexInfoPtr->Get();
            if (indexInfo.IsFinished()) {
                LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "NotifyTxCompletion"
                               << ", index build is ready to notify"
                               << ", txId: " << txId
                               << ", at schemeshard: " << Self->TabletID());
                Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(ui64(txId));
                return;
            }

            indexInfo.AddNotifySubscriber(Ev->Sender);
            Result = new TEvSchemeShard::TEvNotifyTxCompletionRegistered(ui64(txId));
        } else {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "NotifyTxCompletion"
                           << ", unknown transaction"
                           << ", txId: " << rawTxId
                           << ", at schemeshard: " << Self->TabletID());
            Result = new TEvSchemeShard::TEvNotifyTxCompletionResult(rawTxId);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "NotifyTxCompletion"
                       << " transaction is registered"
                       << ", txId: " << rawTxId
                       << ", at schemeshard: " << Self->TabletID());
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
