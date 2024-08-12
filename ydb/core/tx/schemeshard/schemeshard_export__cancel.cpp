#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_export_flow_proposals.h"
#include "schemeshard_export.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TExport::TTxCancel: public TSchemeShard::TXxport::TTxBase {
    TEvExport::TEvCancelExportRequest::TPtr Request;

    explicit TTxCancel(TSelf *self, TEvExport::TEvCancelExportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_EXPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        auto response = MakeHolder<TEvExport::TEvCancelExportResponse>(request.GetTxId());
        auto& cancel = *response->Record.MutableResponse();

        auto it = Self->Exports.find(request.GetRequest().GetId());
        if (it == Self->Exports.end() || !IsSameDomain(it->second, request.GetDatabaseName())) {
            cancel.SetStatus(Ydb::StatusIds::NOT_FOUND);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        TExportInfo::TPtr exportInfo = it->second;

        if (!exportInfo->IsValid()) {
            cancel.SetStatus(Ydb::StatusIds::UNDETERMINED);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        cancel.SetStatus(Ydb::StatusIds::SUCCESS);

        if (exportInfo->IsFinished() || exportInfo->IsDropping()) {
            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        exportInfo->Issue = "Cancelled manually";

        if (exportInfo->State < TExportInfo::EState::Transferring) {
            exportInfo->State = TExportInfo::EState::Cancelled;

            Self->TxIdToExport.erase(exportInfo->WaitTxId);
            exportInfo->WaitTxId = InvalidTxId;
        } else {
            exportInfo->State = TExportInfo::EState::Cancelled;

            for (const auto& item : exportInfo->Items) {
                if (item.State != TExportInfo::EState::Transferring) {
                    continue;
                }

                exportInfo->State = TExportInfo::EState::Cancellation;
                if (item.WaitTxId != InvalidTxId) {
                    Send(Self->SelfId(), CancelPropose(exportInfo, item.WaitTxId), 0, exportInfo->Id);
                }
            }
        }

        if (exportInfo->State == TExportInfo::EState::Cancelled) {
            exportInfo->EndTime = TAppData::TimeProvider->Now();
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistExportState(db, exportInfo);

        Send(Request->Sender, std::move(response), 0, Request->Cookie);
        SendNotificationsIfFinished(exportInfo);
        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancel

struct TSchemeShard::TExport::TTxCancelAck: public TSchemeShard::TXxport::TTxBase {
    TEvSchemeShard::TEvCancelTxResult::TPtr CancelResult;

    explicit TTxCancelAck(TSelf *self, TEvSchemeShard::TEvCancelTxResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , CancelResult(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_EXPORT_ACK;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const ui64 id = CancelResult->Cookie;
        const auto backupTxId = TTxId(CancelResult->Get()->Record.GetTargetTxId());

        if (!Self->Exports.contains(id)) {
            return true;
        }

        TExportInfo::TPtr exportInfo = Self->Exports.at(id);

        if (exportInfo->State != TExportInfo::EState::Cancellation) {
            return true;
        }

        bool found = false;
        ui32 itemIdx;
        ui32 cancelledItems = 0;
        ui32 cancellableItems = 0;

        for (ui32 i : xrange(exportInfo->Items.size())) {
            auto& item = exportInfo->Items.at(i);

            if (item.State != TExportInfo::EState::Transferring) {
                continue;
            }

            if (item.WaitTxId != InvalidTxId) {
                ++cancellableItems;
            }

            if (item.WaitTxId == backupTxId) {
                found = true;

                item.State = TExportInfo::EState::Cancelled;
                itemIdx = i;
            }

            if (item.State == TExportInfo::EState::Cancelled) {
                ++cancelledItems;
            }
        }

        if (!found) {
            return true;
        }

        Self->TxIdToExport.erase(backupTxId);

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistExportItemState(db, exportInfo, itemIdx);

        if (cancelledItems == cancellableItems) {
            exportInfo->State = TExportInfo::EState::Cancelled;
            exportInfo->EndTime = TAppData::TimeProvider->Now();
            Self->PersistExportState(db, exportInfo);
        }

        SendNotificationsIfFinished(exportInfo);
        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancelAck

ITransaction* TSchemeShard::CreateTxCancelExport(TEvExport::TEvCancelExportRequest::TPtr& ev) {
    return new TExport::TTxCancel(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelExportAck(TEvSchemeShard::TEvCancelTxResult::TPtr& ev) {
    return new TExport::TTxCancelAck(this, ev);
}

} // NSchemeShard
} // NKikimr
