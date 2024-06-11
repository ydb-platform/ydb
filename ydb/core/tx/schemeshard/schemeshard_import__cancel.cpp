#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_import.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TImport::TTxCancel: public TSchemeShard::TXxport::TTxBase {
    TEvImport::TEvCancelImportRequest::TPtr Request;

    explicit TTxCancel(TSelf *self, TEvImport::TEvCancelImportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        auto respond = [this, &request](Ydb::StatusIds::StatusCode status) -> bool {
            auto response = MakeHolder<TEvImport::TEvCancelImportResponse>(request.GetTxId());
            auto& proto = *response->Record.MutableResponse();

            proto.SetStatus(status);
            Send(Request->Sender, std::move(response), 0, Request->Cookie);

            return true;
        };

        auto it = Self->Imports.find(request.GetRequest().GetId());
        if (it == Self->Imports.end() || !IsSameDomain(it->second, request.GetDatabaseName())) {
            return respond(Ydb::StatusIds::NOT_FOUND);
        }

        TImportInfo::TPtr importInfo = it->second;
        NIceDb::TNiceDb db(txc.DB);

        switch (importInfo->State) {
        case TImportInfo::EState::Done:
        case TImportInfo::EState::Cancelled:
            return respond(Ydb::StatusIds::SUCCESS);

        case TImportInfo::EState::Waiting:
        case TImportInfo::EState::Cancellation:
            importInfo->Issue = "Cancelled manually";
            importInfo->State = TImportInfo::EState::Cancelled;

            for (ui32 itemIdx : xrange(importInfo->Items.size())) {
                const auto& item = importInfo->Items.at(itemIdx);

                switch (item.State) {
                case TImportInfo::EState::Transferring:
                    if (item.WaitTxId != InvalidTxId) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                        Send(Self->SelfId(), CancelRestorePropose(importInfo, item.WaitTxId), 0, importInfo->Id);
                    } else if (item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                case TImportInfo::EState::BuildIndexes:
                    if (item.WaitTxId != InvalidTxId) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                        Send(Self->SelfId(), CancelIndexBuildPropose(Self, importInfo, item.WaitTxId), 0, importInfo->Id);
                    } else if (item.SubState == TImportInfo::TItem::ESubState::Proposed) {
                        importInfo->State = TImportInfo::EState::Cancellation;
                    }
                    break;

                default:
                    break;
                }
            }

            if (importInfo->State == TImportInfo::EState::Cancelled) {
                importInfo->EndTime = TAppData::TimeProvider->Now();
            }

            Self->PersistImportState(db, importInfo);
            SendNotificationsIfFinished(importInfo);
            return respond(Ydb::StatusIds::SUCCESS);

        default:
            return respond(Ydb::StatusIds::UNDETERMINED);
        }
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancel

struct TSchemeShard::TImport::TTxCancelAck: public TSchemeShard::TXxport::TTxBase {
    TEvSchemeShard::TEvCancelTxResult::TPtr CancelTxResult = nullptr;
    TEvIndexBuilder::TEvCancelResponse::TPtr CancelIndexBuildResult = nullptr;

    explicit TTxCancelAck(TSelf *self, TEvSchemeShard::TEvCancelTxResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , CancelTxResult(ev)
    {
    }

    explicit TTxCancelAck(TSelf *self, TEvIndexBuilder::TEvCancelResponse::TPtr& ev)
        : TXxport::TTxBase(self)
        , CancelIndexBuildResult(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CANCEL_IMPORT_ACK;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        TTxId txId;
        ui64 id;
        if (CancelTxResult) {
            txId = TTxId(CancelTxResult->Get()->Record.GetTargetTxId());
            id = CancelTxResult->Cookie;
        } else if (CancelIndexBuildResult) {
            txId = TTxId(CancelIndexBuildResult->Get()->Record.GetTxId());
            id = CancelIndexBuildResult->Cookie;
        } else {
            Y_ABORT("unreachable");
        }

        if (!Self->Imports.contains(id)) {
            return true;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        if (importInfo->State != TImportInfo::EState::Cancellation) {
            return true;
        }

        bool found = false;
        ui32 itemIdx;
        ui32 cancelledItems = 0;
        ui32 cancellableItems = 0;

        for (ui32 i : xrange(importInfo->Items.size())) {
            auto& item = importInfo->Items.at(i);

            if (item.State != TImportInfo::EState::Transferring && item.State != TImportInfo::EState::BuildIndexes) {
                continue;
            }

            if (item.WaitTxId != InvalidTxId) {
                ++cancellableItems;
            }

            if (item.WaitTxId == txId) {
                found = true;

                item.State = TImportInfo::EState::Cancelled;
                itemIdx = i;
            }

            if (item.State == TImportInfo::EState::Cancelled) {
                ++cancelledItems;
            }
        }

        if (!found) {
            return true;
        }

        Self->TxIdToImport.erase(txId);
        Self->PersistImportItemState(db, importInfo, itemIdx);

        if (cancelledItems != cancellableItems) {
            return true;
        }

        importInfo->State = TImportInfo::EState::Cancelled;
        importInfo->EndTime = TAppData::TimeProvider->Now();
        Self->PersistImportState(db, importInfo);

        SendNotificationsIfFinished(importInfo);
        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxCancelAck

ITransaction* TSchemeShard::CreateTxCancelImport(TEvImport::TEvCancelImportRequest::TPtr& ev) {
    return new TImport::TTxCancel(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelImportAck(TEvSchemeShard::TEvCancelTxResult::TPtr& ev) {
    return new TImport::TTxCancelAck(this, ev);
}

ITransaction* TSchemeShard::CreateTxCancelImportAck(TEvIndexBuilder::TEvCancelResponse::TPtr& ev) {
    return new TImport::TTxCancelAck(this, ev);
}

} // NSchemeShard
} // NKikimr
