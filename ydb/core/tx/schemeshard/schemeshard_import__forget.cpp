#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_import.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TImport::TTxForget: public TSchemeShard::TXxport::TTxBase {
    TEvImport::TEvForgetImportRequest::TPtr Request;

    explicit TTxForget(TSelf *self, TEvImport::TEvForgetImportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        auto respond = [this, &request](Ydb::StatusIds::StatusCode status, const TString& issue = {}) -> bool {
            auto response = MakeHolder<TEvImport::TEvForgetImportResponse>(request.GetTxId());
            auto& proto = *response->Record.MutableResponse();

            proto.SetStatus(status);
            if (issue) {
                AddIssue(proto, issue);
            }

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
            Self->ImportsByUid.erase(importInfo->Uid);
            Self->Imports.erase(importInfo->Id);
            Self->PersistRemoveImport(db, importInfo);
            return respond(Ydb::StatusIds::SUCCESS);

        case TImportInfo::EState::Waiting:
        case TImportInfo::EState::Cancellation:
            return respond(Ydb::StatusIds::PRECONDITION_FAILED, "Import operation is in progress");

        default:
            return respond(Ydb::StatusIds::UNDETERMINED);
        }
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxForget

ITransaction* TSchemeShard::CreateTxForgetImport(TEvImport::TEvForgetImportRequest::TPtr& ev) {
    return new TImport::TTxForget(this, ev);
}

} // NSchemeShard
} // NKikimr
