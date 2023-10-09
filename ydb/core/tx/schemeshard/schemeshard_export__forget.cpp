#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_export_flow_proposals.h"
#include "schemeshard_export_helpers.h"
#include "schemeshard_export.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TExport::TTxForget: public TSchemeShard::TXxport::TTxBase {
    TEvExport::TEvForgetExportRequest::TPtr Request;
    bool Progress;

    explicit TTxForget(TSelf *self, TEvExport::TEvForgetExportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
        , Progress(false)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_FORGET_EXPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        auto response = MakeHolder<TEvExport::TEvForgetExportResponse>(request.GetTxId());
        auto& forget = *response->Record.MutableResponse();

        auto it = Self->Exports.find(request.GetRequest().GetId());
        if (it == Self->Exports.end() || !IsSameDomain(it->second, request.GetDatabaseName())) {
            forget.SetStatus(Ydb::StatusIds::NOT_FOUND);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        TExportInfo::TPtr exportInfo = it->second;

        if (!exportInfo->IsValid()) {
            forget.SetStatus(Ydb::StatusIds::UNDETERMINED);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        if (exportInfo->IsInProgress()) {
            forget.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
            AddIssue(forget, "Export operation is in progress");

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        Y_ABORT_UNLESS(exportInfo->IsFinished());
        NIceDb::TNiceDb db(txc.DB);

        const TPath exportPath = TPath::Init(exportInfo->ExportPathId, Self);
        if (!exportPath.IsResolved()) {
            SendNotificationsIfFinished(exportInfo, true); // for tests

            if (exportInfo->Uid) {
                Self->ExportsByUid.erase(exportInfo->Uid);
            }

            Self->Exports.erase(exportInfo->Id);
            Self->PersistRemoveExport(db, exportInfo);
        } else {
            exportInfo->WaitTxId = InvalidTxId;
            exportInfo->State = TExportInfo::EState::Dropping;
            Self->PersistExportState(db, exportInfo);

            for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
                auto& item = exportInfo->Items.at(itemIdx);

                item.WaitTxId = InvalidTxId;
                item.State = TExportInfo::EState::Dropped;

                const TPath itemPath = TPath::Resolve(ExportItemPathName(Self, exportInfo, itemIdx), Self);
                if (itemPath.IsResolved() && !itemPath.IsDeleted()) {
                    item.State = TExportInfo::EState::Dropping;
                }

                Self->PersistExportItemState(db, exportInfo, itemIdx);
            }

            Progress = true;
        }

        forget.SetStatus(Ydb::StatusIds::SUCCESS);
        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    void DoComplete(const TActorContext& ctx) override {
        if (Progress) {
            const ui64 id = Request->Get()->Record.GetRequest().GetId();
            Self->Execute(Self->CreateTxProgressExport(id), ctx);
        }
    }

}; // TTxForget

ITransaction* TSchemeShard::CreateTxForgetExport(TEvExport::TEvForgetExportRequest::TPtr& ev) {
    return new TExport::TTxForget(this, ev);
}

} // NSchemeShard
} // NKikimr
