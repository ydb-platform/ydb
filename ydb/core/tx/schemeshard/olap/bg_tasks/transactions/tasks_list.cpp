#include "tasks_list.h"
#include <ydb/core/tx/schemeshard/schemeshard_import_helpers.h>
#include <util/string/cast.h>

namespace NKikimr::NSchemeShard::NBackground {

bool TTxTasksList::Execute(NTabletFlatExecutor::TTransactionContext&, const TActorContext&) {
    NKikimrSchemeShardTxBackgroundProto::TEvListResponse protoResponse;
    protoResponse.SetStatus(Ydb::StatusIds::SUCCESS);

    TPath database = TPath::Resolve(DatabaseName, Self);
    if (!database.IsResolved()) {
        ProtoResponse.SetStatus(Ydb::StatusIds::NOT_FOUND);
        auto& issue = *ProtoResponse.MutableIssues()->Add();
        issue.set_severity(NYql::TSeverityIds::S_ERROR);
        issue.set_message("database not resolved");
        return true;
    }

    auto records = Self->BackgroundSessionsManager->GetSessionsInfoForReport();
    auto it = records.begin();
    {
        ui64 skip = (PageIdx)*PageSize;
        while ((it != records.end()) && skip) {
            --skip;
            ++it;
        }
    }

    ui64 size = 0;
    while ((it != records.end()) && size < PageSize) {
        *protoResponse.MutableEntries()->Add() = it->SerializeToProto();
        ++size;
        ++it;
    }

    if (it == records.end()) {
        protoResponse.SetNextPageToken("0");
    } else {
        protoResponse.SetNextPageToken(ToString(PageIdx + 1));
    }
    ProtoResponse = protoResponse;
    return true;
}

void TTxTasksList::Complete(const TActorContext&) {
    auto response = std::make_unique<TEvListResponse>(ProtoResponse);
    NActors::TActivationContext::AsActorContext().Send(SenderId, std::move(response), 0, RequestCookie);
}

TTxTasksList::TTxTasksList(TSelf* self, TEvListRequest::TPtr& ev)
    : TBase(self)
    , SenderId(ev->Sender)
    , RequestCookie(ev->Cookie)
    , DatabaseName(ev->Get()->Record.GetDatabaseName())
{
    if (ev->Get()->Record.HasPageSize() && ev->Get()->Record.GetPageSize()) {
        PageSize = ev->Get()->Record.GetPageSize();
    }
    if (ev->Get()->Record.HasPageToken()) {
        if (!TryFromString<ui32>(ev->Get()->Record.GetPageToken(), PageIdx)) {
            PageIdx = 0;
        }
    }
}

}