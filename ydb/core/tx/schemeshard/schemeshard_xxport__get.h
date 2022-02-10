#pragma once
#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {
namespace NSchemeShard {

template <typename TInfo, typename TEvRequest, typename TEvResponse>
struct TSchemeShard::TXxport::TTxGet: public TSchemeShard::TXxport::TTxBase {
    using TTxGetBase = TTxGet<TInfo, TEvRequest, TEvResponse>;

    typename TEvRequest::TPtr Request;

    explicit TTxGet(TSelf* self, typename TEvRequest::TPtr& ev)
        : TTxBase(self)
        , Request(ev)
    {
    }

    bool DoExecuteImpl(const THashMap<ui64, typename TInfo::TPtr>& container, TTransactionContext&, const TActorContext&) {
        const auto& request = Request->Get()->Record;

        auto response = MakeHolder<TEvResponse>();
        auto& entry = *response->Record.MutableResponse()->MutableEntry();

        auto it = container.find(request.GetRequest().GetId());
        if (it == container.end() || !IsSameDomain(it->second, request.GetDatabaseName())) {
            entry.SetStatus(Ydb::StatusIds::NOT_FOUND);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        Self->FromXxportInfo(entry, it->second);

        Send(Request->Sender, std::move(response), 0, Request->Cookie);
        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxGet

} // NSchemeShard
} // NKikimr
