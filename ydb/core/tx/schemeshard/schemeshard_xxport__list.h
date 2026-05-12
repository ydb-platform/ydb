#pragma once

#include "schemeshard_impl.h"
#include "schemeshard_xxport__tx_base.h"

#include <util/string/cast.h>

namespace NKikimr {
namespace NSchemeShard {

template <typename TInfo, typename TEvRequest, typename TEvResponse, typename TDerived>
struct TSchemeShard::TXxport::TTxList: public TSchemeShard::TXxport::TTxBase {
    using TTxListBase = TTxList<TInfo, TEvRequest, TEvResponse, TDerived>;

    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;

    typename TEvRequest::TPtr Request;

    explicit TTxList(TSelf* self, typename TEvRequest::TPtr& ev)
        : TTxBase(self)
        , Request(ev)
    {
    }

    bool DoExecuteImpl(const THashMap<ui64, typename TInfo::TPtr>& container,
        const TSet<std::pair<TInstant, ui64>> containerByTime, TTransactionContext&, const TActorContext&) {
        const auto& record = Request->Get()->Record;
        const auto& request = record.GetRequest();

        auto response = MakeHolder<TEvResponse>();
        auto& resp = *response->Record.MutableResponse();

        const TPathId domainPathId = DomainPathId(record.GetDatabaseName());
        const ui64 pageSize = Min(request.GetPageSize() ? Max(request.GetPageSize(), MinPageSize) : DefaultPageSize, MaxPageSize);

        ui64 page = DefaultPage;
        if (request.GetPageToken() && !TryFromString(request.GetPageToken(), page)) {
            resp.SetStatus(Ydb::StatusIds::BAD_REQUEST);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }
        page = Max(page, DefaultPage);

        typename TInfo::EKind kind;
        if (!TDerived::TryParseKind(request.GetKind(), kind)) {
            resp.SetStatus(Ydb::StatusIds::BAD_REQUEST);

            Send(Request->Sender, std::move(response), 0, Request->Cookie);
            return true;
        }

        resp.SetStatus(Ydb::StatusIds::SUCCESS);

        auto it = containerByTime.end();
        ui64 skip = (page - 1) * pageSize;
        while (it != containerByTime.begin() && skip) {
            --it;
            auto& item = container.at(it->second);
            if (IsSameDomain(item, domainPathId) && item->Kind == kind) {
                --skip;
            }
        }

        ui64 size = 0;
        while (it != containerByTime.begin() && size < pageSize) {
            --it;
            auto& item = container.at(it->second);
            if (IsSameDomain(item, domainPathId) && item->Kind == kind) {
                Self->FromXxportInfo(*resp.MutableEntries()->Add(), *item);
                ++size;
            }
        }

        if (it == containerByTime.begin()) {
            resp.SetNextPageToken("0");
        } else {
            resp.SetNextPageToken(ToString(page + 1));
        }

        Send(Request->Sender, std::move(response), 0, Request->Cookie);
        return true;
    }

    void DoComplete(const TActorContext&) override {
    }

}; // TTxList

} // NSchemeShard
} // NKikimr
