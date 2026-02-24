#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxList: public TRwTxBase {
private:
    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;

public:
    explicit TTxList(TSelf* self, TEvForcedCompaction::TEvListRequest::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev)
    {}

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        const auto& request = Request->Get()->Record;
        LOG_N("TForcedCompaction::TTxList DoExecute " << request.ShortDebugString());

        auto response = MakeHolder<TEvForcedCompaction::TEvListResponse>();
        TPath database = TPath::Resolve(request.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database " << request.GetDatabaseName() << " not found"
            );
        }
        const TPathId subdomainPathId = database.GetPathIdForDomain();

        ui64 page = DefaultPage;
        if (request.GetPageToken() && !TryFromString(request.GetPageToken(), page)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Unable to parse page token"
            );
        }
        page = Max(page, DefaultPage);
        const ui64 pageSize = Min(request.GetPageSize() ? Max(request.GetPageSize(), MinPageSize) : DefaultPageSize, MaxPageSize);

        auto it = Self->ForcedCompactionsByTime.end();
        ui64 skip = (page - 1) * pageSize;
        while ((it != Self->ForcedCompactionsByTime.begin()) && skip) {
            --it;
            auto info = Self->ForcedCompactions.at(it->second);
            if (info->SubdomainPathId == subdomainPathId) {
                --skip;
            }
        }

        ui64 size = 0;
        while ((it != Self->ForcedCompactionsByTime.begin()) && size < pageSize) {
            --it;
            auto info = Self->ForcedCompactions.at(it->second);
            if (info->SubdomainPathId == subdomainPathId) {
                Self->FromForcedCompactionInfo(*response->Record.MutableEntries()->Add(), *info);
                ++size;
            }
        }

        if (it == Self->ForcedCompactionsByTime.begin()) {
            response->Record.SetNextPageToken("0");
        } else {
            response->Record.SetNextPageToken(ToString(page + 1));
        }

        Reply(std::move(response));

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxList DoComplete");
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    void Reply(
        THolder<TEvForcedCompaction::TEvListResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString())
    {
        auto& record = response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            auto& issue = *record.MutableIssues()->Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(errorMessage);

        }

        SideEffects.Send(Request->Sender, std::move(response), 0, Request->Cookie);
    }

private:
    TSideEffects SideEffects;
    TEvForcedCompaction::TEvListRequest::TPtr Request;
};

ITransaction* TSchemeShard::CreateTxListForcedCompaction(TEvForcedCompaction::TEvListRequest::TPtr& ev) {
    return new TForcedCompaction::TTxList(this, ev);
}

} // namespace NKikimr::NSchemeShard
