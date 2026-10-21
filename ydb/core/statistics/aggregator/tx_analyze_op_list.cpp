#include "aggregator_impl.h"

#include <algorithm>
#include <limits>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeOpList : public TTxBase {
    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;

    TEvStatistics::TEvAnalyzeOpListRequest::TPtr Request;

    TTxAnalyzeOpList(TSelf* self, TEvStatistics::TEvAnalyzeOpListRequest::TPtr ev)
        : TTxBase(self)
        , Request(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_OP_LIST; }

    bool Execute(TTransactionContext&, const TActorContext&) override {
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        const auto& record = Request->Get()->Record;
        YDB_LOG_NOTICE("[AnalyzeOp] TTxAnalyzeOpList::Complete",
            {"tabletId", Self->TabletID()},
            {"dbName", record.GetDatabaseName()});

        auto response = MakeHolder<TEvStatistics::TEvAnalyzeOpListResponse>();
        auto& rec = response->Record;

        const TString& dbName = record.GetDatabaseName();
        if (dbName.empty()) {
            rec.SetStatus(Ydb::StatusIds::BAD_REQUEST);
            auto& issue = *rec.AddIssues();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message("DatabaseName must not be empty");
            ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
            return;
        }

        ui64 page = DefaultPage;
        if (record.GetPageToken() && !TryFromString(record.GetPageToken(), page)) {
            rec.SetStatus(Ydb::StatusIds::BAD_REQUEST);
            auto& issue = *rec.AddIssues();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message("Unable to parse page token");
            ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
            return;
        }
        page = Max(page, DefaultPage);
        const ui64 pageSize = Min(
            record.GetPageSize() ? Max(record.GetPageSize(), MinPageSize) : DefaultPageSize,
            MaxPageSize);
        const ui64 maxPageMinusOne = std::numeric_limits<ui64>::max() / pageSize;
        if (page - 1 > maxPageMinusOne) {
            page = maxPageMinusOne + 1;
        }

        // Collect ops for the requested database, newest first (ForceTraversals is insertion-ordered)
        std::vector<const TForceTraversalOperation*> matching;
        for (const auto& op : Self->ForceTraversals) {
            if (op.DatabaseName == dbName) {
                matching.push_back(&op);
            }
        }

        // Reverse to show newest first
        std::reverse(matching.begin(), matching.end());

        ui64 skip = (page - 1) * pageSize;
        ui64 size = 0;
        bool hasMore = false;

        for (size_t i = 0; i < matching.size(); ++i) {
            if (skip > 0) {
                --skip;
                continue;
            }
            if (size >= pageSize) {
                hasMore = true;
                break;
            }
            Self->FillAnalyzeOperationProto(*matching[i], *rec.AddEntries());
            ++size;
        }

        rec.SetStatus(Ydb::StatusIds::SUCCESS);
        rec.SetNextPageToken(hasMore ? ToString(page + 1) : TString());
        ctx.Send(Request->Sender, response.Release(), 0, Request->Cookie);
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeOpListRequest::TPtr& ev) {
    if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
        SendAnalyzeLongRunningOpDisabled<TEvStatistics::TEvAnalyzeOpListResponse>(ev->Sender, ev->Cookie);
        return;
    }
    Execute(new TTxAnalyzeOpList(this, std::move(ev)), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
