#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxList: public TSchemeShard::TIndexBuilder::TTxSimple<TEvIndexBuilder::TEvListRequest, TEvIndexBuilder::TEvListResponse> {
private:
    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;
public:
    explicit TTxList(TSelf* self, TEvIndexBuilder::TEvListRequest::TPtr& ev)
        : TTxSimple(self, ev, TXTYPE_LIST_INDEX_BUILD, false)
    {}

    bool DoExecute(TTransactionContext&, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_D("DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvIndexBuilder::TEvListResponse>();
        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        ui64 page = DefaultPage;
        if (record.GetPageToken() && !TryFromString(record.GetPageToken(), page)) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Unable to parse page token"
            );
        }
        page = Max(page, DefaultPage);
        const ui64 pageSize = Min(record.GetPageSize() ? Max(record.GetPageSize(), MinPageSize) : DefaultPageSize, MaxPageSize);


        auto it = Self->IndexBuilds.begin();
        ui64 skip = (page - 1) * pageSize;
        while ((it != Self->IndexBuilds.end()) && skip) {
            if (it->second->DomainPathId == domainPathId) {
                --skip;
            }
            ++it;
        }

        auto& respRecord = Response->Record;
        respRecord.SetStatus(Ydb::StatusIds::SUCCESS);

        ui64 size = 0;
        while ((it != Self->IndexBuilds.end()) && size < pageSize) {
            if (it->second->DomainPathId == domainPathId) {
                Fill(*respRecord.MutableEntries()->Add(), *it->second);
                ++size;
            }
            ++it;
        }

        if (it == Self->IndexBuilds.end()) {
            respRecord.SetNextPageToken("0");
        } else {
            respRecord.SetNextPageToken(ToString(page + 1));
        }

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxList(TEvIndexBuilder::TEvListRequest::TPtr& ev) {
    return new TIndexBuilder::TTxList(this, ev);
}

} // NKikimr::NSchemeShard
