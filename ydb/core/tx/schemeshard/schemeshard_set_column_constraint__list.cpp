#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxListSetColumnConstraint
    : public TSchemeShard::TIndexBuilder::TTxSimple<TEvSetColumnConstraint::TEvListRequest, TEvSetColumnConstraint::TEvListResponse>
{
private:
    static constexpr ui64 DefaultPageSize = 10;
    static constexpr ui64 MinPageSize = 1;
    static constexpr ui64 MaxPageSize = 100;
    static constexpr ui64 DefaultPage = 1;

public:
    explicit TTxListSetColumnConstraint(TSelf* self, TEvSetColumnConstraint::TEvListRequest::TPtr& ev)
        : TTxSimple(self, InvalidIndexBuildId, ev, TXTYPE_LIST_SET_COLUMN_CONSTRAINT, false)
    {}

    bool DoExecute(TTransactionContext&, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_D("TTxListSetColumnConstraint::DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvSetColumnConstraint::TEvListResponse>();

        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
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

        auto it = Self->SetColumnConstraintOperationsByTime.end();
        ui64 skip = (page - 1) * pageSize;
        while ((it != Self->SetColumnConstraintOperationsByTime.begin()) && skip) {
            --it;
            const auto& operationInfo = *Self->SetColumnConstraintOperations.at(it->second);
            if (operationInfo.DomainPathId == domainPathId) {
                --skip;
            }
        }

        auto& respRecord = Response->Record;
        respRecord.SetStatus(Ydb::StatusIds::SUCCESS);

        ui64 size = 0;
        while ((it != Self->SetColumnConstraintOperationsByTime.begin()) && size < pageSize) {
            --it;
            const auto& operationInfo = *Self->SetColumnConstraintOperations.at(it->second);
            if (operationInfo.DomainPathId == domainPathId) {
                FillSetColumnConstraint(*respRecord.MutableEntries()->Add(), operationInfo, Self);
                ++size;
            }
        }

        if (it == Self->SetColumnConstraintOperationsByTime.begin()) {
            respRecord.SetNextPageToken("0");
        } else {
            respRecord.SetNextPageToken(ToString(page + 1));
        }

        return Reply();
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxListSetColumnConstraint(TEvSetColumnConstraint::TEvListRequest::TPtr& ev) {
    return new TIndexBuilder::TTxListSetColumnConstraint(this, ev);
}

} // namespace NKikimr::NSchemeShard
