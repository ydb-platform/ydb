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
                FillSetColumnConstraint(*respRecord.MutableEntries()->Add(), operationInfo);
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

private:
    void FillSetColumnConstraint(
        NKikimrSetColumnConstraint::TSetColumnConstraint& proto,
        const TSetColumnConstraintOperationInfo& operationInfo)
    {
        proto.SetId(ui64(operationInfo.Id));

        if (operationInfo.UserSID) {
            proto.SetUserSID(*operationInfo.UserSID);
        }
        if (operationInfo.StartTime != TInstant::Zero()) {
            *proto.MutableStartTime() = SecondsToProtoTimeStamp(operationInfo.StartTime.Seconds());
        }
        if (operationInfo.EndTime != TInstant::Zero()) {
            *proto.MutableEndTime() = SecondsToProtoTimeStamp(operationInfo.EndTime.Seconds());
        }

        // Map internal state to proto state
        using EState = TSetColumnConstraintOperationInfo::EOperationState;
        using ProtoState = Ydb::Table::SetColumnConstraintState;
        switch (operationInfo.OperationState) {
        case EState::Locking:
        case EState::LockingNullWrites:
            proto.SetState(ProtoState::STATE_PREPARING);
            proto.SetProgress(0.0f);
            break;
        case EState::Validating:
            proto.SetState(ProtoState::STATE_VALIDATING);
            proto.SetProgress(CalcValidationProgress(operationInfo));
            break;
        case EState::Finishing:
        case EState::Unlocking:
            proto.SetState(ProtoState::STATE_APPLYING);
            proto.SetProgress(99.9f);
            break;
        case EState::Done:
            proto.SetState(operationInfo.ValidationFailed
                ? ProtoState::STATE_CANCELLED
                : ProtoState::STATE_DONE);
            proto.SetProgress(100.0f);
            break;
        case EState::Invalid:
            proto.SetState(ProtoState::STATE_UNSPECIFIED);
            break;
        }

        auto* settings = proto.MutableSettings();
        TPath tablePath = TPath::Init(operationInfo.TablePathId, Self);
        settings->SetTablePath(tablePath.PathString());
        for (const auto& col : operationInfo.SetNotNullColumns) {
            settings->AddNotNullColumns(TString(col));
        }
    }

    static float CalcValidationProgress(const TSetColumnConstraintOperationInfo& operationInfo) {
        const ui64 total = operationInfo.ValidationShards.size();
        if (total == 0) {
            return 0.0f;
        }
        const ui64 done = operationInfo.DoneValidationShards.size();
        return static_cast<float>(done) / static_cast<float>(total) * 100.0f;
    }
};

ITransaction* TSchemeShard::CreateTxListSetColumnConstraint(TEvSetColumnConstraint::TEvListRequest::TPtr& ev) {
    return new TIndexBuilder::TTxListSetColumnConstraint(this, ev);
}

} // namespace NKikimr::NSchemeShard
