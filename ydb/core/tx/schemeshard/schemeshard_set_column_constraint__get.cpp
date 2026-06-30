#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/index/build_index_helpers.h>
#include <ydb/core/tx/schemeshard/index/build_index_tx_base.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_set_column_constraint.h>


namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxGetSetColumnConstraint
    : public TSchemeShard::TIndexBuilder::TTxSimple<TEvSetColumnConstraint::TEvGetRequest, TEvSetColumnConstraint::TEvGetResponse>
{
public:
    explicit TTxGetSetColumnConstraint(TSelf* self, TEvSetColumnConstraint::TEvGetRequest::TPtr& ev)
        : TTxSimple(self, TIndexBuildId(ev->Get()->Record.GetOperationId()), ev, TXTYPE_GET_SET_COLUMN_CONSTRAINT, false)
    {}

    bool DoExecute(TTransactionContext&, const TActorContext&) override {
        const auto& record = Request->Get()->Record;
        LOG_D("TTxGetSetColumnConstraint::DoExecute " << record.ShortDebugString());

        Response = MakeHolder<TEvSetColumnConstraint::TEvGetResponse>();

        TPath database = TPath::Resolve(record.GetDatabaseName(), Self);
        if (!database.IsResolved()) {
            return Reply(
                Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Database <" << record.GetDatabaseName() << "> not found"
            );
        }
        const TPathId domainPathId = database.GetPathIdForDomain();

        const auto* opPtr = Self->SetColumnConstraintOperations.FindPtr(BuildId);
        if (!opPtr) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId << "> not found"
            );
        }
        const auto& operationInfo = **opPtr;

        if (operationInfo.DomainPathId != domainPathId) {
            return Reply(
                Ydb::StatusIds::NOT_FOUND,
                TStringBuilder() << "SetColumnConstraint operation with id <" << BuildId
                    << "> not found in database <" << record.GetDatabaseName() << ">"
            );
        }

        auto& respRecord = Response->Record;
        respRecord.SetStatus(Ydb::StatusIds::SUCCESS);

        auto* proto = respRecord.MutableSetColumnConstraint();
        FillSetColumnConstraint(*proto, operationInfo);

        if (operationInfo.ValidationFailed && operationInfo.OperationState == TSetColumnConstraintOperationInfo::EOperationState::Done) {
            TPath tablePath = TPath::Init(operationInfo.TablePathId, Self);
            auto* issue = respRecord.AddIssues();
            issue->set_message(TStringBuilder()
                << "Validation failed for SET NOT NULL on table `" << tablePath.PathString()
                << "`: one or more columns contain NULL values");
            issue->set_issue_code(0);
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
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

ITransaction* TSchemeShard::CreateTxGetSetColumnConstraint(TEvSetColumnConstraint::TEvGetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxGetSetColumnConstraint(this, ev);
}

} // NKikimr::NSchemeShard
