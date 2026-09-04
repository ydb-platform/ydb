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
        FillSetColumnConstraint(*proto, operationInfo, Self);

        // Add issue for cancelled operations
        if (operationInfo.IsCancelled) {
            auto* issue = respRecord.AddIssues();
            issue->set_message(operationInfo.CancellationReason);
            issue->set_issue_code(0);
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
        }

        if (operationInfo.ValidationFailed) {
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
};

ITransaction* TSchemeShard::CreateTxGetSetColumnConstraint(TEvSetColumnConstraint::TEvGetRequest::TPtr& ev) {
    return new TIndexBuilder::TTxGetSetColumnConstraint(this, ev);
}

} // NKikimr::NSchemeShard
