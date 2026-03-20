#include "schemeshard_build_index.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_set_column_constraint.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgressSetColumnConstraint
    : public TSchemeShard::TIndexBuilder::TTxBase
{
public:
    explicit TTxProgressSetColumnConstraint(TSelf* self, TIndexBuildId buildId)
        : TTxBase(self, buildId, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
    {}

    bool DoExecute(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        LOG_D("TTxProgressSetColumnConstraint::DoExecute, id# " << BuildId);

        auto it = Self->SetColumnConstraintOperations.find(BuildId);
        if (it == Self->SetColumnConstraintOperations.end()) {
            LOG_W("TTxProgressSetColumnConstraint::DoExecute"
                ": SetColumnConstraintOperation not found"
                ", id# " << BuildId);
            return true;
        }

        const auto& buildInfo = *it->second;

        // TODO: implement progress logic
        auto response = MakeHolder<TEvSetColumnConstraint::TEvCreateResponse>(ui64(BuildId));
        response->Record.SetStatus(Ydb::StatusIds::UNSUPPORTED);
        AddIssue(response->Record.MutableIssues(),
            "SetColumnConstraint operation is not yet implemented");

        LOG_N("TTxProgressSetColumnConstraint::DoExecute: replying UNSUPPORTED"
            << ", id# " << BuildId
            << ", replyTo# " << buildInfo.CreateSender.ToString());

        Send(buildInfo.CreateSender, std::move(response), 0, buildInfo.SenderCookie);

        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* buildInfo, const std::exception& exc) override
    {
        if (!buildInfo) {
            LOG_N("TTxProgressSetColumnConstraint: OnUnhandledException: id not found"
                ", id# " << BuildId);
            return;
        }
        LOG_E("TTxProgressSetColumnConstraint: OnUnhandledException"
            ", id# " << BuildId
            << ", exception: " << exc.what());
    }
};

ITransaction* TSchemeShard::CreateTxSetColumnConstraintProgress(TIndexBuildId id) {
    return new TIndexBuilder::TTxProgressSetColumnConstraint(this, id);
}

} // namespace NSchemeShard
} // namespace NKikimr
