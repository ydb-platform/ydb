#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_index_utils.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/core/base/table_index.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <yql/essentials/public/issue/yql_issue_message.h>


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
        LOG_D("TTxProgressSetColumnConstraint::DoExecute"
            << ", id# " << BuildId);

        auto it = Self->SetColumnConstraintOperations.find(BuildId);
        if (it == Self->SetColumnConstraintOperations.end()) {
            LOG_W("TTxProgressSetColumnConstraint::DoExecute"
                << ": SetColumnConstraintOperation not found"
                << ", id# " << BuildId);
            return true;
        }

        // TODO: implement progress logic
        return true;
    }

    void DoComplete(const TActorContext& /*ctx*/) override {
    }

    void OnUnhandledException(TTransactionContext& /*txc*/, const TActorContext& /*ctx*/,
        TIndexBuildInfo* buildInfo, const std::exception& exc) override
    {
        if (!buildInfo) {
            LOG_N("TTxProgressSetColumnConstraint: OnUnhandledException: id not found"
                << ", id# " << BuildId);
            return;
        }
        LOG_E("TTxProgressSetColumnConstraint: OnUnhandledException"
            << ", id# " << BuildId
            << ", exception: " << exc.what());
    }
};

ITransaction* TSchemeShard::CreateTxSetColumnConstraintProgress(TIndexBuildId id) {
    return new TIndexBuilder::TTxProgressSetColumnConstraint(this, id);
}

} // namespace NSchemeShard
} // namespace NKikimr
