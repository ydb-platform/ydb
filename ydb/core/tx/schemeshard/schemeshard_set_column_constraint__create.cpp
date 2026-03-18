#include "schemeshard_build_index.h"
#include "schemeshard_set_column_constraint.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

class TSchemeShard::TIndexBuilder::TTxCreateSetColumnConstraint: public TSchemeShard::TIndexBuilder::TTxSimple<TEvSetColumnConstraint::TEvCreateRequest, TEvSetColumnConstraint::TEvCreateResponse> {
public:
    explicit TTxCreateSetColumnConstraint(TSelf* self, TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev)
        : TTxSimple(self, TIndexBuildId(ev->Get()->Record.GetTxId()), ev, TXTYPE_CREATE_SET_COLUMN_CONSTRAINT)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& request = Request->Get()->Record;
        const auto& settings = request.GetSettings();
        LOG_N("DoExecute " << request.ShortDebugString());

        Response = MakeHolder<TEvSetColumnConstraint::TEvCreateResponse>(request.GetTxId());

        const auto domainPath = TPath::Resolve(request.GetDatabaseName(), Self);
        {
            const auto checks = domainPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsSubDomain()
                .NotUnderDomainUpgrade();

            if (!checks) {
                return Reply(checks.GetStatus(), checks.GetError());
            }
        }

        auto subDomainPathId = domainPath.GetPathIdForDomain();
        auto subDomainInfo = domainPath.DomainInfo();
        const bool quotaAcquired = subDomainInfo->TryConsumeSchemeQuota(ctx.Now());

        if (!quotaAcquired) {
            return Reply(Ydb::StatusIds::OVERLOADED,
                "Request exceeded a limit on the number of schema operations, try again later.");
        }

        NIceDb::TNiceDb db(txc.DB);
        // We need to persist updated/consumed quotas even if operation fails for other reasons
        Self->PersistSubDomainSchemeQuotas(db, subDomainPathId, *subDomainInfo);

        const auto tablePath = TPath::Resolve(settings.GetTablePath(), Self);
        {
            const auto checks = tablePath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTable()
                .NotAsyncReplicaTable()
                .IsCommonSensePath()
                .IsTheSameDomain(domainPath);

            if (!checks) {
                return Reply(checks.GetStatus(), checks.GetError());
            }
        }

        if (tablePath.Parent()->IsTableIndex()) {
            return Reply(NKikimrScheme::StatusPreconditionFailed, "Cannot set constraint on index");
        }

        return Reply(NKikimrScheme::StatusPreconditionFailed, "Create set constraint not implemented");
        // return true;
    }

    void DoComplete(const TActorContext&) override {}
};

ITransaction* TSchemeShard::CreateTxCreateSetColumnConstraint(TEvSetColumnConstraint::TEvCreateRequest::TPtr& ev) {
    return new TIndexBuilder::TTxCreateSetColumnConstraint(this, ev);
}

}
