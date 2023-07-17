#pragma once

#include "logging.h"
#include "nodes_manager.h"
#include "private_events.h"
#include "public_events.h"
#include "replication.h"
#include "schema.h"
#include "sys_params.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/defs.h>
#include <ydb/core/protos/counters_replication.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/hash.h>

namespace NKikimr::NReplication::NController {

class TController
    : public TActor<TController>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    class TTxBase: public NTabletFlatExecutor::TTransactionBase<TController> {
    public:
        TTxBase(const TString& name, TController* self)
            : TTransactionBase(self)
            , LogPrefix(self, name)
        {
        }

    protected:
        const TTabletLogPrefix LogPrefix;
    };

private:
    using Schema = TControllerSchema;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_ACTOR;
    }

    explicit TController(const TActorId& tablet, TTabletStorageInfo* info);

private:
    // tablet overrides
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    // state functions
    STFUNC(StateInit);
    STFUNC(StateWork);

    void Cleanup(const TActorContext& ctx);
    void SwitchToWork(const TActorContext& ctx);
    void Reset();

    // handlers
    void Handle(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvUpdateTenantNodes::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvDiscovery::TEvError::TPtr& ev, const TActorContext& ctx);

    // local transactions
    class TTxInitSchema;
    class TTxInit;
    class TTxCreateReplication;
    class TTxDropReplication;
    class TTxDiscoveryTargetsResult;
    class TTxAssignStreamName;
    class TTxCreateStreamResult;
    class TTxDropStreamResult;
    class TTxCreateDstResult;
    class TTxDropDstResult;

    // tx runners
    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);
    void RunTxCreateReplication(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDropReplication(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDropReplication(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx);
    void RunTxDiscoveryTargetsResult(TEvPrivate::TEvDiscoveryTargetsResult::TPtr& ev, const TActorContext& ctx);
    void RunTxAssignStreamName(TEvPrivate::TEvAssignStreamName::TPtr& ev, const TActorContext& ctx);
    void RunTxCreateStreamResult(TEvPrivate::TEvCreateStreamResult::TPtr& ev, const TActorContext& ctx);
    void RunTxDropStreamResult(TEvPrivate::TEvDropStreamResult::TPtr& ev, const TActorContext& ctx);
    void RunTxCreateDstResult(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx);
    void RunTxDropDstResult(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx);

    // other
    template <typename T>
    TReplication::TPtr Add(ui64 id, const TPathId& pathId, T&& config) {
        auto replication = MakeIntrusive<TReplication>(id, pathId, std::forward<T>(config));
        {
            const auto res = Replications.emplace(id, replication);
            Y_VERIFY_S(res.second, "Duplication replication: " << id);
        }
        {
            const auto res = ReplicationsByPathId.emplace(pathId, replication);
            Y_VERIFY_S(res.second, "Duplication replication: " << pathId);
        }

        return replication;
    }

    TReplication::TPtr Find(ui64 id);
    TReplication::TPtr Find(const TPathId& pathId);
    void Remove(ui64 id);

private:
    const TTabletLogPrefix LogPrefix;

    TSysParams SysParams;
    THashMap<ui64, TReplication::TPtr> Replications;
    THashMap<TPathId, TReplication::TPtr> ReplicationsByPathId;

    // discovery
    TActorId DiscoveryCache;
    TNodesManager NodesManager;

}; // TController

}
