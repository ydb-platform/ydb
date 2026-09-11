#include "reconcile_actor.h"

#include "private_events.h"

#include <ydb/services/udf_store/table_query.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/services/metadata/request/common.h>
#include <ydb/services/metadata/request/request_actor_cb.h>

namespace NKikimr::NUdfStore {

namespace {

class TReconcileActor : public NActors::TActorBootstrapped<TReconcileActor> {
public:
    TReconcileActor(
        const NActors::TActorId& replyTo,
        const TString& cpuSpec,
        const TString& artifactTablePath)
        : ReplyTo_(replyTo)
        , CpuSpec_(cpuSpec)
        , ArtifactTablePath_(artifactTablePath)
    {}

    void Bootstrap() {
        Become(&TReconcileActor::StateMain);

        auto request = NMetadata::NRequest::TDialogYQLRequest::TRequest();
        request.mutable_query()->set_yql_text(
            NTableQuery::BuildSelectArtifactKeysQuery(ArtifactTablePath_));
        request.mutable_query_cache_policy()->set_keep_in_cache(true);
        request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();

        auto controller = std::make_shared<
            NMetadata::NRequest::TNaiveExternalController<NMetadata::NRequest::TDialogYQLRequest>>(SelfId());
        NMetadata::NRequest::TYQLRequestExecutor::Execute(
            std::move(request), NACLib::TUserToken("metadata@system", {}), controller);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>, HandleResult);
            hFunc(NMetadata::NRequest::TEvRequestFailed, HandleFailed);
            default:
                break;
        }
    }

private:
    void HandleResult(
        NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>::TPtr& ev)
    {
        TVector<NTableQuery::TArtifactKeyRow> rows;
        if (!NTableQuery::ParseArtifactKeysResponse(ev->Get()->GetResult(), rows)) {
            Reply(false, {});
            return;
        }

        THashSet<TString> keys;
        keys.reserve(rows.size());
        for (const auto& row : rows) {
            keys.insert(MakeArtifactKey(row.Id, row.Kind, row.Uid));
        }
        Reply(true, std::move(keys));
    }

    void HandleFailed(NMetadata::NRequest::TEvRequestFailed::TPtr&) {
        // The artifact table of a platform appears lazily, when the first node
        // of that platform initializes it. Until then the read fails and the
        // controller simply keeps the platform's gaps unresolved.
        ALS_DEBUG(NKikimrServices::METADATA_PROVIDER)
            << "TReconcileActor: failed to read " << ArtifactTablePath_;
        Reply(false, {});
    }

    void Reply(bool success, THashSet<TString>&& keys) {
        auto response = std::make_unique<TEvControllerPrivate::TEvReconcileResult>();
        response->CpuSpec = CpuSpec_;
        response->Success = success;
        response->ArtifactKeys = std::move(keys);
        Send(ReplyTo_, response.release());
        PassAway();
    }

    const NActors::TActorId ReplyTo_;
    const TString CpuSpec_;
    const TString ArtifactTablePath_;
};

} // namespace

NActors::IActor* CreateReconcileActor(
    const NActors::TActorId& replyTo,
    const TString& cpuSpec,
    const TString& artifactTablePath)
{
    return new TReconcileActor(replyTo, cpuSpec, artifactTablePath);
}

} // namespace NKikimr::NUdfStore
