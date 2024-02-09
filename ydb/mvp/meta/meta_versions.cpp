#include <ydb/mvp/core/core_ydb_impl.h>
#include "meta_versions.h"

namespace NMVP {

using namespace NKikimr;

//
// Schema for ydb-versions cache
//
// CREATE TABLE `/Root/ydb/MasterClusterVersions.db` (
//     version_str Utf8 NOT NULL,
//     color_class Uint32,
//     PRIMARY KEY (version_str)
// )
//

class TVersionListLoadActor : THandlerActorYdbMeta, THandlerActorYdb, public NActors::TActorBootstrapped<TVersionListLoadActor> {
public:
    NActors::TActorId Owner;
    std::shared_ptr<NYdb::NTable::TTableClient> Client;
    const TString RootDomain;

    TVersionListLoadActor(const NActors::TActorId& owner, std::shared_ptr<NYdb::NTable::TTableClient> client, const TString rootDomain)
        : Owner(owner)
        , Client(client)
        , RootDomain(rootDomain)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;

        Client->CreateSession().Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
            NYdb::NTable::TAsyncCreateSessionResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
        });

        Become(&TVersionListLoadActor::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            LOG_DEBUG_S(ctx, EService::MVP, "MetaVersions: got session, making query");

            NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
            NActors::TActorId actorId = ctx.SelfID;
            TString query = TStringBuilder()
                    << "SELECT version_str, color_class FROM `" << RootDomain << "/ydb/MasterClusterVersions.db`"
                    ;
            auto session = result.GetSession();
            session.ExecuteDataQuery(
                query,
                NYdb::NTable::TTxControl::BeginTx(
                    NYdb::NTable::TTxSettings::OnlineRO(
                        NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true)
                    )
                ).CommitTx()
            ).Subscribe([actorSystem, actorId, session](const NYdb::NTable::TAsyncDataQueryResult& result) {
                NYdb::NTable::TAsyncDataQueryResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(res.ExtractValue()));
            });
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "MetaVersions: failed to get session: " << static_cast<NYdb::TStatus>(result));

            ctx.Send(Owner, new TEvPrivateMeta::TEvVersionListLoadResult());
            Die(ctx);
        }
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        TVersionInfoCachePtr versionInfoCache;
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        if (result.IsSuccess()) {
            LOG_DEBUG_S(ctx, EService::MVP, "MetaVersions: got version info data");

            versionInfoCache = std::make_shared<TVersionInfoCache>();
            auto resultSet = result.GetResultSet(0);
            NYdb::TResultSetParser parser(resultSet);
            while (parser.TryNextRow()) {
                // Almost no type checking: assume here strict conformance to the schema.
                // Records with empty color_class are skipped as invalid.
                TMaybe<ui32> color_class = parser.ColumnParser("color_class").GetOptionalUint32();
                if (!color_class.Empty()) {
                    versionInfoCache->emplace_back(
                        parser.ColumnParser("version_str").GetUtf8(),
                        *color_class
                    );
                }
            }
        } else {
            LOG_DEBUG_S(ctx, EService::MVP, "MetaVersions: failed to get version info data");
        }
        ctx.Send(Owner, new TEvPrivateMeta::TEvVersionListLoadResult(std::move(versionInfoCache)));
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        LOG_ERROR_S(ctx, EService::MVP, "MetaVersions: timeout");

        ctx.Send(Owner, new TEvPrivateMeta::TEvVersionListLoadResult());
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvDataQueryResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

NActors::TActorId CreateLoadVersionsActor(const NActors::TActorId& owner, std::shared_ptr<NYdb::NTable::TTableClient> client, const TString rootDomain, const NActors::TActorContext& ctx) {
    return ctx.Register(new TVersionListLoadActor(owner, client, rootDomain));
}

} // namespace NMVP
