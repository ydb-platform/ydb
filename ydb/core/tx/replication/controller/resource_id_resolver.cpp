#include "logging.h"
#include "private_events.h"
#include "resource_id_resolver.h"
#include "util.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {

class TResourceIdResolver: public TActorBootstrapped<TResourceIdResolver> {
    void DescribeDatabase() {
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(Database, {}));
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe succeeded"
                << ": path# " << Database);

            for (const auto& [k, v] : result.GetTableDescription().GetAttributes()) {
                if (k == "cloud_id") {
                    return Reply(TString{v});
                }
            }

            Reply(false, "Cannot resolve RESOURCE_ID");
        } else {
            LOG_E("Describe failed"
                << ": path# " << Database
                << ", status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString()
                << ", iteration# " << Backoff.GetIteration());

            if (IsRetryableError(result) && Backoff.HasMore()) {
                Schedule(Backoff.Next(), new TEvents::TEvWakeup);
            } else {
                Reply(false, result.GetIssues().ToOneLineString());
            }
        }
    }

    template <typename... Args>
    void Reply(Args&&... args) {
        Send(Parent, new TEvPrivate::TEvResolveResourceIdResult(ReplicationId, std::forward<Args>(args)...));
        PassAway();
    }

    void PassAway() override {
        Send(YdbProxy, new TEvents::TEvPoison());
        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_RESOURCE_ID_RESOLVER;
    }

    explicit TResourceIdResolver(
            const TActorId& parent,
            ui64 rid,
            const TString& endpoint,
            const TString& database,
            bool ssl,
            const TString& caCert,
            const TString& token)
        : Parent(parent)
        , ReplicationId(rid)
        , Endpoint(endpoint)
        , Database(database)
        , Ssl(ssl)
        , CaCert(caCert)
        , Token(token)
        , LogPrefix("ResourceIdResolver", ReplicationId)
        , Backoff(5)
    {
    }

    void Bootstrap() {
        YdbProxy = Register(CreateYdbProxy(Endpoint, Database, Ssl, CaCert, Token));
        DescribeDatabase();
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            sFunc(TEvents::TEvWakeup, DescribeDatabase);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const ui64 ReplicationId;
    const TString Endpoint;
    const TString Database;
    const bool Ssl;
    const TString CaCert;
    const TString Token;
    const TActorLogPrefix LogPrefix;
    TActorId YdbProxy;
    TBackoff Backoff;

}; // ResourceIdResolver

IActor* CreateResourceIdResolver(const TActorId& parent, ui64 rid,
    const TString& endpoint, const TString& database, bool ssl, const TString& caCert, const TString& token)
{
    return new TResourceIdResolver(parent, rid, endpoint, database, ssl, caCert, token);
}

}
