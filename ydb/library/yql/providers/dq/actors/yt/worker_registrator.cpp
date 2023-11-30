#include "worker_registrator.h"
#include "yt_wrapper.h"

#include <ydb/library/yql/providers/yt/lib/log/yt_logger.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/common/attrs.h>

#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/utility.h>

namespace NYql {

using namespace NActors;

class TWorkerRegistrator: public TActor<TWorkerRegistrator> {
public:
    static constexpr char ActorName[] = "WORKER_REGISTRATOR";

    TWorkerRegistrator(TActorId ytWrapper, const TWorkerRegistratorOptions& options)
        : TActor(&TWorkerRegistrator::Handler)
        , YtWrapper(ytWrapper)
        , Options(options)
    { }

    STRICT_STFUNC(Handler, {
        HFunc(TEvSetNodeResponse, OnSetNodeResponse)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    });

private:
    TEvSetNode* SetNodeCommand(TInstant now)
    {
        auto value = NYT::NYson::TYsonString(NYT::NodeToYsonString(NYT::TNode(now.ToString())));
        return new TEvSetNode(Options.Prefix + "/" + Options.NodeName + "/@" + NCommonAttrs::PINGTIME_ATTR, value, NYT::NApi::TSetNodeOptions());
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        Y_UNUSED(parentId);
        return new IEventHandle(YtWrapper, self, SetNodeCommand(TInstant::Now()), 0);
    }

    void OnSetNodeResponse(TEvSetNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());

        if (!result.IsOK()) {
            YQL_CLOG(ERROR, ProviderDq) << "Error on list node " << ToString(result);
        }

        if (!result.IsOK() && result.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
            NYql::FlushYtDebugLog();
            _exit(1);
        }

        YQL_CLOG(DEBUG, ProviderDq) << "OnSetNodeResponse";

        TActivationContext::Schedule(Options.PingPeriod, new IEventHandle(YtWrapper, SelfId(), SetNodeCommand(TInstant::Now()), 0));
    }

    TActorId YtWrapper;
    TWorkerRegistratorOptions Options;
};

IActor* CreateWorkerRegistrator(NActors::TActorId ytWrapper, const TWorkerRegistratorOptions& options) {
    Y_ABORT_UNLESS(!options.Prefix.empty());
    Y_ABORT_UNLESS(!options.NodeName.empty());
    return new TWorkerRegistrator(ytWrapper, options);
}

} // namespace NYql
