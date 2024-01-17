#include "nodeid_cleaner.h"
#include "yt_wrapper.h"

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/common/attrs.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

using namespace NActors;

class TNodeIdCleaner: public TActor<TNodeIdCleaner> {
public:
    static constexpr char ActorName[] = "CLEANER";

    TNodeIdCleaner(TActorId ytWrapper, const TNodeIdCleanerOptions& options)
        : TActor(&TNodeIdCleaner::Handler)
        , YtWrapper(ytWrapper)
        , Options(options)
        , LastUpdateTime(TInstant::Now())
    { }

    STRICT_STFUNC(Handler, {
        HFunc(TEvListNodeResponse, OnListNodeResponse)
        HFunc(TEvRemoveNodeResponse, OnRemoveNodeResponse)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    });

private:
    TEvListNode* ListNodeCommand() {
        NYT::NApi::TListNodeOptions options;
        options.Attributes = {NCommonAttrs::ACTOR_NODEID_ATTR, "modification_time"};
        return new TEvListNode(Options.Prefix, options);
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        // TODO: TakeLock
        Y_UNUSED(parentId);
        return new IEventHandle(YtWrapper, self, ListNodeCommand(), 0);
    }

    void MaybeReschedule(TInstant now) {
        Y_UNUSED(now);
        if (WaitCount == 0) {
            TActivationContext::Schedule(Options.CheckPeriod, new IEventHandle(YtWrapper, SelfId(), ListNodeCommand(), 0));
        }
    }

    void OnRemoveNodeResponse(TEvRemoveNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (!result.IsOK()) {
            YQL_CLOG(WARN, ProviderDq) << "Remove error " << ToString(result);
        }

        --WaitCount;
        MaybeReschedule(TInstant::Now());
    }

    void ScheduleRemove(TInstant now, const TVector<NYT::TNode>& nodes, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        Y_ABORT_UNLESS(WaitCount == 0);
        WaitCount = 0;
        for (const auto& node : nodes) {

            const auto& attributes = node.GetAttributes().AsMap();
            auto maybeNodeId = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);
            if (maybeNodeId == attributes.end()) {
                continue;
            }

            auto maybeModificationTime = attributes.find("modification_time");
            YQL_ENSURE(maybeModificationTime != attributes.end());

            auto modificationTime = TInstant::ParseIso8601(maybeModificationTime->second.AsString());

            if (now - modificationTime > Options.Timeout) {
                WaitCount ++;
                auto removePath = Options.Prefix + "/" + node.AsString();
                YQL_CLOG(DEBUG, ProviderDq) << "Removing node " << removePath;
                Send(YtWrapper, new TEvRemoveNode(removePath, NYT::NApi::TRemoveNodeOptions()));
            }
        }
    }

    void OnListNodeResponse(TEvListNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto now = TInstant::Now();
        auto result = std::get<0>(*ev->Get());
        auto isOk = result.IsOK();
        auto schedule = true;

        try {
            if (LastListNodeOk) {
                // remove only after 2't attempt to minimize Y_ABORT_UNLESS chance on registrator
                ScheduleRemove(now, NYT::NodeFromYsonString(result.ValueOrThrow()).AsList(), ctx);
                MaybeReschedule(now);
                schedule = false;
            }
        } catch (...) {
            WaitCount = 0;
            isOk = false;
            YQL_CLOG(ERROR, ProviderDq) << "Error on list node " << CurrentExceptionMessage();
        }

        if (schedule) {
            TActivationContext::Schedule(Options.RetryPeriod, new IEventHandle(YtWrapper, SelfId(), ListNodeCommand(), 0));
        }

        LastListNodeOk = isOk;
        LastUpdateTime = now;
    }

    TActorId YtWrapper;
    TNodeIdCleanerOptions Options;
    TInstant LastUpdateTime;
    bool LastListNodeOk = true;
    int WaitCount = 0;
};

NActors::IActor* CreateNodeIdCleaner(TActorId ytWrapper, const TNodeIdCleanerOptions& options)
{
    Y_ABORT_UNLESS(!options.Prefix.empty());
    return new TNodeIdCleaner(ytWrapper, options);
}

} // namespace NYql
