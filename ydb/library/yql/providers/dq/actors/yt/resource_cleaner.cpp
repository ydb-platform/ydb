#include "yt_wrapper.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql {

using namespace NActors;
using namespace NMonitoring;

class TYtResourceCleaner: public TRichActor<TYtResourceCleaner> {
public:
    static constexpr char ActorName[] = "CLEANER";

    TYtResourceCleaner(
        const TResourceManagerOptions& options,
        const TIntrusivePtr<ICoordinationHelper>& coordinator)
        : TRichActor<TYtResourceCleaner>(&TYtResourceCleaner::Handler)
        , Coordinator(coordinator)
        , Options(options)
        , FilesCounter(Options.Counters->GetSubgroup("component", "cleaner")->GetCounter("files"))
        , RemovesCounter(Options.Counters->GetSubgroup("component", "cleaner")->GetCounter("removes"))
        , ErrorsCounter(Options.Counters->GetSubgroup("component", "cleaner")->GetCounter("errors"))
    {
    }

private:
    STRICT_STFUNC(Handler, {
        HFunc(TEvListNodeResponse, OnListResponse)
        CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)
        cFunc(TEvTick::EventType, Check)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        HFunc(TEvRemoveNodeResponse, OnRemoveNodeResponse)
    })

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
    }

    void Bootstrap(const TActorContext& ctx) {
        YtWrapper = Coordinator->GetWrapper(
            ctx.ActorSystem(),
            Options.YtBackend.GetClusterName(),
            Options.YtBackend.GetUser(),
            Options.YtBackend.GetToken());
        Check();
    }

    void Check() {
        NYT::NApi::TListNodeOptions options;
        options.Attributes = {
            "modification_time"
        };
        auto command = new TEvListNode(Options.UploadPrefix, options);
        Send(YtWrapper, command);
    }

    void OnRemoveNodeResponse(TEvRemoveNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto result = std::get<0>(*ev->Get());
        if (!result.IsOK()) {
            YQL_CLOG(WARN, ProviderDq) << "Remove error " << ToString(result);
            *ErrorsCounter += 1;
        }
    }

    void OnListResponse(TEvListNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto result = std::get<0>(*ev->Get());

        try {
            auto nodes = NYT::NodeFromYsonString(result.ValueOrThrow()).AsList();
            TVector<std::pair<TInstant, TString>> pairs;

            auto filesDiff = static_cast<int>(nodes.size()) - FilesCount;
            *FilesCounter += filesDiff;
            FilesCount = static_cast<int>(nodes.size());

            for (const auto& node : nodes) {
                const auto& attributes = node.GetAttributes().AsMap();
                auto maybeModificationTime = attributes.find("modification_time");
                YQL_ENSURE(maybeModificationTime != attributes.end());
                auto modificationTime = TInstant::ParseIso8601(maybeModificationTime->second.AsString());

                TString nodeStr = node.AsString();

                if (Options.KeepFilter && nodeStr.find(*Options.KeepFilter) != TString::npos) {
                    continue;
                }

                pairs.emplace_back(modificationTime, nodeStr);
            }

            std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) {
                return a.first > b.first;
            });
            if (Options.KeepFirst) {
                for (ui32 i = *Options.KeepFirst; i < pairs.size(); ++i) {
                    YQL_CLOG(DEBUG, ProviderDq) << "RemoveNode " << pairs[i].second;
                    Send(YtWrapper, new TEvRemoveNode(Options.UploadPrefix + "/" + pairs[i].second, NYT::NApi::TRemoveNodeOptions()));

                    *RemovesCounter += 1;
                }
            }
            if (Options.DropBefore) {
                auto now = TInstant::Now();
                for (const auto& [t, id] : pairs) {
                    if (t < now && now - t > Options.DropBefore) {
                        YQL_CLOG(DEBUG, ProviderDq) << "RemoveNode " << id;
                        Send(YtWrapper, new TEvRemoveNode(Options.UploadPrefix + "/" + id, NYT::NApi::TRemoveNodeOptions()));

                        *RemovesCounter += 1;
                    }
                }
            }
        } catch (...) {
            YQL_CLOG(ERROR, ProviderDq) << "Error on list node " << CurrentExceptionMessage();
        }

        Schedule(TDuration::Seconds(5), new TEvTick);
    }

    TActorId YtWrapper;
    const TIntrusivePtr<ICoordinationHelper> Coordinator;
    const TResourceManagerOptions Options;

    int FilesCount = 0;
    TDynamicCounters::TCounterPtr FilesCounter;
    TDynamicCounters::TCounterPtr RemovesCounter;
    TDynamicCounters::TCounterPtr ErrorsCounter;
};

IActor* CreateYtResourceCleaner(
    const TResourceManagerOptions& options,
    const TIntrusivePtr<ICoordinationHelper>& coordinator)
{
    Y_ABORT_UNLESS(!options.YtBackend.GetClusterName().empty());
    Y_ABORT_UNLESS(!options.YtBackend.GetUser().empty());
    Y_ABORT_UNLESS(!options.YtBackend.GetUploadPrefix().empty());

    return new TYtResourceCleaner(options, coordinator);
}

} // namespace NYql
