#include "yql_yt_provider_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

class TYtBlockInputTransformer : public TOptimizeTransformerBase {
public:
    TYtBlockInputTransformer(TYtState::TPtr state)
        : TOptimizeTransformerBase(
            state ? state->Types : nullptr,
            NLog::EComponent::ProviderYt,
            state ? state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()) : TSet<TString>()
        )
        , State_(std::move(state))
    {
#define HNDL(name) "YtBlockInput-"#name, Hndl(&TYtBlockInputTransformer::name)
        AddHandler(0, &TYtMap::Match, HNDL(TryTransformMap));
        AddHandler(0, &TYtTableContent::Match, HNDL(TryTransformTableContent));
#undef HNDL
    }

private:
    TMaybeNode<TExprBase> TryTransformMap(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();
        if (
            NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockInputApplied)
            || !NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockInputReady)
            || !CanRewriteMap(map, ctx)
        ) {
            return map;
        }

        YQL_CLOG(INFO, ProviderYt) << "Rewrite YtMap with block input";

        auto settings = RemoveSetting(map.Settings().Ref(), EYtSettingType::BlockInputReady, ctx);
        settings = AddSetting(*settings, EYtSettingType::BlockInputApplied, TExprNode::TPtr(), ctx);
        auto mapperLambda = Build<TCoLambda>(ctx, map.Mapper().Pos())
            .Args({"flow"})
            .Body<TExprApplier>()
                .Apply(map.Mapper())
                .With<TCoToFlow>(0)
                    .Input<TCoWideFromBlocks>()
                        .Input<TCoFromFlow>()
                            .Input("flow")
                        .Build()
                    .Build()
                .Build()
            .Build()
            .Done()
            .Ptr();

        return Build<TYtMap>(ctx, node.Pos())
            .InitFrom(map)
            .Settings(settings)
            .Mapper(mapperLambda)
            .Done();
    }

    bool CanRewriteMap(const TYtMap& map, TExprContext& ctx) const {
        if (auto flowSetting = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::Flow); !flowSetting || flowSetting->ChildrenSize() < 2) {
            return false;
        }

        return EnsureWideFlowType(map.Mapper().Args().Arg(0).Ref(), ctx);
    }

    TMaybeNode<TExprBase> TryTransformTableContent(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        auto tableContent = node.Cast<TYtTableContent>();
        if (!NYql::HasSetting(tableContent.Settings().Ref(), EYtSettingType::BlockInputReady)) {
            return tableContent;
        }

        const TParentsMap* parentsMap = getParents();
        if (auto it = parentsMap->find(tableContent.Raw()); it != parentsMap->end() && it->second.size() > 1) {
            return tableContent;
        }

        YQL_CLOG(INFO, ProviderYt) << "Rewrite YtTableContent with block input";

        auto inputStructType = GetSeqItemType(tableContent.Ref().GetTypeAnn())->Cast<TStructExprType>();
        auto asStructBuilder = Build<TCoAsStruct>(ctx, tableContent.Pos());
        TExprNode::TListType narrowMapArgs;
        for (auto& item : inputStructType->GetItems()) {
            auto arg = ctx.NewArgument(tableContent.Pos(), item->GetName());
            asStructBuilder.Add<TCoNameValueTuple>()
                .Name().Build(item->GetName())
                .Value(arg)
                .Build();
            narrowMapArgs.push_back(std::move(arg));
        }

        auto settings = RemoveSetting(tableContent.Settings().Ref(), EYtSettingType::BlockInputReady, ctx);
        return Build<TCoForwardList>(ctx, tableContent.Pos())
            .Stream<TCoNarrowMap>()
                .Input<TCoToFlow>()
                    .Input<TCoWideFromBlocks>()
                        .Input<TYtBlockTableContent>()
                            .Input(tableContent.Input())
                            .Settings(settings)
                        .Build()
                    .Build()
                .Build()
                .Lambda()
                    .Args(narrowMapArgs)
                    .Body(asStructBuilder.Done())
                .Build()
            .Build()
            .Done();
    }

private:
    const TYtState::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateYtBlockInputTransformer(TYtState::TPtr state) {
    return THolder<IGraphTransformer>(new TYtBlockInputTransformer(std::move(state)));
}

} // namespace NYql
