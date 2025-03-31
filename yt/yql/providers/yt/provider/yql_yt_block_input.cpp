#include "yql_yt_provider_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/provider/yql_yt_block_io_utils.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>

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
        AddHandler(0, &TYtMap::Match, HNDL(TryTransformMap<TYtMap>));
        AddHandler(0, &TYtMapReduce::Match, HNDL(TryTransformMap<TYtMapReduce>));
        AddHandler(0, &TYtTableContent::Match, HNDL(TryTransformTableContent));
#undef HNDL
    }

private:
    template<typename TYtOpWithMap>
    TMaybeNode<TExprBase> TryTransformMap(TExprBase node, TExprContext& ctx) const {
        auto op = node.Cast<TYtOpWithMap>();
        if (
            NYql::HasSetting(op.Settings().Ref(), EYtSettingType::BlockInputApplied)
            || !NYql::HasSetting(op.Settings().Ref(), EYtSettingType::BlockInputReady)
            || !CanRewriteMap(op, ctx)
        ) {
            return op;
        }

        YQL_CLOG(INFO, ProviderYt) << "Rewrite " << TYtOpWithMap::CallableName() << " with block input";

        auto settings = RemoveSetting(op.Settings().Ref(), EYtSettingType::BlockInputReady, ctx);
        settings = AddSetting(*settings, EYtSettingType::BlockInputApplied, TExprNode::TPtr(), ctx);

        auto mapperLambda = WrapLambdaWithBlockInput(op.Mapper().template Cast<TCoLambda>(), ctx);
        return Build<TYtOpWithMap>(ctx, node.Pos())
            .InitFrom(op)
            .Settings(settings)
            .Mapper(mapperLambda)
            .Done();
    }

    bool CanRewriteMap(const TYtWithUserJobsOpBase& op, TExprContext& ctx) const {
        auto mapLambda = GetMapLambda(op);
        if (!mapLambda) {
            return false;
        }

        if (auto flowSetting = NYql::GetSetting(op.Settings().Ref(), EYtSettingType::Flow); !flowSetting || flowSetting->ChildrenSize() < 2) {
            return false;
        }

        return EnsureWideFlowType(mapLambda.Cast().Args().Arg(0).Ref(), ctx);
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
