#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/utils/log/log.h>

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
                .With<TCoWideFromBlocks>(0)
                    .Input("flow")
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

private:
    const TYtState::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateYtBlockInputTransformer(TYtState::TPtr state) {
    return THolder<IGraphTransformer>(new TYtBlockInputTransformer(std::move(state)));
}

} // namespace NYql
