#include "yql_yt_provider_impl.h"

#include <yt/yql/providers/yt/provider/yql_yt_block_io_utils.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;

class TYtBlockOutputTransformer : public TOptimizeTransformerBase {
public:
    TYtBlockOutputTransformer(TYtState::TPtr state)
        : TOptimizeTransformerBase(
            state ? state->Types : nullptr,
            NLog::EComponent::ProviderYt,
            state ? state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()) : TSet<TString>()
        )
        , State_(std::move(state))
    {
#define HNDL(name) "YtBlockOutput-"#name, Hndl(&TYtBlockOutputTransformer::name)
        AddHandler(0, &TYtMap::Match, HNDL(TryTransformMap));
#undef HNDL
    }

private:
    TMaybeNode<TExprBase> TryTransformMap(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();
        if (
            NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockOutputApplied)
            || !NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockOutputReady)
            || !CanRewriteMap(map, ctx)
        ) {
            return map;
        }

        YQL_CLOG(INFO, ProviderYt) << "Rewrite YtMap with block output";

        auto settings = RemoveSetting(map.Settings().Ref(), EYtSettingType::BlockOutputReady, ctx);
        settings = AddSetting(*settings, EYtSettingType::BlockOutputApplied, TExprNode::TPtr(), ctx);

        auto mapperLambda = Build<TCoLambda>(ctx, map.Mapper().Pos())
            .Args({"flow"})
            .Body<TCoToFlow>()
                .Input<TCoWideToBlocks>()
                    .Input<TCoFromFlow>()
                        .Input<TExprApplier>()
                            .Apply(map.Mapper())
                            .With(0, "flow")
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
        return map;
    }

    bool CanRewriteMap(const TYtMap& map, TExprContext& ctx) const {
        if (auto flowSetting = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::Flow); !flowSetting || flowSetting->ChildrenSize() < 2) {
            return false;
        }

        auto blockOutputSetting = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::BlockOutputReady);
        auto mode = FromString<EBlockOutputMode>(blockOutputSetting->Child(1)->Content());
        if (mode == EBlockOutputMode::Auto && !map.Mapper().Body().Ref().IsCallable("WideFromBlocks")) {
            return false;
        }

        return EnsureWideFlowType(map.Mapper().Ref(), ctx);
    }

private:
    const TYtState::TPtr State_;
};

} // namespace

THolder<IGraphTransformer> CreateYtBlockOutputTransformer(TYtState::TPtr state) {
    return THolder<IGraphTransformer>(new TYtBlockOutputTransformer(std::move(state)));
}

} // namespace NYql
