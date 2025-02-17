#include "yql_yt_provider_impl.h"
#include "yql_yt_dq_integration.h" // TODO remove

#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/provider/yql_yt_block_io_utils.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;
class YtBlockIOFilterTransformer : public TOptimizeTransformerBase {
public:
    YtBlockIOFilterTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
        , State_(std::move(state))
        , Finalizer_(std::move(finalizer))
    {
#define HNDL(name) "YtBlockIOFilter-"#name, Hndl(&YtBlockIOFilterTransformer::name)
        AddHandler(0, &TYtMap::Match, HNDL(HandleMapInput));
        AddHandler(0, &TYtMap::Match, HNDL(HandleMapOutput));
#undef HNDL
    }

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (const auto status = Finalizer_->Transform(input, output, ctx); status.Level != TStatus::Ok)
            return status;

        return TOptimizeTransformerBase::DoTransform(input, output, ctx);
    }

    void Rewind() final {
        Finalizer_->Rewind();
        TOptimizeTransformerBase::Rewind();
    }

    TMaybeNode<TExprBase> HandleMapInput(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();
        if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockInputApplied)) {
            return map;
        }

        if (!State_->Configuration->JobBlockInput.Get().GetOrElse(Types->UseBlocks)) {
            return map;
        }

        auto settings = map.Settings().Ptr();
        bool canUseBlockInput = CanUseBlockInputForMap(map);
        bool hasSetting = HasSetting(*settings, EYtSettingType::BlockInputReady);
        if (canUseBlockInput && !hasSetting) {
            settings = AddSetting(*settings, EYtSettingType::BlockInputReady, TExprNode::TPtr(), ctx);
        } else if (!canUseBlockInput && hasSetting) {
            settings = RemoveSetting(*settings, EYtSettingType::BlockInputReady, ctx);
        } else {
            return map;
        }
        return Build<TYtMap>(ctx, node.Pos())
            .InitFrom(map)
            .Settings(settings)
            .Done();
    }

    TMaybeNode<TExprBase> HandleMapOutput(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();
        if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockOutputApplied)) {
            return map;
        }

        auto mode = DetermineBlockOutputMode();
        if (mode == EBlockOutputMode::Disable) {
            return map;
        }

        auto settings = map.Settings().Ptr();
        bool canUseBlockOutput = CanUseBlockOutputForMap(map);
        bool hasSetting = HasSetting(*settings, EYtSettingType::BlockOutputReady);
        if (canUseBlockOutput && !hasSetting) {
            settings = AddSetting(
                *settings,
                EYtSettingType::BlockOutputReady,
                ctx.NewAtom(map.Pos(), ToString<EBlockOutputMode>(mode)),
                ctx
            );
        } else if (!canUseBlockOutput && hasSetting) {
            settings = RemoveSetting(*settings, EYtSettingType::BlockOutputReady, ctx);
        } else {
            return map;
        }
        return Build<TYtMap>(ctx, node.Pos())
            .InitFrom(map)
            .Settings(settings)
            .Done();
    }

    bool CanUseBlockInputForMap(const TYtMap& map) const {
        if (!NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
            return false;
        }

        if (map.Input().Size() > 1) {
            return false;
        }

        for (auto path : map.Input().Item(0).Paths()) {
            if (!IsYtTableSuitableForArrowInput(path.Table(), [](const TString&) {})) {
                return false;
            }
        }

        auto supportedTypes = State_->Configuration->JobBlockInputSupportedTypes.Get().GetOrElse(DEFAULT_BLOCK_INPUT_SUPPORTED_TYPES);
        auto supportedDataTypes = State_->Configuration->JobBlockInputSupportedDataTypes.Get().GetOrElse(DEFAULT_BLOCK_INPUT_SUPPORTED_DATA_TYPES);

        auto lambdaInputType = map.Mapper().Args().Arg(0).Ref().GetTypeAnn();
        if (!CheckBlockIOSupportedTypes(*lambdaInputType, supportedTypes, supportedDataTypes, [](const TString&) {})) {
            return false;
        }

        return true;
    }

    bool CanUseBlockOutputForMap(const TYtMap& map) const {
        if (!NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Flow)) {
            return false;
        }

        auto supportedTypes = State_->Configuration->JobBlockOutputSupportedTypes.Get().GetOrElse(DEFAULT_BLOCK_OUTPUT_SUPPORTED_TYPES);
        auto supportedDataTypes = State_->Configuration->JobBlockOutputSupportedDataTypes.Get().GetOrElse(DEFAULT_BLOCK_OUTPUT_SUPPORTED_DATA_TYPES);

        auto lambdaOutputType = map.Mapper().Ref().GetTypeAnn();
        if (!CheckBlockIOSupportedTypes(*lambdaOutputType, supportedTypes, supportedDataTypes, [](const TString&) {}, false)) {
            return false;
        }

        return true;
    }

    EBlockOutputMode DetermineBlockOutputMode() const {
        auto jobBlockOutput = State_->Configuration->JobBlockOutput.Get();
        if (jobBlockOutput.Defined()) {
            return *jobBlockOutput;
        } else if (Types->UseBlocks) {
            return EBlockOutputMode::Auto;
        } else {
            return EBlockOutputMode::Disable;
        }
    }

private:
    const TYtState::TPtr State_;
    const THolder<IGraphTransformer> Finalizer_;
};

} // namespace

THolder<IGraphTransformer> CreateYtBlockIOFilterTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer) {
    return THolder<IGraphTransformer>(new YtBlockIOFilterTransformer(std::move(state), std::move(finalizer)));
}

} // namespace NYql
