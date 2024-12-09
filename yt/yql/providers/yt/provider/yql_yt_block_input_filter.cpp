#include "yql_yt_provider_impl.h"
#include "yql_yt_dq_integration.h" // TODO remove

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yt/yql/providers/yt/provider/yql_yt_helpers.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

using namespace NNodes;
class YtBlockInputFilterTransformer : public TOptimizeTransformerBase {
public:
    YtBlockInputFilterTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
        , State_(std::move(state))
        , Finalizer_(std::move(finalizer))
    {
#define HNDL(name) "YtBlockInputFilter-"#name, Hndl(&YtBlockInputFilterTransformer::name)
        AddHandler(0, &TYtMap::Match, HNDL(HandleMap));
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

    TMaybeNode<TExprBase> HandleMap(TExprBase node, TExprContext& ctx) const {
        auto map = node.Cast<TYtMap>();
        if (!State_->Configuration->JobBlockInput.Get().GetOrElse(Types->UseBlocks)) {
            return map;
        }

        if (NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockInputApplied)) {
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

        auto supportedTypes = State_->Configuration->JobBlockInputSupportedTypes.Get(map.DataSink().Cluster().StringValue()).GetOrElse(DEFAULT_BLOCK_INPUT_SUPPORTED_TYPES);
        auto supportedDataTypes = State_->Configuration->JobBlockInputSupportedDataTypes.Get(map.DataSink().Cluster().StringValue()).GetOrElse(DEFAULT_BLOCK_INPUT_SUPPORTED_DATA_TYPES);

        auto lambdaInputType = map.Mapper().Args().Arg(0).Ref().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        if (lambdaInputType->GetKind() == ETypeAnnotationKind::Multi) {
            auto& items = lambdaInputType->Cast<TMultiExprType>()->GetItems();
            if (items.empty()) {
                return false;
            }

            if (!CheckSupportedTypesOld(items, supportedTypes, supportedDataTypes, [](const TString&) {})) {
                return false;
            }
        } else if (lambdaInputType->GetKind() == ETypeAnnotationKind::Struct) {
            auto& items = lambdaInputType->Cast<TStructExprType>()->GetItems();
            if (items.empty()) {
                return false;
            }

            TTypeAnnotationNode::TListType itemTypes;
            for (auto item: items) {
                itemTypes.push_back(item->GetItemType());
            }

            if (!CheckSupportedTypesOld(itemTypes, supportedTypes, supportedDataTypes, [](const TString&) {})) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

private:
    const TYtState::TPtr State_;
    const THolder<IGraphTransformer> Finalizer_;
};

} // namespace

THolder<IGraphTransformer> CreateYtBlockInputFilterTransformer(TYtState::TPtr state, THolder<IGraphTransformer>&& finalizer) {
    return THolder<IGraphTransformer>(new YtBlockInputFilterTransformer(std::move(state), std::move(finalizer)));
}

} // namespace NYql
