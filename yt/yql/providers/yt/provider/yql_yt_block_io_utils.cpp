#include "yql_yt_block_io_utils.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

namespace NYql {

using namespace NNodes;

bool CheckBlockIOSupportedTypes(
    const TTypeAnnotationNode& type,
    const TSet<TString>& supportedTypes,
    const TSet<NUdf::EDataSlot>& supportedDataTypes,
    std::function<void(const TString&)> unsupportedTypeHandler,
    size_t wideFlowLimit,
    bool allowNestedOptionals
) {
    auto& itemType = GetSeqItemType(type);
    if (itemType.GetKind() == ETypeAnnotationKind::Multi) {
        auto& itemTypes = itemType.Cast<TMultiExprType>()->GetItems();

        if (!CheckSupportedTypes(itemTypes, supportedTypes, supportedDataTypes, std::move(unsupportedTypeHandler), allowNestedOptionals)) {
            return false;
        }
    } else if (itemType.GetKind() == ETypeAnnotationKind::Struct) {
        auto& items = itemType.Cast<TStructExprType>()->GetItems();
        if (items.empty() || items.size() > wideFlowLimit) {
            unsupportedTypeHandler("fields count doesn't satisfy wide flow requirements");
            return false;
        }

        TTypeAnnotationNode::TListType itemTypes;
        for (auto item: items) {
            itemTypes.push_back(item->GetItemType());
        }

        if (!CheckSupportedTypes(itemTypes, supportedTypes, supportedDataTypes, std::move(unsupportedTypeHandler), allowNestedOptionals)) {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

TCoLambda WrapLambdaWithBlockInput(TCoLambda lambda, TExprContext& ctx) {
    return Build<TCoLambda>(ctx, lambda.Pos())
        .Args({"flow"})
        .Body<TExprApplier>()
            .Apply(lambda)
            .With<TCoToFlow>(0)
                .Input<TCoWideFromBlocks>()
                    .Input<TCoFromFlow>()
                        .Input("flow")
                    .Build()
                .Build()
            .Build()
        .Build()
        .Done();
}

}
