#include "yql_yt_block_io_utils.h"

#include <yql/essentials/core/yql_opt_utils.h>

namespace NYql {

bool CheckBlockIOSupportedTypes(
    const TTypeAnnotationNode& type,
    const TSet<TString>& supportedTypes,
    const TSet<NUdf::EDataSlot>& supportedDataTypes,
    std::function<void(const TString&)> unsupportedTypeHandler,
    bool allowNestedOptionals
) {
    auto itemType = type.Cast<TFlowExprType>()->GetItemType();
    if (itemType->GetKind() == ETypeAnnotationKind::Multi) {
        auto& itemTypes = itemType->Cast<TMultiExprType>()->GetItems();
        if (itemTypes.empty()) {
            return false;
        }

        if (!CheckSupportedTypes(itemTypes, supportedTypes, supportedDataTypes, std::move(unsupportedTypeHandler), allowNestedOptionals)) {
            return false;
        }
    } else if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
        auto& items = itemType->Cast<TStructExprType>()->GetItems();
        if (items.empty()) {
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

}
