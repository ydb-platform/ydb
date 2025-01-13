#pragma once

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

bool CheckBlockIOSupportedTypes(
    const TTypeAnnotationNode& containerType,
    const TSet<TString>& supportedTypes,
    const TSet<NUdf::EDataSlot>& supportedDataTypes,
    std::function<void(const TString&)> unsupportedTypeHandler,
    bool allowNestedOptionals = true
);

}
