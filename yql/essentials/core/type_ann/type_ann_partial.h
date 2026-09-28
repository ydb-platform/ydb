#pragma once

#include "type_ann_core.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/udf_meta/udf_meta.h>

#include <cstddef>
#include <functional>

namespace NYql {

using TConfigProviderFactory = std::function<TIntrusivePtr<IDataProvider>(TTypeAnnotationContext&)>;
using TTypeParser = std::function<const TTypeAnnotationNode*(TStringBuf, TExprContext&)>;
using TTypeWriter = std::function<TString(const TTypeAnnotationNode*)>;

struct TPartialAnnotationConfig {
    bool IsLibrary = false;
    TLangVersion LangVer = MinLangVersion;
    const IUdfMeta* UdfMeta = nullptr;
    TConfigProviderFactory ConfigProviderFactory;
    TTypeParser TypeParser;
    TTypeWriter TypeWriter;
    ui32 LimitStrictnessFactor = 1;
};

bool PartiallyAnnotateTypes(
    TAstNode* astRoot,
    TIssues& issues,
    const TPartialAnnotationConfig& config);

} // namespace NYql
