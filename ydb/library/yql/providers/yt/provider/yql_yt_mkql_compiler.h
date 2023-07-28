#pragma once

#include "yql_yt_provider.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

#include <utility>

namespace NYql {

class TExprNode;
class TTypeAnnotationNode;

NKikimr::NMiniKQL::TRuntimeNode BuildTableContentCall(NKikimr::NMiniKQL::TType* outItemType,
    TStringBuf clusterName,
    const TExprNode& input,
    const TMaybe<ui64>& itemsCount,
    NCommon::TMkqlBuildContext& ctx,
    bool forceColumns,
    const THashSet<TString>& extraSysColumns = {},
    bool forceKeyColumns = false);

NKikimr::NMiniKQL::TRuntimeNode BuildTableContentCall(TStringBuf callName,
    NKikimr::NMiniKQL::TType* outItemType,
    TStringBuf clusterName,
    const TExprNode& input,
    const TMaybe<ui64>& itemsCount,
    NCommon::TMkqlBuildContext& ctx,
    bool forceColumns,
    const THashSet<TString>& extraSysColumns = {},
    bool forceKeyColumns = false);

void RegisterYtMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);
void RegisterDqYtMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TYtState::TPtr& state);

}
