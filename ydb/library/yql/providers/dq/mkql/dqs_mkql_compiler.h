#pragma once

#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/maybe.h>

#include <functional>

namespace NYql::NDqs {

void RegisterDqsMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TTypeAnnotationContext& ctx);

}
