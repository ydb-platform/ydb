#pragma once
#include <yql/essentials/core/yql_udf_resolver.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>

namespace NYql::NCommon {

IUdfResolver::TPtr WrapUdfResolverWithQContext(IUdfResolver::TPtr inner, const TQContext& qContext);

}
