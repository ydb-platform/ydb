#include "kqp_module_resolver.h"

#include <yql/essentials/core/services/mounts/yql_mounts.h>

#include <library/cpp/resource/resource.h>

namespace NKikimr::NKqp {

bool GetKqpDefaultModuleResolver(NYql::TExprContext& ctx, NYql::IModuleResolver::TPtr& moduleResolver) {
    return NYql::GetYqlDefaultModuleResolver(ctx, moduleResolver)
        && moduleResolver->AddFromMemory("/lib/ydb/digest.sql", NResource::Find("/lib/ydb/digest.sql"), ctx,
                                         /*syntaxVersion=*/1, /*packageVersion=*/0)
        && moduleResolver->Link(ctx);
}

} // namespace NKikimr::NKqp
