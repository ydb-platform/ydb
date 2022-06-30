#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

namespace NYql {

TExprNode::TPtr ExtractFormat(TExprNode::TListType& settings) {
    for (auto it = settings.cbegin(); settings.cend() != it; ++it) {
        if (const auto item = *it; item->Head().IsAtom("format")) {
            settings.erase(it);
            return item->TailPtr();
        }
    }

    return {};
}

}
