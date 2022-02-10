#pragma once
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/ast/yql_expr.h>

namespace NKikimr {
namespace NMiniKQL {

struct TComputationContext;

extern const char YqlTypeResourceTag[6];
extern const char YqlCodeResourceTag[6];
extern const char YqlExprContextResourceTag[13];

using TYqlTypeResource = NUdf::TBoxedResource<std::pair<std::shared_ptr<NYql::TExprContext>,
    const NYql::TTypeAnnotationNode*>, YqlTypeResourceTag>;
using TYqlCodeResource = NUdf::TBoxedResource<std::pair<std::shared_ptr<NYql::TExprContext>,
    NYql::TExprNode::TPtr>, YqlCodeResourceTag>;
using TYqlExprContextResource = NUdf::TBoxedResource<std::shared_ptr<NYql::TExprContext>, YqlExprContextResourceTag>;

NYql::TExprContext& GetExprContext(TComputationContext& ctx, ui32 index);
std::shared_ptr<NYql::TExprContext> GetExprContextPtr(TComputationContext& ctx, ui32 index);
const NYql::TTypeAnnotationNode* GetYqlType(const NUdf::TUnboxedValue& value);
NYql::TExprNode::TPtr GetYqlCode(const NUdf::TUnboxedValue& value);

}
}
