#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_pos_handle.h>

namespace NKikimr {
namespace NMiniKQL {

class TType;
class TStructType;
class TProgramBuilder;

}
}

namespace NYql {
namespace NCommon {

NKikimr::NMiniKQL::TType* BuildType(const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TProgramBuilder& pgmBuilder, IOutputStream& err);
NKikimr::NMiniKQL::TType* BuildType(TPositionHandle pos, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TProgramBuilder& pgmBuilder);
NKikimr::NMiniKQL::TType* BuildType(const TExprNode& owner, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TProgramBuilder& pgmBuilder);

const TTypeAnnotationNode* ConvertMiniKQLType(TPosition position, NKikimr::NMiniKQL::TType* type, TExprContext& ctx);

} // namespace NCommon
} // namespace NYql
