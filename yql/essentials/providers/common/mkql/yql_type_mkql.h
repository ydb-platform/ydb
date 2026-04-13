#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_pos_handle.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <unordered_map>

namespace NKikimr::NMiniKQL {

class TType;
class TStructType;
class TProgramBuilder;

} // namespace NKikimr::NMiniKQL

namespace NYql::NCommon {

using TMemoizedTypesMap = std::unordered_map<const TTypeAnnotationNode*, NKikimr::NMiniKQL::TType*>;

NKikimr::NMiniKQL::TType* BuildType(const TTypeAnnotationNode& annotation, const NKikimr::NMiniKQL::TTypeBuilder& typeBuilder, TMemoizedTypesMap& memoization, IOutputStream& err);
NKikimr::NMiniKQL::TType* BuildType(TPositionHandle pos, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TTypeBuilder& typeBuilder, TMemoizedTypesMap& memoization);
NKikimr::NMiniKQL::TType* BuildType(const TExprNode& owner, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TTypeBuilder& typeBuilder, TMemoizedTypesMap& memoization);

NKikimr::NMiniKQL::TType* BuildType(const TTypeAnnotationNode& annotation, const NKikimr::NMiniKQL::TTypeBuilder& typeBuilder, IOutputStream& err);
NKikimr::NMiniKQL::TType* BuildType(TPositionHandle pos, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TTypeBuilder& typeBuilder);
NKikimr::NMiniKQL::TType* BuildType(const TExprNode& owner, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TTypeBuilder& typeBuilder);

const TTypeAnnotationNode* ConvertMiniKQLType(TPosition position, NKikimr::NMiniKQL::TType* type, TExprContext& ctx);
ETypeAnnotationKind ConvertMiniKQLTypeKind(NKikimr::NMiniKQL::TType* type);

} // namespace NYql::NCommon
