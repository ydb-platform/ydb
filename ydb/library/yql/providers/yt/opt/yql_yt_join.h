#pragma once

#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_expr_builder.h>

#include <util/generic/maybe.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>

namespace NYql {

enum class TChoice : ui8 {
    None = 0,
    Left,
    Right,
    Both,
};

TChoice Invert(TChoice choice);
bool IsLeftOrRight(TChoice choice);
TChoice Merge(TChoice a, TChoice b);

struct TMapJoinSettings {
    ui64 MapJoinLimit = 0;
    ui64 MapJoinShardMinRows = 1;
    ui64 MapJoinShardCount = 1;
    bool SwapTables = false;
    ui64 LeftRows = 0;
    ui64 RightRows = 0;
    ui64 LeftSize = 0;
    ui64 RightSize = 0;
    ui64 LeftMemSize = 0;
    ui64 RightMemSize = 0;
    bool LeftUnique = false;
    bool RightUnique = false;
    ui64 LeftCount = 0;
    ui64 RightCount = 0;

    TMaybe<ui64> CalculatePartSize(ui64 rows) const;
};

THashSet<TString> BuildJoinKeys(const TJoinLabel& label, const TExprNode& keys);
TVector<TString> BuildJoinKeyList(const TJoinLabel& label, const TExprNode& keys);
TVector<const TTypeAnnotationNode*> BuildJoinKeyType(const TJoinLabel& label, const TExprNode& keys);
TMap<TString, const TTypeAnnotationNode*> BuildJoinKeyTypeMap(const TJoinLabel& label, const TExprNode& keys);
TVector<const TTypeAnnotationNode*> RemoveNullsFromJoinKeyType(const TVector<const TTypeAnnotationNode*>& inputKeyType);
const TTypeAnnotationNode* AsDictKeyType(const TVector<const TTypeAnnotationNode*>& inputKeyType, TExprContext& ctx);
void SwapJoinType(TPositionHandle pos, TExprNode::TPtr& joinType, TExprContext& ctx);
const TStructExprType* MakeOutputJoinColumns(const THashMap<TString, const TTypeAnnotationNode*>& columnTypes,
    const TJoinLabel& label, TExprContext& ctx);
const TTypeAnnotationNode* UnifyJoinKeyType(TPositionHandle pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx);
TVector<const TTypeAnnotationNode*> UnifyJoinKeyType(TPositionHandle pos, const TVector<const TTypeAnnotationNode*>& left,
    const TVector<const TTypeAnnotationNode*>& right, TExprContext& ctx);
TExprNode::TPtr RemapNonConvertibleItems(const TExprNode::TPtr& input, const TJoinLabel& label,
    const TExprNode& keys, const TVector<const TTypeAnnotationNode*>& unifiedKeyTypes,
    TExprNode::TListType& columnNodes, TExprNode::TListType& columnNodesForSkipNull, TExprContext& ctx);

struct TCommonJoinCoreLambdas {
    TExprNode::TPtr MapLambda;
    TExprNode::TPtr ReduceLambda;
    const TTypeAnnotationNode* CommonJoinCoreInputType = nullptr;
};

TCommonJoinCoreLambdas MakeCommonJoinCoreLambdas(TPositionHandle pos, TExprContext& ctx, const TJoinLabel& label,
    const TJoinLabel& otherLabel, const TVector<const TTypeAnnotationNode*>& outputKeyType,
    const TExprNode& keyColumnsNode, TStringBuf joinType,
    const TStructExprType* myOutputSchemeType, const TStructExprType* otherOutputSchemeType,
    ui32 tableIndex, bool useSortedReduce, ui32 sortIndex,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    bool myData, bool otherData, const TVector<TString>& ytReduceByColumns);
TExprNode::TPtr PrepareForCommonJoinCore(TPositionHandle pos, TExprContext& ctx, const TExprNode::TPtr& input,
    const TExprNode::TPtr& reduceLambdaZero, const TExprNode::TPtr& reduceLambdaOne);


TCommonJoinCoreLambdas MakeCommonJoinCoreReduceLambda(TPositionHandle pos, TExprContext& ctx, const TJoinLabel& label,
    const TVector<const TTypeAnnotationNode*>& outputKeyType,
    const TExprNode& keyColumnsNode, TStringBuf joinType,
    const TStructExprType* myOutputSchemeType, const TStructExprType* otherOutputSchemeType,
    ui32 tableIndex, bool useSortedReduce, ui32 sortIndex,
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    bool myData, bool otherData, const TVector<TString>& ytReduceByColumns);

void AddJoinRemappedColumn(TPositionHandle pos, const TExprNode::TPtr& pairArg, TExprNode::TListType& joinedBodyChildren,
    TStringBuf name, TStringBuf newName, TExprContext& ctx);

}
