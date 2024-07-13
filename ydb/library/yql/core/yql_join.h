#pragma once
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_cost_function.h>

#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>

namespace NYql {

inline TString FullColumnName(const TStringBuf& table, const TStringBuf& column) {
    return TString::Join(table, ".", column);
}

inline void SplitTableName(const TStringBuf& fullName, TStringBuf& table, TStringBuf& column) {
    auto pos = fullName.find('.');
    Y_ENSURE(pos != TString::npos, "Expected full column name: " << fullName);
    table = fullName.substr(0, pos);
    column = fullName.substr(pos + 1);
}

struct TJoinLabel {
    TMaybe<TIssue> Parse(TExprContext& ctx, TExprNode& node, const TStructExprType* structType, const TUniqueConstraintNode* unique, const TDistinctConstraintNode* distinct);
    TMaybe<TIssue> ValidateLabel(TExprContext& ctx, const NNodes::TCoAtom& label);
    TString FullName(const TStringBuf& column) const;
    TVector<TString> AllNames(const TStringBuf& column) const;
    TStringBuf ColumnName(const TStringBuf& column) const;
    TStringBuf TableName(const TStringBuf& column) const;
    bool HasTable(const TStringBuf& table) const;
    TMaybe<const TTypeAnnotationNode*> FindColumn(const TStringBuf& table, const TStringBuf& column) const;
    TString MemberName(const TStringBuf& table, const TStringBuf& column) const;
    TVector<TString> EnumerateAllColumns() const;
    TVector<TString> EnumerateAllMembers() const;

    bool AddLabel = false;
    const TStructExprType* InputType;
    TVector<TStringBuf> Tables;
    const TUniqueConstraintNode* Unique = nullptr;
    const TDistinctConstraintNode* Distinct = nullptr;
};

struct TJoinLabels {
    TMaybe<TIssue> Add(TExprContext& ctx, TExprNode& node, const TStructExprType* structType, const TUniqueConstraintNode* unique = nullptr, const TDistinctConstraintNode* distinct = nullptr);
    TMaybe<const TJoinLabel*> FindInput(const TStringBuf& table) const;
    TMaybe<ui32> FindInputIndex(const TStringBuf& table) const;
    TMaybe<const TTypeAnnotationNode*> FindColumn(const TStringBuf& table, const TStringBuf& column) const;
    TMaybe<const TTypeAnnotationNode*> FindColumn(const TStringBuf& fullName) const;
    TVector<TString> EnumerateColumns(const TStringBuf& table) const;

    TVector<TJoinLabel> Inputs;
    THashMap<TStringBuf, ui32> InputByTable;
};

struct TJoinOptions {
    THashMap<TStringBuf, TVector<TStringBuf>> RenameMap;
    TSet<TVector<TStringBuf>> PreferredSortSets;

    bool Flatten = false;
    bool StrictKeys = false;
};

IGraphTransformer::TStatus ValidateEquiJoinOptions(
    TPositionHandle positionHandle,
    TExprNode& optionsNode,
    TJoinOptions& options,
    TExprContext& ctx
);

IGraphTransformer::TStatus EquiJoinAnnotation(
    TPositionHandle position,
    const TStructExprType*& resultType,
    const TJoinLabels& labels,
    TExprNode& joins,
    const TJoinOptions& options,
    TExprContext& ctx
);

IGraphTransformer::TStatus EquiJoinConstraints(
    TPositionHandle positionHandle,
    const TUniqueConstraintNode*& unique,
    const TDistinctConstraintNode*& distinct,
    const TJoinLabels& labels,
    TExprNode& joins,
    TExprContext& ctx
);

THashMap<TStringBuf, THashSet<TStringBuf>> CollectEquiJoinKeyColumnsByLabel(const TExprNode& joinTree);

bool IsLeftJoinSideOptional(const TStringBuf& joinType);
bool IsRightJoinSideOptional(const TStringBuf& joinType);

TExprNode::TPtr FilterOutNullJoinColumns(TPositionHandle pos, const TExprNode::TPtr& input,
    const TJoinLabel& label, const TSet<TString>& optionalKeyColumns, TExprContext& ctx);

TMap<TStringBuf, TVector<TStringBuf>> LoadJoinRenameMap(const TExprNode& settings);
NNodes::TCoLambda BuildJoinRenameLambda(TPositionHandle pos, const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    const TStructExprType& joinResultType, TExprContext& ctx);
TSet<TVector<TStringBuf>> LoadJoinSortSets(const TExprNode& settings);

THashMap<TString, const TTypeAnnotationNode*> GetJoinColumnTypes(const TExprNode& joins,
    const TJoinLabels& labels, TExprContext& ctx);

THashMap<TString, const TTypeAnnotationNode*> GetJoinColumnTypes(const TExprNode& joins,
    const TJoinLabels& labels, const TStringBuf& joinType, TExprContext& ctx);

bool AreSameJoinKeys(const TExprNode& joins, const TStringBuf& table1, const TStringBuf& column1, const TStringBuf& table2, const TStringBuf& column2);
// returns (is required side + allow skip nulls);
std::pair<bool, bool> IsRequiredSide(const TExprNode::TPtr& joinTree, const TJoinLabels& labels, ui32 inputIndex);

TMaybe<bool> IsFilteredSide(const TExprNode::TPtr& joinTree, const TJoinLabels& labels, ui32 inputIndex);

void AppendEquiJoinRenameMap(TPositionHandle pos, const TMap<TStringBuf, TVector<TStringBuf>>& newRenameMap,
    TExprNode::TListType& joinSettingNodes, TExprContext& ctx);

void AppendEquiJoinSortSets(TPositionHandle pos, const TSet<TVector<TStringBuf>>& newSortSets,
    TExprNode::TListType& joinSettingNodes, TExprContext& ctx);

TMap<TStringBuf, TVector<TStringBuf>> UpdateUsedFieldsInRenameMap(
    const TMap<TStringBuf, TVector<TStringBuf>>& renameMap,
    const TSet<TStringBuf>& usedFields,
    const TStructExprType* structType
);


struct TEquiJoinParent {
    TEquiJoinParent(const TExprNode* node, ui32 index, const TExprNode* extractedMembers)
        : Node(node)
        , Index(index)
        , ExtractedMembers(extractedMembers)
    {
    }
    const TExprNode* Node;
    ui32 Index;
    const TExprNode* ExtractedMembers;
};

TVector<TEquiJoinParent> CollectEquiJoinOnlyParents(const NNodes::TCoFlatMapBase& flatMap, const TParentsMap& parents);

struct TEquiJoinLinkSettings {
    TPositionHandle Pos;
    TSet<TString> LeftHints;
    TSet<TString> RightHints;
    EJoinAlgoType JoinAlgo = EJoinAlgoType::Undefined;
    // JOIN implementation may ignore this flags if SortedMerge strategy is not supported
    bool ForceSortedMerge = false;
};

TEquiJoinLinkSettings GetEquiJoinLinkSettings(const TExprNode& linkSettings);
TExprNode::TPtr BuildEquiJoinLinkSettings(const TEquiJoinLinkSettings& linkSettings, TExprContext& ctx);

TExprNode::TPtr RemapNonConvertibleMemberForJoin(TPositionHandle pos, const TExprNode::TPtr& memberValue,
    const TTypeAnnotationNode& memberType, const TTypeAnnotationNode& unifiedType, TExprContext& ctx);

TExprNode::TPtr PrepareListForJoin(TExprNode::TPtr list, const TTypeAnnotationNode::TListType& keyTypes, TExprNode::TListType& keys, bool payload, bool optional, bool filter, TExprContext& ctx);
TExprNode::TPtr PrepareListForJoin(TExprNode::TPtr list, const TTypeAnnotationNode::TListType& keyTypes, TExprNode::TListType& keys, TExprNode::TListType&& payloads, bool payload, bool optional, bool filter, TExprContext& ctx);

template<bool Squeeze = false>
TExprNode::TPtr MakeDictForJoin(TExprNode::TPtr&& list, bool payload, bool multi, TExprContext& ctx);

TExprNode::TPtr MakeCrossJoin(TPositionHandle pos, TExprNode::TPtr left, TExprNode::TPtr right, TExprContext& ctx);

void GatherAndTerms(const TExprNode::TPtr& predicate, TExprNode::TListType& andTerms, bool& isPg, TExprContext& ctx);
TExprNode::TPtr FuseAndTerms(TPositionHandle position, const TExprNode::TListType& andTerms, const TExprNode::TPtr& exclude, bool isPg, TExprContext& ctx);

bool IsEquality(TExprNode::TPtr predicate, TExprNode::TPtr& left, TExprNode::TPtr& right);

void GatherJoinInputs(const TExprNode::TPtr& expr, const TExprNode& row,
    const TParentsMap& parentsMap, const THashMap<TString, TString>& backRenameMap,
    const TJoinLabels& labels, TSet<ui32>& inputs, TSet<TStringBuf>& usedFields);


}
