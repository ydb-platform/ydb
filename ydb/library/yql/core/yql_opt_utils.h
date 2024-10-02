#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_window.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

#include <functional>

namespace NYql {

struct TTypeAnnotationContext;

bool IsJustOrSingleAsList(const TExprNode& node);
bool IsTransparentIfPresent(const TExprNode& node);
bool IsPredicateFlatMap(const TExprNode& node);
bool IsFilterFlatMap(const NNodes::TCoLambda& lambda);
bool IsListReorder(const TExprNode& node);
bool IsRenameFlatMap(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode);
bool IsRenameFlatMapWithMapping(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode, THashMap<TString, TString>& renameMap);
bool IsRenameOrApplyFlatMapWithMapping(const NNodes::TCoFlatMapBase& node, TExprNode::TPtr& structNode, THashMap<TString, TString>& renameMap, TSet<TString>& apply);
bool IsPassthroughFlatMap(const NNodes::TCoFlatMapBase& flatmap, TMaybe<THashSet<TStringBuf>>* passthroughFields, bool analyzeJustMember = false);
bool IsPassthroughLambda(const NNodes::TCoLambda& lambda, TMaybe<THashSet<TStringBuf>>* passthroughFields, bool analyzeJustMember = false);
bool IsTablePropsDependent(const TExprNode& node);

bool HasOnlyOneJoinType(const TExprNode& joinTree, TStringBuf joinType);

TExprNode::TPtr KeepColumnOrder(const TExprNode::TPtr& node, const TExprNode& src, TExprContext& ctx, const TTypeAnnotationContext& typeCtx);
TExprNode::TPtr KeepColumnOrder(const TColumnOrder& order, const TExprNode::TPtr& node, TExprContext& ctx);

// returns true if usedFields contains subset of fields
template<class TFieldsSet>
bool HaveFieldsSubset(const TExprNode::TPtr& start, const TExprNode& arg, TFieldsSet& usedFields, const TParentsMap& parentsMap,
                    bool allowDependsOn = true);

template<class TFieldsSet>
TExprNode::TPtr FilterByFields(TPositionHandle position, const TExprNode::TPtr& input, const TFieldsSet& subsetFields,
    TExprContext& ctx, bool singleValue);

TExprNode::TPtr AddMembersUsedInside(const TExprNode::TPtr& start, const TExprNode& arg, TExprNode::TPtr&& members, const TParentsMap& parentsMap, TExprContext& ctx);

bool IsDepended(const TExprNode& from, const TExprNode& to);
bool MarkDepended(const TExprNode& from, const TExprNode& to, TNodeMap<bool>& deps);
bool IsEmpty(const TExprNode& node, const TTypeAnnotationContext& typeCtx);
bool IsEmptyContainer(const TExprNode& node);

const TTypeAnnotationNode* RemoveOptionalType(const TTypeAnnotationNode* type);
const TTypeAnnotationNode* RemoveAllOptionals(const TTypeAnnotationNode* type);

TExprNode::TPtr GetSetting(const TExprNode& settings, const TStringBuf& name);
TExprNode::TPtr FilterSettings(const TExprNode& settings, const THashSet<TStringBuf>& names, TExprContext& ctx);
bool HasSetting(const TExprNode& settings, const TStringBuf& name);
bool HasAnySetting(const TExprNode& settings, const THashSet<TString>& names);
TExprNode::TPtr RemoveSetting(const TExprNode& settings, const TStringBuf& name, TExprContext& ctx);
TExprNode::TPtr AddSetting(const TExprNode& settings, TPositionHandle pos, const TString& name, const TExprNode::TPtr& value, TExprContext& ctx);
TExprNode::TPtr AddSetting(const TExprNode& settings, const TExprNode::TPtr& newSetting, TExprContext& ctx);
TExprNode::TPtr MergeSettings(const TExprNode& settings1, const TExprNode& settings2, TExprContext& ctx);
TExprNode::TPtr ReplaceSetting(const TExprNode& settings, TPositionHandle pos, const TString& name, const TExprNode::TPtr& value, TExprContext& ctx);
TExprNode::TPtr ReplaceSetting(const TExprNode& settings, const TExprNode::TPtr& newSetting, TExprContext& ctx);

enum class EDictType {
    Hashed,
    Sorted,
    Auto,
};

TMaybe<TIssue> ParseToDictSettings(const TExprNode& node, TExprContext& ctx, TMaybe<EDictType>& type, TMaybe<bool>& isMany, TMaybe<ui64>& itemsCount, bool& isCompact);
EDictType SelectDictType(EDictType type, const TTypeAnnotationNode* keyType);

using MemberUpdaterFunc = std::function<bool (TString& memberName, const TTypeAnnotationNode* TypeAnnotation)>;
bool UpdateStructMembers(TExprContext& ctx, const TExprNode::TPtr& node, const TStringBuf& goal, TExprNode::TListType& members,
    MemberUpdaterFunc updaterFunc = MemberUpdaterFunc(), const TTypeAnnotationNode* nodeType = nullptr);

TExprNode::TPtr MakeSingleGroupRow(const TExprNode& aggregateNode, TExprNode::TPtr reduced, TExprContext& ctx);
TExprNode::TPtr ExpandRemoveMember(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandRemoveMembers(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandRemovePrefixMembers(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandFlattenMembers(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandFlattenStructs(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandDivePrefixMembers(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandAddMember(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandReplaceMember(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandFlattenByColumns(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandCastStruct(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr ExpandSkipNullFields(const TExprNode::TPtr& node, TExprContext& ctx);

void ExtractSimpleKeys(const TExprNode* keySelectorBody, const TExprNode* keySelectorArg, TVector<TStringBuf>& columns);
inline void ExtractSimpleKeys(const TExprNode& keySelectorLambda, TVector<TStringBuf>& columns) {
    ExtractSimpleKeys(keySelectorLambda.Child(1), keySelectorLambda.Child(0)->Child(0), columns);
}

TExprNode::TPtr MakeNull(TPositionHandle position, TExprContext& ctx);
TExprNode::TPtr MakeConstMap(TPositionHandle position, const TExprNode::TPtr& input, const TExprNode::TPtr& value, TExprContext& ctx);
TExprNode::TPtr MakeBoolNothing(TPositionHandle position, TExprContext& ctx);
TExprNode::TPtr MakeBool(TPositionHandle position, bool value, TExprContext& ctx);
TExprNode::TPtr MakeOptionalBool(TPositionHandle position, bool value, TExprContext& ctx);
template <bool Bool>
TExprNode::TPtr MakeBool(TPositionHandle position, TExprContext& ctx);
TExprNode::TPtr MakePgBool(TPositionHandle position, bool value, TExprContext& ctx);
TExprNode::TPtr MakeIdentityLambda(TPositionHandle position, TExprContext& ctx);

constexpr std::initializer_list<std::string_view> SkippableCallables = {"Unordered", "AssumeSorted", "AssumeUnique", "AssumeDistinct",
    "AssumeChopped", "AssumeColumnOrder", "AssumeAllMembersNullableAtOnce", "AssumeConstraints"};

const TExprNode& SkipCallables(const TExprNode& node, const std::initializer_list<std::string_view>& skipCallables);

void ExtractSortKeyAndOrder(TPositionHandle pos, const TExprNode::TPtr& sortTraitsNode, TExprNode::TPtr& sortKey, TExprNode::TPtr& sortOrder, TExprContext& ctx);
void ExtractSessionWindowParams(TPositionHandle pos, const TExprNode::TPtr& sessionTraits, TExprNode::TPtr& sessionKey,
    const TTypeAnnotationNode*& sessionKeyType, const TTypeAnnotationNode*& sessionParamsType, TExprNode::TPtr& sessionSortTraits, TExprNode::TPtr& sessionInit,
    TExprNode::TPtr& sessionUpdate, TExprContext& ctx);

void ExtractSortKeyAndOrder(TPositionHandle pos, const TExprNode::TPtr& sortTraitsNode, TSortParams& sortParams, TExprContext& ctx);
void ExtractSessionWindowParams(TPositionHandle pos, TSessionWindowParams& sessionParams, TExprContext& ctx);

TExprNode::TPtr BuildKeySelector(TPositionHandle pos, const TStructExprType& rowType, const TExprNode::TPtr& keyColumns, TExprContext& ctx);

template <bool Cannonize, bool EnableNewOptimizers = true>
TExprNode::TPtr OptimizeIfPresent(const TExprNode::TPtr& node, TExprContext& ctx);
TExprNode::TPtr OptimizeExists(const TExprNode::TPtr& node, TExprContext& ctx);

bool WarnUnroderedSubquery(const TExprNode& unourderedSubquery, TExprContext& ctx);

std::pair<TExprNode::TPtr, TExprNode::TPtr> ReplaceDependsOn(TExprNode::TPtr lambda, TExprContext& ctx, TTypeAnnotationContext* typeCtx);

TStringBuf GetEmptyCollectionName(ETypeAnnotationKind kind);
inline TStringBuf GetEmptyCollectionName(const TTypeAnnotationNode* type) {
    return GetEmptyCollectionName(type->GetKind());
}

const TItemExprType* GetLightColumn(const TStructExprType& type);

// returned value exists as long as lambda object exists
TVector<TStringBuf> GetCommonKeysFromVariantSelector(const NNodes::TCoLambda& lambda);

bool IsIdentityLambda(const TExprNode& lambda);

TExprNode::TPtr MakeExpandMap(TPositionHandle pos, const TVector<TString>& columns, const TExprNode::TPtr& input, TExprContext& ctx);
TExprNode::TPtr MakeNarrowMap(TPositionHandle pos, const TVector<TString>& columns, const TExprNode::TPtr& input, TExprContext& ctx);

TExprNode::TPtr FindNonYieldTransparentNode(const TExprNode::TPtr& root, const TTypeAnnotationContext& typeCtx, TNodeSet flowSources = TNodeSet());
bool IsYieldTransparent(const TExprNode::TPtr& root, const TTypeAnnotationContext& typeCtx);

bool IsStrict(const TExprNode::TPtr& node);
TMaybe<bool> IsStrictNoRecurse(const TExprNode& node);

bool HasDependsOn(const TExprNode::TPtr& node, const TExprNode::TPtr& arg);

TExprNode::TPtr KeepSortedConstraint(TExprNode::TPtr node, const TSortedConstraintNode* sorted, const TTypeAnnotationNode* rowType, TExprContext& ctx);
TExprNode::TPtr MakeSortByConstraint(TExprNode::TPtr node, const TSortedConstraintNode* sorted, const TTypeAnnotationNode* rowType, TExprContext& ctx);
TExprNode::TPtr KeepConstraints(TExprNode::TPtr node, const TExprNode& src, TExprContext& ctx);

void OptimizeSubsetFieldsForNodeWithMultiUsage(const TExprNode::TPtr& node, const TParentsMap& parentsMap,
    TNodeOnNodeOwnedMap& toOptimize, TExprContext& ctx,
    std::function<TExprNode::TPtr(const TExprNode::TPtr&, const TExprNode::TPtr&, const TParentsMap&, TExprContext&)> handler);

template<bool Ordered = false>
std::optional<TPartOfConstraintBase::TPathType> GetPathToKey(const TExprNode& body, const TExprNode& arg);
template<bool Ordered = false>
std::optional<std::pair<TPartOfConstraintBase::TPathType, ui32>> GetPathToKey(const TExprNode& body, const TExprNode::TChildrenType& args);

template<bool Ordered = false>
TPartOfConstraintBase::TSetType GetPathsToKeys(const TExprNode& body, const TExprNode& arg);

// generates column names with pattern "prefixN" that do not clash with source columns
// prefix should start with "_yql"
TVector<TString> GenNoClashColumns(const TStructExprType& source, TStringBuf prefix, size_t count);

}
