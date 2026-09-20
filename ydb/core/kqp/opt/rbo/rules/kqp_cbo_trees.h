#pragma once

#include <ydb/core/kqp/opt/cbo/cbo_interesting_orderings.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

#include <memory>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NYql {

struct TTypeAnnotationContext;

} // namespace NYql

namespace NKikimr::NKqp {

constexpr size_t MaxShuffleEliminationRelationCount = 256;

struct TShuffleEliminationContext {
    TSimpleSharedPtr<TOrderingsStateMachine> FSM;
    TTableAliasMap TableAliasMap;
};

struct TCBOBoundaryEdge {
    IOperator* Parent = nullptr;
    ui32 ChildIndex = 0;

    bool operator==(const TCBOBoundaryEdge& other) const {
        return Parent == other.Parent && ChildIndex == other.ChildIndex;
    }

    struct THashFunction {
        size_t operator()(const TCBOBoundaryEdge& key) const {
            return THash<IOperator*>()(key.Parent) ^ THash<ui32>()(key.ChildIndex);
        }
    };
};

struct TCBOLeaf {
    TIntrusivePtr<IOperator> Op;
    TCBOBoundaryEdge Edge;
    TString RelationName;
    TString SourceTableName;
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> ColumnsToCBO;
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> CBOToColumns;
};

TVector<TCBOLeaf> BuildCBOLeaves(const TOpCBOTree& cboTree);

TShuffleEliminationContext BuildShuffleEliminationContext(
    const std::shared_ptr<TJoinOptimizerNode>& joinTree,
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    const TVector<TCBOLeaf>& leaves);

std::shared_ptr<TJoinOptimizerNode> ConvertJoinTree(
    TIntrusivePtr<TOpCBOTree>& cboTree,
    NYql::TTypeAnnotationContext& typeCtx,
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    const TVector<TCBOLeaf>& leaves);

TIntrusivePtr<IOperator> ConvertOptimizedTree(
    std::shared_ptr<IBaseOptimizerNode> tree,
    const TVector<TCBOLeaf>& leaves,
    TPositionHandle pos);

std::string FormatJoinTree(const char* title, const std::shared_ptr<IBaseOptimizerNode>& joinTree);

} // namespace NKikimr::NKqp
