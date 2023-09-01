#pragma once

#include "yql_yt_provider.h"
#include "yql_yt_optimize.h"
#include "yql_yt_op_settings.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/bitmap.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>
#include <util/generic/size_literals.h>

#include <utility>
#include <tuple>

namespace NYql {

class THorizontalJoinBase {
public:
    THorizontalJoinBase(const TYtState::TPtr& state, const std::vector<const TExprNode*>& opDepsOrder, const TOpDeps& opDeps, const TNodeSet& hasWorldDeps)
        : State_(state)
        , OpDepsOrder(opDepsOrder)
        , OpDeps(opDeps)
        , HasWorldDeps(hasWorldDeps)
        , MaxTables(State_->Configuration->MaxInputTables.Get().GetOrElse(DEFAULT_MAX_INPUT_TABLES))
        , MaxOutTables(State_->Configuration->MaxOutputTables.Get().GetOrElse(DEFAULT_MAX_OUTPUT_TABLES))
        , SwitchMemoryLimit(State_->Configuration->SwitchLimit.Get().GetOrElse(DEFAULT_SWITCH_MEMORY_LIMIT))
        , MaxJobMemoryLimit(State_->Configuration->MaxExtraJobMemoryToFuseOperations.Get())
        , MaxOperationFiles(State_->Configuration->MaxOperationFiles.Get().GetOrElse(DEFAULT_MAX_OPERATION_FILES))
        , UsedFiles(1) // jobstate
    {
    }

protected:
    enum EFeatureFlags: ui32 {
        YAMR        = 1 << 1,
        Flow        = 1 << 2,
        WeakField   = 1 << 3,
        QB2         = 1 << 4,
    };

    bool IsGoodForHorizontalJoin(NNodes::TYtMap map) const;
    NNodes::TCoLambda CleanupAuxColumns(NNodes::TCoLambda lambda, TExprContext& ctx) const;
    void ClearJoinGroup();
    void AddToJoinGroup(NNodes::TYtMap map);
    NNodes::TCoLambda BuildMapperWithAuxColumnsForSingleInput(TPositionHandle pos, bool ordered, TExprContext& ctx) const;
    NNodes::TCoLambda BuildMapperWithAuxColumnsForMultiInput(TPositionHandle pos, bool ordered, TExprContext& ctx) const;
    NNodes::TCoLambda MakeSwitchLambda(size_t mapIndex, size_t fieldsCount, bool singleInput, TExprContext& ctx) const;

protected:
    TYtState::TPtr State_;
    const std::vector<const TExprNode*>& OpDepsOrder;
    const TOpDeps& OpDeps;
    const TNodeSet& HasWorldDeps;

    const size_t MaxTables;
    const size_t MaxOutTables;
    const ui64 SwitchMemoryLimit;
    const TMaybe<ui64> MaxJobMemoryLimit;
    const size_t MaxOperationFiles;

    NNodes::TMaybeNode<NNodes::TYtDSink> DataSink;
    TDynBitMap UsesTablePath;
    TDynBitMap UsesTableRecord;
    TDynBitMap UsesTableIndex;
    EYtSettingTypes OpSettings;
    TMap<TStringBuf, ui64> MemUsage;
    size_t UsedFiles;
    TVector<NNodes::TYtMap> JoinedMaps;
};

class THorizontalJoinOptimizer: public THorizontalJoinBase {
    // Cluster, Sampling, Dependency, Flags
    using TGroupKey = std::tuple<TString, TMaybe<TSampleParams>, const TExprNode*, ui32>;

public:
    THorizontalJoinOptimizer(const TYtState::TPtr& state, const std::vector<const TExprNode*>& opDepsOrder, const TOpDeps& opDeps, const TNodeSet& hasWorldDeps, TProcessedNodesSet* processedNodes)
        : THorizontalJoinBase(state, opDepsOrder, opDeps, hasWorldDeps)
        , ProcessedNodes(processedNodes)
    {
    }

    IGraphTransformer::TStatus Optimize(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx);

private:
    TExprNode::TPtr HandleList(const TExprNode::TPtr& node, bool sectionList, TExprContext& ctx);
    void AddToGroups(NNodes::TYtMap map, size_t s, size_t p, const TExprNode* section,
        THashMap<TGroupKey, TVector<std::tuple<NNodes::TYtMap, size_t, size_t>>>& groups,
        TNodeMap<TMaybe<TGroupKey>>& processedMaps) const;
    THashMap<TGroupKey, TVector<std::tuple<NNodes::TYtMap, size_t, size_t>>> CollectGroups(const TExprNode::TPtr& node, bool sectionList) const;
    void ClearJoinGroup();
    bool MakeJoinedMap(TPositionHandle pos, TExprContext& ctx);
    TExprNode::TPtr RebuildList(const TExprNode::TPtr& node, bool sectionList, TExprContext& ctx);

private:
    TSyncMap Worlds;
    // operation -> joined_map_out
    TNodeMap<size_t> UniqMaps;
    // {section, columns, ranges} -> TMap{out_num -> path_num}
    TMap<std::tuple<size_t, const TExprNode*, const TExprNode*>, TMap<size_t, size_t>> GroupedOuts;
    // out_num -> TVector{section_num, path_num}. Cannot be joined with other outputs
    TMap<size_t, TVector<std::pair<size_t, size_t>>> ExclusiveOuts;
    size_t InputCount = 0;
    // {section or table, ranges, section settings, WeakField}
    TSet<std::tuple<const TExprNode*, const TExprNode*, const TExprNode*, bool>> GroupedInputs;

    // {section_num, path_num} -> {joined_map, out_num}
    TMap<std::pair<size_t, size_t>, TMaybe<std::pair<NNodes::TYtMap, size_t>>> InputSubsts;

    TProcessedNodesSet* ProcessedNodes;
};

class TMultiHorizontalJoinOptimizer: public THorizontalJoinBase {
    // Cluster, readers, Sampling, Dependency, Flags
    using TGroupKey = std::tuple<TString, std::set<ui64>, TMaybe<TSampleParams>, const TExprNode*, ui32>;

public:
    TMultiHorizontalJoinOptimizer(const TYtState::TPtr& state, const std::vector<const TExprNode*>& opDepsOrder, const TOpDeps& opDeps, const TNodeSet& hasWorldDeps)
        : THorizontalJoinBase(state, opDepsOrder, opDeps, hasWorldDeps)
    {
    }

    IGraphTransformer::TStatus Optimize(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx);

private:
    bool HandleGroup(const TVector<NNodes::TYtMap>& maps, TExprContext& ctx);
    void ClearJoinGroup();
    bool MakeJoinedMap(TExprContext& ctx);

private:
    TSyncMap Worlds;
    size_t InputCount = 0;
    // {section or table, ranges, section settings, WeakField}
    TSet<std::tuple<const TExprNode*, const TExprNode*, const TExprNode*, bool>> GroupedInputs;

    TNodeMap<std::pair<NNodes::TYtMap, size_t>> OutputSubsts; // original map -> joined map, out index
};

class TOutHorizontalJoinOptimizer: public THorizontalJoinBase {
    // Group by: Cluster, World, Input, Range, Sampling, Flags
    using TGroupKey = std::tuple<TString, const TExprNode*, const TExprNode*, const TExprNode*, const TExprNode*, ui32>;

public:
    TOutHorizontalJoinOptimizer(const TYtState::TPtr& state, const std::vector<const TExprNode*>& opDepsOrder, const TOpDeps& opDeps, const TNodeSet& hasWorldDeps)
        : THorizontalJoinBase(state, opDepsOrder, opDeps, hasWorldDeps)
    {
    }

    IGraphTransformer::TStatus Optimize(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx);

private:
    void ClearJoinGroup();
    bool MakeJoinedMap(TPositionHandle pos, const TGroupKey& key, const TStructExprType* itemType, TExprContext& ctx);
    bool HandleGroup(TPositionHandle pos, const TGroupKey& key, const TVector<NNodes::TYtMap>& maps, TExprContext& ctx);
    bool IsGoodForOutHorizontalJoin(const TExprNode* op);

private:
    TSet<TStringBuf> UsedFields;
    TSet<TStringBuf> WeakFields;
    TSet<TString> UsedSysFields;

    TNodeMap<std::pair<NNodes::TYtMap, size_t>> OutputSubsts; // original map -> joined map, out index
    TNodeMap<bool> ProcessedOps;
};

} // NYql
