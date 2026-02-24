#pragma once

#include "kqp_info_unit.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TSortElement {
    TSortElement(const TInfoUnit& column, bool asc, bool nullsFirst) : SortColumn(column), Ascending(asc), NullsFirst(nullsFirst) {}
    TInfoUnit SortColumn;
    bool Ascending = true;
    bool NullsFirst = true;
};

/**
 * Connection structs for the Stage graph
 * We make a special case for a Source connection that is required due to the limitation of the Data shard sources
 */
struct TConnection : TSimpleRefCount<TConnection> {
    TConnection(TString type, NYql::EStorageType fromSourceStageStorageType, ui32 outputIndex)
        : Type(type)
        , FromSourceStageStorageType(fromSourceStageStorageType)
        , OutputIndex(outputIndex) {
    }
    virtual ~TConnection() = default;

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage, TExprContext& ctx) = 0;
    template <typename T>
    TExprNode::TPtr BuildConnectionImpl(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage, TExprContext& ctx);
    ui32 GetOutputIndex() const { return OutputIndex; }

    TString Type;
    NYql::EStorageType FromSourceStageStorageType;
    ui32 OutputIndex;
};

struct TBroadcastConnection: public TConnection {
    TBroadcastConnection(NYql::EStorageType fromSourceStageStorageType = NYql::EStorageType::NA, ui32 outputIndex = 0)
        : TConnection("Broadcast", fromSourceStageStorageType, outputIndex) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                            TExprContext& ctx) override;
};

struct TMapConnection: public TConnection {
    TMapConnection(NYql::EStorageType fromSourceStageStorageType = NYql::EStorageType::NA, ui32 outputIndex = 0)
        : TConnection("Map", fromSourceStageStorageType, outputIndex) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                            TExprContext& ctx) override;
};

struct TUnionAllConnection: public TConnection {
    TUnionAllConnection(NYql::EStorageType fromSourceStageStorageType = NYql::EStorageType::NA, ui32 outputIndex = 0)
        : TConnection("UnionAll", fromSourceStageStorageType, outputIndex) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                            TExprContext& ctx) override;
};

struct TShuffleConnection: public TConnection {
    TShuffleConnection(const TVector<TInfoUnit>& keys, NYql::EStorageType fromSourceStageStorageType = NYql::EStorageType::NA, ui32 outputIndex = 0)
        : TConnection("Shuffle", fromSourceStageStorageType, outputIndex)
        , Keys(keys) {
    }

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                            TExprContext& ctx) override;

    TVector<TInfoUnit> Keys;
};

struct TMergeConnection: public TConnection {
    TMergeConnection(const TVector<TSortElement>& order, NYql::EStorageType fromSourceStageStorageType = NYql::EStorageType::NA, ui32 outputIndex = 0)
        : TConnection("Merge", fromSourceStageStorageType, outputIndex)
        , Order(order) {
    }

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                            TExprContext& ctx) override;

    TVector<TSortElement> Order;
};

struct TSourceConnection: public TConnection {
    TSourceConnection()
        : TConnection("Source", NYql::EStorageType::RowStorage, 0) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprNode::TPtr& newStage,
                                            TExprContext& ctx) override;
};

/**
 * Stage graph
 *
 * TODO: Add validation, clean up interfaces
 */

struct TStageGraph {
    struct TSourceStageTraits {
        TSourceStageTraits(TVector<std::pair<TString, TInfoUnit>>&& renames, const NYql::EStorageType storageType)
            : Renames(std::move(renames))
            , StorageType(storageType) {
        }
        TVector<std::pair<TString, TInfoUnit>> Renames;
        NYql::EStorageType StorageType;
    };

    TVector<ui32> StageIds;
    THashMap<ui32, TSourceStageTraits> SourceStageRenames;
    THashMap<ui32, TVector<ui32>> StageInputs;
    THashMap<ui32, TVector<ui32>> StageOutputs;
    THashMap<std::pair<ui32, ui32>, TVector<TIntrusivePtr<TConnection>>> Connections;
    THashMap<ui32, ui32> StageOutputIndices;

    ui32 AddStage() {
        ui32 newStageId = StageIds.size();
        StageIds.push_back(newStageId);
        StageInputs[newStageId] = TVector<ui32>();
        StageOutputs[newStageId] = TVector<ui32>();
        return newStageId;
    }

    ui32 AddSourceStage(const TVector<TString>& columns, const TVector<TInfoUnit>& renames, const NYql::EStorageType& storageType,
                       bool needsMap = true) {
        ui32 res = AddStage();
        TVector<std::pair<TString, TInfoUnit>> renamePairs;
        if (needsMap) {
            for (size_t i = 0; i < columns.size(); i++) {
                renamePairs.emplace_back(columns[i], renames[i]);
            }
        }

        SourceStageRenames.insert({res, TSourceStageTraits(std::move(renamePairs), storageType)});
        return res;
    }

    bool IsSourceStage(const ui32 id) const {
        return SourceStageRenames.contains(id);
    }

    bool IsSourceStageRowType(const ui32 id) const {
        return IsSourceStageTypeImpl(id, NYql::EStorageType::RowStorage);
    }

    bool IsSourceStageColumnType(const ui32 id) const {
        return IsSourceStageTypeImpl(id, NYql::EStorageType::ColumnStorage);
    }

    NYql::EStorageType GetStorageType(const ui32 id) const {
        auto it = SourceStageRenames.find(id);
        if (it != SourceStageRenames.end()) {
            return it->second.StorageType;
        }
        return NYql::EStorageType::NA;
    }

    void Connect(ui32 from, ui32 to, TIntrusivePtr<TConnection> connection) {
        auto &outputs = StageOutputs.at(from);
        outputs.push_back(to);
        auto &inputs = StageInputs.at(to);
        inputs.push_back(from);
        Connections[std::make_pair(from, to)].push_back(connection);
    }

    TVector<TIntrusivePtr<TConnection>> GetConnections(ui32 from, ui32 to) { return Connections.at(std::make_pair(from, to)); }

    /**
     * Generate an expression for stage inputs
     * The complication is the special handling of Source stage due to limitation of data shard reader
     */
    std::pair<TExprNode::TPtr, TExprNode::TPtr> GenerateStageInput(ui32 &stageInputCounter, TExprNode::TPtr &node, TExprContext &ctx,
                                                                   ui32 fromStage);

    ui32 GetOutputIndex(ui32 stageIndex) {
        ui32 outputIndex{0};
        auto it = StageOutputIndices.find(stageIndex);
        if (it != StageOutputIndices.end()) {
            it->second++;
            outputIndex = it->second;
        } else {
            StageOutputIndices[stageIndex] = 0;
        }
        return outputIndex;
    }

    void TopologicalSort();
private:

    bool IsSourceStageTypeImpl(const ui32 id, const NYql::EStorageType tableStorageType) const {
        auto it = SourceStageRenames.find(id);
        if (it != SourceStageRenames.end()) {
            return it->second.StorageType == tableStorageType;
        }
        return false;
    }
};

}
}
