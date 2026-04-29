#pragma once

#include "kqp_info_unit.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/ast/yql_expr.h>
#include <ydb/library/yql/dq/common/dq_common.h>

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
struct TConnection: TSimpleRefCount<TConnection> {
    TConnection(TString type, ui32 outputIndex)
        : Type(type)
        , OutputIndex(outputIndex) {
    }
    virtual ~TConnection() = default;

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) = 0;
    template <typename T>
    TExprNode::TPtr BuildConnectionImpl(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx);
    ui32 GetOutputIndex() const {
        return OutputIndex;
    }
    virtual TString GetExplainName() const = 0;

    TString Type;
    ui32 OutputIndex;
};

struct TBroadcastConnection: public TConnection {
    TBroadcastConnection(ui32 outputIndex = 0)
        : TConnection("Broadcast", outputIndex) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) override;
    virtual TString GetExplainName() const override {
        return "Broadcast";
    }
};

struct TMapConnection: public TConnection {
    TMapConnection(ui32 outputIndex = 0)
        : TConnection("Map", outputIndex) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) override;
    virtual TString GetExplainName() const override {
        return "Map";
    }
};

struct TUnionAllConnection: public TConnection {
    TUnionAllConnection(ui32 outputIndex = 0, bool parallel = false)
        : TConnection("UnionAll", outputIndex)
        , Parallel(parallel) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) override;
    virtual TString GetExplainName() const override {
        return "UnionAll";
    }

private:
    bool Parallel{false};
};

struct TShuffleConnection: public TConnection {
    TShuffleConnection(const TVector<TInfoUnit>& keys,
                       ui32 outputIndex,
                       NDq::EHashShuffleFuncType hashFuncType,
                       bool useSpilling = false)
        : TConnection("Shuffle", outputIndex)
        , Keys(keys)
        , HashFuncType(hashFuncType)
        , UseSpilling(useSpilling) {
    }

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) override;
    virtual TString GetExplainName() const override {
        return "HashShuffle";
    }

    TVector<TInfoUnit> Keys;
    NDq::EHashShuffleFuncType HashFuncType;
    bool UseSpilling = false;
};

struct TMergeConnection: public TConnection {
    TMergeConnection(const TVector<TSortElement>& order, ui32 outputIndex = 0)
        : TConnection("Merge", outputIndex)
        , Order(order) {
    }

    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) override;
    virtual TString GetExplainName() const override {
        return "Merge";
    }

    TVector<TSortElement> Order;
};

struct TSourceConnection: public TConnection {
    TSourceConnection()
        : TConnection("Source", 0) {
    }
    virtual TExprNode::TPtr BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) override;
    virtual TString GetExplainName() const override {
        return "Source";
    }
};

template <typename T>
bool IsConnection(TIntrusivePtr<TConnection> connection) {
    return dynamic_cast<T*>(connection.get());
}

/**
 * Stage graph
 *
 * TODO: Add validation, clean up interfaces
 */
struct TStageGraph {
    struct TSourceStageTraits {
        TSourceStageTraits(const NYql::EStorageType storageType)
            : StorageType(storageType) {
        }
        NYql::EStorageType StorageType;
    };

    TList<ui32> StageIds;
    THashMap<ui32, TSourceStageTraits> SourceStages;
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

    ui32 AddSourceStage(const NYql::EStorageType& storageType) {
        ui32 res = AddStage();

        SourceStages.insert({res, TSourceStageTraits(storageType)});
        return res;
    }

    bool IsSourceStage(const ui32 id) const {
        return SourceStages.contains(id);
    }

    bool IsSourceStageRowType(const ui32 id) const {
        return IsSourceStageTypeImpl(id, NYql::EStorageType::RowStorage);
    }

    bool IsSourceStageColumnType(const ui32 id) const {
        return IsSourceStageTypeImpl(id, NYql::EStorageType::ColumnStorage);
    }

    NYql::EStorageType GetStorageType(const ui32 id) const {
        auto it = SourceStages.find(id);
        if (it != SourceStages.end()) {
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

    void UpdateConnection(ui32 from, ui32 to, TIntrusivePtr<TConnection> connection) {
        const auto it = Connections.find(std::make_pair(from, to));
        Y_ENSURE(it != Connections.end(), "Cannot find a connection to update.");
        auto& connections = it->second;
        Y_ENSURE(connections.size() == 1);
        connections.clear();
        connections.push_back(connection);
    }

    TVector<TIntrusivePtr<TConnection>> GetConnections(ui32 from, ui32 to) { return Connections.at(std::make_pair(from, to)); }

    TIntrusivePtr<TConnection> GetInputConnection(ui32 stageId, ui32 inputIndex) const;

    /**
     * Generate an expression for stage inputs
     * The complication is the special handling of Source stage due to limitation of data shard reader
     */
    std::pair<TExprNode::TPtr, TExprNode::TPtr> GenerateStageInput(ui32& stageInputCounter, TPositionHandle pos, TExprContext& ctx) const;

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
        auto it = SourceStages.find(id);
        if (it != SourceStages.end()) {
            return it->second.StorageType == tableStorageType;
        }
        return false;
    }
};

}
}
