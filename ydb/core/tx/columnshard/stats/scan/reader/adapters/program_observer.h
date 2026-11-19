#pragma once

#include <ydb/core/formats/arrow/program/observer.h>
#include <ydb/core/tx/columnshard/stats/scan/observation.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash.h>

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {
class TCompiledGraph;
}

namespace NKikimr::NOlap::NStats::NScan {

class TDecodeObserver;

enum class EProgramPhase {
    Filter,
    IndexCheck,
    Aggregation,
    FetchColumns,
    Projection,
};

class TProgramObserver final: public NArrow::NSSA::NGraph::NExecution::IExecutionObserver {
    struct TFrame {
        ui64 NodeId = 0;
        std::shared_ptr<TOperationScope> Scope;
        bool InputObserved = false;
        bool Finished = false;
        bool MeasureExecution = false;
        bool Pending = false;
        EProgramPhase Phase = EProgramPhase::FetchColumns;
    };

    struct TPhaseState {
        std::shared_ptr<TOperationScope> Scope;
        size_t PendingCalls = 0;
        std::optional<ui64> LastOutputRows;
    };

    std::unique_ptr<TLocalStats> LocalStats;
    std::unique_ptr<TDecodeObserver> DecodeObserver;
    const THashMap<ui32, TString> ColumnNames;
    const THashMap<ui32, TString> Expressions;
    const THashMap<ui64, EProgramPhase> Phases;
    const THashMap<ui64, ui64> NodeOrder;
    THashMap<EProgramPhase, TPhaseState> PhaseScopes;
    std::optional<EProgramPhase> CurrentPhase;
    THashMap<ui64, std::unique_ptr<TFrame>> Frames;
    std::vector<TFrame*> ActiveFrames;
    YDB_ACCESSOR_DEF(TCallHandle, ParentCall);

    TFrame* FindFrame(ui64 nodeId) noexcept;
    EProgramPhase PhaseFor(ui64 nodeId, const NArrow::NSSA::IResourceProcessor& processor) const;
    TOperationNode* FindPhaseNode(EProgramPhase phase) const noexcept;
    TOperationNode* PhaseNode(EProgramPhase phase);
    TOperationNode* FrameNode(const TFrame& frame) const;
    void ActivatePhase(TFrame& frame, const NArrow::NSSA::TAccessorsCollection& resources);
    void FinishPhase(EProgramPhase phase);
    void RefreshCurrentNode();
    static THashMap<ui64, EProgramPhase> DescribePhases(const NArrow::NSSA::NGraph::NExecution::TCompiledGraph& graph);

public:
    TOperationNode* GetCurrentNode() const;
    ~TProgramObserver() override;
    explicit TProgramObserver(std::unique_ptr<TLocalStats> localStats, THashMap<ui32, TString> columnNames = {},
        THashMap<ui32, TString> expressions = {}, THashMap<ui64, EProgramPhase> phases = {}, THashMap<ui64, ui64> nodeOrder = {});

    NArrow::NAccessor::IDecodeObserver* GetDecodeObserver() const noexcept override;
    TCallHandle GetCurrentCallHandle() const noexcept;
    void FinishObservedExecution();

    void OnEnter(ui64 nodeId, const NArrow::NSSA::IResourceProcessor& processor, const NArrow::NSSA::TAccessorsCollection& resources) override;
    void OnExecute(ui64 nodeId, const NArrow::NSSA::TAccessorsCollection& resources) override;
    void OnResult(
        ui64 nodeId, const NArrow::NSSA::TAccessorsCollection& resources, const TConclusion<NArrow::NSSA::TExecutionResult>& result) override;
    void OnExit(ui64 nodeId, const NArrow::NSSA::TAccessorsCollection& resources, bool skipped) override;
    void OnException(ui64 nodeId) override;

    static std::shared_ptr<TProgramObserver> Create(std::unique_ptr<TLocalStats> localStats,
        const NArrow::NSSA::NGraph::NExecution::TCompiledGraph& graph, const NArrow::NSSA::IColumnResolver& resolver);

    template <class TSource, class TVisitor>
    static std::shared_ptr<TProgramObserver> CreateForSource(
        TSource& source, const NArrow::NSSA::NGraph::NExecution::TCompiledGraph& graph, TVisitor& visitor) {
        auto* local = source.GetLocalStats();
        if (!local) {
            return {};
        }
        const auto* resolver = source.GetContext()->GetCommonContext()->GetResolver();
        if (!resolver) {
            local->ObserveLoss();
            return {};
        }
        auto observer = Create(local->Fork(), graph, *resolver);
        if (observer) {
            observer->SetParentCall(source.GetActiveStatistics());
        }
        visitor.SetExecutionObserver(observer);
        return observer;
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
