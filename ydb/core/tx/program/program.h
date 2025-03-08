#pragma once

#include "registry.h"

#include <ydb/core/formats/arrow/process_columns.h>
#include <ydb/core/formats/arrow/program/custom_registry.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/checker.h>

#include <ydb/library/formats/arrow/protos/ssa.pb.h>

namespace NKikimr::NOlap {

class TProgramContainer {
private:
    using TColumnInfo = NArrow::NSSA::TColumnInfo;
    NKikimrSSA::TProgram ProgramProto;
    std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph> Program;
    std::shared_ptr<arrow::RecordBatch> ProgramParameters;   // TODO
    NArrow::NSSA::TKernelsRegistry KernelsRegistry;
    std::optional<THashSet<ui32>> OverrideProcessingColumnsSet;
    std::optional<std::vector<ui32>> OverrideProcessingColumnsVector;

public:
    bool IsGenerated(const ui32 columnId) const {
        if (!Program) {
            return false;
        }
        return Program->IsGenerated(columnId);
    }

    const THashSet<ui32>& GetSourceColumns() const;
    const THashSet<ui32>& GetEarlyFilterColumns() const;
    const THashSet<ui32>& GetProcessingColumns() const;

    TString ProtoDebugString() const {
        return ProgramProto.DebugString();
    }

    TString DebugString() const {
        return Program ? Program->DebugString() : "NO_PROGRAM";
    }

    bool HasOverridenProcessingColumnIds() const {
        return !!OverrideProcessingColumnsVector;
    }

    bool HasProcessingColumnIds() const {
        return !!Program || !!OverrideProcessingColumnsVector;
    }
    void OverrideProcessingColumns(const std::vector<TString>& data, const NArrow::NSSA::IColumnResolver& resolver) {
        if (data.empty()) {
            return;
        }
        AFL_VERIFY(!Program);
        std::vector<ui32> columnsVector;
        THashSet<ui32> columnsSet;
        for (auto&& i : data) {
            const ui32 id = resolver.GetColumnIdVerified(i);
            columnsVector.emplace_back(id);
            columnsSet.emplace(id);
        }
        OverrideProcessingColumnsVector = std::move(columnsVector);
        OverrideProcessingColumnsSet = std::move(columnsSet);
    }

    void OverrideProcessingColumns(const std::vector<ui32>& data) {
        std::vector<ui32> columnsVector = data;
        THashSet<ui32> columnsSet(data.begin(), data.end());
        OverrideProcessingColumnsVector = std::move(columnsVector);
        OverrideProcessingColumnsSet = std::move(columnsSet);
    }

    [[nodiscard]] TConclusionStatus Init(
        const NArrow::NSSA::IColumnResolver& columnResolver, NKikimrSchemeOp::EOlapProgramType programType, TString serializedProgram) noexcept;
    [[nodiscard]] TConclusionStatus Init(const NArrow::NSSA::IColumnResolver& columnResolver, const NKikimrSSA::TOlapProgram& olapProgramProto) noexcept;
    [[nodiscard]] TConclusionStatus Init(const NArrow::NSSA::IColumnResolver& columnResolver, const NKikimrSSA::TProgram& programProto) noexcept;

    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& GetChainVerified() const {
        AFL_VERIFY(!!Program);
        return Program;
    }

    const std::shared_ptr<NArrow::NSSA::NGraph::NExecution::TCompiledGraph>& GetGraphOptional() const {
        return Program;
    }

    [[nodiscard]] TConclusionStatus ApplyProgram(const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& collection,
        const std::shared_ptr<NArrow::NSSA::IDataSource>& source) const;
    [[nodiscard]] TConclusion<std::shared_ptr<arrow::RecordBatch>> ApplyProgram(
        const std::shared_ptr<arrow::RecordBatch>& batch, const NArrow::NSSA::IColumnResolver& resolver) const;

    bool HasProgram() const;

private:
    [[nodiscard]] TConclusionStatus ParseProgram(const NArrow::NSSA::IColumnResolver& columnResolver, const NKikimrSSA::TProgram& program);
};

}   // namespace NKikimr::NOlap
