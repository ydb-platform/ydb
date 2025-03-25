#pragma once
#include "registry.h"

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/formats/arrow/program/aggr_common.h>
#include <ydb/core/formats/arrow/program/assign_const.h>
#include <ydb/core/formats/arrow/program/functions.h>
#include <ydb/core/formats/arrow/program/graph_execute.h>
#include <ydb/core/formats/arrow/program/graph_optimization.h>

#include <ydb/library/formats/arrow/protos/ssa.pb.h>

namespace NKikimr::NArrow::NSSA {

namespace NAggregation {
class TAggregateFunction;
}

class TProgramBuilder {
private:
    const IColumnResolver& ColumnResolver;
    const TKernelsRegistry& KernelsRegistry;
    mutable THashMap<ui32, std::shared_ptr<arrow::Scalar>> Constants;

    NArrow::NSSA::NGraph::NOptimization::TGraph::TBuilder Builder;

public:
    mutable THashMap<ui32, TColumnInfo> Sources;

    explicit TProgramBuilder(const NArrow::NSSA::IColumnResolver& columnResolver, const TKernelsRegistry& kernelsRegistry)
        : ColumnResolver(columnResolver)
        , KernelsRegistry(kernelsRegistry)
        , Builder(ColumnResolver) {
    }

private:
    TColumnInfo GetColumnInfo(const NKikimrSSA::TProgram::TColumn& column) const;

    std::string GenerateName(const NKikimrSSA::TProgram::TColumn& column) const;
    [[nodiscard]] TConclusion<std::shared_ptr<IStepFunction>> MakeFunction(
        const TColumnInfo& name, const NKikimrSSA::TProgram::TAssignment::TFunction& func, std::vector<TColumnChainInfo>& arguments) const;
    [[nodiscard]] TConclusion<std::shared_ptr<TConstProcessor>> MakeConstant(
        const TColumnInfo& name, const NKikimrSSA::TProgram::TConstant& constant) const;
    [[nodiscard]] TConclusion<std::shared_ptr<TConstProcessor>> MaterializeParameter(const TColumnInfo& name,
        const NKikimrSSA::TProgram::TParameter& parameter, const std::shared_ptr<arrow::RecordBatch>& parameterValues) const;
    [[nodiscard]] TConclusion<std::shared_ptr<IStepFunction>> MakeAggrFunction(
        const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func) const;
    [[nodiscard]] TConclusion<NAggregation::EAggregate> GetAggregationType(
        const NKikimrSSA::TProgram::TAggregateAssignment::TAggregateFunction& func) const;

public:
    [[nodiscard]] TConclusionStatus ReadAssign(
        const NKikimrSSA::TProgram::TAssignment& assign, const std::shared_ptr<arrow::RecordBatch>& parameterValues);
    [[nodiscard]] TConclusionStatus ReadFilter(const NKikimrSSA::TProgram::TFilter& filter);
    [[nodiscard]] TConclusionStatus ReadProjection(const NKikimrSSA::TProgram::TProjection& projection);
    [[nodiscard]] TConclusionStatus ReadGroupBy(const NKikimrSSA::TProgram::TGroupBy& groupBy);

    TConclusion<std::shared_ptr<NGraph::NExecution::TCompiledGraph>> Finish() {
        return Builder.Finish();
    }
};

}   // namespace NKikimr::NArrow::NSSA
