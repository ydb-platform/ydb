#pragma once
#include "kernels_wrapper.h"

#include <ydb/core/formats/arrow/program/aggr_common.h>

#include <ydb/library/formats/arrow/protos/ssa.pb.h>

#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NTxUT {

class TProgramProtoBuilder {
private:
    NKikimrSSA::TProgram Proto;
    ui32 CurrentGenericColumnId = 10000;
    THashMap<NYql::TKernelRequestBuilder::EBinaryOp, ui32> KernelOperations;
    TKernelsWrapper Kernels;
    bool Finished = false;

public:
    const NKikimrSSA::TProgram& GetProto() const;
    const NKikimrSSA::TProgram& FinishProto();

    TProgramProtoBuilder() = default;
    ui32 AddConstant(const TString& bytes);
    ui32 AddOperation(const NYql::TKernelRequestBuilder::EBinaryOp op, const std::vector<ui32>& arguments);
    ui32 AddOperation(const NKikimrSSA::TProgram::TAssignment::EFunction op, const std::vector<ui32>& arguments);
    ui32 AddOperation(const TString& kernelName, const std::vector<ui32>& arguments);
    ui32 AddAggregation(
        const NArrow::NSSA::NAggregation::EAggregate op, const std::vector<ui32>& arguments, const std::vector<ui32>& groupByKeys);
    void AddFilter(const ui32 colId);
    void AddProjection(const std::vector<ui32>& arguments);
};

}   //namespace NKikimr::NTxUT
