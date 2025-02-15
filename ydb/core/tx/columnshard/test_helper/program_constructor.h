#pragma once
#include <ydb/library/formats/arrow/protos/ssa.pb.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NTxUT {

class TProgramProtoBuilder {
private:
    NKikimrSSA::TProgram Proto;
    ui32 CurrentGenericColumnId = 10000;

public:
    NKikimrSSA::TProgram& MutableProto() {
        return Proto;
    }

    const NKikimrSSA::TProgram& GetProto() const {
        return Proto;
    }

    TProgramProtoBuilder();
    ui32 AddConstant(const TString& bytes);
    ui32 AddOperation(const NYql::TKernelRequestBuilder::EBinaryOp op, const std::vector<ui32>& arguments);
    void AddFilter(const ui32 colId);
};

}   //namespace NKikimr::NTxUT
