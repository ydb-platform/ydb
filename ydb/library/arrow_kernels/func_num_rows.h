#pragma once
#include "clickhouse_type_traits.h"
#include "func_common.h"

namespace NKikimr::NKernels {

struct TNumRows: public arrow::compute::MetaFunction {
public:
    TNumRows(const TString name)
        : arrow::compute::MetaFunction(name.data(), arrow::compute::Arity::Unary(), nullptr) {
    }

    arrow::Result<arrow::Datum> ExecuteImpl(const std::vector<arrow::Datum>& args, const arrow::compute::FunctionOptions* /*options*/,
        arrow::compute::ExecContext* /*ctx*/) const override {
        Y_ABORT_UNLESS(args.size() == 1);
        return arrow::Datum(std::make_shared<arrow::UInt64Scalar>(args[0].make_array()->length()));
    }
};

}   // namespace NKikimr::NKernels
