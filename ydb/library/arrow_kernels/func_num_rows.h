#pragma once
#include "clickhouse_type_traits.h"
#include "func_common.h"

namespace NKikimr::NKernels {

struct TNumRows: public arrow20::compute::MetaFunction {
public:
    TNumRows(const TString name)
        : arrow20::compute::MetaFunction(name.data(), arrow20::compute::Arity::Unary(), nullptr) {
    }

    arrow20::Result<arrow20::Datum> ExecuteImpl(const std::vector<arrow20::Datum>& args, const arrow20::compute::FunctionOptions* /*options*/,
        arrow20::compute::ExecContext* /*ctx*/) const override {
        Y_ABORT_UNLESS(args.size() == 1);
        return arrow20::Datum(std::make_shared<arrow20::UInt64Scalar>(args[0].make_array()->length()));
    }
};

}   // namespace NKikimr::NKernels
