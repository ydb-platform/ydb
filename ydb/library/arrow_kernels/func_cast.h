#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/function.h>

#include <type_traits>

namespace NKikimr::NKernels {

// Metafunction for dispatching to appropriate CastFunction. This corresponds
// to the standard SQL CAST(expr AS target_type)
class YdbCastMetaFunction : public ::arrow::compute::MetaFunction {
 public:
  YdbCastMetaFunction();

  ::arrow::Result<const ::arrow::compute::CastOptions*> ValidateOptions(const ::arrow::compute::FunctionOptions* options) const;

  ::arrow::Result<::arrow::Datum> ExecuteImpl(const std::vector<::arrow::Datum>& args,
                            const ::arrow::compute::FunctionOptions* options,
                            ::arrow::compute::ExecContext* ctx) const override;
};

}
