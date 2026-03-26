#pragma once

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/function.h>

#include <type_traits>

namespace NKikimr::NKernels {

// Metafunction for dispatching to appropriate CastFunction. This corresponds
// to the standard SQL CAST(expr AS target_type)
class YdbCastMetaFunction : public ::arrow20::compute::MetaFunction {
 public:
  YdbCastMetaFunction();

  ::arrow20::Result<const ::arrow20::compute::CastOptions*> ValidateOptions(const ::arrow20::compute::FunctionOptions* options) const;

  ::arrow20::Result<::arrow20::Datum> ExecuteImpl(const std::vector<::arrow20::Datum>& args,
                            const ::arrow20::compute::FunctionOptions* options,
                            ::arrow20::compute::ExecContext* ctx) const override;
};

}
