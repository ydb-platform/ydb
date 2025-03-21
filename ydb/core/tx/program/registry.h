#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/function.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernel.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow::NSSA {

class TKernelsRegistry {
public:
    using TKernels = std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>>;

private:
    TKernels Kernels;
    std::vector<std::shared_ptr<arrow::compute::ScalarFunction>> Functions;

public:
    bool Parse(const TString& serialized);
    std::shared_ptr<arrow::compute::ScalarFunction> GetFunction(const size_t index) const;
};

}   // namespace NKikimr::NArrow::NSSA
