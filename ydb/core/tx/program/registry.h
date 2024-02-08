#pragma once

#include <ydb/core/formats/arrow/program.h>

namespace NKikimr::NOlap {

class TKernelsRegistry {
public:
    using TKernels = std::vector<std::shared_ptr<const arrow::compute::ScalarKernel>>;
    
private:
    TKernels Kernels;
    std::vector<NSsa::TFunctionPtr> Functions;

public: 
    bool Parse(const TString& serialized);
    NSsa::TFunctionPtr GetFunction(const size_t index) const;
};

}
