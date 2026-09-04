#include "cpu_spec.h"
#include "metadata_subscription/storage_paths.h"

#include <contrib/restricted/wavm/Include/WAVM/LLVMJIT/LLVMJIT.h>

namespace NKikimr::NUdfStore {

TString DetectLocalCpuSpec(const TString& overrideSpec) {
    if (!overrideSpec.empty()) {
        return overrideSpec;
    }
    const auto targetSpec = WAVM::LLVMJIT::getHostTargetSpec();
    return NormalizeCpuSpec(targetSpec.triple, targetSpec.cpu);
}

} // namespace NKikimr::NUdfStore
