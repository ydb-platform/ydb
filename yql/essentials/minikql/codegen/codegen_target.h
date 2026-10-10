#pragma once

#include <string>
#include <vector>

namespace llvm {
class EngineBuilder;
class Triple;
} // namespace llvm

namespace NYql::NCodegen::NPrivate {

void ConfigureNativeTarget(llvm::EngineBuilder& builder, const llvm::Triple& triple,
                           const std::string& hostCpu, const std::vector<std::string>& hostFeatures);

} // namespace NYql::NCodegen::NPrivate
