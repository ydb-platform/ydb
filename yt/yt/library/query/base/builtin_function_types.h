#pragma once

#include "builtin_function_registry.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFunctionRegistryBuilder> CreateTypeInferrerFunctionRegistryBuilder(
    const TTypeInferrerMapPtr& typeInferrers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
