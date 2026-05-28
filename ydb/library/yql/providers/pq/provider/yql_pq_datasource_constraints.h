#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NYql {

struct TPqState;
class IGraphTransformer;

std::unique_ptr<IGraphTransformer> CreatePqDataSourceConstraintTransformer(TIntrusivePtr<TPqState> state);

} // namespace NYql
