#pragma once

#include <util/generic/ptr.h>

namespace NYql {

struct TPqState;
class IGraphTransformer;

TAutoPtr<IGraphTransformer> CreatePqDataSourceConstraintTransformer(TIntrusivePtr<TPqState> state);

} // namespace NYql
