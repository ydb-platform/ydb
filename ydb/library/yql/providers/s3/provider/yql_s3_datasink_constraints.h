#pragma once

#include <util/generic/ptr.h>

namespace NYql {

struct TS3State;
class IGraphTransformer;

TAutoPtr<IGraphTransformer> CreateS3DataSinkConstraintTransformer(TIntrusivePtr<TS3State> state);

} // namespace NYql
