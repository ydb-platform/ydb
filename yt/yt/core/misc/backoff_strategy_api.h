#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TExponentialBackoffOptions;
struct TConstantBackoffOptions;

DECLARE_REFCOUNTED_CLASS(TSerializableExponentialBackoffOptions)
DECLARE_REFCOUNTED_CLASS(TSerializableConstantlBackoffOptions)

class TBackoffStrategy;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
