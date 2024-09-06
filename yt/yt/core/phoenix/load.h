#pragma once

#include "public.h"

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

//! Installs a given schema into FLS, matches it against the type registry metadata,
//! and computes the schedule to be used during loading.
class TLoadSessionGuard
{
public:
    explicit TLoadSessionGuard(const TUniverseSchemaPtr& schema);
    TLoadSessionGuard(const TLoadSessionGuard&) = delete;

    ~TLoadSessionGuard();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
