#pragma once

#include "public.h"

#include <util/generic/hash_set.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBytecodeFormat,
    (HumanReadable)
    (Binary)
);

struct TModuleBytecode
{
    EBytecodeFormat Format;
    TSharedRef Data;
    TSharedRef ObjectCode;

    //! Identity for maps/sets: Format + Data + ObjectCode (byte-exact).
    bool operator==(const TModuleBytecode& other) const;
    operator size_t() const;
};

//! Compact cache/map key: hashes + sizes (O(1) compare). Use with a full
//! TModuleBytecode equality check on hit to rule out hash collisions.
struct TModuleBytecodeKey
{
    EBytecodeFormat Format = EBytecodeFormat::Binary;
    ui64 DataHash = 0;
    ui64 ObjectCodeHash = 0;
    size_t DataSize = 0;
    size_t ObjectCodeSize = 0;

    static TModuleBytecodeKey From(const TModuleBytecode& bytecode);

    bool operator==(const TModuleBytecodeKey& other) const;
    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

class TModuleBytecodeHashSet final
    : public THashSet<TModuleBytecode>
{ };

DEFINE_REFCOUNTED_TYPE(TModuleBytecodeHashSet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
