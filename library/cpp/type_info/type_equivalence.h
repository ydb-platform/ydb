#pragma once

//! @file type_equivalence.h
//!
//! Relations between types.
//!
//! Type info declares multiple ways to compare types. There's strict, nominal, structural and other equivalences,
//! as well as subtyping. See corresponding functors for more info.
//!
//! At the moment, only strict equivalence is implemented because others are not yet standartized.

#include <type_traits>

#include "fwd.h"

#include <util/system/types.h>

namespace NTi::NEq {
    /// Strict equivalence is the strongest form of type equivalence. If two types are strictly equal,
    /// they're literally the same type for all intents and purposes. This includes struct, tuple, variant and enum
    /// names, order of their items, names of their items, names of tagged types, and so on.
    struct TStrictlyEqual {
        bool operator()(const TType* lhs, const TType* rhs) const;
        bool operator()(TTypePtr lhs, TTypePtr rhs) const;

        /// Compare types without calculating and comparing their hashes first.
        bool IgnoreHash(const TType* lhs, const TType* rhs) const;
        bool IgnoreHash(TTypePtr lhs, TTypePtr rhs) const;
    };

    /// Hash that follows the strict equality rules (see `TStrictlyEqual`).
    struct TStrictlyEqualHash {
        ui64 operator()(const TType* type) const;
        ui64 operator()(TTypePtr type) const;
    };
}
