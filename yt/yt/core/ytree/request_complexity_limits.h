#pragma once

#include "public.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReadRequestComplexityLimits;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TReadRequestComplexity
{
    i64 NodeCount = 0;
    i64 ResultSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadRequestComplexityOverrides
{
    std::optional<i64> NodeCount;
    std::optional<i64> ResultSize;

    void ApplyTo(TReadRequestComplexity& complexity) const noexcept;
    void Validate(TReadRequestComplexity max) const;
};

void FromProto(
    TReadRequestComplexityOverrides* original,
    const NProto::TReadRequestComplexityLimits& serialized);

void ToProto(
    NProto::TReadRequestComplexityLimits* serialized,
    const TReadRequestComplexityOverrides& original);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
