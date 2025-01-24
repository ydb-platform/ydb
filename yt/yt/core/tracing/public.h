#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/memory/allocation_tags.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTracingExt;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTraceContext)

constexpr int TypicalAllocationTagCount = 8;
using TAllocationTags = TCompactVector<TAllocationTag, TypicalAllocationTagCount>;

DECLARE_REFCOUNTED_CLASS(TAllocationTagList)

using TTraceId = TGuid;
constexpr TTraceId InvalidTraceId = {};

using TSpanId = ui64;
constexpr TSpanId InvalidSpanId = 0;

struct TTracingAttributes;

// Request ids come from RPC infrastructure but
// we should avoid include-dependencies here.
using TRequestId = TGuid;
constexpr TRequestId InvalidRequestId = {};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
