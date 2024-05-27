#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr int GrpcDispatcherThreadShutdownPriority = 0;
constexpr int GrpcServerShutdownPriority = 120;

static_assert(GrpcServerShutdownPriority > GrpcDispatcherThreadShutdownPriority);

////////////////////////////////////////////////////////////////////////////////

constexpr int ResolverShutdownPriority = 5;
constexpr int ResolverThreadShutdownPriority = 0;

static_assert(ResolverShutdownPriority > ResolverThreadShutdownPriority);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
