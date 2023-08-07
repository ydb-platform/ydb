#pragma once

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// A configuration that controls retries of a single request.
struct TRetryConfig
{
    ///
    /// @brief How long retries of a single YT request can go on.
    ///
    /// If this limit is reached while retry count is not yet exceeded @ref TRequestRetriesTimeout exception is thrown.
    TDuration RetriesTimeLimit = TDuration::Max();
};

/// The library uses this class to understand how to retry individual requests.
class IRetryConfigProvider
    : public virtual TThrRefBase
{
public:
    ///
    /// @brief Gets retry policy for single request.
    ///
    /// CreateRetryConfig is called before ANY request.
    /// Returned config controls retries of this request.
    ///
    /// Must be thread safe since it can be used from different threads
    /// to perform internal library requests (e.g. pings).
    ///
    /// Some methods (e.g. IClient::Map) involve multiple requests to YT and therefore
    /// this method will be called several times during execution of single method.
    ///
    /// If user needs to limit overall retries inside long operation they might create
    /// retry policy that knows about overall deadline
    /// @ref NYT::TRetryConfig::RetriesTimeLimit taking into account that overall deadline.
    /// (E.g. when deadline reached it returns zero limit for retries).
    virtual TRetryConfig CreateRetryConfig() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

