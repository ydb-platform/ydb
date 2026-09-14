#pragma once

#include <util/stream/input.h>

#include <exception>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @brief Input stream that supports aborting the read operation.
///
/// Extends @ref IInputStream with the ability to immediately cancel current or future reads.
class IAbortableInputStream
    : public IInputStream
{
public:
    ~IAbortableInputStream() override = default;

    /// @brief Immediately abort the stream, cancelling current and future reads.
    ///
    /// Some clients have already implemented this interface, so using pure virtual method leads to build errors.
    virtual void Abort();

    /// @brief Check whether the stream has been aborted.
    virtual bool IsAborted() const;

    static bool IsAbortedError(const std::exception_ptr& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
