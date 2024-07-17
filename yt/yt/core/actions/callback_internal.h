#pragma once
/*
$$==============================================================================
$$ The following code is merely an adaptation of Chromium's Binds and Callbacks.
$$ Kudos to Chromium authors.
$$
$$ Original Chromium revision:
$$   - git-treeish: 206a2ae8a1ebd2b040753fff7da61bbca117757f
$$   - git-svn-id:  svn://svn.chromium.org/chrome/trunk/src@115607
$$
$$ See bind.h for an extended commentary.
$$==============================================================================
*/

#include <yt/yt/core/misc/common.h>

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
#include <library/cpp/yt/misc/source_location.h>
#endif

namespace NYT::NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

//! An opaque handle representing bound arguments.
/*!
 * #TBindStateBase is used to provide an opaque handle that the #TCallback<> class
 * can use to represent a function object with bound arguments. It behaves as
 * an existential type that is used by a corresponding invoke function
 * to perform the function execution. This allows us to shield the #TCallback<>
 * class from the types of the bound argument via "type erasure."
 */
struct TBindStateBase
    : public TRefCounted
{
    explicit TBindStateBase(
#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
        const TSourceLocation& location
#endif
    );

#ifdef YT_ENABLE_BIND_LOCATION_TRACKING
    TSourceLocation Location;
#endif
};

//! Holds the TCallback methods that don't require specialization to reduce
//! template bloat.
class TCallbackBase
{
public:
    //! Returns true iff #TCallback<> is not null (does not refer to anything).
    explicit operator bool() const;

    //! Returns the #TCallback<> into an uninitialized state.
    void Reset();

    //! Returns a magical handle.
    void* GetHandle() const;

#ifndef __cpp_impl_three_way_comparison
    //! Returns |true| iff this callback is equal to the other (which may be null).
    bool operator == (const TCallbackBase& other) const;

    //! Returns |true| iff this callback is not equal to the other (which may be null).
    bool operator != (const TCallbackBase& other) const;
#else
    bool operator== (const TCallbackBase& other) const = default;
#endif

protected:
    //! Swaps the state and the invoke function with other callback (without typechecking!).
    void Swap(TCallbackBase& other);

    /*!
     * Yup, out-of-line copy constructor. Yup, explicit.
     */
    explicit TCallbackBase(const TCallbackBase& other) = default;

    /*!
     * We can efficiently move-construct callbacks avoiding extra interlocks
     * while moving reference counted #TBindStateBase.
     */
    explicit TCallbackBase(TCallbackBase&& other) noexcept = default;

    /*!
     * We can construct #TCallback<> from a rvalue reference to the #TBindStateBase
     * since the #TBindStateBase is created at the #Bind() site.
     */
    explicit TCallbackBase(TIntrusivePtr<TBindStateBase>&& bindState);

protected:
    /*!
     * In C++, it is safe to cast function pointers to function pointers of
     * another type. It is not okay to use void*.
     * We create a TUntypedInvokeFunction type that can store our
     * function pointer, and then cast it back to the original type on usage.
     */
    using TUntypedInvokeFunction = void(*)();

    TIntrusivePtr<TBindStateBase> BindState;
    TUntypedInvokeFunction UntypedInvoke;

private:
    TCallbackBase& operator=(const TCallbackBase&) = delete;
    TCallbackBase& operator=(TCallbackBase&&) noexcept = delete;
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NYT::NDetail

#define CALLBACK_INTERNAL_INL_H_
#include "callback_internal-inl.h"
#undef CALLBACK_INTERNAL_INL_H_

