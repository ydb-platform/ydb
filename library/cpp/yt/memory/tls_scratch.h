#pragma once

#include "range.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Returns a temporary buffer (stored on TLS) of a given size.
//! The content is initialized with default values of T before being returned.
template <class T>
TMutableRange<T> GetTlsScratchBuffer(size_t size);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TLS_SCRATH_INL_H_
#include "tls_scratch-inl.h"
#undef TLS_SCRATH_INL_H_
