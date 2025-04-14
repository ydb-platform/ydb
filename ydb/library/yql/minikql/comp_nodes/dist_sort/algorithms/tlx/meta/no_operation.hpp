/*******************************************************************************
 * tlx/meta/no_operation.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_NO_OPERATION_HEADER
#define TLX_META_NO_OPERATION_HEADER

namespace tlx {

//! \addtogroup tlx_meta
//! \{

//! The noop functor, which takes any arguments and does nothing. This is a good
//! default argument for lambda function parameters.
template <typename ReturnType>
class NoOperation
{
public:
    explicit NoOperation(ReturnType return_value = ReturnType())
        : return_value_(return_value) { }

    ReturnType operator () (...) const noexcept {
        return return_value_;
    }

protected:
    ReturnType return_value_;
};

//! Specialized noop functor which returns a void.
template <>
class NoOperation<void>
{
public:
    void operator () (...) const noexcept { }
};

//! \}

} // namespace tlx

#endif // !TLX_META_NO_OPERATION_HEADER

/******************************************************************************/
