/*******************************************************************************
 * tlx/meta/vexpand.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_VEXPAND_HEADER
#define TLX_META_VEXPAND_HEADER

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// vexpand(variables...) -- macro to gobble up expanded parameter packes. This
// is obviously identical to tlx::unused() but used in a different context.

template <typename... Types>
void vexpand(Types&& ...) {
}

//! \}

} // namespace tlx

#endif // !TLX_META_VEXPAND_HEADER

/******************************************************************************/
