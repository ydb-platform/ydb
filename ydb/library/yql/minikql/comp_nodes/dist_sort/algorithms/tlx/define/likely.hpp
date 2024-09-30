/*******************************************************************************
 * tlx/define/likely.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_DEFINE_LIKELY_HEADER
#define TLX_DEFINE_LIKELY_HEADER

namespace tlx {

//! \addtogroup tlx_define
//! \{

#if defined(__GNUC__) || defined(__clang__)
#define TLX_LIKELY(c)   __builtin_expect((c), 1)
#define TLX_UNLIKELY(c) __builtin_expect((c), 0)
#else
#define TLX_LIKELY(c)   c
#define TLX_UNLIKELY(c) c
#endif

//! \}

} // namespace tlx

#endif // !TLX_DEFINE_LIKELY_HEADER

/******************************************************************************/
