/*******************************************************************************
 * tlx/timestamp.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_TIMESTAMP_HEADER
#define TLX_TIMESTAMP_HEADER

namespace tlx {

//! Returns number of seconds since the epoch, currently microsecond resolution.
double timestamp();

} // namespace tlx

#endif // !TLX_TIMESTAMP_HEADER

/******************************************************************************/
