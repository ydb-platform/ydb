/*******************************************************************************
 * tlx/port/setenv.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_PORT_SETENV_HEADER
#define TLX_PORT_SETENV_HEADER

namespace tlx {

//! \addtogroup tlx_port
//! \{

/*!
 * setenv - change or add an environment variable.
 * Windows porting madness because setenv() is apparently dangerous.
 */
int setenv(const char* name, const char* value, int overwrite);

//! \}

} // namespace tlx

#endif // !TLX_PORT_SETENV_HEADER

/******************************************************************************/
