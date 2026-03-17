//
// HTTPBasicStreamBuf.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPBasicStreamBuf
//
// Definition of the HTTPBasicStreamBuf class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_HTTPBasicStreamBuf_INCLUDED
#define CHDB_Net_HTTPBasicStreamBuf_INCLUDED


#include "CHDBPoco/BufferedStreamBuf.h"
#include "CHDBPoco/Net/Net.h"


namespace CHDBPoco
{
namespace Net
{
    constexpr size_t HTTP_DEFAULT_BUFFER_SIZE = 8 * 1024;

    typedef CHDBPoco::BasicBufferedStreamBuf<char, std::char_traits<char>> HTTPBasicStreamBuf;


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_HTTPBasicStreamBuf_INCLUDED
