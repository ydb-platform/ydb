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


#ifndef DB_Net_HTTPBasicStreamBuf_INCLUDED
#define DB_Net_HTTPBasicStreamBuf_INCLUDED


#include "DBPoco/BufferedStreamBuf.h"
#include "DBPoco/Net/Net.h"


namespace DBPoco
{
namespace Net
{
    constexpr size_t HTTP_DEFAULT_BUFFER_SIZE = 8 * 1024;

    typedef DBPoco::BasicBufferedStreamBuf<char, std::char_traits<char>> HTTPBasicStreamBuf;


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPBasicStreamBuf_INCLUDED
