//
// BufferAllocator.h
//
// Library: Foundation
// Package: Streams
// Module:  BufferAllocator
//
// Definition of the BufferAllocator class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_BufferAllocator_INCLUDED
#define CHDB_Foundation_BufferAllocator_INCLUDED


#include <cstddef>
#include <ios>
#include "CHDBPoco/Foundation.h"


namespace CHDBPoco
{


template <typename ch>
class BufferAllocator
/// The BufferAllocator used if no specific
/// BufferAllocator has been specified.
{
public:
    typedef ch char_type;

    static char_type * allocate(std::streamsize size) { return new char_type[static_cast<std::size_t>(size)]; }

    static void deallocate(char_type * ptr, std::streamsize /*size*/) throw() { delete[] ptr; }
};


} // namespace CHDBPoco


#endif // CHDB_Foundation_BufferAllocator_INCLUDED
