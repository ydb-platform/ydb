//
// SharedMemory.cpp
//
// Library: Foundation
// Package: Processes
// Module:  SharedMemory
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/SharedMemory.h"
#include "DBPoco/Exception.h"
#if defined(DB_POCO_NO_SHAREDMEMORY)
/// #include "SharedMemory_DUMMY.cpp"
#elif defined(DB_POCO_OS_FAMILY_UNIX)
#include "SharedMemory_POSIX.cpp"
#else
#error #include "SharedMemory_DUMMY.cpp"
#endif


namespace DBPoco {


SharedMemory::SharedMemory():
	_pImpl(0)
{
}


SharedMemory::SharedMemory(const std::string& name, std::size_t size, AccessMode mode, const void* addrHint, bool server):
	_pImpl(new SharedMemoryImpl(name, size, mode, addrHint, server))
{
}


SharedMemory::SharedMemory(const DBPoco::File& file, AccessMode mode, const void* addrHint):
	_pImpl(new SharedMemoryImpl(file, mode, addrHint))
{
}


SharedMemory::SharedMemory(const SharedMemory& other):
	_pImpl(other._pImpl)
{
	if (_pImpl)
		_pImpl->duplicate();
}


SharedMemory::~SharedMemory()
{
	if (_pImpl)
		_pImpl->release();
}


SharedMemory& SharedMemory::operator = (const SharedMemory& other)
{
	SharedMemory tmp(other);
	swap(tmp);
	return *this;
}


char* SharedMemory::begin() const
{
	if (_pImpl)
		return _pImpl->begin();
	else
		return 0;
}


char* SharedMemory::end() const
{
	if (_pImpl)
		return _pImpl->end();
	else
		return 0;
}


} // namespace DBPoco
