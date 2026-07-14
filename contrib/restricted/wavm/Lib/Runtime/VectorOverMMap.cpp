#include "WAVM/Platform/VectorOverMMap.h"

#include <limits.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include <iostream>
#include "WAVM/Inline/Assert.h"

using namespace WAVM;
using namespace WAVM::Runtime;

VectorOverMMap::VectorOverMMap()
: committedPageCount(0), capacityPageCount(2), data(allocateAndProtect(0, 2))
{
}

VectorOverMMap::~VectorOverMMap()
{
	int err = ::munmap(data, (capacityPageCount + guardPageCount) * pageSize);
	if(err < 0) { checkForOOM("Failed to unmap VectorOverMMap; terminating"); }
}

void VectorOverMMap::grow(size_t morePages)
{
	if(capacityPageCount < committedPageCount + morePages)
	{
		resizeWithDoubling(morePages);
		return;
	}

	int err = ::mprotect(static_cast<uint8_t*>(data) + (committedPageCount * pageSize),
						 morePages * pageSize,
						 PROT_READ | PROT_WRITE);

	if(err < 0) { checkForOOM("Failed to protect VectorOverMMap; terminating"); }

	committedPageCount += morePages;
};

size_t VectorOverMMap::getNumReservedBytes() const { return committedPageCount * pageSize; }

void* VectorOverMMap::getData() const { return data; }

void VectorOverMMap::resizeWithDoubling(size_t morePages)
{
	auto oldCommitted = committedPageCount;
	auto newCommitted = oldCommitted + morePages;

	auto oldCapacity = capacityPageCount;
	auto newCapacity = std::max(oldCapacity * 2, newCommitted * 2);

	auto oldData = data;
	auto newData = allocateAndProtect(newCommitted, newCapacity);

	::memcpy(newData, oldData, oldCommitted * pageSize);

	committedPageCount = newCommitted;
	capacityPageCount = newCapacity;
	data = newData;

	int err = ::munmap(oldData, (oldCapacity + guardPageCount) * pageSize);
	if(err < 0) { checkForOOM("Failed to unmap VectorOverMMap; terminating"); }
}

void* VectorOverMMap::allocateAndProtect(size_t committed, size_t capacity)
{
	WAVM_ASSERT(committed <= capacity);

	auto allocated
		= ::mmap(nullptr, (capacity + guardPageCount) * pageSize, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

	if(!allocated) { checkForOOM("Failed to allocate VectorOverMMap; terminating"); }

	int err = ::mprotect(allocated, committed * pageSize, PROT_READ | PROT_WRITE);

	if(err < 0) { checkForOOM("Failed to protect VectorOverMMap; terminating"); }

	return allocated;
}

void VectorOverMMap::checkForOOM(const char* message)
{
	if(errno == ENOMEM)
	{
		fprintf(stderr,
				"Out-of-memory condition detected while allocating VectorOverMMap; terminating\n");
		_exit(9);
	}

	fprintf(stderr, "%s\n", message);
	_exit(9);
}
