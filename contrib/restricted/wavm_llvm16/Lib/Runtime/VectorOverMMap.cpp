#include "WAVM/Platform/VectorOverMMap.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Memory.h"

using namespace WAVM;
using namespace WAVM::Runtime;

namespace {
	constexpr size_t WasmPageSize = 65536;
	constexpr size_t GuardPageCount = 2;

	size_t hostPagesPerWasmPage()
	{
		const size_t hostPageSize = Platform::getBytesPerPage();
		WAVM_ASSERT(WasmPageSize % hostPageSize == 0);
		return WasmPageSize / hostPageSize;
	}
}

VectorOverMMap::VectorOverMMap()
: committedPageCount(0), capacityPageCount(2), data(allocateAndProtect(0, 2))
{
}

VectorOverMMap::~VectorOverMMap()
{
	const size_t hostPages = (capacityPageCount + GuardPageCount) * hostPagesPerWasmPage();
	Platform::freeVirtualPages(static_cast<U8*>(data), hostPages);
}

void VectorOverMMap::grow(size_t morePages)
{
	if(capacityPageCount < committedPageCount + morePages)
	{
		resizeWithDoubling(morePages);
		return;
	}

	if(!Platform::commitVirtualPages(
		   static_cast<U8*>(data) + (committedPageCount * WasmPageSize),
		   morePages * hostPagesPerWasmPage()))
	{
		checkForOOM("Failed to protect VectorOverMMap; terminating");
	}

	committedPageCount += morePages;
};

size_t VectorOverMMap::getNumReservedBytes() const { return committedPageCount * WasmPageSize; }

void* VectorOverMMap::getData() const { return data; }

void VectorOverMMap::resizeWithDoubling(size_t morePages)
{
	auto oldCommitted = committedPageCount;
	auto newCommitted = oldCommitted + morePages;

	auto oldCapacity = capacityPageCount;
	auto newCapacity = std::max(oldCapacity * 2, newCommitted * 2);

	auto oldData = data;
	auto newData = allocateAndProtect(newCommitted, newCapacity);

	::memcpy(newData, oldData, oldCommitted * WasmPageSize);

	committedPageCount = newCommitted;
	capacityPageCount = newCapacity;
	data = newData;

	const size_t hostPages = (oldCapacity + GuardPageCount) * hostPagesPerWasmPage();
	Platform::freeVirtualPages(static_cast<U8*>(oldData), hostPages);
}

void* VectorOverMMap::allocateAndProtect(size_t committed, size_t capacity)
{
	WAVM_ASSERT(committed <= capacity);

	const size_t hostPages = (capacity + GuardPageCount) * hostPagesPerWasmPage();
	auto allocated = Platform::allocateVirtualPages(hostPages);

	if(!allocated) { checkForOOM("Failed to allocate VectorOverMMap; terminating"); }

	if(committed > 0
	   && !Platform::commitVirtualPages(allocated, committed * hostPagesPerWasmPage()))
	{
		Platform::freeVirtualPages(allocated, hostPages);
		checkForOOM("Failed to protect VectorOverMMap; terminating");
	}

	return allocated;
}

void VectorOverMMap::checkForOOM(const char* message)
{
	fprintf(stderr, "%s\n", message);
	std::_Exit(9);
}
