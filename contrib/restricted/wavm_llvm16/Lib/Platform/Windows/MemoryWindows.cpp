#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Intrinsic.h"
#include "WAVM/Platform/Memory.h"

#define NOMINMAX
#include <Windows.h>

#include <Psapi.h>

using namespace WAVM;
using namespace WAVM::Platform;

static Uptr internalGetPreferredVirtualPageSizeLog2()
{
	SYSTEM_INFO systemInfo;
	GetSystemInfo(&systemInfo);
	Uptr preferredVirtualPageSize = systemInfo.dwPageSize;
	// Verify our assumption that the virtual page size is a power of two.
	WAVM_ERROR_UNLESS(!(preferredVirtualPageSize & (preferredVirtualPageSize - 1)));
	return floorLogTwo(preferredVirtualPageSize);
}
Uptr Platform::getBytesPerPageLog2()
{
	static Uptr preferredVirtualPageSizeLog2 = internalGetPreferredVirtualPageSizeLog2();
	return preferredVirtualPageSizeLog2;
}

static U32 memoryAccessAsWin32Flag(MemoryAccess access)
{
	switch(access)
	{
	default:
	case MemoryAccess::none: return PAGE_NOACCESS;
	case MemoryAccess::readOnly: return PAGE_READONLY;
	case MemoryAccess::readWrite: return PAGE_READWRITE;
	case MemoryAccess::readExecute: return PAGE_EXECUTE_READ;
	case MemoryAccess::readWriteExecute: return PAGE_EXECUTE_READWRITE;
	}
}

static bool isPageAligned(U8* address)
{
	const Uptr addressBits = reinterpret_cast<Uptr>(address);
	return (addressBits & (getBytesPerPage() - 1)) == 0;
}

U8* Platform::allocateVirtualPages(Uptr numPages)
{
	const Uptr pageSizeLog2 = getBytesPerPageLog2();
	const Uptr numBytes = numPages << pageSizeLog2;
	return (U8*)VirtualAlloc(nullptr, numBytes, MEM_RESERVE, PAGE_NOACCESS);
}

#define ENABLE_VIRTUALALLOC2 defined(MEM_EXTENDED_PARAMETER_TYPE_BITS)

#if ENABLE_VIRTUALALLOC2
typedef void*(
	__stdcall* VirtualAlloc2PointerType)(HANDLE, PVOID, size_t, ULONG, ULONG, PVOID, ULONG);
static VirtualAlloc2PointerType loadVirtualAlloc2()
{
	// VirtualAlloc2 is only available on Windows 10+ and Windows Server 2016+. Try to dynamically
	// look it up by name from the API DLL.
	HMODULE kernelBaseHandle = LoadLibraryA("kernelbase.dll");
	if(!kernelBaseHandle) { return nullptr; }
	VirtualAlloc2PointerType virtualAlloc2 = reinterpret_cast<VirtualAlloc2PointerType>(
		::GetProcAddress(kernelBaseHandle, "VirtualAlloc2"));
	WAVM_ERROR_UNLESS(FreeLibrary(kernelBaseHandle));
	return virtualAlloc2;
}
static VirtualAlloc2PointerType getVirtualAlloc2()
{
	static const VirtualAlloc2PointerType virtualAlloc2 = loadVirtualAlloc2();
	return virtualAlloc2;
}
#endif

U8* Platform::allocateAlignedVirtualPages(Uptr numPages,
										  Uptr alignmentLog2,
										  U8*& outUnalignedBaseAddress)
{
	const Uptr pageSizeLog2 = getBytesPerPageLog2();
	const Uptr numBytes = numPages << pageSizeLog2;

#if ENABLE_VIRTUALALLOC2
	// If VirtualAlloc2 is available on the host Windows version, use it to allocate aligned memory.
	if(const VirtualAlloc2PointerType virtualAlloc2 = getVirtualAlloc2())
	{
		MEM_ADDRESS_REQUIREMENTS addressRequirements = {0};
		addressRequirements.Alignment = Uptr(1) << alignmentLog2;
		MEM_EXTENDED_PARAMETER alignmentParam = {0};
		alignmentParam.Type = MemExtendedParameterAddressRequirements;
		alignmentParam.Pointer = &addressRequirements;
		outUnalignedBaseAddress = (U8*)(*virtualAlloc2)(
			GetCurrentProcess(), nullptr, numBytes, MEM_RESERVE, PAGE_NOACCESS, &alignmentParam, 1);
		return outUnalignedBaseAddress;
	}
	else
#endif
	{
		if(alignmentLog2 > pageSizeLog2)
		{
			Uptr numTries = 0;
			while(true)
			{
				// Call VirtualAlloc with enough padding added to the size to align the allocation
				// within the unaligned mapping.
				const Uptr alignmentBytes = 1ull << alignmentLog2;
				void* probeResult
					= VirtualAlloc(nullptr, numBytes + alignmentBytes, MEM_RESERVE, PAGE_NOACCESS);
				if(!probeResult) { return nullptr; }

				const Uptr address = reinterpret_cast<Uptr>(probeResult);
				const Uptr alignedAddress = (address + alignmentBytes - 1) & ~(alignmentBytes - 1);

				if(numTries < 10)
				{
					// Free the unaligned+padded allocation, and try to immediately reserve just the
					// aligned middle part again. This can fail due to races with other threads, so
					// handle the VirtualAlloc failing by just retrying with a new unaligned+padded
					// allocation.
					WAVM_ERROR_UNLESS(VirtualFree(probeResult, 0, MEM_RELEASE));
					outUnalignedBaseAddress
						= (U8*)VirtualAlloc(reinterpret_cast<void*>(alignedAddress),
											numBytes,
											MEM_RESERVE,
											PAGE_NOACCESS);
					if(outUnalignedBaseAddress) { return outUnalignedBaseAddress; }

					++numTries;
				}
				else
				{
					// If the below free and re-alloc of the aligned address fails too many times,
					// just return the padded allocation.
					outUnalignedBaseAddress = (U8*)probeResult;
					return reinterpret_cast<U8*>(alignedAddress);
				}
			}
		}
		else
		{
			outUnalignedBaseAddress = allocateVirtualPages(numPages);
			return outUnalignedBaseAddress;
		}
	}
}

bool Platform::commitVirtualPages(U8* baseVirtualAddress, Uptr numPages, MemoryAccess access)
{
	WAVM_ERROR_UNLESS(isPageAligned(baseVirtualAddress));
	return baseVirtualAddress
		   == VirtualAlloc(baseVirtualAddress,
						   numPages << getBytesPerPageLog2(),
						   MEM_COMMIT,
						   memoryAccessAsWin32Flag(access));
}

bool Platform::setVirtualPageAccess(U8* baseVirtualAddress, Uptr numPages, MemoryAccess access)
{
	WAVM_ERROR_UNLESS(isPageAligned(baseVirtualAddress));
	DWORD oldProtection = 0;
	return VirtualProtect(baseVirtualAddress,
						  numPages << getBytesPerPageLog2(),
						  memoryAccessAsWin32Flag(access),
						  &oldProtection)
		   != 0;
}

void Platform::decommitVirtualPages(U8* baseVirtualAddress, Uptr numPages)
{
	WAVM_ERROR_UNLESS(isPageAligned(baseVirtualAddress));
	auto result = VirtualFree(baseVirtualAddress, numPages << getBytesPerPageLog2(), MEM_DECOMMIT);
	if(baseVirtualAddress && !result) { Errors::fatal("VirtualFree(MEM_DECOMMIT) failed"); }
}

void Platform::freeVirtualPages(U8* baseVirtualAddress, Uptr numPages)
{
	WAVM_ERROR_UNLESS(isPageAligned(baseVirtualAddress));
	auto result = VirtualFree(baseVirtualAddress, 0, MEM_RELEASE);
	if(baseVirtualAddress && !result) { Errors::fatal("VirtualFree(MEM_RELEASE) failed"); }
}

void Platform::freeAlignedVirtualPages(U8* unalignedBaseAddress, Uptr numPages, Uptr alignmentLog2)
{
	WAVM_ERROR_UNLESS(isPageAligned(unalignedBaseAddress));
	auto result = VirtualFree(unalignedBaseAddress, 0, MEM_RELEASE);
	if(unalignedBaseAddress && !result) { Errors::fatal("VirtualFree(MEM_RELEASE) failed"); }
}

Uptr Platform::getPeakMemoryUsageBytes()
{
	PROCESS_MEMORY_COUNTERS processMemoryCounters;
	WAVM_ERROR_UNLESS(GetProcessMemoryInfo(
		GetCurrentProcess(), &processMemoryCounters, sizeof(processMemoryCounters)));
	return processMemoryCounters.PeakWorkingSetSize;
}
