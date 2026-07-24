#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Platform {
	// Describes allowed memory accesses.
	enum class MemoryAccess
	{
		none,
		readOnly,
		readWrite,
		readExecute,
		readWriteExecute
	};

	// Returns the base 2 logarithm of the number of bytes in the smallest virtual page.
	WAVM_API Uptr getBytesPerPageLog2();

	// Returns the number of bytes in the smallest virtual page.
	inline Uptr getBytesPerPage() { return Uptr(1) << getBytesPerPageLog2(); }

	// Allocates virtual addresses without commiting physical pages to them.
	// Returns the base virtual address of the allocated addresses, or nullptr if the virtual
	// address space has been exhausted.
	WAVM_API U8* allocateVirtualPages(Uptr numPages);

	// Allocates virtual addresses without commiting physical pages to them.
	// Returns the base virtual address of the allocated addresses, or nullptr if the virtual
	// address space has been exhausted.
	WAVM_API U8* allocateAlignedVirtualPages(Uptr numPages,
											 Uptr alignmentLog2,
											 U8*& outUnalignedBaseAddress);

	// Commits physical memory to the specified virtual pages.
	// baseVirtualAddress must be a multiple of the preferred page size.
	// Return true if successful, or false if physical memory has been exhausted.
	WAVM_API bool commitVirtualPages(U8* baseVirtualAddress,
									 Uptr numPages,
									 MemoryAccess access = MemoryAccess::readWrite);

	// Changes the allowed access to the specified virtual pages.
	// baseVirtualAddress must be a multiple of the preferred page size.
	// Return true if successful, or false if the access-level could not be set.
	WAVM_API bool setVirtualPageAccess(U8* baseVirtualAddress, Uptr numPages, MemoryAccess access);

	// Decommits the physical memory that was committed to the specified virtual pages.
	// baseVirtualAddress must be a multiple of the preferred page size.
	WAVM_API void decommitVirtualPages(U8* baseVirtualAddress, Uptr numPages);

	// Frees virtual addresses. baseVirtualAddress must also be the address returned by
	// allocateVirtualPages.
	WAVM_API void freeVirtualPages(U8* baseVirtualAddress, Uptr numPages);

	// Frees an aligned virtual address block. unalignedBaseAddress must be the unaligned base
	// address returned in the outUnalignedBaseAddress parameter of a call to
	// allocateAlignedVirtualPages.
	WAVM_API void freeAlignedVirtualPages(U8* unalignedBaseAddress,
										  Uptr numPages,
										  Uptr alignmentLog2);

	// Gets memory usage information for this process.
	WAVM_API Uptr getPeakMemoryUsageBytes();
}}
