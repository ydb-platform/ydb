#include <inttypes.h>
#include <stdint.h>
#include <string>
#include "LLVMJITPrivate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Defines.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/StringRef.h>
#include <llvm/ADT/iterator_range.h>
#include <llvm/DebugInfo/DIContext.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Object/SymbolicFile.h>
#include <llvm/Support/Error.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

#define PRINT_SEH_TABLES 0

using namespace WAVM;

enum UnwindOpcode
{
	UWOP_PUSH_NONVOL = 0,
	UWOP_ALLOC_LARGE = 1,
	UWOP_ALLOC_SMALL = 2,
	UWOP_SET_FPREG = 3,
	UWOP_SAVE_NONVOL = 4,
	UWOP_SAVE_NONVOL_FAR = 5,
	UWOP_SAVE_XMM128 = 6,
	UWOP_SAVE_XMM128_FAR = 7,
	UWOP_PUSH_MACHFRAME = 8
};

WAVM_PACKED_STRUCT(struct SEHLanguageSpecificDataEntry {
	U32 startAddress;
	U32 endAddress;
	U32 filterOrFinallyAddress;
	U32 landingPadAddress;
});

WAVM_PACKED_STRUCT(struct SEHLanguageSpecificData {
	U32 numEntries;
	SEHLanguageSpecificDataEntry entries[1];
});

WAVM_PACKED_STRUCT(struct UnwindInfoSuffix {
	U32 exceptionHandlerAddress;
	SEHLanguageSpecificData sehLSDA;
});

WAVM_PACKED_STRUCT(struct UnwindCode {
	U8 codeOffset;
	UnwindOpcode opcode : 4;
	U8 opInfo : 4;
});

namespace UnwindInfoFlags {
	enum UnwindInfoFlags : U8
	{
		UNW_FLAG_EHANDLER = 0x1,
		UNW_FLAG_UHANDLER = 0x2,
		UNW_FLAG_CHAININFO = 0x4
	};
};

WAVM_PACKED_STRUCT(struct UnwindInfoPrefix {
	U8 version : 3;
	U8 flags : 5;
	U8 sizeOfProlog;
	U8 countOfCodes;
	U8 frameRegister : 4;
	U8 frameOffset : 4;
	UnwindCode unwindCodes[1];
});

WAVM_PACKED_STRUCT(struct RuntimeFunction {
	U32 beginAddress;
	U32 endAddress;
	union
	{
		U32 unwindInfoAddress;
		U32 unwindData;
	};
});

static const char* getUnwindRegisterName(U8 registerIndex)
{
	static const char* names[] = {"rax",
								  "rcx",
								  "rdx",
								  "rbx",
								  "rsp",
								  "rbp",
								  "rsi",
								  "rdi",
								  "r8",
								  "r9",
								  "r10",
								  "r11",
								  "r12",
								  "r13",
								  "r14",
								  "r15"};
	WAVM_ERROR_UNLESS(registerIndex < (sizeof(names) / sizeof(names[0])));
	return names[registerIndex];
}

static void applyImageRelativeRelocations(const llvm::LoadedObjectInfo& loadedObject,
										  llvm::object::SectionRef section,
										  const U8* sectionCopy,
										  Uptr imageBaseAddress,
										  Uptr sehTrampolineAddress)
{
	U8* sectionData = reinterpret_cast<U8*>(Uptr(loadedObject.getSectionLoadAddress(section)));
	for(auto relocIt : section.relocations())
	{
		// Only handle type 3 (IMAGE_REL_AMD64_ADDR32NB).
		if(relocIt.getType() == 3)
		{
			const auto symbol = relocIt.getSymbol();

			U64 symbolAddress;
#if LLVM_VERSION_MAJOR >= 11
			auto maybeFlags = symbol->getFlags();
			if(maybeFlags && *maybeFlags & llvm::object::SymbolRef::SF_Undefined)
#else
			if(symbol->getFlags() & llvm::object::SymbolRef::SF_Undefined)
#endif
			{
				// Resolve __CxxFrameHandler3 to the trampoline previously created in the image's
				// 32-bit address space.
				const std::string symbolName = cantFail(symbol->getName()).str();
				if(symbolName == "__CxxFrameHandler3") { symbolAddress = sehTrampolineAddress; }
				else
				{
					llvm::JITSymbol resolvedSymbol
						= LLVMJIT::resolveJITImport(cantFail(symbol->getName()));
					symbolAddress = U64(cantFail(resolvedSymbol.getAddress()));
				}
			}
			else
			{
				const llvm::object::section_iterator symbolSection = cantFail(symbol->getSection());
				symbolAddress = (cantFail(symbol->getAddress()) - symbolSection->getAddress())
								+ loadedObject.getSectionLoadAddress(*symbolSection);
			}

			U32* valueToRelocate = (U32*)(sectionData + relocIt.getOffset());
			const U32* originalValue = (U32*)(sectionCopy + relocIt.getOffset());
			const U64 relocatedValue64 = symbolAddress + *originalValue - imageBaseAddress;
			WAVM_ERROR_UNLESS(relocatedValue64 <= UINT32_MAX);
			*valueToRelocate = (U32)relocatedValue64;
		}
	}
}

void printFunctionSEH(U8* imageBase, const RuntimeFunction& function)
{
	U8* unwindInfo = reinterpret_cast<U8*>(imageBase + function.unwindInfoAddress);
	UnwindInfoPrefix* unwindInfoPrefix = (UnwindInfoPrefix*)unwindInfo;

	Log::printf(
		Log::debug, "  address: 0x%04x-0x%04x\n", function.beginAddress, function.endAddress);
	Log::printf(Log::debug, "  unwind info:\n");
	Log::printf(Log::debug, "   version: %u\n", unwindInfoPrefix->version);
	Log::printf(Log::debug, "   flags: %x\n", unwindInfoPrefix->flags);
	Log::printf(Log::debug, "   prolog bytes: %u\n", unwindInfoPrefix->sizeOfProlog);
	Log::printf(Log::debug,
				"   frame register: %s\n",
				getUnwindRegisterName(unwindInfoPrefix->frameRegister));
	Log::printf(Log::debug, "   frame offset: 0x%x\n", unwindInfoPrefix->frameOffset * 16);

	UnwindCode* const unwindCodes = unwindInfoPrefix->unwindCodes;
	UnwindCode* unwindCode = unwindCodes;
	if(unwindInfoPrefix->countOfCodes)
	{
		Log::printf(Log::debug, "   prolog unwind codes:\n");
		while(unwindCode < unwindCodes + unwindInfoPrefix->countOfCodes)
		{
			switch(unwindCode->opcode)
			{
			case UWOP_PUSH_NONVOL:
				Log::printf(Log::debug,
							"    0x%02x UWOP_PUSH_NONVOL %s\n",
							unwindCode->codeOffset,
							getUnwindRegisterName(unwindCode->opInfo));
				++unwindCode;
				break;
			case UWOP_ALLOC_LARGE:
				if(unwindCode->opInfo == 0)
				{
					Log::printf(Log::debug,
								"    0x%02x UWOP_ALLOC_LARGE 0x%x\n",
								unwindCode->codeOffset,
								*(U16*)&unwindCode[1] * 8);
					unwindCode += 2;
				}
				else
				{
					WAVM_ERROR_UNLESS(unwindCode->opInfo == 1);
					Log::printf(Log::debug,
								"    0x%02x UWOP_ALLOC_LARGE 0x%x\n",
								unwindCode->codeOffset,
								*(U32*)&unwindCode[1]);
					unwindCode += 3;
				}
				break;
			case UWOP_ALLOC_SMALL:
				Log::printf(Log::debug,
							"    0x%02x UWOP_ALLOC_SMALL 0x%x\n",
							unwindCode->codeOffset,
							unwindCode->opInfo * 8 + 8);
				++unwindCode;
				break;
			case UWOP_SET_FPREG:
				Log::printf(Log::debug, "    0x%02x UWOP_SET_FPREG\n", unwindCode->codeOffset);
				++unwindCode;
				break;
			case UWOP_SAVE_NONVOL:
				Log::printf(Log::debug,
							"    0x%02x UWOP_SAVE_NONVOL %s 0x%x\n",
							unwindCode->codeOffset,
							getUnwindRegisterName(unwindCode->opInfo),
							*(U16*)&unwindCode[1] * 8);
				unwindCode += 2;
				break;
			case UWOP_SAVE_NONVOL_FAR:
				Log::printf(Log::debug,
							"    0x%02x UWOP_SAVE_NONVOL_FAR %s 0x%x\n",
							unwindCode->codeOffset,
							getUnwindRegisterName(unwindCode->opInfo),
							*(U32*)&unwindCode[1]);
				unwindCode += 3;
				break;
			case UWOP_SAVE_XMM128:
				Log::printf(Log::debug,
							"    0x%02x UWOP_SAVE_XMM128 xmm%u 0x%x\n",
							unwindCode->codeOffset,
							unwindCode->opInfo,
							*(U16*)&unwindCode[1] * 8);
				unwindCode += 2;
				break;
			case UWOP_SAVE_XMM128_FAR:
				Log::printf(Log::debug,
							"    0x%02x UWOP_SAVE_XMM128_FAR xmm%u 0x%x\n",
							unwindCode->codeOffset,
							unwindCode->opInfo,
							*(U32*)&unwindCode[1]);
				unwindCode += 3;
				break;
			case UWOP_PUSH_MACHFRAME:
				Log::printf(Log::debug,
							"    0x%02x UWOP_PUSH_MACHFRAME %u\n",
							unwindCode->codeOffset,
							unwindCode->opInfo);
				++unwindCode;
				break;

			default: Errors::fatalf("Unrecognized unwind opcode: %u", unwindCode->opcode);
			}
		}
	}

	if(unwindInfoPrefix->countOfCodes & 1) { ++unwindCode; }

	if((unwindInfoPrefix->flags & UnwindInfoFlags::UNW_FLAG_EHANDLER)
	   || (unwindInfoPrefix->flags & UnwindInfoFlags::UNW_FLAG_UHANDLER))
	{
		UnwindInfoSuffix* suffix = (UnwindInfoSuffix*)unwindCode;
		Log::printf(
			Log::debug, "   exception handler address: 0x%x\n", suffix->exceptionHandlerAddress);
		for(U32 entryIndex = 0; entryIndex < suffix->sehLSDA.numEntries; ++entryIndex)
		{
			const SEHLanguageSpecificDataEntry& entry = suffix->sehLSDA.entries[entryIndex];
			Log::printf(Log::debug, "   LSDA entry %u:\n", entryIndex);
			Log::printf(
				Log::debug, "    address: 0x%x-0x%x\n", entry.startAddress, entry.endAddress);
			Log::printf(
				Log::debug, "    filterOrFinally address: 0x%x\n", entry.filterOrFinallyAddress);
			Log::printf(Log::debug, "    landing pad address: 0x%x\n", entry.landingPadAddress);
		}
	}
}

void LLVMJIT::processSEHTables(U8* imageBase,
							   const llvm::LoadedObjectInfo& loadedObject,
							   const llvm::object::SectionRef& pdataSection,
							   const U8* pdataCopy,
							   Uptr pdataNumBytes,
							   const llvm::object::SectionRef& xdataSection,
							   const U8* xdataCopy,
							   Uptr sehTrampolineAddress)
{
	WAVM_ASSERT(pdataCopy);
	WAVM_ASSERT(xdataCopy);

	applyImageRelativeRelocations(loadedObject,
								  pdataSection,
								  pdataCopy,
								  reinterpret_cast<Uptr>(imageBase),
								  sehTrampolineAddress);
	applyImageRelativeRelocations(loadedObject,
								  xdataSection,
								  xdataCopy,
								  reinterpret_cast<Uptr>(imageBase),
								  sehTrampolineAddress);

	if(PRINT_SEH_TABLES)
	{
		Log::printf(Log::debug, "Win64 SEH function table:\n");

		const RuntimeFunction* functionTable = reinterpret_cast<const RuntimeFunction*>(
			Uptr(loadedObject.getSectionLoadAddress(pdataSection)));
		const Uptr numFunctions = pdataNumBytes / sizeof(RuntimeFunction);
		for(Uptr functionIndex = 0; functionIndex < numFunctions; ++functionIndex)
		{
			Log::printf(Log::debug, " Function %" WAVM_PRIuPTR "\n", functionIndex);
			printFunctionSEH(imageBase, functionTable[functionIndex]);
		}
	}
}
