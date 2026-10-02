#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <map>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Memory.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/Platform/Signal.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/StringRef.h>
#include <llvm/DebugInfo/DIContext.h>
#include <llvm/DebugInfo/DWARF/DWARFContext.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ExecutionEngine/RuntimeDyld.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Object/SymbolSize.h>
#include <llvm/Object/SymbolicFile.h>
#include <llvm/Support/Alignment.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/Memory.h>
#include <llvm/Support/MemoryBuffer.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

#ifdef _WIN32
#define USE_WINDOWS_SEH 1
#else
#define USE_WINDOWS_SEH 0
#endif

#if !USE_WINDOWS_SEH
#include <cxxabi.h>
#endif

namespace WAVM { namespace Runtime {
	struct ExceptionType;
}}

#define KEEP_UNLOADED_MODULE_ADDRESSES_RESERVED 0

using namespace WAVM;
using namespace WAVM::LLVMJIT;

struct LLVMJIT::GlobalModuleState
{
	Platform::Mutex gdbRegistrationListenerMutex;
	llvm::JITEventListener* gdbRegistrationListener = nullptr;

	// A map from address to loaded JIT symbols.
	Platform::RWMutex addressToModuleMapMutex;
	std::map<Uptr, LLVMJIT::Module*> addressToModuleMap;

	static const std::shared_ptr<GlobalModuleState>& get()
	{
		static std::shared_ptr<GlobalModuleState> singleton = std::make_shared<GlobalModuleState>();
		return singleton;
	}

	// These constructor and destructor should not be called directly, but must be public in order
	// to be accessible by std::make_shared.
	GlobalModuleState()
	{
		gdbRegistrationListener = llvm::JITEventListener::createGDBRegistrationListener();
	}
	~GlobalModuleState() = default; // NB: Should not delete gdbRegistrationListener in llvm16.
};

// Allocates memory for the LLVM object loader.
struct LLVMJIT::ModuleMemoryManager : llvm::RTDyldMemoryManager
{
	ModuleMemoryManager()
	: imageBaseAddress(nullptr)
	, isFinalized(false)
	, codeSection({nullptr, 0, 0})
	, readOnlySection({nullptr, 0, 0})
	, readWriteSection({nullptr, 0, 0})
	, hasRegisteredEHFrames(false)
	{
	}
	virtual ~ModuleMemoryManager() override
	{
		// Deregister the exception handling frame info.
		deregisterEHFrames();

		if(!KEEP_UNLOADED_MODULE_ADDRESSES_RESERVED)
		{ Platform::freeVirtualPages(imageBaseAddress, numAllocatedImagePages); }
		else
		{
			// Decommit the image pages, but leave them reserved to catch any references to them
			// that might erroneously remain.
			Platform::decommitVirtualPages(imageBaseAddress, numAllocatedImagePages);
		}
		Platform::deregisterVirtualAllocation(numAllocatedImagePages
											  << Platform::getBytesPerPageLog2());
	}

	void registerEHFrames(U8* addr, U64 loadAddr, uintptr_t numBytes) override
	{
		if(!USE_WINDOWS_SEH)
		{
			Platform::registerEHFrames(imageBaseAddress, addr, numBytes);
			hasRegisteredEHFrames = true;
			ehFramesAddr = addr;
			ehFramesNumBytes = numBytes;
		}
	}
	void registerFixedSEHFrames(U8* addr, Uptr numBytes)
	{
		Platform::registerEHFrames(imageBaseAddress, addr, numBytes);
		hasRegisteredEHFrames = true;
		ehFramesAddr = addr;
		ehFramesNumBytes = numBytes;
	}
	void deregisterEHFrames() override
	{
		if(hasRegisteredEHFrames)
		{
			hasRegisteredEHFrames = false;
			Platform::deregisterEHFrames(imageBaseAddress, ehFramesAddr, ehFramesNumBytes);
		}
	}

	virtual bool needsToReserveAllocationSpace() override { return true; }

	void reserveAllocationSpace(uintptr_t numCodeBytes,
										llvm::Align codeAlignment,
										uintptr_t numReadOnlyBytes,
										llvm::Align readOnlyAlignment,
										uintptr_t numReadWriteBytes,
										llvm::Align readWriteAlignment)
	{
		if(USE_WINDOWS_SEH)
		{
			// Pad the code section to allow for the SEH trampoline.
			numCodeBytes += 32;
		}

		// Calculate the number of pages to be used by each section.
		codeSection.numPages = shrAndRoundUp(numCodeBytes, Platform::getBytesPerPageLog2());
		readOnlySection.numPages = shrAndRoundUp(numReadOnlyBytes, Platform::getBytesPerPageLog2());
		readWriteSection.numPages
			= shrAndRoundUp(numReadWriteBytes, Platform::getBytesPerPageLog2());
		numAllocatedImagePages
			= codeSection.numPages + readOnlySection.numPages + readWriteSection.numPages;
		if(numAllocatedImagePages)
		{
			// Reserve enough contiguous pages for all sections.
			imageBaseAddress = Platform::allocateVirtualPages(numAllocatedImagePages);
			if(!imageBaseAddress
			   || !Platform::commitVirtualPages(imageBaseAddress, numAllocatedImagePages))
			{ Errors::fatal("memory allocation for JIT code failed"); }
			Platform::registerVirtualAllocation(numAllocatedImagePages
												<< Platform::getBytesPerPageLog2());
			codeSection.baseAddress = imageBaseAddress;
			readOnlySection.baseAddress
				= codeSection.baseAddress
				  + (codeSection.numPages << Platform::getBytesPerPageLog2());
			readWriteSection.baseAddress
				= readOnlySection.baseAddress
				  + (readOnlySection.numPages << Platform::getBytesPerPageLog2());
		}
	}
	virtual U8* allocateCodeSection(uintptr_t numBytes,
									U32 alignment,
									U32 sectionID,
									llvm::StringRef sectionName) override
	{
		return allocateBytes(sectionName, (Uptr)numBytes, alignment, codeSection);
	}
	virtual U8* allocateDataSection(uintptr_t numBytes,
									U32 alignment,
									U32 sectionID,
									llvm::StringRef sectionName,
									bool isReadOnly) override
	{
		return allocateBytes(sectionName,
							 (Uptr)numBytes,
							 alignment,
							 isReadOnly ? readOnlySection : readWriteSection);
	}
	virtual bool finalizeMemory(std::string* ErrMsg = nullptr) override
	{
		// finalizeMemory is called before we manually apply SEH relocations, so don't do anything
		// here and let the finalize callback call reallyFinalizeMemory when it's done applying the
		// SEH relocations.
		return true;
	}
	void reallyFinalizeMemory()
	{
		WAVM_ASSERT(!isFinalized);
		isFinalized = true;
		if(codeSection.numPages)
		{
			WAVM_ERROR_UNLESS(Platform::setVirtualPageAccess(codeSection.baseAddress,
															 codeSection.numPages,
															 Platform::MemoryAccess::readExecute));
		}
		if(readOnlySection.numPages)
		{
			WAVM_ERROR_UNLESS(Platform::setVirtualPageAccess(readOnlySection.baseAddress,
															 readOnlySection.numPages,
															 Platform::MemoryAccess::readOnly));
		}
		if(readWriteSection.numPages)
		{
			WAVM_ERROR_UNLESS(Platform::setVirtualPageAccess(readWriteSection.baseAddress,
															 readWriteSection.numPages,
															 Platform::MemoryAccess::readWrite));
		}

		// Invalidate the instruction cache.
		invalidateInstructionCache();
	}
	virtual void invalidateInstructionCache()
	{
		// Invalidate the instruction cache for the whole image.
		llvm::sys::Memory::InvalidateInstructionCache(
			imageBaseAddress, numAllocatedImagePages << Platform::getBytesPerPageLog2());
	}

	U8* getImageBaseAddress() const { return imageBaseAddress; }
	Uptr getNumImageBytes() const
	{
		return numAllocatedImagePages << Platform::getBytesPerPageLog2();
	}

	Uptr getNumCodeBytes() const { return codeSection.numCommittedBytes; }
	Uptr getNumReadOnlyBytes() const { return readOnlySection.numCommittedBytes; }
	Uptr getNumReadWriteBytes() const { return readWriteSection.numCommittedBytes; }

	const llvm::StringMap<std::unique_ptr<llvm::MemoryBuffer>>& getSectionNameToContentsMap() const
	{
		return sectionNameToContentsMap;
	}

private:
	struct Section
	{
		U8* baseAddress;
		Uptr numPages;
		Uptr numCommittedBytes;
	};

	U8* imageBaseAddress;
	Uptr numAllocatedImagePages;
	bool isFinalized;

	Section codeSection;
	Section readOnlySection;
	Section readWriteSection;

	bool hasRegisteredEHFrames;
	const U8* ehFramesAddr;
	Uptr ehFramesNumBytes;

	llvm::StringMap<std::unique_ptr<llvm::MemoryBuffer>> sectionNameToContentsMap;

	U8* allocateBytes(llvm::StringRef sectionName, Uptr numBytes, Uptr alignment, Section& section)
	{
		if(alignment == 0) { alignment = 1; }

		WAVM_ASSERT(section.baseAddress);
		WAVM_ASSERT(!(alignment & (alignment - 1)));
		WAVM_ASSERT(!isFinalized);

		// Allocate the section at the lowest uncommitted byte of image memory.
		U8* allocationBaseAddress
			= section.baseAddress + align(section.numCommittedBytes, alignment);
		WAVM_ASSERT(!(reinterpret_cast<Uptr>(allocationBaseAddress) & (alignment - 1)));
		section.numCommittedBytes
			= align(section.numCommittedBytes, alignment) + align(numBytes, alignment);

		// Check that enough space was reserved in the section.
		if(section.numCommittedBytes > (section.numPages << Platform::getBytesPerPageLog2()))
		{ Errors::fatal("didn't reserve enough space in section"); }

		// Drop the '.' or '__' prefix on section names.
		if(sectionName.size() && sectionName[0] == '.') { sectionName = sectionName.drop_front(1); }
		else if(sectionName.size() > 2 && sectionName[0] == '_' && sectionName[1] == '_')
		{
			sectionName = sectionName.drop_front(2);
		}

		// Record the address the section was allocated at.
		sectionNameToContentsMap.insert(std::make_pair(
			sectionName,
			llvm::MemoryBuffer::getMemBuffer(
				llvm::StringRef((const char*)allocationBaseAddress, numBytes), "", false)));

		return allocationBaseAddress;
	}

	static Uptr align(Uptr size, Uptr alignment)
	{
		return (size + alignment - 1) & ~(alignment - 1);
	}
	static Uptr shrAndRoundUp(Uptr value, Uptr shift)
	{
		return (value + (Uptr(1) << shift) - 1) >> shift;
	}

	ModuleMemoryManager(const ModuleMemoryManager&) = delete;
	void operator=(const ModuleMemoryManager&) = delete;
};

Module::Module(const std::vector<U8>& objectBytes,
			   HashMap<std::string, Uptr>* importedSymbolMap,
			   bool shouldLogMetrics,
			   std::string&& inDebugName,
			   const std::unordered_map<std::string, std::string>& weakFunctionsToPatch)
: debugName(std::move(inDebugName))
, memoryManager(new ModuleMemoryManager())
, globalModuleState(GlobalModuleState::get())
#if LLVM_VERSION_MAJOR < 8
, objectBytes(objectBytes)
#endif
{
	Timing::Timer loadObjectTimer;

#if LLVM_VERSION_MAJOR >= 8
	std::unique_ptr<llvm::object::ObjectFile> object;
#endif

	object = cantFail(llvm::object::ObjectFile::createObjectFile(llvm::MemoryBufferRef(
		llvm::StringRef((const char*)objectBytes.data(), objectBytes.size()), "memory")));

	// Create the LLVM object loader.
	struct SymbolResolver : llvm::JITSymbolResolver
	{
		const HashMap<std::string, Uptr>& importedSymbolMap;

		SymbolResolver(const HashMap<std::string, Uptr>& inImportedSymbolMap)
		: importedSymbolMap(inImportedSymbolMap)
		{
		}

#if LLVM_VERSION_MAJOR >= 8
		virtual void lookup(const LookupSet& symbols,
							llvm::JITSymbolResolver::OnResolvedFunction onResolvedFunction) override
		{
			LookupResult result;
			for(auto symbol : symbols) { result.emplace(symbol, findSymbolImpl(symbol)); }
			onResolvedFunction(result);
		}
		virtual llvm::Expected<LookupSet> getResponsibilitySet(const LookupSet& symbols) override
		{
			return LookupSet();
		}
#elif LLVM_VERSION_MAJOR == 7
		virtual llvm::Expected<LookupResult> lookup(const LookupSet& symbols) override
		{
			LookupResult result;
			for(auto symbol : symbols) { result.emplace(symbol, findSymbolImpl(symbol)); }
			return result;
		}
		virtual llvm::Expected<LookupFlagsResult> lookupFlags(const LookupSet& symbols) override
		{
			LookupFlagsResult result;
			for(auto symbol : symbols)
			{ result.emplace(symbol, findSymbolImpl(symbol).getFlags()); }
			return result;
		}
#else
		virtual llvm::JITSymbol findSymbolInLogicalDylib(const std::string& name) override
		{
			return findSymbolImpl(name);
		}
		virtual llvm::JITSymbol findSymbol(const std::string& name) override
		{
			return findSymbolImpl(name);
		}
#endif

	private:
		llvm::JITEvaluatedSymbol findSymbolImpl(llvm::StringRef name)
		{
			const std::string nameString = demangleSymbol(name.str());
			const Uptr* symbolValue = importedSymbolMap.get(nameString);
			if(!symbolValue) { return resolveJITImport(nameString); }
			else
			{
				// LLVM assumes that a symbol value of zero is a symbol that wasn't resolved.
				WAVM_ASSERT(*symbolValue);
				return llvm::JITEvaluatedSymbol(U64(*symbolValue), llvm::JITSymbolFlags::None);
			}
		}
	};
	SymbolResolver symbolResolver(*importedSymbolMap);
	llvm::RuntimeDyld loader(*memoryManager, symbolResolver);
	// Process all sections on non-Windows platforms. On Windows, this triggers errors due to
	// unimplemented relocation types in the debug sections.
#if !defined(_WIN32) || LAZY_PARSE_DWARF_LINE_INFO
	loader.setProcessAllSections(true);
#endif

	// The LLVM dynamic loader doesn't correctly apply the IMAGE_REL_AMD64_ADDR32NB relocations in
	// the pdata and xdata sections
	// (https://github.com/llvm-mirror/llvm/blob/e84d8c12d5157a926db15976389f703809c49aa5/lib/ExecutionEngine/RuntimeDyld/Targets/RuntimeDyldCOFFX86_64.h#L96)
	// Make a copy of those sections before they are clobbered, so we can do the fixup ourselves
	// later.
	llvm::object::SectionRef pdataSection;
	U8* pdataCopy = nullptr;
	Uptr pdataNumBytes = 0;
	llvm::object::SectionRef xdataSection;
	U8* xdataCopy = nullptr;
	if(USE_WINDOWS_SEH)
	{
		for(auto section : object->sections())
		{
#if LLVM_VERSION_MAJOR >= 10
			llvm::Expected<llvm::StringRef> sectionNameOrError = section.getName();
			if(sectionNameOrError)
			{
				const llvm::StringRef& sectionName = sectionNameOrError.get();
#else
			llvm::StringRef sectionName;
			if(!section.getName(sectionName))
			{
#endif

#if LLVM_VERSION_MAJOR >= 9
				llvm::Expected<llvm::StringRef> sectionContentsOrError = section.getContents();
				if(sectionContentsOrError)
				{
					const llvm::StringRef& sectionContents = sectionContentsOrError.get();
#else
				llvm::StringRef sectionContents;
				if(!section.getContents(sectionContents))
				{
#endif
					const U8* loadedSection = (const U8*)sectionContents.data();
					if(sectionName == ".pdata")
					{
						pdataCopy = new U8[section.getSize()];
						pdataNumBytes = section.getSize();
						pdataSection = section;
						memcpy(pdataCopy, loadedSection, section.getSize());
					}
					else if(sectionName == ".xdata")
					{
						xdataCopy = new U8[section.getSize()];
						xdataSection = section;
						memcpy(xdataCopy, loadedSection, section.getSize());
					}
				}
			}
		}
	}

	// Use the LLVM object loader to load the object.
	std::unique_ptr<llvm::RuntimeDyld::LoadedObjectInfo> loadedObject = loader.loadObject(*object);
	auto symbolTable = loader.getSymbolTable();
	for (auto& [function, import] : weakFunctionsToPatch) {
		WAVM_ASSERT(symbolTable.contains(function));
		WAVM_ASSERT(importedSymbolMap->contains(import));
		(*importedSymbolMap)[import] = symbolTable[function].getAddress();
	}
	loader.finalizeWithMemoryManagerLocking();
	if(loader.hasError())
	{ Errors::fatalf("RuntimeDyld failed: %s", loader.getErrorString().data()); }

	if(USE_WINDOWS_SEH && pdataCopy)
	{
		// Lookup the real address of _CxxFrameHandler3.
		const llvm::JITEvaluatedSymbol sehHandlerSymbol = resolveJITImport("__CxxFrameHandler3");
		WAVM_ERROR_UNLESS(sehHandlerSymbol);
		const U64 sehHandlerAddress = U64(sehHandlerSymbol.getAddress());

		// Create a trampoline within the image's 2GB address space that jumps to
		// __CxxFrameHandler3. jmp [rip+0] <64-bit address>
		U8* trampolineBytes = memoryManager->allocateCodeSection(16, 16, 0, "seh_trampoline");
		trampolineBytes[0] = 0xff;
		trampolineBytes[1] = 0x25;
		memset(trampolineBytes + 2, 0, 4);
		memcpy(trampolineBytes + 6, &sehHandlerAddress, sizeof(U64));

		processSEHTables(memoryManager->getImageBaseAddress(),
						 *loadedObject,
						 pdataSection,
						 pdataCopy,
						 pdataNumBytes,
						 xdataSection,
						 xdataCopy,
						 reinterpret_cast<Uptr>(trampolineBytes));

		memoryManager->registerFixedSEHFrames(
			reinterpret_cast<U8*>(Uptr(loadedObject->getSectionLoadAddress(pdataSection))),
			pdataNumBytes);
	}

	// Free the copies of the Windows SEH sections created above.
	if(pdataCopy)
	{
		delete[] pdataCopy;
		pdataCopy = nullptr;
	}
	if(xdataCopy)
	{
		delete[] xdataCopy;
		xdataCopy = nullptr;
	}

	// After having a chance to manually apply relocations for the pdata/xdata sections, apply the
	// final non-writable memory permissions.
	memoryManager->reallyFinalizeMemory();

	// Notify GDB of the new object.
	{
		Platform::Mutex::Lock lock(globalModuleState->gdbRegistrationListenerMutex);
#if LLVM_VERSION_MAJOR >= 8
		globalModuleState->gdbRegistrationListener->notifyObjectLoaded(
			reinterpret_cast<Uptr>(this), *object, *loadedObject);
#else
		globalModuleState->gdbRegistrationListener->NotifyObjectEmitted(*object, *loadedObject);
#endif
	}

	// Create a DWARF context to interpret the debug information in this compilation unit.
#if LAZY_PARSE_DWARF_LINE_INFO
	Platform::Mutex::Lock dwarfContextLock(dwarfContextMutex);
	dwarfContext
		= llvm::DWARFContext::create(memoryManager->getSectionNameToContentsMap(), sizeof(Uptr));
#else
	auto dwarfContext = llvm::DWARFContext::create(*object, &*loadedObject);
#endif

	// Iterate over the functions in the loaded object.
	for(std::pair<llvm::object::SymbolRef, U64> symbolSizePair :
		llvm::object::computeSymbolSizes(*object))
	{
		llvm::object::SymbolRef symbol = symbolSizePair.first;

		// Only process global symbols, which excludes SEH funclets.
#if LLVM_VERSION_MAJOR >= 11
		auto maybeFlags = symbol.getFlags();
		if(!(maybeFlags && *maybeFlags & llvm::object::SymbolRef::SF_Global)) { continue; }
#else
		if(!(symbol.getFlags() & llvm::object::SymbolRef::SF_Global)) { continue; }
#endif

		// Get the type, name, and address of the symbol. Need to be careful not to get the
		// Expected<T> for each value unless it will be checked for success before continuing.
		llvm::Expected<llvm::object::SymbolRef::Type> type = symbol.getType();
		if(!type || *type != llvm::object::SymbolRef::ST_Function) { continue; }
		llvm::Expected<llvm::StringRef> name = symbol.getName();
		if(!name) { continue; }
		llvm::Expected<U64> address = symbol.getAddress();
		if(!address) { continue; }

		// Compute the address the function was loaded at.
		WAVM_ASSERT(*address <= UINTPTR_MAX);
		Uptr loadedAddress = Uptr(*address);
		if(llvm::Expected<llvm::object::section_iterator> symbolSection = symbol.getSection())
		{ loadedAddress += (Uptr)loadedObject->getSectionLoadAddress(*symbolSection.get()); }

		std::map<U32, U32> offsetToOpIndexMap;
#if !LAZY_PARSE_DWARF_LINE_INFO
		// Get the DWARF line info for this symbol, which maps machine code addresses to
		// WebAssembly op indices.
#if LLVM_VERSION_MAJOR >= 9
		llvm::Expected<llvm::object::section_iterator> section = symbol.getSection();
		if(!section) { continue; }
		llvm::DILineInfoTable lineInfoTable = dwarfContext->getLineInfoForAddressRange(
			llvm::object::SectionedAddress{loadedAddress, section.get()->getIndex()},
			symbolSizePair.second);
#else
		llvm::DILineInfoTable lineInfoTable
			= dwarfContext->getLineInfoForAddressRange(loadedAddress, symbolSizePair.second);
#endif
		for(auto lineInfo : lineInfoTable)
		{ offsetToOpIndexMap.emplace(U32(lineInfo.first - loadedAddress), lineInfo.second.Line); }
#endif

		// Add the function to the module's name and address to function maps.
		WAVM_ASSERT(symbolSizePair.second <= UINTPTR_MAX);
		Runtime::Function* function
			= (Runtime::Function*)(loadedAddress - offsetof(Runtime::Function, code));
		nameToFunctionMap.addOrFail(std::string(*name), function);
		addressToFunctionMap.emplace(Uptr(loadedAddress + symbolSizePair.second), function);

		// Initialize the function mutable data.
		WAVM_ASSERT(function->mutableData);
		function->mutableData->jitModule = this;
		function->mutableData->function = function;
		function->mutableData->numCodeBytes = Uptr(symbolSizePair.second);
		function->mutableData->offsetToOpIndexMap = std::move(offsetToOpIndexMap);
	}

	const Uptr moduleEndAddress = reinterpret_cast<Uptr>(memoryManager->getImageBaseAddress()
														 + memoryManager->getNumImageBytes());
	{
		Platform::RWMutex::ExclusiveLock addressToModuleMapLock(
			globalModuleState->addressToModuleMapMutex);
		globalModuleState->addressToModuleMap.emplace(moduleEndAddress, this);
	}

	if(shouldLogMetrics)
	{
		Timing::logRatePerSecond((std::string("Loaded ") + debugName).c_str(),
								 loadObjectTimer,
								 (F64)objectBytes.size() / 1024.0 / 1024.0,
								 "MiB");
		Log::printf(Log::Category::metrics,
					"Code: %.1f KiB, read-only data: %.1f KiB, read-write data: %.1f KiB\n",
					memoryManager->getNumCodeBytes() / 1024.0,
					memoryManager->getNumReadOnlyBytes() / 1024.0,
					memoryManager->getNumReadWriteBytes() / 1024.0);
	}
}

Module::~Module()
{
	// Notify GDB that the object is being unloaded.
	{
		Platform::Mutex::Lock lock(globalModuleState->gdbRegistrationListenerMutex);
#if LLVM_VERSION_MAJOR >= 8
		globalModuleState->gdbRegistrationListener->notifyFreeingObject(
			reinterpret_cast<Uptr>(this));
#else
		globalModuleState->gdbRegistrationListener->NotifyFreeingObject(*object);
#endif
	}

	// Remove the module from the global address to module map.
	{
		Platform::RWMutex::ExclusiveLock addressToModuleMapLock(
			globalModuleState->addressToModuleMapMutex);
		globalModuleState->addressToModuleMap.erase(
			globalModuleState->addressToModuleMap.find(reinterpret_cast<Uptr>(
				memoryManager->getImageBaseAddress() + memoryManager->getNumImageBytes())));
	}

	// Free the FunctionMutableData objects.
	for(const auto& pair : addressToFunctionMap) { delete pair.second->mutableData; }

	// Delete the memory manager.
	delete memoryManager;
}

std::shared_ptr<LLVMJIT::Module> LLVMJIT::loadModule(
	const std::vector<U8>& objectFileBytes,
	HashMap<std::string, FunctionBinding>&& wavmIntrinsicsExportMap,
	std::vector<IR::FunctionType>&& types,
	std::vector<FunctionBinding>&& functionImports,
	std::vector<TableBinding>&& tables,
	std::vector<MemoryBinding>&& memories,
	std::vector<GlobalBinding>&& globals,
	std::vector<ExceptionTypeBinding>&& exceptionTypes,
	InstanceBinding instance,
	Uptr tableReferenceBias,
	const std::vector<Runtime::FunctionMutableData*>& functionDefMutableDatas,
	const std::unordered_map<Uptr, Uptr>& importIndexToSelfDefinedFunctionIndex,
	std::string&& debugName)
{
	// Bind undefined symbols in the compiled object to values.
	HashMap<std::string, Uptr> importedSymbolMap;

	// Bind the wavmIntrinsic function symbols; the compiled module assumes they have the intrinsic
	// calling convention, so no thunking is necessary.
	for(auto exportMapPair : wavmIntrinsicsExportMap)
	{
		importedSymbolMap.addOrFail(exportMapPair.key,
									reinterpret_cast<Uptr>(exportMapPair.value.code));
	}

	// Bind the type ID symbols.
	for(Uptr typeIndex = 0; typeIndex < types.size(); ++typeIndex)
	{
		importedSymbolMap.addOrFail(getExternalName("typeId", typeIndex),
									types[typeIndex].getEncoding().impl);
	}

	// Bind imported function symbols.
	for(Uptr importIndex = 0; importIndex < functionImports.size(); ++importIndex)
	{
		if (!importIndexToSelfDefinedFunctionIndex.contains(importIndex)) {
			importedSymbolMap.addOrFail(getExternalName("functionImport", importIndex),
										reinterpret_cast<Uptr>(functionImports[importIndex].code));
		}
	}

	// Bind the table symbols. The compiled module uses the symbol's value as an offset into
	// CompartmentRuntimeData to the table's entry in CompartmentRuntimeData::tableBases.
	for(Uptr tableIndex = 0; tableIndex < tables.size(); ++tableIndex)
	{
		importedSymbolMap.addOrFail(
			getExternalName("tableOffset", tableIndex),
			offsetof(Runtime::CompartmentRuntimeData, tables)
				+ sizeof(Runtime::TableRuntimeData) * tables[tableIndex].id);
	}

	// Bind the memory symbols. The compiled module uses the symbol's value as an offset into
	// CompartmentRuntimeData to the memory's entry in CompartmentRuntimeData::memoryBases.
	for(Uptr memoryIndex = 0; memoryIndex < memories.size(); ++memoryIndex)
	{
		importedSymbolMap.addOrFail(
			getExternalName("memoryOffset", memoryIndex),
			offsetof(Runtime::CompartmentRuntimeData, memories)
				+ sizeof(Runtime::MemoryRuntimeData) * memories[memoryIndex].id);
	}

	// Bind the globals symbols.
	for(Uptr globalIndex = 0; globalIndex < globals.size(); ++globalIndex)
	{
		const GlobalBinding& globalSpec = globals[globalIndex];
		Uptr value;
		if(globalSpec.type.isMutable)
		{
			// If the global is mutable, bind the symbol to the offset into
			// ContextRuntimeData::globalData where it is stored.
			value = offsetof(Runtime::ContextRuntimeData, mutableGlobals)
					+ globalSpec.mutableGlobalIndex * sizeof(IR::UntaggedValue);
		}
		else
		{
			// Otherwise, bind the symbol to a pointer to the global's immutable value.
			value = reinterpret_cast<Uptr>(globalSpec.immutableValuePointer);
		}
		importedSymbolMap.addOrFail(getExternalName("global", globalIndex), value);
	}

	// Bind exception type symbols to point to the exception type instance.
	for(Uptr exceptionTypeIndex = 0; exceptionTypeIndex < exceptionTypes.size();
		++exceptionTypeIndex)
	{
		importedSymbolMap.addOrFail(getExternalName("biasedExceptionTypeId", exceptionTypeIndex),
									exceptionTypes[exceptionTypeIndex].id + 1);
	}

	std::unordered_map<Uptr, Uptr> selfDefinedFunctionIndexToimportIndex;
	for (auto [importIndex, selfDefinedFunctionIndex] : importIndexToSelfDefinedFunctionIndex) {
		selfDefinedFunctionIndexToimportIndex[selfDefinedFunctionIndex] = importIndex;
	}

	WAVM_ASSERT(selfDefinedFunctionIndexToimportIndex.size() == importIndexToSelfDefinedFunctionIndex.size());

	std::unordered_map<std::string, std::string> weakFunctionsToPatch;

	// Allocate FunctionMutableData objects for each function def, and bind them to the symbols
	// imported by the compiled module.
	for(Uptr functionDefIndex = 0; functionDefIndex < functionDefMutableDatas.size();
		++functionDefIndex)
	{
		Runtime::FunctionMutableData* functionMutableData
			= functionDefMutableDatas[functionDefIndex];
		importedSymbolMap.addOrFail(getExternalName("functionDefMutableDatas", functionDefIndex),
									reinterpret_cast<Uptr>(functionMutableData));

		Uptr indexWithFunctionOffsets = functionDefIndex + functionImports.size();
		auto it = selfDefinedFunctionIndexToimportIndex.find(indexWithFunctionOffsets);
		if (it != selfDefinedFunctionIndexToimportIndex.end()) {
			Uptr importIndex = it->second;
			WAVM_ASSERT(!weakFunctionsToPatch.contains(getExternalName("functionDef", functionDefIndex)));
			weakFunctionsToPatch[getExternalName("functionDef", functionDefIndex)] = getExternalName("functionImport", importIndex);
			importedSymbolMap.addOrFail(getExternalName("functionImport", importIndex), 0ul);
		}
	}

	WAVM_ASSERT(weakFunctionsToPatch.size() == importIndexToSelfDefinedFunctionIndex.size());

	// Bind the instance symbol to point to the Instance.
	WAVM_ASSERT(instance.id != UINTPTR_MAX);
	importedSymbolMap.addOrFail("biasedInstanceId", instance.id + 1);

	// Bind the tableReferenceBias symbol to the tableReferenceBias.
	importedSymbolMap.addOrFail("tableReferenceBias", tableReferenceBias);

#if LLVM_VERSION_MAJOR < 10
	// Bind the unoptimizableOne symbol to 1.
	importedSymbolMap.addOrFail("unoptimizableOne", 1);
#endif

#if !USE_WINDOWS_SEH
	// Use __cxxabiv1::__cxa_current_exception_type to get a reference to the std::type_info for
	// Runtime::Exception* without enabling RTTI.
	static auto* runtimeExceptionPointerTypeInfo = [] -> std::type_info* {
		try
		{
			throw(Runtime::Exception*) nullptr;
		}
		catch(Runtime::Exception*)
		{
			return __cxxabiv1::__cxa_current_exception_type();
		}
		return nullptr;
	}();

	// Bind the std::type_info for Runtime::Exception.
	importedSymbolMap.addOrFail("runtimeExceptionTypeInfo",
								reinterpret_cast<Uptr>(runtimeExceptionPointerTypeInfo));
#endif

	// Load the module.
	return std::make_shared<Module>(objectFileBytes, &importedSymbolMap, true, std::move(debugName), weakFunctionsToPatch);
}

bool LLVMJIT::getInstructionSourceByAddress(Uptr address, InstructionSource& outSource)
{
	Module* jitModule;
	{
		auto globalModuleState = GlobalModuleState::get();
		Platform::RWMutex::ShareableLock addressToModuleMapLock(
			globalModuleState->addressToModuleMapMutex);
		auto moduleIt = globalModuleState->addressToModuleMap.upper_bound(address);
		if(moduleIt == globalModuleState->addressToModuleMap.end()) { return false; }
		jitModule = moduleIt->second;
	}

	auto functionIt = jitModule->addressToFunctionMap.upper_bound(address);
	if(functionIt == jitModule->addressToFunctionMap.end()) { return false; }
	outSource.function = functionIt->second;
	const Uptr codeAddress = reinterpret_cast<Uptr>(outSource.function->code);
	if(address < codeAddress
	   || address >= codeAddress + outSource.function->mutableData->numCodeBytes)
	{ return false; }

#if LAZY_PARSE_DWARF_LINE_INFO
	Platform::Mutex::Lock dwarfContextLock(jitModule->dwarfContextMutex);
	llvm::DILineInfo lineInfo = jitModule->dwarfContext->getLineInfoForAddress(
		llvm::object::SectionedAddress{address, llvm::object::SectionedAddress::UndefSection},
		llvm::DILineInfoSpecifier(
#if LLVM_VERSION_MAJOR >= 11
			llvm::DILineInfoSpecifier::FileLineInfoKind::RawValue,
#else
			llvm::DILineInfoSpecifier::FileLineInfoKind::Default,
#endif
			llvm::DINameKind::None));

	outSource.instructionIndex = Uptr(lineInfo.Line);
	return true;
#else
	// Find the highest entry in the offsetToOpIndexMap whose offset is <= the symbol-relative IP.
	U32 ipOffset = (U32)(address - codeAddress);
	Iptr opIndex = -1;
	for(auto offsetMapIt : outSource.function->mutableData->offsetToOpIndexMap)
	{
		if(offsetMapIt.first <= ipOffset) { opIndex = offsetMapIt.second; }
		else
		{
			break;
		}
	}

	outSource.instructionIndex = opIndex > 0 ? Uptr(opIndex) : 0;
	return true;
#endif
}
