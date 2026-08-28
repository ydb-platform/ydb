#include "WAVM/Runtime/ModuleDebugInfo.h"

#include <string>
#include <vector>

#include "WAVM/Inline/Assert.h"

#include <llvm/DebugInfo/DIContext.h>
#include <llvm/DebugInfo/DWARF/DWARFContext.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/MemoryBuffer.h>

using namespace WAVM;
using namespace WAVM::Runtime;

struct ModuleDebugInfo::Impl
{
	// Owning copy of the original wasm bytes (MemoryBuffer views this).
	std::vector<U8> wasmBytes;
	std::unique_ptr<llvm::MemoryBuffer> memoryBuffer;
	std::unique_ptr<llvm::object::ObjectFile> objectFile;
	std::unique_ptr<llvm::DWARFContext> dwarfContext;
};

ModuleDebugInfo::ModuleDebugInfo(std::unique_ptr<Impl>&& inImpl) : impl(std::move(inImpl)) {}
ModuleDebugInfo::~ModuleDebugInfo() = default;

std::shared_ptr<ModuleDebugInfo> ModuleDebugInfo::tryCreate(const U8* wasmBytes, Uptr numWasmBytes)
{
	if(!wasmBytes || !numWasmBytes) { return nullptr; }

	auto impl = std::make_unique<Impl>();
	impl->wasmBytes.assign(wasmBytes, wasmBytes + numWasmBytes);
	impl->memoryBuffer = llvm::MemoryBuffer::getMemBuffer(
		llvm::StringRef(reinterpret_cast<const char*>(impl->wasmBytes.data()),
						impl->wasmBytes.size()),
		"wavm-module.wasm",
		/*RequiresNullTerminator=*/false);

	llvm::Expected<std::unique_ptr<llvm::object::ObjectFile>> objectOrErr
		= llvm::object::ObjectFile::createObjectFile(impl->memoryBuffer->getMemBufferRef());
	if(!objectOrErr) { return nullptr; }

	impl->objectFile = std::move(*objectOrErr);
	impl->dwarfContext = llvm::DWARFContext::create(*impl->objectFile);
	if(!impl->dwarfContext) { return nullptr; }

	return std::shared_ptr<ModuleDebugInfo>(new ModuleDebugInfo(std::move(impl)));
}

bool ModuleDebugInfo::lookup(U32 codeSectionRelativePc, Location& outLocation) const
{
	WAVM_ASSERT(impl && impl->dwarfContext);
	outLocation = Location{};

	llvm::DILineInfo lineInfo = impl->dwarfContext->getLineInfoForAddress(
		llvm::object::SectionedAddress{codeSectionRelativePc,
									   llvm::object::SectionedAddress::UndefSection},
		llvm::DILineInfoSpecifier(
#if LLVM_VERSION_MAJOR >= 11
			llvm::DILineInfoSpecifier::FileLineInfoKind::AbsoluteFilePath,
#else
			llvm::DILineInfoSpecifier::FileLineInfoKind::Default,
#endif
			llvm::DINameKind::None));

	if(lineInfo.FileName.empty() || lineInfo.FileName == "<invalid>" || lineInfo.Line == 0)
	{ return false; }

	outLocation.fileName = lineInfo.FileName;
	outLocation.line = Uptr(lineInfo.Line);
	return true;
}
