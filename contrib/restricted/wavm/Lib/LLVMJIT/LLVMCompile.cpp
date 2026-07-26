#include <string.h>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Module.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Defines.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm-c/Disassembler.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/TargetParser/Triple.h>
#include <llvm/ADT/ilist_iterator.h>
#include <llvm/CodeGen/TargetSubtargetInfo.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Object/SymbolSize.h>
#include <llvm/Pass.h>
#include <llvm/Support/FileSystem.h>
#include <llvm/TargetParser/Host.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/Scalar.h>
#if LLVM_VERSION_MAJOR >= 7
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Utils.h>
#endif
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

namespace llvm {
	class MCContext;
}

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

static std::string printModule(const llvm::Module& llvmModule)
{
	std::string result;
	llvm::raw_string_ostream printStream(result);
	llvmModule.print(printStream, nullptr);
	return result;
}

// Define a LLVM raw output stream that can write directly to a std::vector.
struct LLVMArrayOutputStream : llvm::raw_pwrite_stream
{
	LLVMArrayOutputStream() { SetUnbuffered(); }

	~LLVMArrayOutputStream() override = default;

	void flush() = delete;

	std::vector<U8>&& getOutput() { return std::move(output); }

protected:
	virtual void write_impl(const char* data, size_t numBytes) override
	{
		const Uptr startOffset = output.size();
		output.resize(output.size() + numBytes);
		memcpy(output.data() + startOffset, data, numBytes);
	}
	virtual void pwrite_impl(const char* data, size_t numBytes, U64 offset) override
	{
		WAVM_ASSERT(offset + numBytes > offset && offset + numBytes <= U64(output.size()));
		memcpy(output.data() + offset, data, numBytes);
	}
	virtual U64 current_pos() const override { return output.size(); }

private:
	std::vector<U8> output;
};

static void optimizeLLVMModule(llvm::Module& llvmModule, bool shouldLogMetrics)
{
	// Run some optimization on the module's functions.
	Timing::Timer optimizationTimer;

	llvm::legacy::FunctionPassManager fpm(&llvmModule);
	fpm.add(llvm::createPromoteMemoryToRegisterPass());
	fpm.add(llvm::createInstructionCombiningPass());
	fpm.add(llvm::createCFGSimplificationPass());
#if LLVM_VERSION_MAJOR <= 16
	fpm.add(llvm::createJumpThreadingPass());
#endif
#if LLVM_VERSION_MAJOR >= 12
	// LLVM 12 removed the constant propagation pass in favor of the instsimplify pass, which is
	// itself marked as legacy.
	// TODO: evaluate if this is the best pass configuration in LLVM 12.
	fpm.add(llvm::createInstSimplifyLegacyPass());
#else
	fpm.add(llvm::createConstantPropagationPass());
#endif

	// This DCE pass is necessary to work around a bug in LLVM's CodeGenPrepare that's triggered
	// if there's a dead div/rem with limited-range divisor:
	// https://bugs.llvm.org/show_bug.cgi?id=43514
	fpm.add(llvm::createDeadCodeEliminationPass());

	fpm.doInitialization();
	for(auto functionIt = llvmModule.begin(); functionIt != llvmModule.end(); ++functionIt)
	{ fpm.run(*functionIt); }

	if(shouldLogMetrics)
	{
		Timing::logRatePerSecond(
			"Optimized LLVM module", optimizationTimer, (F64)llvmModule.size(), "functions");
	}
}

std::vector<U8> LLVMJIT::compileLLVMModule(LLVMContext& llvmContext,
										   llvm::Module&& llvmModule,
										   bool shouldLogMetrics,
										   llvm::TargetMachine* targetMachine)
{
	// Verify the module.
	if(WAVM_ENABLE_ASSERTS)
	{
		std::string verifyOutputString;
		llvm::raw_string_ostream verifyOutputStream(verifyOutputString);
		if(llvm::verifyModule(llvmModule, &verifyOutputStream))
		{
			verifyOutputStream.flush();
			Errors::fatalf("LLVM verification errors:\n%s", verifyOutputString.c_str());
		}
	}

	// Optimize the module;
	optimizeLLVMModule(llvmModule, shouldLogMetrics);

	// Generate machine code for the module.
	Timing::Timer machineCodeTimer;
	std::vector<U8> objectBytes;
	{
		llvm::legacy::PassManager passManager;
		llvm::MCContext* mcContext;
		LLVMArrayOutputStream objectStream;
		WAVM_ERROR_UNLESS(!targetMachine->addPassesToEmitMC(passManager, mcContext, objectStream));
		passManager.run(llvmModule);
		objectBytes = objectStream.getOutput();
	}
	if(shouldLogMetrics)
	{
		Timing::logRatePerSecond(
			"Generated machine code", machineCodeTimer, (F64)llvmModule.size(), "functions");
	}

	return objectBytes;
}

static std::unique_ptr<llvm::TargetMachine> getAndValidateTargetMachine(
	const IR::FeatureSpec& featureSpec,
	const TargetSpec& targetSpec)
{
	// Get the target machine.
	std::unique_ptr<llvm::TargetMachine> targetMachine = getTargetMachine(targetSpec);
	if(!targetMachine)
	{
		Errors::fatalf("Invalid target spec (triple=%s, cpu=%s).",
					   targetSpec.triple.c_str(),
					   targetSpec.cpu.c_str());
	}

	// Validate that the target machine supports the module's FeatureSpec.
	switch(validateTarget(targetSpec, featureSpec))
	{
	case TargetValidationResult::valid: break;

	case TargetValidationResult::invalidTargetSpec:
		Errors::fatalf("Invalid target spec (triple=%s, cpu=%s).\n",
					   targetSpec.triple.c_str(),
					   targetSpec.cpu.c_str());
	case TargetValidationResult::unsupportedArchitecture:
		Errors::fatalf("Unsupported target architecture (triple=%s, cpu=%s)",
					   targetSpec.triple.c_str(),
					   targetSpec.cpu.c_str());
	case TargetValidationResult::x86CPUDoesNotSupportSSE41:
		Errors::fatalf(
			"Target X86 CPU (%s) does not support SSE 4.1, which"
			" WAVM requires for WebAssembly SIMD code.\n",
			targetSpec.cpu.c_str());
	case TargetValidationResult::wavmDoesNotSupportSIMDOnArch:
		Errors::fatalf("WAVM does not support SIMD on the host CPU architecture.\n");
	case TargetValidationResult::memory64Requires64bitTarget:
		Errors::fatalf("Target CPU (%s) does not support 64-bit memories.\n",
					   targetSpec.cpu.c_str());
	case TargetValidationResult::table64Requires64bitTarget:
		Errors::fatalf("Target CPU (%s) does not support 64-bit tables.\n", targetSpec.cpu.c_str());

	default: WAVM_UNREACHABLE();
	};

	return targetMachine;
}

std::vector<U8> LLVMJIT::compileModule(const IR::Module& irModule, const TargetSpec& targetSpec)
{
	std::unique_ptr<llvm::TargetMachine> targetMachine
		= getAndValidateTargetMachine(irModule.featureSpec, targetSpec);

	targetMachine->setFastISel(true);
	targetMachine->setOptLevel(llvm::CodeGenOptLevel::None);

	// Emit LLVM IR for the module.
	LLVMContext llvmContext;
	llvm::Module llvmModule("", llvmContext);
	emitModule(irModule, llvmContext, llvmModule, targetMachine.get());

	// Compile the LLVM IR to object code.
	return compileLLVMModule(llvmContext, std::move(llvmModule), true, targetMachine.get());
}

std::string LLVMJIT::emitLLVMIR(const IR::Module& irModule,
								const TargetSpec& targetSpec,
								bool optimize)
{
	std::unique_ptr<llvm::TargetMachine> targetMachine
		= getAndValidateTargetMachine(irModule.featureSpec, targetSpec);

	targetMachine->setFastISel(true);
	targetMachine->setOptLevel(llvm::CodeGenOptLevel::None);

	// Emit LLVM IR for the module.
	LLVMContext llvmContext;
	llvm::Module llvmModule("", llvmContext);
	emitModule(irModule, llvmContext, llvmModule, targetMachine.get());

	// Optimize the LLVM IR.
	if(optimize) { optimizeLLVMModule(llvmModule, true); }

	// Print the LLVM IR.
	return printModule(llvmModule);
}

std::string LLVMJIT::disassembleObject(const TargetSpec& targetSpec,
									   const std::vector<U8>& objectBytes)
{
	std::string result;

	LLVMDisasmContextRef disasmRef
		= LLVMCreateDisasm(targetSpec.triple.c_str(), nullptr, 0, nullptr, nullptr);
	WAVM_ERROR_UNLESS(LLVMSetDisasmOptions(disasmRef, LLVMDisassembler_Option_PrintLatency));

	std::unique_ptr<llvm::object::ObjectFile> object
		= cantFail(llvm::object::ObjectFile::createObjectFile(llvm::MemoryBufferRef(
			llvm::StringRef((const char*)objectBytes.data(), objectBytes.size()), "memory")));

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
		llvm::Expected<U64> addressInSection = symbol.getAddress();
		if(!addressInSection) { continue; }

		// Compute the address the function was loaded at.
		llvm::StringRef sectionContents((const char*)objectBytes.data(), objectBytes.size());
		if(llvm::Expected<llvm::object::section_iterator> symbolSection = symbol.getSection())
		{
#if LLVM_VERSION_MAJOR >= 9
			if(llvm::Expected<llvm::StringRef> maybeSectionContents
			   = (*symbolSection)->getContents())
			{ sectionContents = maybeSectionContents.get(); }
#else
			(*symbolSection)->getContents(sectionContents);
#endif
		}

		WAVM_ERROR_UNLESS(addressInSection.get() + symbolSizePair.second >= addressInSection.get()
						  && addressInSection.get() + symbolSizePair.second
								 <= sectionContents.size());

		result += name.get().str();
		result += ": # ";
		result += std::to_string(addressInSection.get());
		result += '-';
		result += std::to_string(addressInSection.get() + symbolSizePair.second);
		result += '\n';

		const U8* nextByte = (const U8*)sectionContents.data() + addressInSection.get();
		Uptr numBytesRemaining = Uptr(symbolSizePair.second);
		while(numBytesRemaining)
		{
			char instructionBuffer[256];
			Uptr numInstructionBytes = LLVMDisasmInstruction(disasmRef,
															 const_cast<U8*>(nextByte),
															 numBytesRemaining,
															 reinterpret_cast<Uptr>(nextByte),
															 instructionBuffer,
															 sizeof(instructionBuffer));
			if(numInstructionBytes == 0)
			{
				numInstructionBytes = 1;
				result += "\t# skipped ";
				result += std::to_string(numInstructionBytes);
				result += " bytes.\n";
			}
			WAVM_ASSERT(numInstructionBytes <= numBytesRemaining);
			numBytesRemaining -= numInstructionBytes;
			nextByte += numInstructionBytes;

			result += instructionBuffer;
			result += '\n';
		};
	}

	LLVMDisasmDispose(disasmRef);

	return result;
}
