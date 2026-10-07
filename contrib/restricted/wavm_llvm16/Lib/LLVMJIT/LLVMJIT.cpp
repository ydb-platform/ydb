#include "WAVM/LLVMJIT/LLVMJIT.h"
#include <utility>
#include "LLVMJITPrivate.h"
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/HashMap.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/APInt.h>
#include <llvm/ADT/StringRef.h>
#include <llvm/CodeGen/TargetSubtargetInfo.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Type.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Target/TargetMachine.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

namespace llvm {
	class Constant;
}

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

namespace LLVMRuntimeSymbols {
#ifdef _WIN32
	// the LLVM X86 code generator calls __chkstk when allocating more than 4KB of stack space
	extern "C" void __chkstk();
	extern "C" void __CxxFrameHandler3();
#else
#if defined(__APPLE__)
	// LLVM's memset intrinsic lowers to calling __bzero on MacOS when writing a constant zero.
	extern "C" void __bzero();
#endif
#if defined(__i386__) || defined(__x86_64__)
	extern "C" void wavm_probe_stack();
#endif
	extern "C" int __gxx_personality_v0();
	extern "C" void* __cxa_begin_catch(void*) throw();
	extern "C" void __cxa_end_catch();
#endif

	static HashMap<std::string, void*> map = {
		{"memmove", (void*)&memmove},
		{"memset", (void*)&memset},
#ifdef _WIN32
		{"__chkstk", (void*)&__chkstk},
		{"__CxxFrameHandler3", (void*)&__CxxFrameHandler3},
#else
#if defined(__APPLE__)
		{"__bzero", (void*)&__bzero},
#endif
#if defined(__i386__) || defined(__x86_64__)
		{"wavm_probe_stack", (void*)&wavm_probe_stack},
#endif
		{"__gxx_personality_v0", (void*)&__gxx_personality_v0},
		{"__cxa_begin_catch", (void*)&__cxa_begin_catch},
		{"__cxa_end_catch", (void*)&__cxa_end_catch},
#endif
	};
}

llvm::JITEvaluatedSymbol LLVMJIT::resolveJITImport(llvm::StringRef name)
{
	// Allow some intrinsics used by LLVM
	void** symbolValue = LLVMRuntimeSymbols::map.get(name.str());
	if(!symbolValue)
	{
		Errors::fatalf("LLVM generated code references unknown external symbol: %s",
					   name.str().c_str());
	}

	return llvm::JITEvaluatedSymbol(reinterpret_cast<Uptr>(*symbolValue),
									llvm::JITSymbolFlags::None);
}

static bool globalInitLLVM()
{
	llvm::InitializeNativeTarget();
	llvm::InitializeNativeTargetAsmParser();
	llvm::InitializeNativeTargetAsmPrinter();
	llvm::InitializeNativeTargetDisassembler();

	llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
	return true;
}

static void globalInitLLVMOnce()
{
	static bool isLLVMInitialized = globalInitLLVM();
	WAVM_ASSERT(isLLVMInitialized);
}

LLVMContext::LLVMContext()
{
	globalInitLLVMOnce();

	i8Type = llvm::Type::getInt8Ty(*this);
	i16Type = llvm::Type::getInt16Ty(*this);
	i32Type = llvm::Type::getInt32Ty(*this);
	i64Type = llvm::Type::getInt64Ty(*this);
	f32Type = llvm::Type::getFloatTy(*this);
	f64Type = llvm::Type::getDoubleTy(*this);
	i8PtrType = i8Type->getPointerTo();

	externrefType = llvm::StructType::create("Object", i8Type)->getPointerTo();

	i8x8Type = FixedVectorType::get(i8Type, 8);
	i16x4Type = FixedVectorType::get(i16Type, 4);
	i32x2Type = FixedVectorType::get(i32Type, 2);
	i64x1Type = FixedVectorType::get(i64Type, 1);
	f32x2Type = FixedVectorType::get(f32Type, 2);
	f64x1Type = FixedVectorType::get(f64Type, 1);

	i8x16Type = FixedVectorType::get(i8Type, 16);
	i16x8Type = FixedVectorType::get(i16Type, 8);
	i32x4Type = FixedVectorType::get(i32Type, 4);
	i64x2Type = FixedVectorType::get(i64Type, 2);
	f32x4Type = FixedVectorType::get(f32Type, 4);
	f64x2Type = FixedVectorType::get(f64Type, 2);

	i8x32Type = FixedVectorType::get(i8Type, 32);
	i16x16Type = FixedVectorType::get(i16Type, 16);
	i32x8Type = FixedVectorType::get(i32Type, 8);
	i64x4Type = FixedVectorType::get(i64Type, 4);

	i8x48Type = FixedVectorType::get(i8Type, 48);
	i16x24Type = FixedVectorType::get(i16Type, 24);
	i32x12Type = FixedVectorType::get(i32Type, 12);
	i64x6Type = FixedVectorType::get(i64Type, 6);

	i8x64Type = FixedVectorType::get(i8Type, 64);
	i16x32Type = FixedVectorType::get(i16Type, 32);
	i32x16Type = FixedVectorType::get(i32Type, 16);
	i64x8Type = FixedVectorType::get(i64Type, 8);

	valueTypes[(Uptr)ValueType::none] = valueTypes[(Uptr)ValueType::any] = nullptr;
	valueTypes[(Uptr)ValueType::i32] = i32Type;
	valueTypes[(Uptr)ValueType::i64] = i64Type;
	valueTypes[(Uptr)ValueType::f32] = f32Type;
	valueTypes[(Uptr)ValueType::f64] = f64Type;
	valueTypes[(Uptr)ValueType::v128] = i64x2Type;
	valueTypes[(Uptr)ValueType::externref] = externrefType;
	valueTypes[(Uptr)ValueType::funcref] = externrefType;

	// Create zero constants of each type.
	typedZeroConstants[(Uptr)ValueType::none] = nullptr;
	typedZeroConstants[(Uptr)ValueType::any] = nullptr;
	typedZeroConstants[(Uptr)ValueType::i32] = emitLiteral(*this, (U32)0);
	typedZeroConstants[(Uptr)ValueType::i64] = emitLiteral(*this, (U64)0);
	typedZeroConstants[(Uptr)ValueType::f32] = emitLiteral(*this, (F32)0.0f);
	typedZeroConstants[(Uptr)ValueType::f64] = emitLiteral(*this, (F64)0.0);
	typedZeroConstants[(Uptr)ValueType::v128] = emitLiteral(*this, V128());
	typedZeroConstants[(Uptr)ValueType::externref] = typedZeroConstants[(Uptr)ValueType::funcref]
		= llvm::Constant::getNullValue(externrefType);
}

TargetSpec LLVMJIT::getHostTargetSpec()
{
	TargetSpec result;
	result.triple = llvm::sys::getProcessTriple();
	result.cpu = std::string(llvm::sys::getHostCPUName());
	return result;
}

std::unique_ptr<llvm::TargetMachine> LLVMJIT::getTargetMachine(const TargetSpec& targetSpec)
{
	globalInitLLVMOnce();

	llvm::Triple triple(targetSpec.triple);
	llvm::SmallVector<std::string, 1> targetAttributes;

#if LLVM_VERSION_MAJOR < 10
	if(triple.getArch() == llvm::Triple::x86 || triple.getArch() == llvm::Triple::x86_64)
	{
		// Disable AVX-512 on X86 targets to workaround a LLVM backend bug:
		// https://bugs.llvm.org/show_bug.cgi?id=43750
		targetAttributes.push_back("-avx512f");
	}
#endif

	return std::unique_ptr<llvm::TargetMachine>(
		llvm::EngineBuilder().selectTarget(triple, "", targetSpec.cpu, targetAttributes));
}

TargetValidationResult LLVMJIT::validateTargetMachine(
	const std::unique_ptr<llvm::TargetMachine>& targetMachine,
	const FeatureSpec& featureSpec)
{
	const llvm::Triple::ArchType targetArch = targetMachine->getTargetTriple().getArch();
	if(targetArch == llvm::Triple::x86_64)
	{
		// If the SIMD feature is enabled, then require the SSE4.1 CPU feature.
		if(featureSpec.simd && !targetMachine->getMCSubtargetInfo()->checkFeatures("+sse4.1"))
		{ return TargetValidationResult::x86CPUDoesNotSupportSSE41; }

		return TargetValidationResult::valid;
	}
	else if(targetArch == llvm::Triple::aarch64)
	{
		if(featureSpec.simd && !targetMachine->getMCSubtargetInfo()->checkFeatures("+neon"))
		{ return TargetValidationResult::wavmDoesNotSupportSIMDOnArch; }

		return TargetValidationResult::valid;
	}
	else
	{
		if(featureSpec.simd) { return TargetValidationResult::wavmDoesNotSupportSIMDOnArch; }
		if(featureSpec.memory64) { return TargetValidationResult::memory64Requires64bitTarget; }
		if(featureSpec.table64) { return TargetValidationResult::table64Requires64bitTarget; }
		return TargetValidationResult::unsupportedArchitecture;
	}
}

TargetValidationResult LLVMJIT::validateTarget(const TargetSpec& targetSpec,
											   const IR::FeatureSpec& featureSpec)
{
	std::unique_ptr<llvm::TargetMachine> targetMachine = getTargetMachine(targetSpec);
	if(!targetMachine) { return TargetValidationResult::invalidTargetSpec; }
	return validateTargetMachine(targetMachine, featureSpec);
}

Version LLVMJIT::getVersion()
{
	return Version{LLVM_VERSION_MAJOR, LLVM_VERSION_MINOR, LLVM_VERSION_PATCH, 5};
}
