#include "WAVM/Emscripten/Emscripten.h"
#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <atomic>
#include <initializer_list>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include "EmscriptenABI.h"
#include "EmscriptenPrivate.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/FloatComponents.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/LEB128.h"
#include "WAVM/Inline/Serialization.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Clock.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/VFS/VFS.h"
#include "WAVM/WASI/WASIABI.h"

#ifndef _WIN32
#include <sys/uio.h>
#endif

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;
using namespace WAVM::Emscripten;

typedef uint32_t __wasi_errno_return_t;

WAVM_DEFINE_INTRINSIC_MODULE(env)
WAVM_DEFINE_INTRINSIC_MODULE(asm2wasm)
WAVM_DEFINE_INTRINSIC_MODULE(global)

WAVM_DEFINE_INTRINSIC_MODULE(emscripten_wasi_snapshot_preview1)

static emabi::Result asEmscriptenErrNo(VFS::Result result)
{
	switch(result)
	{
	case VFS::Result::success: return emabi::esuccess;
	case VFS::Result::ioPending: return emabi::einprogress;
	case VFS::Result::ioDeviceError: return emabi::eio;
	case VFS::Result::interruptedBySignal: return emabi::eintr;
	case VFS::Result::interruptedByCancellation: return emabi::eintr;
	case VFS::Result::wouldBlock: return emabi::eagain;
	case VFS::Result::inaccessibleBuffer: return emabi::efault;
	case VFS::Result::invalidOffset: return emabi::einval;
	case VFS::Result::notSeekable: return emabi::espipe;
	case VFS::Result::notPermitted: return emabi::eperm;
	case VFS::Result::notAccessible: return emabi::eacces;
	case VFS::Result::notSynchronizable: return emabi::einval;
	case VFS::Result::tooManyBufferBytes: return emabi::einval;
	case VFS::Result::notEnoughBufferBytes: return emabi::einval;
	case VFS::Result::tooManyBuffers: return emabi::einval;
	case VFS::Result::notEnoughBits: return emabi::eoverflow;
	case VFS::Result::exceededFileSizeLimit: return emabi::efbig;
	case VFS::Result::outOfSystemFDs: return emabi::enfile;
	case VFS::Result::outOfProcessFDs: return emabi::emfile;
	case VFS::Result::outOfMemory: return emabi::enomem;
	case VFS::Result::outOfQuota: return emabi::edquot;
	case VFS::Result::outOfFreeSpace: return emabi::enospc;
	case VFS::Result::outOfLinksToParentDir: return emabi::emlink;
	case VFS::Result::invalidNameCharacter: return emabi::eacces;
	case VFS::Result::nameTooLong: return emabi::enametoolong;
	case VFS::Result::tooManyLinksInPath: return emabi::eloop;
	case VFS::Result::alreadyExists: return emabi::eexist;
	case VFS::Result::doesNotExist: return emabi::enoent;
	case VFS::Result::isDirectory: return emabi::eisdir;
	case VFS::Result::isNotDirectory: return emabi::enotdir;
	case VFS::Result::isNotEmpty: return emabi::enotempty;
	case VFS::Result::brokenPipe: return emabi::epipe;
	case VFS::Result::missingDevice: return emabi::enxio;
	case VFS::Result::busy: return emabi::ebusy;
	case VFS::Result::notSupported: return emabi::enotsup;

	default: WAVM_UNREACHABLE();
	};
}

WAVM_DEFINE_INTRINSIC_GLOBAL(env, "ABORT", I32, ABORT, 0);
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "cttz_i8", I32, cttz_i8, 0);
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "___dso_handle", U32, ___dso_handle, 0);

WAVM_DEFINE_INTRINSIC_GLOBAL(env, "__memory_base", U32, memory_base, 1024);
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "memoryBase", U32, emscriptenMemoryBase, 1024);

WAVM_DEFINE_INTRINSIC_GLOBAL(env, "__table_base", U32, table_base, 0);
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "tableBase", U32, emscriptenTableBase, 0);

WAVM_DEFINE_INTRINSIC_GLOBAL(env, "_environ", U32, em_environ, 0)
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "EMTSTACKTOP", U32, EMTSTACKTOP, 0)
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "EMT_STACK_MAX", U32, EMT_STACK_MAX, 0)
WAVM_DEFINE_INTRINSIC_GLOBAL(env, "eb", I32, eb, 0)

emabi::Address Emscripten::dynamicAlloc(Emscripten::Process* process,
										Context* context,
										emabi::Size numBytes)
{
	if(!process->malloc) { return 0; }

	static FunctionType mallocSignature({ValueType::i32}, {ValueType::i32});

	UntaggedValue args[1] = {numBytes};
	UntaggedValue results[1];
	invokeFunction(context, process->malloc, mallocSignature, args, results);

	return results[0].u32;
}

void Emscripten::initThreadLocals(Thread* thread)
{
#if 0
	// Call the establishStackSpace function exported by the module to set the thread's stack
	// address and size.
	Function* establishStackSpace = asFunctionNullable(
		getInstanceExport(thread->process->instance, "establishStackSpace"));
	if(establishStackSpace
	   && getFunctionType(establishStackSpace)
			  == FunctionType(TypeTuple{}, TypeTuple{ValueType::i32, ValueType::i32}))
	{
		IR::UntaggedValue args[2]{thread->stackAddress, thread->numStackBytes};
		invokeFunction(thread->context,
					   establishStackSpace,
					   FunctionType({}, {ValueType::i32, ValueType::i32}),
					   args);
	}
#endif
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_emscripten_get_heap_size",
							   emabi::Result,
							   emscripten_get_heap_size)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	return coerce32bitAddress(process->memory,
							  Runtime::getMemoryNumPages(process->memory) * IR::numBytesPerPage);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "getTotalMemory", emabi::Result, getTotalMemory)
{
	return emscripten_get_heap_size(contextRuntimeData);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "__assert_fail",
							   void,
							   emscripten_assert_fail,
							   I32,
							   I32,
							   I32,
							   I32)
{
	throwException(ExceptionTypes::calledAbort);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "abortOnCannotGrowMemory",
							   emabi::Result,
							   abortOnCannotGrowMemory,
							   I32 size)
{
	throwException(ExceptionTypes::calledAbort);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "abortStackOverflow", void, abortStackOverflow, I32 size)
{
	throwException(ExceptionTypes::stackOverflow);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_time", emabi::Result, emscripten_time, U32 address)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	time_t t = time(nullptr);
	if(address) { memoryRef<I32>(process->memory, address) = (I32)t; }
	return (emabi::Result)t;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___setErrNo", void, emscripten___seterrno, I32 value)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	if(process->errnoLocation)
	{
		UntaggedValue errnoLocationResult;
		invokeFunction(getContextFromRuntimeData(contextRuntimeData),
					   process->errnoLocation,
					   FunctionType({ValueType::i32}, {}),
					   nullptr,
					   &errnoLocationResult);

		memoryRef<I32>(process->memory, errnoLocationResult.i32) = (I32)value;
	}
}

static thread_local I32 tempRet0;
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "setTempRet0", void, setTempRet0, I32 value)
{
	tempRet0 = value;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "getTempRet0", emabi::Result, getTempRet0) { return tempRet0; }

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_sysconf", emabi::Result, emscripten_sysconf, I32 a)
{
	enum : U32
	{
		sysConfPageSize = 30
	};
	switch(a)
	{
	case sysConfPageSize: return 16384;
	default: throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___ctype_b_loc", emabi::Address, emscripten___ctype_b_loc)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	unsigned short data[384] = {
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     2,     2,
		2,     2,     2,     2,     2,     2,     2,     8195,  8194,  8194,  8194,  8194,  2,
		2,     2,     2,     2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
		2,     2,     2,     2,     24577, 49156, 49156, 49156, 49156, 49156, 49156, 49156, 49156,
		49156, 49156, 49156, 49156, 49156, 49156, 49156, 55304, 55304, 55304, 55304, 55304, 55304,
		55304, 55304, 55304, 55304, 49156, 49156, 49156, 49156, 49156, 49156, 49156, 54536, 54536,
		54536, 54536, 54536, 54536, 50440, 50440, 50440, 50440, 50440, 50440, 50440, 50440, 50440,
		50440, 50440, 50440, 50440, 50440, 50440, 50440, 50440, 50440, 50440, 50440, 49156, 49156,
		49156, 49156, 49156, 49156, 54792, 54792, 54792, 54792, 54792, 54792, 50696, 50696, 50696,
		50696, 50696, 50696, 50696, 50696, 50696, 50696, 50696, 50696, 50696, 50696, 50696, 50696,
		50696, 50696, 50696, 50696, 49156, 49156, 49156, 49156, 2,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
		0,     0,     0,     0,     0,     0,     0};
	static emabi::Address vmAddress = 0;
	if(vmAddress == 0)
	{
		emabi::Address allocAddress
			= dynamicAlloc(process, getContextFromRuntimeData(contextRuntimeData), sizeof(data));
		if(!allocAddress) { return 0; }
		vmAddress = coerce32bitAddress(process->memory, allocAddress);
		memcpy(memoryArrayPtr<U8>(process->memory, vmAddress, sizeof(data)), data, sizeof(data));
	}
	return vmAddress + sizeof(short) * 128;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___ctype_toupper_loc",
							   emabi::Address,
							   emscripten___ctype_toupper_loc)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	I32 data[384]
		= {128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145,
		   146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163,
		   164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181,
		   182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199,
		   200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217,
		   218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235,
		   236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253,
		   254, -1,  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
		   16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,
		   34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,
		   52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,
		   70,  71,  72,  73,  74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,
		   88,  89,  90,  91,  92,  93,  94,  95,  96,  65,  66,  67,  68,  69,  70,  71,  72,  73,
		   74,  75,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,  90,  123,
		   124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141,
		   142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
		   160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,
		   178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195,
		   196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213,
		   214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231,
		   232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249,
		   250, 251, 252, 253, 254, 255};
	static emabi::Address vmAddress = 0;
	if(vmAddress == 0)
	{
		emabi::Address allocAddress
			= dynamicAlloc(process, getContextFromRuntimeData(contextRuntimeData), sizeof(data));
		if(!allocAddress) { return 0; }
		vmAddress = coerce32bitAddress(process->memory, allocAddress);
		memcpy(memoryArrayPtr<U8>(process->memory, vmAddress, sizeof(data)), data, sizeof(data));
	}
	return vmAddress + sizeof(I32) * 128;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___ctype_tolower_loc",
							   emabi::Address,
							   emscripten___ctype_tolower_loc)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	I32 data[384]
		= {128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145,
		   146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163,
		   164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181,
		   182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199,
		   200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217,
		   218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235,
		   236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253,
		   254, -1,  0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,
		   16,  17,  18,  19,  20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,
		   34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,
		   52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  62,  63,  64,  97,  98,  99,  100, 101,
		   102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
		   120, 121, 122, 91,  92,  93,  94,  95,  96,  97,  98,  99,  100, 101, 102, 103, 104, 105,
		   106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123,
		   124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141,
		   142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
		   160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177,
		   178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195,
		   196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213,
		   214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231,
		   232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249,
		   250, 251, 252, 253, 254, 255};
	static emabi::Address vmAddress = 0;
	if(vmAddress == 0)
	{
		emabi::Address allocAddress
			= dynamicAlloc(process, getContextFromRuntimeData(contextRuntimeData), sizeof(data));
		if(!allocAddress) { return 0; }
		vmAddress = coerce32bitAddress(process->memory, allocAddress);
		memcpy(memoryArrayPtr<U8>(process->memory, vmAddress, sizeof(data)), data, sizeof(data));
	}
	return vmAddress + sizeof(I32) * 128;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___assert_fail",
							   void,
							   emscripten___assert_fail,
							   I32 condition,
							   I32 filename,
							   I32 line,
							   I32 function)
{
	throwException(Runtime::ExceptionTypes::calledAbort);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___cxa_atexit",
							   emabi::Result,
							   emscripten___cxa_atexit,
							   I32 a,
							   I32 b,
							   I32 c)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___cxa_guard_acquire",
							   emabi::Result,
							   emscripten___cxa_guard_acquire,
							   U32 address)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	if(!memoryRef<U8>(process->memory, address))
	{
		memoryRef<U8>(process->memory, address) = 1;
		return 1;
	}
	else
	{
		return emabi::esuccess;
	}
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___cxa_guard_release",
							   void,
							   emscripten___cxa_guard_release,
							   I32 a)
{
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___cxa_throw",
							   void,
							   emscripten___cxa_throw,
							   I32 a,
							   I32 b,
							   I32 c)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___cxa_begin_catch",
							   emabi::Address,
							   emscripten___cxa_begin_catch,
							   I32 a)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___cxa_allocate_exception",
							   U32,
							   emscripten___cxa_allocate_exception,
							   U32 size)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	return coerce32bitAddress(
		process->memory,
		dynamicAlloc(process, getContextFromRuntimeData(contextRuntimeData), size));
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "__ZSt18uncaught_exceptionv",
							   emabi::Result,
							   emscripten__ZSt18uncaught_exceptionv)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_abort", void, emscripten__abort)
{
	throwException(Runtime::ExceptionTypes::calledAbort);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_exit", void, emscripten__exit, U32 code)
{
	throw Emscripten::ExitException{code};
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "abort", void, emscripten_abort, I32 code)
{
	Log::printf(Log::error, "env.abort(%i)\n", code);
	throwException(Runtime::ExceptionTypes::calledAbort);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "proc_exit",
							   void,
							   wasi_proc_exit,
							   __wasi_exitcode_t exitCode)
{
	throw ExitException{exitCode};
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_i", void, emscripten_nullFunc_i, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_ii", void, emscripten_nullFunc_ii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_iii", void, emscripten_nullFunc_iii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_iiii", void, emscripten_nullFunc_iiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_iiiii", void, emscripten_nullFunc_iiiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_iiiiii", void, emscripten_nullFunc_iiiiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_iiiiiii", void, emscripten_nullFunc_iiiiiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_v", void, emscripten_nullFunc_v, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_vi", void, emscripten_nullFunc_vi, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_vii", void, emscripten_nullFunc_vii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_viii", void, emscripten_nullFunc_viii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_viiii", void, emscripten_nullFunc_viiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_viiiii", void, emscripten_nullFunc_viiiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_viiiiii", void, emscripten_nullFunc_viiiiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_iidiiii", void, emscripten_nullFunc_iidiiii, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "nullFunc_jiji", void, emscripten_nullFunc_jiji, I32 code)
{
	throwException(Runtime::ExceptionTypes::uninitializedTableElement);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_uselocale",
							   emabi::Address,
							   emscripten_uselocale,
							   emabi::Address newLocale)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	return process->currentLocale.exchange(newLocale);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_newlocale",
							   emabi::Address,
							   emscripten_newlocale,
							   I32 mask,
							   emabi::Address locale,
							   emabi::Address base)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	if(!base)
	{
		base = coerce32bitAddress(
			process->memory,
			dynamicAlloc(process, getContextFromRuntimeData(contextRuntimeData), 4));
	}
	return base;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_freelocale", void, emscripten__freelocale, emabi::Address a)
{
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_strftime_l",
							   I32,
							   emscripten__strftime_l,
							   I32 a,
							   I32 b,
							   I32 c,
							   I32 d,
							   I32 e)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_strerror", emabi::Address, emscripten__strerror, I32 a)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_catopen", emabi::Result, emscripten__catopen, I32 a, I32 b)
{
	return emabi::enosys;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_catgets",
							   emabi::Result,
							   emscripten__catgets,
							   I32 catd,
							   I32 set_id,
							   I32 msg_id,
							   I32 s)
{
	return s;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_catclose", emabi::Result, emscripten__catclose, I32 a)
{
	return emabi::esuccess;
}

enum class ioStreamVMHandle
{
	StdIn = 0,
	StdOut = 1,
	StdErr = 2,
};
static VFS::VFD* getVFD(Emscripten::Process* process, emabi::FD vmHandle)
{
	switch((ioStreamVMHandle)vmHandle)
	{
	case ioStreamVMHandle::StdIn: return process->stdIn;
	case ioStreamVMHandle::StdOut: return process->stdOut;
	case ioStreamVMHandle::StdErr: return process->stdErr;
	default: return nullptr;
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_vfprintf",
							   emabi::Result,
							   emscripten_vfprintf,
							   emabi::FD file,
							   emabi::Address formatStringAddress,
							   emabi::Address argListAddress)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_fread",
							   emabi::Result,
							   emscripten_fread,
							   U32 destAddress,
							   U32 size,
							   U32 count,
							   emabi::FD file)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	VFS::VFD* fd = getVFD(process, file);
	if(!fd) { return emabi::esuccess; }

	const U64 numBytes64 = U64(size) * U64(count);
	WAVM_ERROR_UNLESS(numBytes64 <= UINTPTR_MAX);
	const Uptr numBytes = Uptr(numBytes64);
	Uptr numBytesRead = 0;
	const VFS::Result result = fd->read(
		memoryArrayPtr<U8>(process->memory, destAddress, numBytes), numBytes, &numBytesRead);
	if(result != VFS::Result::success) { return emabi::esuccess; }
	else
	{
		WAVM_ASSERT(numBytesRead < UINT32_MAX);
		return U32(numBytesRead);
	}
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_fwrite",
							   emabi::Result,
							   emscripten_fwrite,
							   emabi::Address sourceAddress,
							   emabi::Size size,
							   emabi::Size count,
							   emabi::FD file)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	VFS::VFD* fd = getVFD(process, file);
	if(!fd) { return emabi::esuccess; }

	const U64 numBytes64 = U64(size) * U64(count);
	const Uptr numBytes = Uptr(numBytes64);
	if(numBytes > emabi::sizeMax || numBytes > emabi::resultMax) { return emabi::eoverflow; }
	Uptr numBytesWritten = 0;
	const VFS::Result result = fd->write(
		memoryArrayPtr<U8>(process->memory, sourceAddress, numBytes), numBytes, &numBytesWritten);
	if(result != VFS::Result::success) { return emabi::esuccess; }
	else
	{
		WAVM_ASSERT(numBytesWritten < emabi::resultMax);
		return emabi::Result(numBytesWritten);
	}
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_fputc",
							   emabi::Result,
							   emscripten_fputc,
							   I32 character,
							   emabi::FD file)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	VFS::VFD* fd = getVFD(process, file);
	if(!fd) { return emabi::ebadf; }

	char c = char(character);

	Uptr numBytesWritten = 0;
	const VFS::Result result = fd->write(&c, 1, &numBytesWritten);
	return result == VFS::Result::success ? emabi::esuccess : asEmscriptenErrNo(result);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_fflush", emabi::Result, emscripten_fflush, emabi::FD file)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	VFS::VFD* fd = getVFD(process, file);
	if(!fd) { return emabi::ebadf; }

	return fd->sync(VFS::SyncType::contentsAndMetadata) == VFS::Result::success ? 0 : -1;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___lock", void, emscripten___lock, I32 a) {}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___unlock", void, emscripten___unlock, I32 a) {}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___lockfile", emabi::Result, emscripten___lockfile, I32 a)
{
	return emabi::enosys;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___unlockfile", void, emscripten___unlockfile, I32 a) {}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___syscall6", I32, emscripten_close, emabi::FD file, I32)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	VFS::VFD* fd = getVFD(process, file);
	if(!fd) { return emabi::ebadf; }

	return emabi::enosys;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___syscall54", I32, emscripten_ioctl, I32 a, I32 b)
{
	// ioctl
	return emabi::esuccess;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___syscall140", I32, emscripten_llseek, I32 a, I32 b)
{
	// llseek
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___syscall145",
							   emabi::Result,
							   emscripten_readv,
							   I32,
							   emabi::Address argsPtr)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	U32* args = memoryArrayPtr<U32>(process->memory, argsPtr, 3);
	emabi::FD file = args[0];
	U32 iov = args[1];
	U32 iovcnt = args[2];

	VFS::VFD* fd = getVFD(process, file);
	if(!fd) { return emabi::ebadf; }

	Uptr totalNumBytesRead = 0;
	for(U32 i = 0; i < iovcnt; i++)
	{
		const U32 destAddress = memoryRef<U32>(process->memory, iov + i * 8);
		const U32 numBytes = memoryRef<U32>(process->memory, iov + i * 8 + 4);

		Uptr numBytesRead = 0;
		if(numBytes > 0
		   && fd->read(memoryArrayPtr<U8>(process->memory, destAddress, numBytes),
					   numBytes,
					   &numBytesRead)
				  != VFS::Result::success)
		{ return -1; }
		totalNumBytesRead += numBytesRead;
		if(numBytesRead < numBytes) { break; }
	}
	return coerce32bitAddressResult(process->memory, totalNumBytesRead);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___syscall4",
							   emabi::Result,
							   emscripten_write,
							   I32,
							   emabi::Address argsAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	emabi::Result result = 0;
	Runtime::catchRuntimeExceptions(
		[&] {
			struct ArgStruct
			{
				emabi::FD file;
				emabi::Address bufferAddress;
				emabi::Address numBytes;
			};
			ArgStruct args = memoryRef<ArgStruct>(process->memory, argsAddress);

			VFS::VFD* fd = getVFD(process, args.file);
			if(!fd) { result = emabi::ebadf; }
			else
			{
				U8* buffer = memoryArrayPtr<U8>(process->memory, args.bufferAddress, args.numBytes);

				// Ensure that it will be possible to return the number of bytes in a successful
				// write in the I32 return value.
				if(args.numBytes > INT32_MAX) { result = emabi::eoverflow; }
				else
				{
					// Do the write.
					U64 numBytesWritten = 0;
					VFS::Result vfsResult
						= fd->write(buffer, args.numBytes, &numBytesWritten, nullptr);
					if(vfsResult != VFS::Result::success) { result = asEmscriptenErrNo(vfsResult); }
					else
					{
						WAVM_ASSERT(numBytesWritten <= emabi::resultMax);
						result = I32(numBytesWritten);
					}
				}
			}
		},
		[&](Exception* exception) {
			// If we catch an out-of-bounds memory exception, return EFAULT.
			WAVM_ERROR_UNLESS(getExceptionType(exception)
							  == ExceptionTypes::outOfBoundsMemoryAccess);
			Log::printf(Log::debug,
						"Caught runtime exception while accessing memory at address 0x%" PRIx64,
						getExceptionArgument(exception, 1).i64);
			destroyException(exception);
			result = emabi::efault;
		});

	return result;
}

static emabi::Result writeImpl(Emscripten::Process* process,
							   emabi::FD fd,
							   emabi::Address iovsAddress,
							   I32 numIOVs,
							   const __wasi_filesize_t* offset,
							   Uptr& outNumBytesWritten,
							   Uptr maxNumBytesWritten)
{
	if(numIOVs < 0 || numIOVs > __WASI_IOV_MAX) { return emabi::einval; }

	VFS::VFD* vfd = getVFD(process, fd);
	if(!vfd) { return emabi::ebadf; }

	// Allocate memory for the IOWriteBuffers.
	auto vfsWriteBuffers = (VFS::IOWriteBuffer*)malloc(numIOVs * sizeof(VFS::IOWriteBuffer));

	// Catch any out-of-bounds memory access exceptions that are thrown.
	emabi::Result result = emabi::esuccess;
	Runtime::catchRuntimeExceptions(
		[&] {
			// Translate the IOVs to IOWriteBuffers
			const __wasi_ciovec_t* iovs
				= memoryArrayPtr<__wasi_ciovec_t>(process->memory, iovsAddress, numIOVs);
			U64 numBufferBytes = 0;
			for(I32 iovIndex = 0; iovIndex < numIOVs; ++iovIndex)
			{
				__wasi_ciovec_t iov = iovs[iovIndex];
				vfsWriteBuffers[iovIndex].data
					= memoryArrayPtr<const U8>(process->memory, iov.buf, iov.buf_len);
				vfsWriteBuffers[iovIndex].numBytes = iov.buf_len;
				numBufferBytes += iov.buf_len;
			}
			if(numBufferBytes > maxNumBytesWritten) { result = emabi::eoverflow; }
			else
			{
				// Do the writes.
				result = asEmscriptenErrNo(
					vfd->writev(vfsWriteBuffers, numIOVs, &outNumBytesWritten, offset));
			}
		},
		[&](Exception* exception) {
			// If we catch an out-of-bounds memory exception, return EFAULT.
			WAVM_ERROR_UNLESS(getExceptionType(exception)
							  == ExceptionTypes::outOfBoundsMemoryAccess);
			Log::printf(Log::debug,
						"Caught runtime exception while reading memory at address 0x%" PRIx64,
						getExceptionArgument(exception, 1).i64);
			destroyException(exception);
			result = emabi::efault;
		});

	// Free the VFS write buffers.
	free(vfsWriteBuffers);

	return result;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___syscall146",
							   emabi::Result,
							   emscripten_writev,
							   I32,
							   emabi::Address argsAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	// Read the args from memory.
	struct ArgStruct
	{
		emabi::FD fd;
		emabi::Address iovsAddress;
		emabi::Size numIOVs;
	};
	ArgStruct args;
	Runtime::unwindSignalsAsExceptions(
		[&] { args = memoryRef<ArgStruct>(process->memory, argsAddress); });

	// Do the write.
	Uptr numBytesWritten = 0;
	emabi::Result result = writeImpl(process,
									 args.fd,
									 args.iovsAddress,
									 args.numIOVs,
									 nullptr,
									 numBytesWritten,
									 emabi::resultMax);
	if(result == emabi::esuccess)
	{
		WAVM_ASSERT(numBytesWritten <= emabi::resultMax);
		result = emabi::Result(numBytesWritten);
	}

	return result;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "fd_close",
							   __wasi_errno_return_t,
							   emscripten_fd_close,
							   emabi::FD fd)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	VFS::VFD* vfd = getVFD(process, fd);
	if(!vfd) { return __WASI_EBADF; }

	// We only support stdio at the moment, so don't bother actually closing those VFDs.
	return __WASI_ESUCCESS;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "fd_write",
							   __wasi_errno_return_t,
							   emscripten_fd_write,
							   emabi::FD fd,
							   emabi::Address iovsAddress,
							   I32 numIOVs,
							   emabi::Address numBytesWrittenAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	Uptr numBytesWritten = 0;
	const emabi::Result emscriptenResult
		= writeImpl(process, fd, iovsAddress, numIOVs, nullptr, numBytesWritten, emabi::sizeMax);

	// Write the number of bytes written to memory.
	WAVM_ASSERT(numBytesWritten <= emabi::sizeMax);
	memoryRef<emabi::Size>(process->memory, numBytesWrittenAddress) = emabi::Size(numBytesWritten);

	return (__wasi_errno_t)-emscriptenResult;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "fd_seek",
							   __wasi_errno_return_t,
							   wasi_fd_seek,
							   emabi::FD fd,
							   __wasi_filedelta_t offset,
							   U32 whence,
							   emabi::Address newOffsetAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	VFS::VFD* vfd = getVFD(process, fd);
	if(!vfd) { return __WASI_EBADF; }

	VFS::SeekOrigin origin;
	switch(whence)
	{
	case __WASI_WHENCE_CUR: origin = VFS::SeekOrigin::cur; break;
	case __WASI_WHENCE_END: origin = VFS::SeekOrigin::end; break;
	case __WASI_WHENCE_SET: origin = VFS::SeekOrigin::begin; break;
	default: return __WASI_EINVAL;
	};

	U64 newOffset;
	const VFS::Result result = vfd->seek(offset, origin, &newOffset);
	if(result != VFS::Result::success) { return (__wasi_errno_t)-asEmscriptenErrNo(result); }

	memoryRef<__wasi_filesize_t>(process->memory, newOffsetAddress) = newOffset;
	return __WASI_ESUCCESS;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(asm2wasm, "f64-to-int", I32, f64_to_int, F64 f) { return (I32)f; }

static F64 makeNaN()
{
	FloatComponents<F64> floatBits;
	floatBits.bits.sign = 0;
	floatBits.bits.exponent = FloatComponents<F64>::maxExponentBits;
	floatBits.bits.significand = FloatComponents<F64>::canonicalSignificand;
	return floatBits.value;
}
static F64 makeInf()
{
	FloatComponents<F64> floatBits;
	floatBits.bits.sign = 0;
	floatBits.bits.exponent = FloatComponents<F64>::maxExponentBits;
	floatBits.bits.significand = FloatComponents<F64>::maxSignificand;
	return floatBits.value;
}

WAVM_DEFINE_INTRINSIC_GLOBAL(global, "NaN", F64, NaN, makeNaN())
WAVM_DEFINE_INTRINSIC_GLOBAL(global, "Infinity", F64, Infinity, makeInf())

WAVM_DEFINE_INTRINSIC_FUNCTION(asm2wasm, "i32u-rem", U32, I32_remu, U32 left, U32 right)
{
	return left % right;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(asm2wasm, "i32s-rem", I32, I32_rems, I32 left, I32 right)
{
	return left % right;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(asm2wasm, "i32u-div", U32, I32_divu, U32 left, U32 right)
{
	return left / right;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(asm2wasm, "i32s-div", I32, I32_divs, I32 left, I32 right)
{
	return left / right;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(asm2wasm, "f64-rem", F64, F64_rems, F64 left, F64 right)
{
	return (F64)fmod(left, right);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "getenv", emabi::Result, emscripten_getenv, I32 a)
{
	return emabi::esuccess;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "gettimeofday",
							   emabi::Result,
							   emscripten_gettimeofday,
							   I32 timevalAddress,
							   I32)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	const Time realtimeClock = Platform::getClockTime(Platform::Clock::realtime);
	memoryRef<U32>(process->memory, timevalAddress + 0) = U32(realtimeClock.ns / 1000000000);
	memoryRef<U32>(process->memory, timevalAddress + 4) = U32(realtimeClock.ns / 1000 % 1000000000);
	return emabi::esuccess;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "gmtime", emabi::Address, emscripten_gmtime, I32 timeAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	emabi::time_t emscriptenTime = 0;
	Runtime::unwindSignalsAsExceptions(
		[&] { emscriptenTime = memoryRef<emabi::time_t>(process->memory, timeAddress); });

	time_t hostTime = (time_t)emscriptenTime;
	struct tm* tmPtr = WAVM_SCOPED_DISABLE_SECURE_CRT_WARNINGS(gmtime(&hostTime));
	if(!tmPtr) { return 0; }
	else
	{
		struct tm hostTM = *tmPtr;

		const emabi::Address emTMAddress = dynamicAlloc(
			process, getContextFromRuntimeData(contextRuntimeData), sizeof(emabi::tm));
		if(emTMAddress)
		{
			emabi::tm& emTM = memoryRef<emabi::tm>(process->memory, emTMAddress);

			emTM.tm_sec = hostTM.tm_sec;
			emTM.tm_min = hostTM.tm_min;
			emTM.tm_hour = hostTM.tm_hour;
			emTM.tm_mday = hostTM.tm_mday;
			emTM.tm_mon = hostTM.tm_mon;
			emTM.tm_year = hostTM.tm_year;
			emTM.tm_wday = hostTM.tm_wday;
			emTM.tm_yday = hostTM.tm_yday;
			emTM.tm_isdst = hostTM.tm_isdst;
			emTM.__tm_gmtoff = 0;
			emTM.__tm_zone = 0;
		}

		return emTMAddress;
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_sem_init",
							   emabi::Result,
							   emscripten_sem_init,
							   I32 a,
							   I32 b,
							   I32 c)
{
	return emabi::esuccess;
}

WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env, "invoke_ii", U32, invoke_ii, U32 index, U32 a);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "invoke_iii",
											 U32,
											 invoke_iii,
											 U32 index,
											 U32 a,
											 U32 b);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env, "invoke_v", void, invoke_v, U32 index);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "invoke_vii",
											 void,
											 invoke_vii,
											 U32 index,
											 U32 a,
											 U32 b);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "___buildEnvironment",
											 void,
											 ___buildEnvironment,
											 U32);

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "___cxa_pure_virtual", void, emscripten___cxa_pure_virtual)
{
	Errors::fatal("env.__cxa_pure_virtual called");
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "___syscall192",
							   emabi::Result,
							   _mmap2,
							   U32 which,
							   U32 varargsAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	// const U32 address = memoryRef<U32>(process->memory, varargsAddress + 0);
	const emabi::Size numBytes = memoryRef<emabi::Size>(process->memory, varargsAddress + 4);
	// const U32 protection = memoryRef<U32>(process->memory, varargsAddress + 8);
	// const U32 flags = memoryRef<U32>(process->memory, varargsAddress + 12);
	const emabi::FD file = memoryRef<emabi::FD>(process->memory, varargsAddress + 16);
	// const U32 offset = memoryRef<U32>(process->memory, varargsAddress + 20);

	if(file != -1) { return emabi::enosys; }
	else
	{
		Function* memalignFunction = getTypedInstanceExport(
			process->instance,
			"_memalign",
			FunctionType({ValueType::i32}, {ValueType::i32, ValueType::i32}));
		if(!memalignFunction) { return emabi::enosys; }

		UntaggedValue memalignArgs[2] = {U32(16384), numBytes};
		UntaggedValue memalignResult;
		invokeFunction(getContextFromRuntimeData(contextRuntimeData),
					   memalignFunction,
					   FunctionType(ValueType::i32, {ValueType::i32, ValueType::i32}),
					   memalignArgs,
					   &memalignResult);
		if(!memalignResult.u32) { return emabi::enomem; }
		memset(memoryArrayPtr<char>(process->memory, memalignResult.i32, numBytes), 0, numBytes);

		WAVM_ERROR_UNLESS(memalignResult.u32 < emabi::resultMax);
		return (emabi::Result)memalignResult.u32;
	}
}
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "___syscall195",
											 emabi::Result,
											 emscripten_stat64,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "___syscall20",
											 emabi::Result,
											 emscripten_getpid,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "___syscall221",
											 emabi::Result,
											 emscriptenfcntl64,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "___syscall5",
											 emabi::Result,
											 emscripten_open,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "___syscall91",
											 emabi::Result,
											 emscripten_munmap,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env, "_atexit", emabi::Result, _atexit, U32);

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "clock_time_get",
							   __wasi_errno_return_t,
							   __wasi_clock_time_get,
							   __wasi_clockid_t clockId,
							   __wasi_timestamp_t precision,
							   emabi::Address timeAddress)
{
	Process* process = getProcess(contextRuntimeData);

	Platform::Clock platformClock;
	Time platformClockOrigin;
	switch(clockId)
	{
	case __WASI_CLOCK_REALTIME: platformClock = Platform::Clock::realtime; break;
	case __WASI_CLOCK_MONOTONIC: platformClock = Platform::Clock::monotonic; break;
	case __WASI_CLOCK_PROCESS_CPUTIME_ID:
	case __WASI_CLOCK_THREAD_CPUTIME_ID:
		platformClock = Platform::Clock::processCPUTime;
		platformClockOrigin = process->processClockOrigin;
		break;
	default: return __WASI_EINVAL;
	}

	Time clockTime = Platform::getClockTime(platformClock);

	clockTime.ns -= platformClockOrigin.ns;

	__wasi_timestamp_t wasiClockTime = __wasi_timestamp_t(clockTime.ns);
	memoryRef<__wasi_timestamp_t>(process->memory, timeAddress) = wasiClockTime;

	return __WASI_ESUCCESS;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "clock_gettime",
							   emabi::Result,
							   emscripten_clock_gettime,
							   U32 clockId,
							   U32 timespecAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);

	Platform::Clock platformClock;
	switch(clockId)
	{
	case 0:
		// CLOCK_REALTIME
		platformClock = Platform::Clock::realtime;
		break;
	case 1:
		// CLOCK_MONOTONIC
		platformClock = Platform::Clock::monotonic;
		break;
	case 2:
		// CLOCK_PROCESS_CPUTIME_ID
		platformClock = Platform::Clock::processCPUTime;
		break;
	case 3:
		// CLOCK_THREAD_CPUTIME_ID
		platformClock = Platform::Clock::processCPUTime;
		break;
	default: return emabi::einval;
	}
	const Time clockTime = Platform::getClockTime(platformClock);

	memoryRef<U32>(process->memory, timespecAddress + 0) = U32(clockTime.ns / 1000000000);
	memoryRef<U32>(process->memory, timespecAddress + 4) = U32(clockTime.ns % 1000000000);

	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_getpagesize", U32, emscripten_getpagesize) { return 16384; }
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "_llvm_log10_f64",
											 F64,
											 emscripten_llvm_log10_f64,
											 F64);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "_llvm_log2_f64",
											 F64,
											 emscripten_llvm_log2_f64,
											 F64);
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_llvm_trap", void, emscripten_llvm_trap)
{
	throwException(ExceptionTypes::calledAbort);
}
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "_llvm_trunc_f64",
											 F64,
											 emscripten_llvm_trunc_f64,
											 F64);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env,
											 "_localtime_r",
											 U32,
											 emscripten_localtime_r,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env, "_longjmp", void, emscripten_longjmp, U32, U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(env, "_mktime", U32, emscripten_mktime, U32);

WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_sched_yield", emabi::Result, emscripten_sched_yield)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_sem_destroy", emabi::Result, emscripten_sem_destroy, U32)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(env,
							   "_strftime",
							   emabi::Result,
							   emscripten_strftime,
							   U32,
							   U32,
							   U32,
							   U32)
{
	return emabi::esuccess;
}
// WAVM_DEFINE_INTRINSIC_FUNCTION(env, "_tzset", void, _tzset) { }

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "args_sizes_get",
							   __wasi_errno_return_t,
							   wasi_args_sizes_get,
							   U32 argcAddress,
							   U32 argBufSizeAddress)
{
	Process* process = getProcess(contextRuntimeData);

	Uptr numArgBufferBytes = 0;
	for(const std::string& arg : process->args) { numArgBufferBytes += arg.size() + 1; }

	if(process->args.size() > emabi::addressMax || numArgBufferBytes > emabi::addressMax)
	{ return __WASI_EOVERFLOW; }
	memoryRef<emabi::Address>(process->memory, argcAddress) = emabi::Address(process->args.size());
	memoryRef<emabi::Address>(process->memory, argBufSizeAddress)
		= emabi::Address(numArgBufferBytes);

	return __WASI_ESUCCESS;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "args_get",
							   __wasi_errno_return_t,
							   wasi_args_get,
							   U32 argvAddress,
							   U32 argBufAddress)
{
	Process* process = getProcess(contextRuntimeData);

	emabi::Address nextArgBufAddress = argBufAddress;
	for(Uptr argIndex = 0; argIndex < process->args.size(); ++argIndex)
	{
		const std::string& arg = process->args[argIndex];
		const Uptr numArgBytes = arg.size() + 1;

		if(numArgBytes > emabi::addressMax
		   || nextArgBufAddress > emabi::addressMax - numArgBytes - 1)
		{ return __WASI_EOVERFLOW; }

		if(numArgBytes > 0)
		{
			memcpy(memoryArrayPtr<U8>(process->memory, nextArgBufAddress, numArgBytes),
				   (const U8*)arg.c_str(),
				   numArgBytes);
		}
		memoryRef<emabi::Address>(process->memory, argvAddress + argIndex * sizeof(U32))
			= emabi::Address(nextArgBufAddress);

		nextArgBufAddress += emabi::Address(numArgBytes);
	}

	return __WASI_ESUCCESS;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "environ_sizes_get",
							   __wasi_errno_return_t,
							   wasi_environ_sizes_get,
							   emabi::Address envCountAddress,
							   emabi::Address envBufSizeAddress)
{
	Process* process = getProcess(contextRuntimeData);

	Uptr numEnvBufferBytes = 0;
	for(const std::string& env : process->envs) { numEnvBufferBytes += env.size() + 1; }

	if(process->envs.size() > emabi::addressMax || numEnvBufferBytes > emabi::addressMax)
	{ return __WASI_EOVERFLOW; }
	memoryRef<emabi::Address>(process->memory, envCountAddress)
		= emabi::Address(process->envs.size());
	memoryRef<emabi::Address>(process->memory, envBufSizeAddress)
		= emabi::Address(numEnvBufferBytes);

	return __WASI_ESUCCESS;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(emscripten_wasi_snapshot_preview1,
							   "environ_get",
							   __wasi_errno_return_t,
							   wasi_environ_get,
							   emabi::Address envvAddress,
							   emabi::Address envBufAddress)
{
	Process* process = getProcess(contextRuntimeData);

	emabi::Address nextEnvBufAddress = envBufAddress;
	for(Uptr argIndex = 0; argIndex < process->envs.size(); ++argIndex)
	{
		const std::string& env = process->envs[argIndex];
		const Uptr numEnvBytes = env.size() + 1;

		if(numEnvBytes > emabi::addressMax
		   || nextEnvBufAddress > emabi::addressMax - numEnvBytes - 1)
		{ return __WASI_EOVERFLOW; }

		if(numEnvBytes > 0)
		{
			memcpy(memoryArrayPtr<U8>(process->memory, nextEnvBufAddress, numEnvBytes),
				   (const U8*)env.c_str(),
				   numEnvBytes);
		}
		memoryRef<emabi::Address>(process->memory, envvAddress + argIndex * sizeof(U32))
			= emabi::Address(nextEnvBufAddress);

		nextEnvBufAddress += emabi::Address(numEnvBytes);
	}

	return __WASI_ESUCCESS;
}

Emscripten::Process::~Process()
{
	// Instead of allowing an Instance to live on until all its threads exit, wait for all threads
	// to exit before destroying the Instance.
	joinAllThreads(*this);
}

static bool loadEmscriptenMetadata(const IR::Module& module, EmscriptenModuleMetadata& outMetadata)
{
	for(const CustomSection& customSection : module.customSections)
	{
		if(customSection.name == "emscripten_metadata")
		{
			try
			{
				Serialization::MemoryInputStream sectionStream(customSection.data.data(),
															   customSection.data.size());

				serializeVarUInt32(sectionStream, outMetadata.metadataVersionMajor);
				serializeVarUInt32(sectionStream, outMetadata.metadataVersionMinor);

				if(outMetadata.metadataVersionMajor != 0 || outMetadata.metadataVersionMinor < 2)
				{
					Log::printf(Log::error,
								"Unsupported Emscripten module metadata version: %u\n",
								outMetadata.metadataVersionMajor);
					return false;
				}

				serializeVarUInt32(sectionStream, outMetadata.abiVersionMajor);
				serializeVarUInt32(sectionStream, outMetadata.abiVersionMinor);

				serializeVarUInt32(sectionStream, outMetadata.backendID);
				serializeVarUInt32(sectionStream, outMetadata.numMemoryPages);
				serializeVarUInt32(sectionStream, outMetadata.numTableElems);
				serializeVarUInt32(sectionStream, outMetadata.globalBaseAddress);
				serializeVarUInt32(sectionStream, outMetadata.dynamicBaseAddress);
				serializeVarUInt32(sectionStream, outMetadata.dynamicTopAddressAddress);
				serializeVarUInt32(sectionStream, outMetadata.tempDoubleAddress);

				if(outMetadata.metadataVersionMinor >= 3)
				{ serializeVarUInt32(sectionStream, outMetadata.standaloneWASM); }
				else
				{
					outMetadata.standaloneWASM = 0;
				}

				return true;
			}
			catch(Serialization::FatalSerializationException const& exception)
			{
				Log::printf(Log::error,
							"Error while deserializing Emscripten metadata section: %s\n",
							exception.message.c_str());
				return false;
			}
		}
	}
	return false;
}

std::shared_ptr<Emscripten::Process> Emscripten::createProcess(Compartment* compartment,
															   std::vector<std::string>&& inArgs,
															   std::vector<std::string>&& inEnvs,
															   VFS::VFD* stdIn,
															   VFS::VFD* stdOut,
															   VFS::VFD* stdErr)
{
	std::shared_ptr<Process> process = std::make_shared<Process>();

	process->args = std::move(inArgs);
	process->envs = std::move(inEnvs);
	process->stdIn = stdIn;
	process->stdOut = stdOut;
	process->stdErr = stdErr;

	process->env = Intrinsics::instantiateModule(
		compartment,
		{WAVM_INTRINSIC_MODULE_REF(env), WAVM_INTRINSIC_MODULE_REF(envThreads)},
		"env");
	process->asm2wasm = Intrinsics::instantiateModule(
		compartment, {WAVM_INTRINSIC_MODULE_REF(asm2wasm)}, "asm2wasm");
	process->global
		= Intrinsics::instantiateModule(compartment, {WAVM_INTRINSIC_MODULE_REF(global)}, "global");
	process->wasi_snapshot_preview1 = Intrinsics::instantiateModule(
		compartment,
		{WAVM_INTRINSIC_MODULE_REF(emscripten_wasi_snapshot_preview1)},
		"wasi_unstable");

	process->compartment = compartment;

	setUserData(compartment, process.get());

	return process;
}

bool Emscripten::initializeProcess(Process& process,
								   Context* context,
								   const IR::Module& module,
								   Runtime::Instance* instance)
{
	process.instance = instance;

	// Read the module metadata.
	EmscriptenModuleMetadata metadata;
	if(loadEmscriptenMetadata(module, metadata))
	{
		// Check the ABI version used by the module.
		if(metadata.abiVersionMajor != 0)
		{
			Log::printf(Log::error,
						"Unsupported Emscripten ABI major version (%u)\n",
						metadata.abiVersionMajor);
			return false;
		}

		// Check whether the module was compiled as "standalone".
		if(!metadata.standaloneWASM)
		{
			Log::printf(
				Log::error,
				"WAVM only supports Emscripten modules compiled with '-s STANDALONE_WASM=1'.");
			return false;
		}
	}

	// Find the various Emscripten ABI objects exported by the module.

	process.memory = asMemoryNullable(getInstanceExport(instance, "memory"));
	if(!process.memory)
	{
		Log::printf(Log::error, "Emscripten module does not export memory.\n");
		return false;
	}

	process.table = asTableNullable(getInstanceExport(instance, "table"));

	process.malloc = getTypedInstanceExport(
		instance, "malloc", FunctionType({ValueType::i32}, {ValueType::i32}));
	process.free = getTypedInstanceExport(
		instance, "free", FunctionType({ValueType::i32}, {ValueType::i32}));

	process.stackAlloc = getTypedInstanceExport(
		instance, "stackAlloc", FunctionType({ValueType::i32}, {ValueType::i32}));
	process.stackSave
		= getTypedInstanceExport(instance, "stackSave", FunctionType({ValueType::i32}, {}));
	process.stackRestore
		= getTypedInstanceExport(instance, "stackRestore", FunctionType({}, {ValueType::i32}));
	process.errnoLocation
		= getTypedInstanceExport(instance, "__errno_location", FunctionType({ValueType::i32}, {}));

	// Create an Emscripten "main thread" and associate it with this context.
	process.mainThread = new Emscripten::Thread(&process, context, nullptr, 0);
	setUserData(context, process.mainThread);

	// TODO
	process.mainThread->stackAddress = 0;
	process.mainThread->numStackBytes = 0;

	// Initialize the Emscripten "thread local" state.
	initThreadLocals(process.mainThread);

	process.processClockOrigin = Platform::getClockTime(Platform::Clock::processCPUTime);

	return true;
}

Runtime::Resolver& Emscripten::getInstanceResolver(Process& process) { return process; }

bool Emscripten::Process::resolve(const std::string& moduleName,
								  const std::string& exportName,
								  IR::ExternType type,
								  Runtime::Object*& outObject)
{
	Runtime::Instance* intrinsicInstance = nullptr;
	if(moduleName == "env") { intrinsicInstance = env; }
	else if(moduleName == "asm2wasm")
	{
		intrinsicInstance = asm2wasm;
	}
	else if(moduleName == "global")
	{
		intrinsicInstance = global;
	}
	else if(moduleName == "wasi_snapshot_preview1")
	{
		intrinsicInstance = wasi_snapshot_preview1;
	}

	if(intrinsicInstance)
	{
		outObject = getInstanceExport(intrinsicInstance, exportName);
		if(outObject)
		{
			if(isA(outObject, type)) { return true; }
			else
			{
				Log::printf(Log::debug,
							"Resolved import %s.%s to a %s, but was expecting %s\n",
							moduleName.c_str(),
							exportName.c_str(),
							asString(getExternType(outObject)).c_str(),
							asString(type).c_str());
			}
		}
	}

	return false;
}

I32 Emscripten::catchExit(std::function<I32()>&& thunk)
{
	try
	{
		return std::move(thunk)();
	}
	catch(ExitException const& exitException)
	{
		return I32(exitException.exitCode);
	}
}
