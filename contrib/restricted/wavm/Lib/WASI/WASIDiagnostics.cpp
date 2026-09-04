#include <atomic>
#include "./WASIPrivate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/WASI/WASI.h"

using namespace WAVM;
using namespace WAVM::WASI;

std::atomic<SyscallTraceLevel> syscallTraceLevel{SyscallTraceLevel::none};

void WASI::setSyscallTraceLevel(SyscallTraceLevel newLevel)
{
	syscallTraceLevel.store(newLevel, std::memory_order_relaxed);
}

static const char* describeErrNo(__wasi_errno_t wasiErrNo)
{
	switch(wasiErrNo)
	{
	case __WASI_ESUCCESS: return "ESUCCESS";
	case __WASI_E2BIG: return "E2BIG";
	case __WASI_EACCES: return "EACCES";
	case __WASI_EADDRINUSE: return "EADDRINUSE";
	case __WASI_EADDRNOTAVAIL: return "EADDRNOTAVAIL";
	case __WASI_EAFNOSUPPORT: return "EAFNOSUPPORT";
	case __WASI_EAGAIN: return "EAGAIN";
	case __WASI_EALREADY: return "EALREADY";
	case __WASI_EBADF: return "EBADF";
	case __WASI_EBADMSG: return "EBADMSG";
	case __WASI_EBUSY: return "EBUSY";
	case __WASI_ECANCELED: return "ECANCELED";
	case __WASI_ECHILD: return "ECHILD";
	case __WASI_ECONNABORTED: return "ECONNABORTED";
	case __WASI_ECONNREFUSED: return "ECONNREFUSED";
	case __WASI_ECONNRESET: return "ECONNRESET";
	case __WASI_EDEADLK: return "EDEADLK";
	case __WASI_EDESTADDRREQ: return "EDESTADDRREQ";
	case __WASI_EDOM: return "EDOM";
	case __WASI_EDQUOT: return "EDQUOT";
	case __WASI_EEXIST: return "EEXIST";
	case __WASI_EFAULT: return "EFAULT";
	case __WASI_EFBIG: return "EFBIG";
	case __WASI_EHOSTUNREACH: return "EHOSTUNREACH";
	case __WASI_EIDRM: return "EIDRM";
	case __WASI_EILSEQ: return "EILSEQ";
	case __WASI_EINPROGRESS: return "EINPROGRESS";
	case __WASI_EINTR: return "EINTR";
	case __WASI_EINVAL: return "EINVAL";
	case __WASI_EIO: return "EIO";
	case __WASI_EISCONN: return "EISCONN";
	case __WASI_EISDIR: return "EISDIR";
	case __WASI_ELOOP: return "ELOOP";
	case __WASI_EMFILE: return "EMFILE";
	case __WASI_EMLINK: return "EMLINK";
	case __WASI_EMSGSIZE: return "EMSGSIZE";
	case __WASI_EMULTIHOP: return "EMULTIHOP";
	case __WASI_ENAMETOOLONG: return "ENAMETOOLONG";
	case __WASI_ENETDOWN: return "ENETDOWN";
	case __WASI_ENETRESET: return "ENETRESET";
	case __WASI_ENETUNREACH: return "ENETUNREACH";
	case __WASI_ENFILE: return "ENFILE";
	case __WASI_ENOBUFS: return "ENOBUFS";
	case __WASI_ENODEV: return "ENODEV";
	case __WASI_ENOENT: return "ENOENT";
	case __WASI_ENOEXEC: return "ENOEXEC";
	case __WASI_ENOLCK: return "ENOLCK";
	case __WASI_ENOLINK: return "ENOLINK";
	case __WASI_ENOMEM: return "ENOMEM";
	case __WASI_ENOMSG: return "ENOMSG";
	case __WASI_ENOPROTOOPT: return "ENOPROTOOPT";
	case __WASI_ENOSPC: return "ENOSPC";
	case __WASI_ENOSYS: return "ENOSYS";
	case __WASI_ENOTCONN: return "ENOTCONN";
	case __WASI_ENOTDIR: return "ENOTDIR";
	case __WASI_ENOTEMPTY: return "ENOTEMPTY";
	case __WASI_ENOTRECOVERABLE: return "ENOTRECOVERABLE";
	case __WASI_ENOTSOCK: return "ENOTSOCK";
	case __WASI_ENOTSUP: return "ENOTSUP";
	case __WASI_ENOTTY: return "ENOTTY";
	case __WASI_ENXIO: return "ENXIO";
	case __WASI_EOVERFLOW: return "EOVERFLOW";
	case __WASI_EOWNERDEAD: return "EOWNERDEAD";
	case __WASI_EPERM: return "EPERM";
	case __WASI_EPIPE: return "EPIPE";
	case __WASI_EPROTO: return "EPROTO";
	case __WASI_EPROTONOSUPPORT: return "EPROTONOSUPPORT";
	case __WASI_EPROTOTYPE: return "EPROTOTYPE";
	case __WASI_ERANGE: return "ERANGE";
	case __WASI_EROFS: return "EROFS";
	case __WASI_ESPIPE: return "ESPIPE";
	case __WASI_ESRCH: return "ESRCH";
	case __WASI_ESTALE: return "ESTALE";
	case __WASI_ETIMEDOUT: return "ETIMEDOUT";
	case __WASI_ETXTBSY: return "ETXTBSY";
	case __WASI_EXDEV: return "EXDEV";
	case __WASI_ENOTCAPABLE: return "ENOTCAPABLE";
	default: WAVM_UNREACHABLE();
	}
}

static void traceSyscallv(const char* syscallName, const char* argFormat, va_list argList)
{
	SyscallTraceLevel syscallTraceLevelSnapshot = syscallTraceLevel.load(std::memory_order_relaxed);
	if(syscallTraceLevelSnapshot != SyscallTraceLevel::none)
	{
		Log::printf(Log::output, "SYSCALL: %s", syscallName);
		Log::vprintf(Log::output, argFormat, argList);
		va_end(argList);

		if(syscallTraceLevelSnapshot != SyscallTraceLevel::syscallsWithCallstacks)
		{ Log::printf(Log::output, "\n"); }
		else
		{
			Log::printf(Log::output, " - Call stack:\n");

			Platform::CallStack callStack = Platform::captureCallStack(4);
			if(callStack.frames.size() > 4) { callStack.frames.resize(4); }
			std::vector<std::string> callStackFrameDescriptions
				= Runtime::describeCallStack(callStack);
			for(const std::string& frameDescription : callStackFrameDescriptions)
			{ Log::printf(Log::output, "SYSCALL:     %s\n", frameDescription.c_str()); }
		}
	}
}

WAVM_VALIDATE_AS_PRINTF(2, 3)
void WASI::traceSyscallf(const char* syscallName, const char* argFormat, ...)
{
	va_list argList;
	va_start(argList, argFormat);
	traceSyscallv(syscallName, argFormat, argList);
	va_end(argList);
}

WAVM_VALIDATE_AS_PRINTF(3, 4)
__wasi_errno_t WASI::traceSyscallReturnf(const char* syscallName,
										 __wasi_errno_t wasiErrNo,
										 const char* returnFormat,
										 ...)
{
	SyscallTraceLevel syscallTraceLevelSnapshot = syscallTraceLevel.load(std::memory_order_relaxed);
	if(syscallTraceLevelSnapshot != SyscallTraceLevel::none)
	{
		va_list argList;
		va_start(argList, returnFormat);
		Log::printf(Log::output, "SYSCALL: %s -> %s", syscallName, describeErrNo(wasiErrNo));
		Log::vprintf(Log::output, returnFormat, argList);
		Log::printf(Log::output, "\n");
		va_end(argList);
	}
	return wasiErrNo;
}
