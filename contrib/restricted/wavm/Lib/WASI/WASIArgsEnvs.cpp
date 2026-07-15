#include "./WASIPrivate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/WASI/WASIABI.h"

using namespace WAVM;
using namespace WAVM::WASI;
using namespace WAVM::Runtime;

namespace WAVM { namespace WASI {
	WAVM_DEFINE_INTRINSIC_MODULE(wasiArgsEnvs)
}}

WAVM_DEFINE_INTRINSIC_FUNCTION(wasiArgsEnvs,
							   "args_sizes_get",
							   __wasi_errno_return_t,
							   wasi_args_sizes_get,
							   U32 argcAddress,
							   U32 argBufSizeAddress)
{
	TRACE_SYSCALL("args_sizes_get",
				  "(" WASIADDRESS_FORMAT ", " WASIADDRESS_FORMAT ")",
				  argcAddress,
				  argBufSizeAddress);

	Process* process = getProcessFromContextRuntimeData(contextRuntimeData);

	Uptr numArgBufferBytes = 0;
	for(const std::string& arg : process->args) { numArgBufferBytes += arg.size() + 1; }

	if(process->args.size() > WASIADDRESS_MAX || numArgBufferBytes > WASIADDRESS_MAX)
	{ return TRACE_SYSCALL_RETURN(__WASI_EOVERFLOW); }
	memoryRef<WASIAddress>(process->memory, argcAddress) = WASIAddress(process->args.size());
	memoryRef<WASIAddress>(process->memory, argBufSizeAddress) = WASIAddress(numArgBufferBytes);

	return TRACE_SYSCALL_RETURN(__WASI_ESUCCESS);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wasiArgsEnvs,
							   "args_get",
							   __wasi_errno_return_t,
							   wasi_args_get,
							   U32 argvAddress,
							   U32 argBufAddress)
{
	TRACE_SYSCALL(
		"args_get", "(" WASIADDRESS_FORMAT ", " WASIADDRESS_FORMAT ")", argvAddress, argBufAddress);

	Process* process = getProcessFromContextRuntimeData(contextRuntimeData);

	WASIAddress nextArgBufAddress = argBufAddress;
	for(Uptr argIndex = 0; argIndex < process->args.size(); ++argIndex)
	{
		const std::string& arg = process->args[argIndex];
		const Uptr numArgBytes = arg.size() + 1;

		if(numArgBytes > WASIADDRESS_MAX || nextArgBufAddress > WASIADDRESS_MAX - numArgBytes - 1)
		{ return TRACE_SYSCALL_RETURN(__WASI_EOVERFLOW); }

		if(numArgBytes > 0)
		{
			memcpy(memoryArrayPtr<U8>(process->memory, nextArgBufAddress, numArgBytes),
				   (const U8*)arg.c_str(),
				   numArgBytes);
		}
		memoryRef<WASIAddress>(process->memory, argvAddress + argIndex * sizeof(U32))
			= WASIAddress(nextArgBufAddress);

		nextArgBufAddress += WASIAddress(numArgBytes);
	}

	return TRACE_SYSCALL_RETURN(__WASI_ESUCCESS);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wasiArgsEnvs,
							   "environ_sizes_get",
							   __wasi_errno_return_t,
							   wasi_environ_sizes_get,
							   WASIAddress envCountAddress,
							   WASIAddress envBufSizeAddress)
{
	TRACE_SYSCALL("environ_sizes_get",
				  "(" WASIADDRESS_FORMAT ", " WASIADDRESS_FORMAT ")",
				  envCountAddress,
				  envBufSizeAddress);

	Process* process = getProcessFromContextRuntimeData(contextRuntimeData);

	Uptr numEnvBufferBytes = 0;
	for(const std::string& env : process->envs) { numEnvBufferBytes += env.size() + 1; }

	if(process->envs.size() > WASIADDRESS_MAX || numEnvBufferBytes > WASIADDRESS_MAX)
	{ return TRACE_SYSCALL_RETURN(__WASI_EOVERFLOW); }
	memoryRef<WASIAddress>(process->memory, envCountAddress) = WASIAddress(process->envs.size());
	memoryRef<WASIAddress>(process->memory, envBufSizeAddress) = WASIAddress(numEnvBufferBytes);

	return TRACE_SYSCALL_RETURN(__WASI_ESUCCESS);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wasiArgsEnvs,
							   "environ_get",
							   __wasi_errno_return_t,
							   wasi_environ_get,
							   WASIAddress envvAddress,
							   WASIAddress envBufAddress)
{
	TRACE_SYSCALL("environ_get",
				  "(" WASIADDRESS_FORMAT ", " WASIADDRESS_FORMAT ")",
				  envvAddress,
				  envBufAddress);

	Process* process = getProcessFromContextRuntimeData(contextRuntimeData);

	WASIAddress nextEnvBufAddress = envBufAddress;
	for(Uptr argIndex = 0; argIndex < process->envs.size(); ++argIndex)
	{
		const std::string& env = process->envs[argIndex];
		const Uptr numEnvBytes = env.size() + 1;

		if(numEnvBytes > WASIADDRESS_MAX || nextEnvBufAddress > WASIADDRESS_MAX - numEnvBytes - 1)
		{ return TRACE_SYSCALL_RETURN(__WASI_EOVERFLOW); }

		if(numEnvBytes > 0)
		{
			memcpy(memoryArrayPtr<U8>(process->memory, nextEnvBufAddress, numEnvBytes),
				   (const U8*)env.c_str(),
				   numEnvBytes);
		}
		memoryRef<WASIAddress>(process->memory, envvAddress + argIndex * sizeof(U32))
			= WASIAddress(nextEnvBufAddress);

		nextEnvBufAddress += WASIAddress(numEnvBytes);
	}

	return TRACE_SYSCALL_RETURN(__WASI_ESUCCESS);
}
