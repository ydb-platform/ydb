#pragma once

#include <functional>
#include <memory>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Runtime/Runtime.h"

namespace WAVM { namespace VFS {
	struct FileSystem;
	struct VFD;
}}

namespace WAVM { namespace Runtime {
	struct Resolver;
}}

namespace WAVM { namespace WASI {

	struct Process;

	WAVM_API std::shared_ptr<Process> createProcess(Runtime::Compartment* compartment,
													std::vector<std::string>&& inArgs,
													std::vector<std::string>&& inEnvs,
													VFS::FileSystem* fileSystem,
													VFS::VFD* stdIn,
													VFS::VFD* stdOut,
													VFS::VFD* stdErr);

	WAVM_API Runtime::Resolver& getProcessResolver(Process& process);

	WAVM_API Process* getProcessFromContextRuntimeData(Runtime::ContextRuntimeData*);
	WAVM_API Runtime::Memory* getProcessMemory(const Process& process);
	WAVM_API void setProcessMemory(Process& process, Runtime::Memory* memory);

	enum class SyscallTraceLevel
	{
		none,
		syscalls,
		syscallsWithCallstacks
	};

	WAVM_API void setSyscallTraceLevel(SyscallTraceLevel newLevel);

	WAVM_API I32 catchExit(std::function<I32()>&& thunk);
}}
