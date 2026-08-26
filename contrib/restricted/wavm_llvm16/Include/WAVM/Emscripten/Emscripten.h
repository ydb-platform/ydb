#pragma once

#include <functional>
#include <memory>
#include <vector>
#include "WAVM/IR/Value.h"

namespace WAVM { namespace IR {
	struct Module;
}}

namespace WAVM { namespace VFS {
	struct VFD;
}}

namespace WAVM { namespace Runtime {
	struct Compartment;
	struct Context;
	struct Instance;
	struct Resolver;
}}

namespace WAVM { namespace Emscripten {

	struct Process;

	WAVM_API std::shared_ptr<Process> createProcess(Runtime::Compartment* compartment,
													std::vector<std::string>&& inArgs,
													std::vector<std::string>&& inEnvs,
													VFS::VFD* stdIn = nullptr,
													VFS::VFD* stdOut = nullptr,
													VFS::VFD* stdErr = nullptr);
	WAVM_API bool initializeProcess(Process& process,
									Runtime::Context* context,
									const IR::Module& module,
									Runtime::Instance* instance);

	WAVM_API Runtime::Resolver& getInstanceResolver(Process& process);

	WAVM_API void joinAllThreads(Process& process);

	WAVM_API I32 catchExit(std::function<I32()>&& thunk);
}}
