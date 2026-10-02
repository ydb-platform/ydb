#include "WAVM/IR/Module.h"
#include <memory>
#include <utility>
#include "RuntimePrivate.h"
#include "WAVM/IR/IR.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Platform/Intrinsic.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/WASM/WASM.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

Platform::RWMutex globalObjectCacheMutex;
std::shared_ptr<ObjectCacheInterface> globalObjectCache;

void Runtime::setGlobalObjectCache(std::shared_ptr<ObjectCacheInterface>&& objectCache)
{
	Platform::RWMutex::ExclusiveLock globalObjectCacheLock(globalObjectCacheMutex);
	globalObjectCache = std::move(objectCache);
}

static std::shared_ptr<ObjectCacheInterface> getGlobalObjectCache()
{
	Platform::RWMutex::ShareableLock globalObjectCacheLock(globalObjectCacheMutex);
	return globalObjectCache;
}

ModuleRef Runtime::compileModule(const IR::Module& irModule)
{
	// Get a pointer to the global object cache, if there is one.
	std::shared_ptr<ObjectCacheInterface> objectCache = getGlobalObjectCache();

	std::vector<U8> objectCode;
	if(!objectCache)
	{
		// If there's no global object cache, just compile the module.
		objectCode = LLVMJIT::compileModule(irModule, LLVMJIT::getHostTargetSpec());
	}
	else
	{
		// Serialize the IR module to WASM.
		Timing::Timer keyTimer;
		std::vector<U8> wasmBytes = WASM::saveBinaryModule(irModule);
		Timing::logTimer("Created object cache key from IR module", keyTimer);

		// Check for cached object code for the module before compiling it.
		objectCode
			= objectCache->getCachedObject(wasmBytes.data(), wasmBytes.size(), [&irModule]() {
				  return LLVMJIT::compileModule(irModule, LLVMJIT::getHostTargetSpec());
			  });
	}

	return std::make_shared<Runtime::Module>(IR::Module(irModule), std::move(objectCode));
}

bool Runtime::loadBinaryModule(const U8* wasmBytes,
							   Uptr numWASMBytes,
							   ModuleRef& outModule,
							   const IR::FeatureSpec& featureSpec,
							   WASM::LoadError* outError)
{
	// Load the module IR.
	IR::Module irModule(std::move(featureSpec));
	if(!WASM::loadBinaryModule(wasmBytes, numWASMBytes, irModule, outError)) { return false; }

	// Get a pointer to the global object cache, if there is one.
	std::shared_ptr<ObjectCacheInterface> objectCache = getGlobalObjectCache();

	std::vector<U8> objectCode;
	if(!objectCache)
	{
		// If there's no global object cache, just compile the module.
		objectCode = LLVMJIT::compileModule(irModule, LLVMJIT::getHostTargetSpec());
	}
	else
	{
		// Check for cached object code for the module before compiling it.
		objectCode = objectCache->getCachedObject(wasmBytes, numWASMBytes, [&irModule]() {
			return LLVMJIT::compileModule(irModule, LLVMJIT::getHostTargetSpec());
		});
	}

	outModule = std::make_shared<Runtime::Module>(std::move(irModule), std::move(objectCode));
	return true;
}

ModuleRef Runtime::loadPrecompiledModule(const IR::Module& irModule,
										 const std::vector<U8>& objectCode)
{
	return std::make_shared<Module>(IR::Module(irModule), std::vector<U8>(objectCode));
}

const IR::Module& Runtime::getModuleIR(ModuleConstRefParam module) { return module->ir; }
std::vector<U8> Runtime::getObjectCode(ModuleConstRefParam module) { return module->objectCode; }
