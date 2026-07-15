#include <string.h>
#include <atomic>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Memory.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::Runtime;

Context* Runtime::createContext(Compartment* compartment, std::string&& debugName)
{
	WAVM_ASSERT(compartment);
	Context* context = new Context(compartment, std::move(debugName));
	{
		Platform::RWMutex::ExclusiveLock lock(compartment->mutex);

		// Allocate an ID for the context in the compartment.
		context->id = compartment->contexts.add(UINTPTR_MAX, context);
		if(context->id == UINTPTR_MAX)
		{
			delete context;
			return nullptr;
		}
		context->runtimeData = &compartment->runtimeData->contexts[context->id];

		// Commit the page(s) for the context's runtime data.
		if(!Platform::commitVirtualPages(
			   (U8*)context->runtimeData,
			   sizeof(ContextRuntimeData) >> Platform::getBytesPerPageLog2()))
		{
			delete context;
			return nullptr;
		}
		Platform::registerVirtualAllocation(sizeof(ContextRuntimeData));

		// Initialize the context's global data.
		memcpy(context->runtimeData->mutableGlobals,
			   compartment->initialContextMutableGlobals,
			   maxMutableGlobals * sizeof(IR::UntaggedValue));

		context->runtimeData->context = context;
	}

	return context;
}

Runtime::Context::~Context()
{
	WAVM_ASSERT_RWMUTEX_IS_EXCLUSIVELY_LOCKED_BY_CURRENT_THREAD(compartment->mutex);
	if(id != UINTPTR_MAX) { compartment->contexts.removeOrFail(id); }

	Platform::decommitVirtualPages((U8*)runtimeData,
								   sizeof(ContextRuntimeData) >> Platform::getBytesPerPageLog2());
	Platform::deregisterVirtualAllocation(sizeof(ContextRuntimeData));
}

Compartment* Runtime::getCompartment(const Context* context) { return context->compartment; }

void Runtime::setCheckStackDepthCallback(Context* context, void (*callback)())
{
	context->setCheckStackDepthCallback(callback);
}

Context* Runtime::cloneContext(const Context* context, Compartment* newCompartment)
{
	// Create a new context and initialize its runtime data with the values from the source context.
	Context* clonedContext = createContext(newCompartment);
	if(clonedContext)
	{
		memcpy(clonedContext->runtimeData->mutableGlobals,
			   context->runtimeData->mutableGlobals,
			   maxMutableGlobals * sizeof(IR::UntaggedValue));
		clonedContext->checkStackDepthCallback = context->checkStackDepthCallback;
	}
	return clonedContext;
}
