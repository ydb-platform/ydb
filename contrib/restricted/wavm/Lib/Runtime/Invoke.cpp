#include <string.h>
#include <memory>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

void Runtime::invokeFunction(Context* context,
							 const Function* function,
							 FunctionType invokeSig,
							 const UntaggedValue arguments[],
							 UntaggedValue outResults[])
{
	FunctionType functionType{function->encodedType};

	// Verify that the invoke signature matches the function being invoked.
	if(invokeSig != functionType && !isSubtype(functionType, invokeSig))
	{
		if(Log::isCategoryEnabled(Log::debug))
		{
			Log::printf(
				Log::debug,
				"Invoke signature mismatch:\n  Invoke signature: %s\n  Invoked function type: %s\n",
				asString(invokeSig).c_str(),
				asString(getFunctionType(function)).c_str());
		}
		throwException(ExceptionTypes::invokeSignatureMismatch);
	}

	// Assert that the function, the context, and any reference arguments are all in the same
	// compartment.
	if(false)
	{
		WAVM_ASSERT(isInCompartment(asObject(function), context->compartment));
		for(Uptr argumentIndex = 0; argumentIndex < invokeSig.params().size(); ++argumentIndex)
		{
			const ValueType argType = invokeSig.params()[argumentIndex];
			const UntaggedValue& arg = arguments[argumentIndex];
			WAVM_ASSERT(!isReferenceType(argType) || !arg.object
						|| isInCompartment(arg.object, context->compartment));
		}
	}

	// Get the invoke thunk for this function type. Cache it in the function's FunctionMutableData
	// to avoid the global lock implied by LLVMJIT::getInvokeThunk.
	InvokeThunkPointer invokeThunk
		= function->mutableData->invokeThunk.load(std::memory_order_acquire);
	if(WAVM_UNLIKELY(!invokeThunk))
	{
		invokeThunk = LLVMJIT::getInvokeThunk(functionType);

		// Replace the cached thunk pointer, but since LLVMJIT::getInvokeThunk is guaranteed to
		// return the same thunk when called with the same FunctionType, we can assume that any
		// other writes this might race with were are writing the same value.
		function->mutableData->invokeThunk.store(invokeThunk, std::memory_order_release);
	}
	WAVM_ASSERT(invokeThunk);

	// MacOS std::function is a little more pessimistic about heap allocating captures, and without
	// wrapping these captured variables into a single reference, does a heap allocation for the
	// thunk passed to unwindSignalsAsExceptions below.
	struct InvokeContext
	{
		Context* context;
		const Function* function;
		const UntaggedValue* arguments;
		UntaggedValue* outResults;
		InvokeThunkPointer invokeThunk;
	};
	InvokeContext invokeContext;
	invokeContext.context = context;
	invokeContext.function = function;
	invokeContext.arguments = arguments;
	invokeContext.outResults = outResults;
	invokeContext.invokeThunk = invokeThunk;

	// Use unwindSignalsAsExceptions to ensure that any signal that occurs in WebAssembly code calls
	// C++ destructors on the stack between here and where it is caught.
	unwindSignalsAsExceptions([&invokeContext] {
		ContextRuntimeData* contextRuntimeData = getContextRuntimeData(invokeContext.context);

		// Call the invoke thunk.
		(*invokeContext.invokeThunk)(invokeContext.function,
									 contextRuntimeData,
									 invokeContext.arguments,
									 invokeContext.outResults);
	});
}
