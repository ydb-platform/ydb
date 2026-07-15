#include <stdint.h>
#include <cmath>
#include <string>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/FloatComponents.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::Runtime;

namespace WAVM { namespace Runtime {
	WAVM_DEFINE_INTRINSIC_MODULE(wavmIntrinsics)
}}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics,
							   "divideByZeroOrIntegerOverflowTrap",
							   void,
							   divideByZeroOrIntegerOverflowTrap)
{
	throwException(ExceptionTypes::integerDivideByZeroOrOverflow);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics, "unreachableTrap", void, unreachableTrap)
{
	throwException(ExceptionTypes::reachedUnreachable);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics,
							   "invalidFloatOperationTrap",
							   void,
							   invalidFloatOperationTrap)
{
	throwException(ExceptionTypes::invalidFloatOperation);
}

static thread_local Uptr indentLevel = 0;

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics,
							   "debugEnterFunction",
							   void,
							   debugEnterFunction,
							   const Function* function)
{
	Log::printf(Log::debug,
				"ENTER: %*s\n",
				U32(indentLevel * 4 + function->mutableData->debugName.size()),
				function->mutableData->debugName.c_str());
	++indentLevel;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics,
							   "debugExitFunction",
							   void,
							   debugExitFunction,
							   const Function* function)
{
	--indentLevel;
	Log::printf(Log::debug,
				"EXIT:  %*s\n",
				U32(indentLevel * 4 + function->mutableData->debugName.size()),
				function->mutableData->debugName.c_str());
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics, "checkCallStackDepth", void, checkCallStackDepth)
{
	Context* context = getContextFromRuntimeData(contextRuntimeData);
	if(context->checkStackDepthCallback) { context->checkStackDepthCallback(); }
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics, "debugBreak", void, debugBreak)
{
	Log::printf(Log::debug, "================== wavmIntrinsics.debugBreak\n");
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsics,
							   "throwIfCurrentTimeoutExpired",
							   void,
							   throwIfCurrentTimeoutExpired)
{
	static thread_local int counter = 0;
	static constexpr int COUNTER_CHECK_PERIOD = 8192;

	if (counter == COUNTER_CHECK_PERIOD) {
		if(isCurrentDeadlineReached()) { throwException(ExceptionTypes::timeoutExpired); }
		counter = 0;
	} else {
		++counter;
	}
}
