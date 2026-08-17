#pragma once

#include "WAVM/Inline/BasicTypes.h"

namespace WAVM {
	struct RandomStream;
}

namespace WAVM { namespace IR {
	struct Module;

	WAVM_API void generateValidModule(IR::Module& module, RandomStream& randomStream);
}}
