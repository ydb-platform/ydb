#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"

using namespace WAVM;
using namespace WAVM::IR;

const char* IR::getOpcodeName(Opcode opcode)
{
	switch(opcode)
	{
#define VISIT_OPCODE(encoding, name, nameString, Imm, ...)                                         \
	case Opcode::name: return nameString;
		WAVM_ENUM_OPERATORS(VISIT_OPCODE)
#undef VISIT_OPCODE
	default: return "unknown";
	};
}
