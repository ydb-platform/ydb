#include "WAVM/VFS/VFS.h"
#include "WAVM/Inline/Errors.h"

using namespace WAVM;
using namespace WAVM::VFS;

const char* VFS::describeResult(Result result)
{
	switch(result)
	{
		// clang-format off
#define V(name, description) case VFS::Result::name: return description;
WAVM_ENUM_VFS_RESULTS(V)
#undef V
		// clang-format on

	default: WAVM_UNREACHABLE();
	};
}
