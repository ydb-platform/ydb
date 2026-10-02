#include "WAVM/ObjectCache/ObjectCache.h"

#include <cstdlib>

using namespace WAVM;
using namespace WAVM::ObjectCache;

OpenResult ObjectCache::open(const char* path,
							 Uptr maxBytes,
							 U64 codeKey,
							 std::shared_ptr<Runtime::ObjectCacheInterface>& outObjectCache)
{
	std::abort();
}
