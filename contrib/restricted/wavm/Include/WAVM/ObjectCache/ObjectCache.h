#pragma once

#include <memory>
#include "WAVM/Inline/BasicTypes.h"

namespace WAVM { namespace Runtime {
	struct ObjectCacheInterface;
}}

namespace WAVM { namespace ObjectCache {

	enum class OpenResult
	{
		success,
		doesNotExist,
		notDirectory,
		notAccessible,
		invalidDatabase,
		tooManyReaders,
	};

	WAVM_API OpenResult open(const char* path,
							 Uptr maxBytes,
							 U64 codeKey,
							 std::shared_ptr<Runtime::ObjectCacheInterface>& outObjectCache);
}}
