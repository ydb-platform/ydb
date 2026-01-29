#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ICachingAllocator;
using ICachingAllocatorPtr = std::shared_ptr<ICachingAllocator>;

}   // namespace NCloud::NBlockStore
