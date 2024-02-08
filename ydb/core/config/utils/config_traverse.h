#pragma once

#include <contrib/libs/protobuf/src/google/protobuf/descriptor.h>

#include <util/generic/deque.h>

#include <functional>

namespace NKikimr::NConfig {

using namespace NProtoBuf;

using TOnEntryFn = std::function<void(const Descriptor*, const TDeque<const Descriptor*>&, const TDeque<const FieldDescriptor*>&, const FieldDescriptor*, ssize_t)>;

void Traverse(TOnEntryFn onEntry);

} // namespace NKikimr::NConfig
