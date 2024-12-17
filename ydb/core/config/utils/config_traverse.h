#pragma once

#include <util/generic/deque.h>

#include <functional>

namespace google::protobuf {
    class Descriptor;
    class FieldDescriptor;
}

namespace NKikimr::NConfig {

using namespace google::protobuf;

using TOnEntryFn = std::function<void(const Descriptor*, const TDeque<const Descriptor*>&, const TDeque<const FieldDescriptor*>&, const FieldDescriptor*, ssize_t)>;

void Traverse(TOnEntryFn onEntry, const Descriptor* descriptor);

} // namespace NKikimr::NConfig
