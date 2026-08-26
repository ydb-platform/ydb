#pragma once

#include <google/protobuf/any.pb.h>

namespace NYql {

bool HasSharedReading(const ::google::protobuf::Any& maybePqTopicSource);

} // namespace NYql
