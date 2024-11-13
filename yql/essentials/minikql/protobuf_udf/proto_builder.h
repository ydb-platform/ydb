#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <yql/essentials/minikql/protobuf_udf/type_builder.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NYql {
namespace NUdf {

void FillProtoFromValue(const NKikimr::NUdf::TUnboxedValuePod& source, NProtoBuf::Message& target,
                        const NKikimr::NUdf::TProtoInfo& info);


} // namespace NUdf
} // namespace NYql
