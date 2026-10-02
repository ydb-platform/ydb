#include "mvp_security_printer.h"

#include <ydb/public/api/client/nc_private/annotations.pb.h>
#include <ydb/public/api/client/yc_private/accessservice/sensitive.pb.h>

namespace NMVP {

bool MvpShouldHideSensitiveField(const google::protobuf::Descriptor*, const google::protobuf::FieldDescriptor* field) {
    const auto& ops = field->options();
    return ops.GetExtension(nebius::sensitive) || ops.GetExtension(nebius::credentials) || ops.GetExtension(yandex::cloud::sensitive);
}

} // namespace NMVP
