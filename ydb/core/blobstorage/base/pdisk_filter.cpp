#include "pdisk_filter.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/unknown_field_set.h>

#include <vector>

namespace NKikimr {
namespace {

static bool HasUnknownFieldsRecursive(const NProtoBuf::Message& msg) {
    const auto* reflection = msg.GetReflection();
    if (!reflection->GetUnknownFields(msg).empty()) {
        return true;
    }

    std::vector<const NProtoBuf::FieldDescriptor*> fields;
    reflection->ListFields(msg, &fields);
    for (const auto* field : fields) {
        if (field->cpp_type() != NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            for (int i = 0; i < reflection->FieldSize(msg, field); ++i) {
                if (HasUnknownFieldsRecursive(reflection->GetRepeatedMessage(msg, field, i))) {
                    return true;
                }
            }
        } else if (HasUnknownFieldsRecursive(reflection->GetMessage(msg, field))) {
            return true;
        }
    }
    return false;
}

} // namespace

bool HasUnknownPDiskFilterFields(const NKikimrBlobStorage::TPDiskFilter::TRequiredProperty& property) {
    return HasUnknownFieldsRecursive(property);
}

} // namespace NKikimr
