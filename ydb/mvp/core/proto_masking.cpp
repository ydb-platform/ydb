#include "proto_masking.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "ydb/public/api/client/nc_private/annotations.pb.h"
#include "ydb/library/security/util.h"

using namespace google::protobuf;

namespace NMVP {

static void MaskMessageRecursively(Message* msg) {
    const Descriptor* desc = msg->GetDescriptor();
    const Reflection* refl = msg->GetReflection();

    for (int i = 0; i < desc->field_count(); ++i) {
        const FieldDescriptor* field = desc->field(i);
        const FieldOptions& opts = field->options();

        bool isCred = false;
        bool isSens = false;
        // read extensions from annotations.proto
        if (opts.HasExtension(nebius::credentials)) {
            isCred = opts.GetExtension(nebius::credentials);
        }
        if (opts.HasExtension(nebius::sensitive)) {
            isSens = opts.GetExtension(nebius::sensitive);
        }

        if (field->is_repeated()) {
            if (isCred || isSens) {
                if (field->cpp_type() == FieldDescriptor::CPPTYPE_STRING) {
                    int cnt = refl->FieldSize(*msg, field);
                    for (int j = 0; j < cnt; ++j) {
                        std::string val = refl->GetRepeatedString(*msg, field, j);
                        refl->SetRepeatedString(msg, field, j, NKikimr::MaskTicket(val));
                    }
                } else {
                    refl->ClearField(msg, field);
                }
            } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                int cnt = refl->FieldSize(*msg, field);
                for (int j = 0; j < cnt; ++j) {
                    Message* sub = refl->MutableRepeatedMessage(msg, field, j);
                    MaskMessageRecursively(sub);
                }
            }
        } else {
            if (!refl->HasField(*msg, field)) continue;
            if (isCred || isSens) {
                if (field->cpp_type() == FieldDescriptor::CPPTYPE_STRING) {
                    std::string val = refl->GetString(*msg, field);
                    refl->SetString(msg, field, NKikimr::MaskTicket(val));
                } else {
                    refl->ClearField(msg, field);
                }
            } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                Message* sub = refl->MutableMessage(msg, field);
                MaskMessageRecursively(sub);
            }
        }
    }
}

std::string MaskedShortDebugString(const Message& msg) {
    std::unique_ptr<Message> copy(msg.New());
    copy->CopyFrom(msg);
    MaskMessageRecursively(copy.get());
    return copy->ShortDebugString();
}

} // namespace NMVP
