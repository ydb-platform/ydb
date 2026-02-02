#include "proto_masking.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "ydb/public/api/client/nc_private/annotations.pb.h"
#include "ydb/library/security/util.h"

using namespace google::protobuf;

namespace NKikimr {

static void MaskMessageRecursively(Message* m) {
    const Descriptor* desc = m->GetDescriptor();
    const Reflection* refl = m->GetReflection();

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
            if (field->cpp_type() == FieldDescriptor::CPPTYPE_STRING && (isCred || isSens)) {
                int cnt = refl->FieldSize(*m, field);
                for (int j = 0; j < cnt; ++j) {
                    std::string val = refl->GetRepeatedString(*m, field, j);
                    refl->SetRepeatedString(m, field, j, NKikimr::MaskTicket(val));
                }
            } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                int cnt = refl->FieldSize(*m, field);
                for (int j = 0; j < cnt; ++j) {
                    Message* sub = refl->MutableRepeatedMessage(m, field, j);
                    MaskMessageRecursively(sub);
                }
            }
        } else {
            if (!refl->HasField(*m, field)) continue;

            if (field->cpp_type() == FieldDescriptor::CPPTYPE_STRING && (isCred || isSens)) {
                std::string val = refl->GetString(*m, field);
                refl->SetString(m, field, NKikimr::MaskTicket(val));
            } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                Message* sub = refl->MutableMessage(m, field);
                MaskMessageRecursively(sub);
            }
        }
    }
}

std::string SecureShortDebugStringMasked(const Message& msg) {
    std::unique_ptr<Message> copy(msg.New());
    copy->CopyFrom(msg);
    MaskMessageRecursively(copy.get());
    return copy->ShortDebugString();
}

} // namespace NKikimr
