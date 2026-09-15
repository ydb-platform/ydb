#include "reflection.h"

namespace NSQLTranslationV1 {

TString GetDescription(
    const google::protobuf::Message& node,
    const google::protobuf::FieldDescriptor* d)
{
    const auto& field = node.GetReflection()->GetMessage(node, d);
    return field.GetReflection()->GetString(field, d->message_type()->FindFieldByName("Descr"));
}

TString AltDescription(
    const google::protobuf::Message& node,
    ui32 altCase,
    const google::protobuf::Descriptor* descr)
{
    return GetDescription(node, descr->FindFieldByNumber(altCase));
}

} // namespace NSQLTranslationV1
