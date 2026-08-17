#pragma once

#include <google/protobuf/message.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLTranslationV1 {

TString GetDescription(
    const google::protobuf::Message& node,
    const google::protobuf::FieldDescriptor* d);

TString AltDescription(
    const google::protobuf::Message& node,
    ui32 altCase,
    const google::protobuf::Descriptor* descr);

template <typename TNode>
TString AltDescription(const TNode& node) {
    return AltDescription(node, node.Alt_case(), TNode::descriptor());
}

} // namespace NSQLTranslationV1
