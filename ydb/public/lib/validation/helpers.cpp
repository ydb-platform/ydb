#include "helpers.h"

#include <google/protobuf/compiler/cpp/cpp_helpers.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/duration.pb.h>
#include <google/protobuf/empty.pb.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/wrappers.pb.h>

#include <util/string/builder.h>
#include <util/string/subst.h>

namespace NKikimr::NValidation {

static TString ProtoFileNameStripped(const google::protobuf::Descriptor* message) {
    return google::protobuf::compiler::cpp::StripProto(message->file()->name());
}

TString HeaderFileName(const google::protobuf::Descriptor* message) {
    return ProtoFileNameStripped(message).append(".pb.h");
}

TString SourceFileName(const google::protobuf::Descriptor* message) {
    return ProtoFileNameStripped(message).append(".pb.cc");
}

TString ClassScope(const google::protobuf::Descriptor* message) {
    return "class_scope:" + message->full_name();
}

TString NamespaceScope() {
    return "namespace_scope";
}

TString ClassName(const google::protobuf::Descriptor* message) {
    const TString ns = message->file()->package();
    TString className = !ns.empty() ? message->full_name().substr(ns.size() + 1) : message->full_name();
    SubstGlobal(className, ".", "::");
    return className;
}

bool IsCustomMessage(const google::protobuf::Descriptor* message) {
    if (!message) {
        return false;
    }
    if (message->full_name() == google::protobuf::Any::descriptor()->full_name()) {
        return false;
    }
    if (message->full_name() == google::protobuf::Duration::descriptor()->full_name()) {
        return false;
    }
    if (message->full_name() == google::protobuf::Empty::descriptor()->full_name()) {
        return false;
    }
    if (message->full_name() == google::protobuf::Struct::descriptor()->full_name()) {
        return false;
    } 
    if (message->full_name() == google::protobuf::Timestamp::descriptor()->full_name()) {
        return false;
    }
    if (message->full_name() == google::protobuf::Int64Value::descriptor()->full_name()) {
        return false;
    }
    if (message->full_name() == google::protobuf::BoolValue::descriptor()->full_name()) {
        return false;
    }

    if (message->options().map_entry()) {
        return false;
    }

    return true;
}

}
