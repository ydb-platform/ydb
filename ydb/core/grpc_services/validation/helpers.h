#pragma once

#include <google/protobuf/descriptor.h>

#include <util/generic/string.h>

namespace NKikimr {
namespace NValidation {

TString HeaderFileName(const google::protobuf::Descriptor* message);
TString SourceFileName(const google::protobuf::Descriptor* message);
TString ClassScope(const google::protobuf::Descriptor* message);
TString NamespaceScope();
TString ClassName(const google::protobuf::Descriptor* message);

bool IsCustomMessage(const google::protobuf::Descriptor* message);

} // NValidation
} // NKikimr
