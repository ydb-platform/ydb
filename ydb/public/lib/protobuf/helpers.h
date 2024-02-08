#pragma once

#include <google/protobuf/descriptor.h>

#include <util/generic/string.h>

namespace NKikimr::NProtobuf {

TString HeaderFileName(const google::protobuf::Descriptor* message);
TString SourceFileName(const google::protobuf::Descriptor* message);
TString ClassScope(const google::protobuf::Descriptor* message);
TString NamespaceScope();
TString IncludesScope();
TString FullyQualifiedClassName(const google::protobuf::Descriptor* message);
TString ClassName(const google::protobuf::Descriptor* message);

bool IsCustomMessage(const google::protobuf::Descriptor* message);

} // namespace NKikimr::NProtobuf
