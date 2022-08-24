#pragma once

#include <google/protobuf/message.h>

#include <util/string/builder.h>

namespace NKikimr {

class TSizeFormatPrinter {
    const google::protobuf::Message& Message;

public:
    TSizeFormatPrinter(google::protobuf::Message& message);
    TString ToString() const;

private:
    void PrintMessage(const google::protobuf::Message& message, TStringBuilder& builder) const;
    void PrintFieldName(const google::protobuf::FieldDescriptor* field, TStringBuilder& builder) const;
    size_t RepeatedByteSizeLong(const google::protobuf::Message& message, const google::protobuf::Reflection* reflection, const google::protobuf::FieldDescriptor* field) const;
    size_t ItemByteSizeLong(const google::protobuf::Message& message, const google::protobuf::Reflection* reflection, const google::protobuf::FieldDescriptor* field) const;
    void PrintField(const google::protobuf::Message& message,
                    const google::protobuf::Reflection* reflection,
                    const google::protobuf::FieldDescriptor* field,
                    TStringBuilder& builder) const;
};

} // namespace NKikimr
