#pragma once
#include <google/protobuf/text_format.h>

namespace NKikimr {

class THideFieldValuePrinter : public google::protobuf::TextFormat::FastFieldValuePrinter {
public:
    void PrintBool(bool val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintInt32(i32 val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintUInt32(ui32 val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintInt64(i64 val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintUInt64(ui64 val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintFloat(float val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintDouble(double val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintString(const TString& val,
                     google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintBytes(const TString& val,
                    google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    void PrintEnum(i32 val, const TString& name,
                   google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
    bool PrintMessageContent(const google::protobuf::Message& message, int fieldIndex,
                             int fieldCount, bool singleLineMode,
                             google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
};

} // namespace NKikimr
