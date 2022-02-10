#pragma once
#include <google/protobuf/text_format.h>

namespace NKikimr {

class TTokenFieldValuePrinter : public google::protobuf::TextFormat::FastFieldValuePrinter {
public:
    void PrintString(const TProtoStringType& val, google::protobuf::TextFormat::BaseTextGenerator* generator) const override;
};

} // namespace NKikimr
