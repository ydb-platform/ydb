#pragma once
#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {

class TCustomizableTextFormatPrinter : public google::protobuf::TextFormat::Printer {
public:
    template <class TPrinter>
    bool RegisterFieldValuePrinters(const google::protobuf::Descriptor* desc, const char* name) {
        const google::protobuf::FieldDescriptor* field = desc->FindFieldByName(name);
        Y_ASSERT(field != nullptr);
        auto printer = std::make_unique<TPrinter>();
        const auto success = RegisterFieldValuePrinter(field, printer.get());
        if (success) {
            // RegisterFieldValuePrinter took a ownership of the printer => release it.
            printer.release();
        }
        return success;
    }

    template <class TPrinter, class... T>
    bool RegisterFieldValuePrinters(const google::protobuf::Descriptor* desc, const char* name, T... fieldNames) {
        const bool firstRegister = RegisterFieldValuePrinters<TPrinter>(desc, name);
        const bool otherRegisters = RegisterFieldValuePrinters<TPrinter>(desc, fieldNames...);
        return firstRegister && otherRegisters;
    }

    template <class TMsg, class TPrinter, class... T>
    bool RegisterFieldValuePrinters(T... fieldNames) {
        const google::protobuf::Descriptor* desc = TMsg::descriptor();
        return RegisterFieldValuePrinters<TPrinter>(desc, fieldNames...);
    }
};

} // namespace NKikimr
