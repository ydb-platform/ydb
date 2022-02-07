#pragma once

namespace NKikimr {
    namespace NDriverClient {

        inline TString GetProtobufEnumOptions(const google::protobuf::EnumDescriptor *desc) {
            TStringStream options;
            options << "{";
            for (int i = 0; i < desc->value_count(); ++i) {
                options << (i ? "|" : "") << desc->value(i)->name();
            }
            options << "}";
            return options.Str();
        }

        inline int GetProtobufOption(const google::protobuf::EnumDescriptor *desc, const char *option, const TString& optval) {
            const google::protobuf::EnumValueDescriptor *value = desc->FindValueByName(optval);
            if (!value) {
                int num;
                if (TryFromString(optval, num)) {
                    value = desc->FindValueByNumber(num);
                }
            }
            if (!value) {
                ythrow yexception() << "invalid value for option " << option << ": '" << optval << "'; possible values: "
                    << GetProtobufEnumOptions(desc);
            }
            return value->number();
        }

    } // NDriverClient
} // NKikimr
