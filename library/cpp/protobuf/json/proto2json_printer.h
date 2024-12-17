#pragma once

#include "json_output.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NProtobufJson {
    struct TProto2JsonConfig;

    class TProto2JsonPrinter {
    public:
        TProto2JsonPrinter(const TProto2JsonConfig& config);
        virtual ~TProto2JsonPrinter();

        virtual void Print(const NProtoBuf::Message& proto, IJsonOutput& json, bool closeMap = true);

        virtual const TProto2JsonConfig& GetConfig() const {
            return Config;
        }

    protected:
        virtual TStringBuf MakeKey(const NProtoBuf::FieldDescriptor& field);

        virtual void PrintField(const NProtoBuf::Message& proto,
                                const NProtoBuf::FieldDescriptor& field,
                                IJsonOutput& json,
                                TStringBuf key = {});

        void PrintRepeatedField(const NProtoBuf::Message& proto,
                                const NProtoBuf::FieldDescriptor& field,
                                IJsonOutput& json,
                                TStringBuf key = {});

        void PrintSingleField(const NProtoBuf::Message& proto,
                              const NProtoBuf::FieldDescriptor& field,
                              IJsonOutput& json,
                              TStringBuf key = {}, bool inProtoMap = false);

        void PrintKeyValue(const NProtoBuf::Message& proto,
                           IJsonOutput& json);

        TString MakeKey(const NProtoBuf::Message& proto,
                        const NProtoBuf::FieldDescriptor& field);

        template <bool InMapContext>
        void PrintEnumValue(const TStringBuf& key,
                            const NProtoBuf::EnumValueDescriptor* value,
                            IJsonOutput& json);

        template <bool InMapContext>
        void PrintStringValue(const NProtoBuf::FieldDescriptor& field,
                              const TStringBuf& key, const TString& value,
                              IJsonOutput& json);

        template <class T>
        bool NeedStringifyNumber(T value) const;

        template <class T>
        bool NeedStringifyRepeatedNumber(T value) const;

        bool TryPrintAny(const NProtoBuf::Message& proto, IJsonOutput& json);
        void PrintFields(const NProtoBuf::Message& proto, IJsonOutput& json);

    protected:
        const TProto2JsonConfig& Config;
        TString TmpBuf;
    };

}
