#pragma once

// A printer from protobuf to json string, with ability to inline some string fields of given protobuf message
// into output as ready json without additional escaping. These fields should be marked using special field option.
// An example of usage:
// 1) Define a field option in your .proto to identify fields which should be inlined, e.g.
//
//     import "google/protobuf/descriptor.proto";
//     extend google.protobuf.FieldOptions {
//         optional bool this_is_json = 58253;   // do not forget assign some more or less unique tag
//     }
//
// 2) Mark some fields of your protobuf message with this option, e.g.:
//
//     message TMyObject {
//         optional string A = 1 [(this_is_json) = true];
//     }
//
// 3) In the C++ code you prepare somehow an object of TMyObject type
//
//     TMyObject o;
//     o.Set("{\"inner\":\"value\"}");
//
// 4) And then serialize it to json string with inlining, e.g.:
//
//     Cout << NProtobufJson::PrintInlined(o, MakeFieldOptionFunctor(this_is_json)) << Endl;
//
// 5) Alternatively you can specify a some more abstract functor for defining raw json fields
//
// which will print following json to stdout:
//     {"A":{"inner":"value"}}
// instead of
//     {"A":"{\"inner\":\"value\"}"}
// which would be printed with normal Proto2Json printer.
//
// See ut/inline_ut.cpp for additional examples of usage.

#include "config.h"
#include "proto2json_printer.h"
#include "json_output_create.h"

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/generic/utility.h>

#include <functional>

namespace NProtobufJson {
    template <typename TBasePrinter = TProto2JsonPrinter> // TBasePrinter is assumed to be a TProto2JsonPrinter descendant
    class TInliningPrinter: public TBasePrinter {
    public:
        using TFieldPredicate = std::function<bool(const NProtoBuf::Message&,
                                                   const NProtoBuf::FieldDescriptor*)>;

        template <typename... TArgs>
        TInliningPrinter(TFieldPredicate isInlined, TArgs&&... args)
            : TBasePrinter(std::forward<TArgs>(args)...)
            , IsInlined(std::move(isInlined))
        {
        }

        virtual void PrintField(const NProtoBuf::Message& proto,
                                const NProtoBuf::FieldDescriptor& field,
                                IJsonOutput& json,
                                TStringBuf key) override {
            const NProtoBuf::TConstField f(proto, &field);
            if (!key && IsInlined(proto, &field) && ShouldPrint(f)) {
                key = this->MakeKey(field);
                json.WriteKey(key);
                if (!field.is_repeated()) {
                    json.WriteRawJson(f.Get<TString>());
                } else {
                    json.BeginList();
                    for (size_t i = 0, sz = f.Size(); i < sz; ++i)
                        json.WriteRawJson(f.Get<TString>(i));
                    json.EndList();
                }

            } else {
                TBasePrinter::PrintField(proto, field, json, key);
            }
        }

    private:
        bool ShouldPrint(const NProtoBuf::TConstField& f) const {
            if (!f.IsString())
                ythrow yexception() << "TInliningPrinter: json field "
                                    << f.Field()->name() << " should be a string";

            if (f.HasValue())
                return true;

            // we may want write default value for given field in case of its absence
            const auto& cfg = this->GetConfig();
            return (f.Field()->is_repeated() ? cfg.MissingRepeatedKeyMode : cfg.MissingSingleKeyMode) == TProto2JsonConfig::MissingKeyDefault;
        }

    private:
        TFieldPredicate IsInlined;
    };

    inline void PrintInlined(const NProtoBuf::Message& msg, TInliningPrinter<>::TFieldPredicate isInlined, IJsonOutput& output, const TProto2JsonConfig& config = TProto2JsonConfig()) {
        TInliningPrinter<> printer(std::move(isInlined), config);
        printer.Print(msg, output);
    }

    inline TString PrintInlined(const NProtoBuf::Message& msg, TInliningPrinter<>::TFieldPredicate isInlined, const TProto2JsonConfig& config = TProto2JsonConfig()) {
        TString ret;
        PrintInlined(msg, std::move(isInlined), *CreateJsonMapOutput(ret, config), config);
        return ret;
    }

}
