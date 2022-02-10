#pragma once

#include "config.h"
#include "proto2json_printer.h"
#include "json_output_create.h"

#include <util/generic/yexception.h>
#include <util/generic/utility.h>

#include <functional>

namespace NProtobufJson {
    template <typename TBasePrinter = TProto2JsonPrinter> // TBasePrinter is assumed to be a TProto2JsonPrinter descendant
    class TFilteringPrinter: public TBasePrinter {
    public:
        using TFieldPredicate = std::function<bool(const NProtoBuf::Message&, const NProtoBuf::FieldDescriptor*)>;

        template <typename... TArgs>
        TFilteringPrinter(TFieldPredicate isPrinted, TArgs&&... args)
            : TBasePrinter(std::forward<TArgs>(args)...)
            , IsPrinted(std::move(isPrinted))
        {
        }

        virtual void PrintField(const NProtoBuf::Message& proto,
                                const NProtoBuf::FieldDescriptor& field,
                                IJsonOutput& json,
                                TStringBuf key) override {
            if (key || IsPrinted(proto, &field))
                TBasePrinter::PrintField(proto, field, json, key);
        }

    private:
        TFieldPredicate IsPrinted;
    };

    inline void PrintWithFilter(const NProtoBuf::Message& msg, TFilteringPrinter<>::TFieldPredicate filter, IJsonOutput& output, const TProto2JsonConfig& config = TProto2JsonConfig()) {
        TFilteringPrinter<> printer(std::move(filter), config);
        printer.Print(msg, output);
    }

    inline TString PrintWithFilter(const NProtoBuf::Message& msg, TFilteringPrinter<>::TFieldPredicate filter, const TProto2JsonConfig& config = TProto2JsonConfig()) {
        TString ret;
        PrintWithFilter(msg, std::move(filter), *CreateJsonMapOutput(ret, config), config);
        return ret;
    }

}
