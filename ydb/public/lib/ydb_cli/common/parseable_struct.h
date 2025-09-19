#pragma once

#include "command.h"
#include "client_command_options.h"

#include <library/cpp/colorizer/colors.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/va_args.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb {
namespace NConsoleClient {

template <typename T>
class TParseableStruct {
    static const TOptionParseResult* FindOptParseResult(const TOptionsParseResult* parseResult, const TClientCommandOption* opt) {
        return parseResult->FindResult(opt);
    }

    static const TOptionParseResult* FindOptParseResult(const TOptionsParseResult* parseResult, const TString& name) {
        return parseResult->FindResult(name);
    }

    static const TOptionParseResult* FindOptParseResult(const TOptionsParseResult* parseResult, char c) {
        return parseResult->FindResult(c);
    }

    static T FromString(const char* data) {
        T t;

        TStringBuf buf(data);
        TStringBuf property = buf.NextTok('=');
        THashSet<TString> matched;

        while (property.IsInited()) {
            if (const TString field = t.LoadProperty(property, buf.NextTok(','))) {
                if (!matched.insert(field).second) {
                    throw TMisuseException() << "Duplicate value for \"" << field << "\"";
                }
            } else {
                throw TMisuseException() << "Bad property: \"" << property << "\"";
            }

            property = buf.NextTok('=');
        }

        for (auto it = Fields.begin(), last = Fields.end(); it != last; ++it) {
            if (it->second.Required && !matched.contains(it->first)) {
                throw TMisuseException() << "Missing required property \"" << it->first << "\"";
            }
        }

        return t;
    }

    static TString MakeIndent(size_t indentSize, char indentC = ' ') {
        TStringBuilder indent;

        while (indentSize) {
            indent << indentC;
            --indentSize;
        }

        return indent;
    }

public:
    template <typename TOpt>
    static TVector<T> Parse(const TClientCommand::TConfig& config, const TOpt opt) {
        const auto* parseResult = FindOptParseResult(config.ParseResult, opt);
        if (!parseResult) {
            return {};
        }

        TVector<T> result(Reserve(parseResult->Count()));

        for (const TString& value : parseResult->Values()) {
            result.push_back(FromString(value.c_str()));
        }

        return result;
    }

    static TString FormatHelp(const TStringBuf helpMessage, size_t verbosityLevel, size_t indentSize = 0, char indentC = ' ') {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);

        TStringBuilder help;
        help << helpMessage;

        TString indent;
        if (verbosityLevel >= 2 && indentSize) {
            indent = MakeIndent(indentSize, indentC);
        }

        if (verbosityLevel >= 2) {
            help << Endl << indent << "Possible property names:" << Endl;
        } else {
            help << ". Possible property names: ";
        }

        bool first = true;
        for (const auto& kv : Fields) {
            if (verbosityLevel >= 2) {
                help << indent << indent << colors.BoldColor() << kv.first << colors.OldColor();

                if (kv.second.Aliases) {
                    help << " (aliases: ";

                    bool first = true;
                    for (const auto& alias : kv.second.Aliases) {
                        if (!first) {
                            help << ", ";
                        }
                        first = false;
                        help << colors.BoldColor() << alias << colors.OldColor();
                    }

                    help << ")";
                }

                help << Endl;

                if (kv.second.Description) {
                    help << indent << "    ";
                    if (kv.second.Required) {
                        help << "[Required] ";
                    }
                    help << kv.second.Description << Endl;
                }
            } else {
                if (first) {
                    first = false;
                } else {
                    help << ", ";
                }
                help << colors.BoldColor() << kv.first << colors.OldColor();
            }
        }

        return help;
    }

protected:
    struct TField {
        TVector<TString> Aliases;
        TString Description;
        bool Required;
    };

    static THashMap<TString, TField> Fields;

    static void DefineField(const TString& name, TField field) {
        auto it = Fields.find(name);
        Y_ABORT_UNLESS(it != Fields.end());
        it->second = std::move(field);
    }

    static bool MatchField(const TString& name, const TStringBuf property) {
        if (name == property) {
            return true;
        }

        auto it = Fields.find(name);
        Y_ABORT_UNLESS(it != Fields.end());
        return Find(it->second.Aliases, property) != it->second.Aliases.end();
    }

};

template <typename T>
THashMap<TString, typename TParseableStruct<T>::TField> TParseableStruct<T>::Fields;

#if defined DEFINE_PARSEABLE_STRUCT
#error DEFINE_PARSEABLE_STRUCT macro redefinition
#endif

#define DEFINE_PARSEABLE_STRUCT(name, fields, ...)                             \
    struct name : public fields, public TParseableStruct<name> {               \
        static void DefineFields(THashMap<TString, TField> aliases) {\
            Y_PASS_VA_ARGS(Y_MAP_ARGS(__INIT_STRUCT_FIELD__, __VA_ARGS__))     \
            for (auto& kv : aliases) {                                         \
                DefineField(kv.first, std::move(kv.second));                   \
            }                                                                  \
        }                                                                      \
        TString LoadProperty(const TStringBuf property, const TStringBuf value) { \
            Y_PASS_VA_ARGS(Y_MAP_ARGS(__PARSE_STRUCT_FIELD__, __VA_ARGS__))    \
            return TString();                                                  \
        }                                                                      \
    }

#if defined __INIT_STRUCT_FIELD__
#error __INIT_STRUCT_FIELD__ macro redefinition
#endif

#define __INIT_STRUCT_FIELD__(field)                                           \
    Fields[#field] = {};

#if defined __PARSE_STRUCT_FIELD__
#error __PARSE_STRUCT_FIELD__ macro redefinition
#endif

#define __PARSE_STRUCT_FIELD__(field)                                          \
    if (MatchField(#field, property)) {                                        \
        field = ::FromString<decltype(field)>(value);                          \
        return #field;                                                           \
    }

}
}
