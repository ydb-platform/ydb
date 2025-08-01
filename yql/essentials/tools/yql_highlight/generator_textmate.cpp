#include "generator_textmate.h"

#include "json.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/on_disk/tar_archive/archive_writer.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/memory/blob.h>

namespace NSQLHighlight {

    namespace NTextMate {

        using TRegex = TString;

        struct TRange {
            TRegex Begin;
            TRegex End;
        };

        struct TMatcher {
            TString Name;
            TString Group;
            std::variant<TRegex, TRange> Pattern;
        };

        struct TLanguage {
            TString Name;
            TString ScopeName;
            TVector<TString> FileTypes;
            TVector<TMatcher> Matchers;
        };

    } // namespace NTextMate

    namespace {

        NTextMate::TRegex ToTextMateRegex(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
            TStringBuilder regex;

            if (pattern.IsCaseInsensitive) {
                regex << "(?i)";
            }

            if (unit.IsPlain) {
                regex << R"re(\b)re";
            }

            regex << "(" << pattern.Body << ")";

            if (!pattern.After.empty()) {
                regex << "(?=" << pattern.After << ")";
            }

            if (unit.IsPlain) {
                regex << R"re(\b)re";
            }

            return regex;
        }

        TString ToTextMateGroup(EUnitKind kind) {
            switch (kind) {
                case EUnitKind::Keyword:
                    return "keyword.control";
                case EUnitKind::Punctuation:
                    return "keyword.operator";
                case EUnitKind::QuotedIdentifier:
                    return "string.interpolated";
                case EUnitKind::BindParameterIdentifier:
                    return "variable.parameter";
                case EUnitKind::TypeIdentifier:
                    return "entity.name.type";
                case EUnitKind::FunctionIdentifier:
                    return "entity.name.function";
                case EUnitKind::Identifier:
                    return "variable.other";
                case EUnitKind::Literal:
                    return "constant.numeric";
                case EUnitKind::StringLiteral:
                    return "string.quoted.double";
                case EUnitKind::Comment:
                    return "comment.block";
                case EUnitKind::Whitespace:
                    return "";
                case EUnitKind::Error:
                    return "";
            }
        }

        TString ToTextMateName(EUnitKind kind) {
            return ToString(kind);
        }

        TMaybe<NTextMate::TMatcher> TextMateMultilinePattern(const TUnit& unit) {
            auto range = unit.RangePattern;
            if (!range) {
                return Nothing();
            }

            return NTextMate::TMatcher{
                .Name = ToTextMateName(unit.Kind),
                .Group = ToTextMateGroup(unit.Kind),
                .Pattern = NTextMate::TRange{
                    .Begin = range->Begin,
                    .End = range->End,
                },
            };
        }

        NTextMate::TMatcher ToTextMatePattern(const TUnit& unit, const NSQLTranslationV1::TRegexPattern& pattern) {
            return NTextMate::TMatcher{
                .Name = ToTextMateName(unit.Kind),
                .Group = ToTextMateGroup(unit.Kind),
                .Pattern = ToTextMateRegex(unit, pattern),
            };
        }

    } // namespace

    NTextMate::TLanguage ToTextMateLanguage(const THighlighting& highlighting) {
        NTextMate::TLanguage language = {
            .Name = "YQL",
            .ScopeName = "source.yql",
            .FileTypes = {"yql"},
        };

        for (const TUnit& unit : highlighting.Units) {
            if (unit.IsCodeGenExcluded) {
                continue;
            }

            for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
                language.Matchers.emplace_back(ToTextMatePattern(unit, pattern));
            }
            if (auto textmate = TextMateMultilinePattern(unit)) {
                language.Matchers.emplace_back(*textmate);
            }
        }

        return language;
    }

    NJson::TJsonValue ToJson(const NTextMate::TMatcher& matcher) {
        NJson::TJsonMap json = {{"name", matcher.Group}};
        std::visit([&](const auto& pattern) {
            using T = std::decay_t<decltype(pattern)>;

            if constexpr (std::is_same_v<T, NTextMate::TRegex>) {
                json["match"] = pattern;
            } else if constexpr (std::is_same_v<T, NTextMate::TRange>) {
                json["begin"] = pattern.Begin;
                json["end"] = pattern.End;
            } else {
                static_assert(false);
            }
        }, matcher.Pattern);
        return json;
    }

    NJson::TJsonValue ToJson(const NTextMate::TLanguage& language) {
        NJson::TJsonMap root;
        root["$schema"] = "https://raw.githubusercontent.com/martinring/tmlanguage/master/tmlanguage.json";
        root["name"] = language.Name;
        root["scopeName"] = language.ScopeName;

        for (const TString& type : language.FileTypes) {
            root["fileTypes"].AppendValue(type);
        }

        THashSet<TString> visited;
        for (const NTextMate::TMatcher& matcher : language.Matchers) {
            root["repository"][matcher.Name]["patterns"].AppendValue(ToJson(matcher));

            if (!visited.contains(matcher.Name)) {
                root["patterns"].AppendValue(NJson::TJsonMap({{"include", "#" + matcher.Name}}));
                visited.emplace(matcher.Name);
            }
        }

        return root;
    }

    TString EscapeXML(TString string) {
        SubstGlobal(string, "<", "&lt;");
        SubstGlobal(string, ">", "&gt;");
        return string;
    }

    void WriteXML(IOutputStream& out, const NJson::TJsonValue& json, TString indent = "") {
        static constexpr TStringBuf extra = "    ";

        if (TString string; json.GetString(&string)) {
            out << indent << "<string>" << EscapeXML(string) << "</string>" << "\n";
        } else if (NJson::TJsonValue::TMapType dict; json.GetMap(&dict)) {
            out << indent << "<dict>" << '\n';
            for (const auto& [key, value] : dict) {
                out << indent << extra << "<key>" << EscapeXML(key) << "</key>" << '\n';
                WriteXML(out, value, indent + extra);
            }
            out << indent << "</dict>" << '\n';
        } else if (NJson::TJsonValue::TArray array; json.GetArray(&array)) {
            out << indent << "<array>" << '\n';
            for (const auto& value : array) {
                WriteXML(out, value, indent + extra);
            }
            out << indent << "</array>" << '\n';
        } else {
            TStringStream str;
            Print(str, json);
            ythrow yexception() << "Unexpected JSON '" + str.Str() + "'";
        }
    }

    void GenerateTextMateJson(IOutputStream& out, const THighlighting& highlighting) {
        Print(out, ToJson(ToTextMateLanguage(highlighting)));
    }

    class TTextMateBundleGenerator: public IGenerator {
    private:
        static constexpr TStringBuf InfoUUID = "9BB0DBAF-E65C-4E14-A6A7-467D4AA535E0";
        static constexpr TStringBuf SyntaxUUID = "1C3868E4-F96B-4E55-B204-1DCB5A20748B";
        static constexpr TStringBuf BundleDir = "YQL.tmbundle";
        static constexpr TStringBuf InfoFile = "info.plist";
        static constexpr TStringBuf SyntaxFile = "Syntaxes/YQL.tmLanguage";

        template <class TWriter>
        void Write(
            NTar::TArchiveWriter& acrhive,
            TStringBuf path,
            TWriter writer,
            const NTextMate::TLanguage& langugage)
        {
            TStringStream stream;
            writer(stream, langugage);
            TBlob blob = TBlob::FromString(stream.Str());
            acrhive.WriteFile(TString(path), blob);
        }

    public:
        void Write(IOutputStream& out, const THighlighting& highlighting) final {
            out << "File " << BundleDir << "/" << InfoFile << ":" << '\n';
            WriteInfo(out, ToTextMateLanguage(highlighting));
            out << "File " << BundleDir << "/" << SyntaxFile << ":" << '\n';
            WriteSyntax(out, ToTextMateLanguage(highlighting));
        }

        void Write(const TFsPath& path, const THighlighting& highlighting) final {
            if (TString name = path.GetName(); !name.StartsWith(BundleDir)) {
                ythrow yexception()
                    << "Invalid path '" << name
                    << "', expected '" << BundleDir << "' "
                    << "as an archive name";
            }

            NTextMate::TLanguage language = ToTextMateLanguage(highlighting);

            NTar::TArchiveWriter archive(path);
            Write(archive, InfoFile, WriteInfo, language);
            Write(archive, SyntaxFile, WriteSyntax, language);
        }

    private:
        static void WriteInfo(IOutputStream& out, const NTextMate::TLanguage& language) {
            out << R"(<?xml version="1.0" encoding="UTF-8"?>)" << '\n';
            out << R"(<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">)" << '\n';
            out << R"(<plist version="1.0">)" << '\n';
            out << R"(<dict>)" << '\n';
            out << R"(    <key>name</key>)" << '\n';
            out << R"(    <string>)" << language.Name << R"(</string>)" << '\n';
            out << R"(    <key>uuid</key>)" << '\n';
            out << R"(    <string>)" << InfoUUID << R"(</string>)" << '\n';
            out << R"(</dict>)" << '\n';
            out << R"(</plist>)" << '\n';
        }

        static void WriteSyntax(IOutputStream& out, const NTextMate::TLanguage& language) {
            NJson::TJsonValue json = ToJson(language);
            json.EraseValue("$schema");
            json["uuid"] = SyntaxUUID;

            out << R"(<?xml version="1.0" encoding="UTF-8"?>)" << '\n';
            out << R"(<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">)" << '\n';
            out << R"(<plist version="1.0">)" << '\n';
            WriteXML(out, json);
            out << R"(</plist>)" << '\n';
        }
    };

    IGenerator::TPtr MakeTextMateJsonGenerator() {
        return MakeOnlyFileGenerator(GenerateTextMateJson);
    }

    IGenerator::TPtr MakeTextMateBundleGenerator() {
        return new TTextMateBundleGenerator();
    }

} // namespace NSQLHighlight
