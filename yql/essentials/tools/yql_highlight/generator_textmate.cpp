#include "generator_textmate.h"

#include "json.h"

#include <contrib/libs/re2/re2/re2.h>

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
    TMaybe<TRegex> Escape;
};

struct TMatcher {
    TString Name;
    TString Group;
    std::variant<TRegex, TRange> Pattern;
};

struct TLanguage {
    TString Name;
    TString ScopeName;
    TString FileType;
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

    if (!pattern.Before.empty()) {
        regex << "(?<=" << pattern.Before << ")";
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
        case EUnitKind::OptionIdentifier:
            return "identifier";
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

NTextMate::TMatcher TextMateMultilinePattern(const TUnit& unit, const TRangePattern& range) {
    return NTextMate::TMatcher{
        .Name = ToTextMateName(unit.Kind),
        .Group = ToTextMateGroup(unit.Kind),
        .Pattern = NTextMate::TRange{
            .Begin = RE2::QuoteMeta(range.BeginPlain),
            .End = RE2::QuoteMeta(range.EndPlain),
            .Escape = range.EscapeRegex,
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
        .Name = highlighting.Name,
        .ScopeName = "source." + highlighting.Extension,
        .FileType = highlighting.Extension,
    };

    for (const TUnit& unit : highlighting.Units) {
        if (unit.IsCodeGenExcluded) {
            continue;
        }

        for (const NSQLTranslationV1::TRegexPattern& pattern : unit.Patterns) {
            language.Matchers.emplace_back(ToTextMatePattern(unit, pattern));
        }
        for (const TRangePattern& range : unit.RangePatterns) {
            language.Matchers.emplace_back(TextMateMultilinePattern(unit, range));
        }
    }

    return language;
}

TMaybe<TString> EmbeddedLanguage(const NTextMate::TRange& range) {
    if (range.Begin.StartsWith(RE2::QuoteMeta(TRangePattern::EmbeddedPythonBegin))) {
        return "source.python";
    }

    if (range.Begin.StartsWith(RE2::QuoteMeta(TRangePattern::EmbeddedJavaScriptBegin))) {
        return "source.js";
    }

    return Nothing();
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
            if (auto embedded = EmbeddedLanguage(pattern)) {
                json["patterns"].AppendValue(NJson::TJsonMap{{"include", *embedded}});
            }
            if (pattern.Escape) {
                json["patterns"].AppendValue(NJson::TJsonMap{
                    {"name", "constant.character.escape.untitled"},
                    {"match", *pattern.Escape},
                });
            }
        } else {
            static_assert(false);
        }
    }, matcher.Pattern);
    return json;
}

NJson::TJsonValue ToJson(const NTextMate::TLanguage& language) {
    NJson::TJsonMap root;
    root["$schema"] = "https://raw.githubusercontent.com/martinring/tmlanguage/master/tmlanguage.json";
    root["name"] = language.FileType;
    root["scopeName"] = language.ScopeName;
    root["scope"] = language.ScopeName;
    root["fileTypes"] = NJson::TJsonArray({language.FileType});

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

void GenerateTextMateJson(IOutputStream& out, const THighlighting& highlighting, bool /* ansi */) {
    Print(out, ToJson(ToTextMateLanguage(highlighting)));
}

static const THashMap<TString, TString> UUID = {
    {"InfoYQL", "059de4a7-ff49-4dbd-8a9d-a8114b77c4b9"},
    {"SyntaxYQL", "bb7a80e5-733c-4ea6-9654-40db0675950c"},
    {"InfoYQLs", "7f536d44-2667-430e-b145-540992400cb3"},
    {"SyntaxYQLs", "6e62e13a-487b-4333-bbb2-9453d0783f8f"},
};

class TTextMateBundleGenerator: public IGenerator {
private:
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
    void Write(IOutputStream& out, const THighlighting& highlighting, bool /* ansi */) final {
        const auto [bundle, info, syntax] = Paths(highlighting);

        out << "File " << bundle << "/" << info << ":" << '\n';
        WriteInfo(out, ToTextMateLanguage(highlighting));
        out << "File " << bundle << "/" << syntax << ":" << '\n';
        WriteSyntax(out, ToTextMateLanguage(highlighting));
    }

    void Write(const TFsPath& path, const THighlighting& highlighting, bool /* ansi */) final {
        const auto [bundle, info, syntax] = Paths(highlighting);

        if (TString name = path.GetName(); !name.StartsWith(bundle)) {
            ythrow yexception()
                << "Invalid path '" << name
                << "', expected '" << bundle << "' "
                << "as an archive name";
        }

        NTextMate::TLanguage language = ToTextMateLanguage(highlighting);

        NTar::TArchiveWriter archive(path);
        Write(archive, info, WriteInfo, language);
        Write(archive, syntax, WriteSyntax, language);
    }

private:
    static std::tuple<TString, TString, TString> Paths(const THighlighting& h) {
        return {
            TStringBuilder() << h.Name << ".tmbundle",
            TStringBuilder() << "info.plist",
            TStringBuilder() << "Syntaxes/" << h.Name << ".tmLanguage",
        };
    }

    static void WriteInfo(IOutputStream& out, const NTextMate::TLanguage& language) {
        out << R"(<?xml version="1.0" encoding="UTF-8"?>)" << '\n';
        out << R"(<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">)" << '\n';
        out << R"(<plist version="1.0">)" << '\n';
        out << R"(<dict>)" << '\n';
        out << R"(    <key>name</key>)" << '\n';
        out << R"(    <string>)" << language.Name << R"(</string>)" << '\n';
        out << R"(    <key>uuid</key>)" << '\n';
        out << R"(    <string>)" << UUID.at("Info" + language.Name) << R"(</string>)" << '\n';
        out << R"(</dict>)" << '\n';
        out << R"(</plist>)" << '\n';
    }

    static void WriteSyntax(IOutputStream& out, const NTextMate::TLanguage& language) {
        NJson::TJsonValue json = ToJson(language);
        json.EraseValue("$schema");
        json["uuid"] = UUID.at("Syntax" + language.Name);

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
