#include "sql_reflect.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/case_insensitive_string/case_insensitive_string.h>

#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/charset/utf8.h>

namespace NSQLReflect {

    const TStringBuf ReflectPrefix = "//!";
    const TStringBuf SectionPrefix = "//! section:";
    const TStringBuf SectionPunctuation = "//! section:punctuation";
    const TStringBuf SectionLetter = "//! section:letter";
    const TStringBuf SectionKeyword = "//! section:keyword";
    const TStringBuf SectionOther = "//! section:other";
    const TStringBuf FragmentPrefix = "fragment ";

    const TStringBuf TLexerGrammar::KeywordBlockByName(const TStringBuf name Y_LIFETIME_BOUND) {
        if (name == "TSKIP") {
            return "SKIP";
        }
        return name;
    }

    const TString TLexerGrammar::KeywordNameByBlock(const TStringBuf block) {
        if (TCaseInsensitiveStringBuf(block) == "SKIP") {
            return "TSKIP";
        }
        return ToUpperUTF8(block);
    }

    TVector<TString> GetResourceLines(const TStringBuf key) {
        TString text;
        Y_ENSURE(NResource::FindExact(key, &text));

        TVector<TString> lines;
        Split(text, "\n", lines);
        for (auto& line : lines) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
        }
        return lines;
    }

    void Format(TVector<TString>& lines) {
        for (size_t i = 0; i < lines.size(); ++i) {
            auto& line = lines[i];

            StripInPlace(line);

            if (line.StartsWith("//") || (line.Contains(':') && line.Contains(';'))) {
                continue;
            }

            size_t j = i + 1;
            do {
                line += lines.at(j);
            } while (!lines.at(j++).Contains(';'));

            auto first = std::next(std::begin(lines), i + 1);
            auto last = std::next(std::begin(lines), j);
            lines.erase(first, last);
        }

        for (auto& line : lines) {
            CollapseInPlace(line);
            SubstGlobal(line, " ;", ";");
            SubstGlobal(line, " :", ":");
            SubstGlobal(line, " )", ")");
            SubstGlobal(line, "( ", "(");
        }
    }

    void Purify(TVector<TString>& lines) {
        const auto [first, last] = std::ranges::remove_if(lines, [](const TString& line) {
            return (line.StartsWith("//") && !line.StartsWith(ReflectPrefix)) || line.empty();
        });
        lines.erase(first, last);
    }

    THashMap<TStringBuf, TVector<TString>> GroupBySection(TVector<TString>&& lines) {
        TVector<TStringBuf> sections = {
            "",
            SectionPunctuation,
            SectionLetter,
            SectionKeyword,
            SectionOther,
        };

        size_t section = 0;

        THashMap<TStringBuf, TVector<TString>> groups;
        for (auto& line : lines) {
            if (line.StartsWith(SectionPrefix)) {
                Y_ENSURE(sections.at(section + 1) == line);
                section += 1;
                continue;
            }

            groups[sections.at(section)].emplace_back(std::move(line));
        }

        groups.erase("");
        groups.erase(SectionLetter);

        return groups;
    }

    std::tuple<TString, TString> ParseLexerRule(TString&& line) {
        size_t colonPos = line.find(':');
        size_t semiPos = line.rfind(';');

        Y_ENSURE(
            colonPos != TString::npos &&
            semiPos != TString::npos &&
            colonPos < semiPos);

        TString block = line.substr(colonPos + 2, semiPos - colonPos - 2);
        SubstGlobal(block, "\\\\", "\\");

        TString name = std::move(line);
        name.resize(colonPos);

        return std::make_tuple(std::move(name), std::move(block));
    }

    void ParsePunctuationLine(TString&& line, TLexerGrammar& grammar) {
        auto [name, block] = ParseLexerRule(std::move(line));
        block = block.erase(std::begin(block));
        block.pop_back();

        SubstGlobal(block, "\\\'", "\'");

        if (!name.StartsWith(FragmentPrefix)) {
            grammar.PunctuationNames.emplace(name);
        }

        SubstGlobal(name, FragmentPrefix, "");
        grammar.BlockByName.emplace(std::move(name), std::move(block));
    }

    void ParseKeywordLine(TString&& line, TLexerGrammar& grammar) {
        auto [name, block] = ParseLexerRule(std::move(line));
        SubstGlobal(block, "'", "");
        SubstGlobal(block, " ", "");

        Y_ENSURE(name == block || (name == "TSKIP" && block == TLexerGrammar::KeywordBlockByName("TSKIP")));
        grammar.KeywordNames.emplace(std::move(name));
    }

    void ParseOtherLine(TString&& line, TLexerGrammar& grammar) {
        auto [name, block] = ParseLexerRule(std::move(line));

        if (!name.StartsWith(FragmentPrefix)) {
            grammar.OtherNames.emplace_back(name);
        }

        SubstGlobal(name, FragmentPrefix, "");
        SubstGlobal(block, " -> channel(HIDDEN)", "");
        grammar.BlockByName.emplace(std::move(name), std::move(block));
    }

    TLexerGrammar LoadLexerGrammar() {
        TVector<TString> lines = GetResourceLines("SQLv1Antlr4.g.in");
        Purify(lines);
        Format(lines);
        Purify(lines);

        THashMap<TStringBuf, TVector<TString>> sections;
        sections = GroupBySection(std::move(lines));

        TLexerGrammar grammar;

        for (auto& [section, lines] : sections) {
            for (auto& line : lines) {
                if (section == SectionPunctuation) {
                    ParsePunctuationLine(std::move(line), grammar);
                } else if (section == SectionKeyword) {
                    ParseKeywordLine(std::move(line), grammar);
                } else if (section == SectionOther) {
                    ParseOtherLine(std::move(line), grammar);
                } else {
                    Y_ABORT("Unexpected section %s", section);
                }
            }
        }

        return grammar;
    }

} // namespace NSQLReflect
