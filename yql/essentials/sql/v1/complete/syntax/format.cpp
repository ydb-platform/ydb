#include "format.h"

#include "grammar.h"

#include <yql/essentials/sql/v1/complete/antlr4/vocabulary.h>

#include <util/generic/hash_set.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        const THashSet<std::string> Keywords = [] {
            const auto& grammar = GetSqlGrammar();
            const auto& vocabulary = grammar.GetVocabulary();

            THashSet<std::string> keywords;
            for (auto& token : grammar.GetKeywordTokens()) {
                keywords.emplace(Display(vocabulary, token));
            }
            return keywords;
        }();

        const THashSet<TString> TypeConstructors = {
            "DECIMAL",
            "OPTIONAL",
            "TUPLE",
            "STRUCT",
            "VARIANT",
            "LIST",
            "STREAM",
            "FLOW",
            "DICT",
            "SET",
            "ENUM",
            "RESOURCE",
            "TAGGED",
            "CALLABLE",
        };

    } // namespace

    TString FormatKeywords(const TVector<TString>& seq) {
        if (seq.empty()) {
            return "";
        }

        size_t i = 0;
        TString text = seq[i++];

        if (2 <= seq.size() &&
            TypeConstructors.contains(text) &&
            (seq[i] == "<" || seq[i] == "(")) {
            text.to_title();
            text.append(seq[i]);
            i += 1;
        }

        for (; i < seq.size(); ++i) {
            const auto& token = seq[i];
            if (Keywords.contains(token)) {
                text += " ";
            }
            text += token;
        }

        return text;
    }

    TString Quoted(TString content) {
        content.prepend('`');
        content.append('`');
        return content;
    }

    TString Unquoted(TString content) {
        Y_ENSURE(2 <= content.size() && content.front() == '`' && content.back() == '`');
        content.erase(0, 1);
        content.pop_back();
        return content;
    }

} // namespace NSQLComplete
