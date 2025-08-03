#include "hints.h"
#include "parse_hints_impl.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/string/escape.h>

namespace NSQLTranslation {

using NYql::TPosition;

TString TSQLHint::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TSQLHint::Out(IOutputStream& o) const {
    o << "\"" << EscapeC(Name) << "\":{";
    for (size_t i = 0; i < Values.size(); ++i) {
        o << "\"" << EscapeC(Values[i]) << "\"";
        if (i + 1 < Values.size()) {
            o << ",";
        }
    }
    o << "}";
}

namespace {

class TTokenProcessor {
public:
    TTokenProcessor(const TString& queryFile, TSQLHints& hints, bool utf8Aware)
        : QueryFile_(queryFile)
        , Hints_(hints)
        , Utf8Aware_(utf8Aware)
    {}

    TPosition ExtractPosition(const TParsedToken& token) const {
        return TPosition(token.LinePos + 1, token.Line, QueryFile_);
    }

    void ProcessToken(TParsedToken&& token) {
        if (token.Name == "EOF") {
            return;
        }
        auto pos = ExtractPosition(token);
        YQL_ENSURE(!PrevNonCommentPos_.Defined() || *PrevNonCommentPos_ < pos, "Tokens positions should increase monotonically");
        if (token.Name == "WS") {
            return;
        }
        if (token.Name != "COMMENT") {
            PrevNonCommentPos_ = pos;
            return;
        }
        if (!PrevNonCommentPos_) {
            // skip leading comments
            return;
        }
        TVector<TSQLHint> currentHints = NDetail::ParseSqlHints(pos, token.Content, Utf8Aware_);
        if (currentHints.empty()) {
            // no hints here
            return;
        }
        auto& target = Hints_[*PrevNonCommentPos_];
        target.insert(target.end(), currentHints.begin(), currentHints.end());
    }

private:
    TMaybe<TPosition> PrevNonCommentPos_;
    const TString QueryFile_;
    TSQLHints& Hints_;
    const bool Utf8Aware_;
};

}

bool CollectSqlHints(ILexer& lexer, const TString& query, const TString& queryName,
    const TString& queryFile, TSQLHints& hints, NYql::TIssues& issues, size_t maxErrors, bool utf8Aware) {
    TTokenProcessor tp(queryFile, hints, utf8Aware);
    return lexer.Tokenize(query, queryName, [&tp](TParsedToken&& token) { tp.ProcessToken(std::move(token)); }, issues, maxErrors);
}


}
