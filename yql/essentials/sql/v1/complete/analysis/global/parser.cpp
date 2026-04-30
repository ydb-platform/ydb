#include "parser.h"

#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/text/word.h>

#include <util/system/yassert.h>

namespace NSQLComplete {

namespace {

class TErrorStrategy: public antlr4::DefaultErrorStrategy {
public:
    antlr4::Token* singleTokenDeletion(antlr4::Parser* /* recognizer */) override {
        return nullptr;
    }
};

template <bool IsAnsiLexer>
class TParser: public IParser {
public:
    using TLexer = std::conditional_t<
        IsAnsiLexer,
        NALAAnsiAntlr4::SQLv1Antlr4Lexer,
        NALADefaultAntlr4::SQLv1Antlr4Lexer>;

    TParser()
        : Chars_()
        , Lexer_(&Chars_)
        , Tokens_(&Lexer_)
        , Parser_(&Tokens_)
    {
        Lexer_.removeErrorListeners();
        Parser_.removeErrorListeners();
        Parser_.setErrorHandler(std::make_shared<TErrorStrategy>());
    }

    TParsedInput Parse(TCompletionInput input) override {
        Recovered_.clear();
        if (IsRecoverable(input)) {
            Recovered_ = TString(input.Text);
            // "_" is to parse `SELECT x._ FROM table`
            //        instead of `SELECT x.FROM table`
            Recovered_.insert(input.CursorPosition, "_");
            input.Text = Recovered_;
        }

        SQLv1::Sql_queryContext* sqlQuery = ParseText(input.Text);
        Y_ENSURE(sqlQuery);

        return TParsedInput{
            .Original = input,
            .Tokens = &Tokens_,
            .Parser = &Parser_,
            .SqlQuery = sqlQuery,
        };
    }

private:
    bool IsRecoverable(TCompletionInput input) const {
        TStringBuf s = input.Text;
        size_t i = input.CursorPosition;
        return (i < s.size() && IsWordBoundary(s[i]) || i == s.size());
    }

    SQLv1::Sql_queryContext* ParseText(TStringBuf text) {
        Chars_.load(text.Data(), text.Size(), /* lenient = */ false);
        Lexer_.reset();
        Tokens_.setTokenSource(&Lexer_);
        Parser_.reset();
        return Parser_.sql_query();
    }

    antlr4::ANTLRInputStream Chars_;
    TLexer Lexer_;
    antlr4::CommonTokenStream Tokens_;
    SQLv1 Parser_;
    TString Recovered_;
};

} // namespace

IParser::TPtr MakeParser(bool isAnsiLexer) {
    if (isAnsiLexer) {
        return MakeHolder<TParser<true>>();
    }
    return MakeHolder<TParser<false>>();
}

} // namespace NSQLComplete
