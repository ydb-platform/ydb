#include "parser.h"

#include <yql/essentials/sql/v1/ide/completion/text/word.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parser.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

namespace {

class TParser: public IParser {
public:
    explicit TParser(NSQLPureAST::IParser::TPtr parser)
        : Parser_(std::move(parser))
    {
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

        TStringBuf prefix = TStringBuf(input.Text).Head(input.CursorPosition);
        input.CursorPosition = GetNumberOfUTF8Chars(prefix);

        NSQLPureAST::TParseTree tree = Parser_->Parse(input.Text);

        return {
            .Original = {
                .Text = tree.Text,
                .CursorPosition = input.CursorPosition,
            },
            .Tokens = tree.Tokens,
            .Parser = tree.Parser,
            .SqlQuery = tree.SqlQuery,
        };
    }

private:
    bool IsRecoverable(TCompletionInput input) const {
        TStringBuf s = input.Text;
        size_t i = input.CursorPosition;
        return (i < s.size() && IsWordBoundary(s[i]) || i == s.size());
    }

    TString Recovered_;
    NSQLPureAST::IParser::TPtr Parser_;
};

} // namespace

IParser::TPtr MakeParser(bool isAnsiLexer) {
    return MakeHolder<TParser>(NSQLPureAST::MakeParser(isAnsiLexer));
}

} // namespace NSQLComplete
