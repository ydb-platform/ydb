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

    TParsedInput Parse(TCompletionInput input) const override {
        TParsedInput output;

        if (IsRecoverable(input)) {
            // "_" is to parse `SELECT x._ FROM table`
            //        instead of `SELECT x.FROM table`
            output.RecoveredText.ConstructInPlace(input.Text);
            output.RecoveredText->insert(input.CursorPosition, "_");
            input.Text = *output.RecoveredText;
        }

        TStringBuf prefix = TStringBuf(input.Text).Head(input.CursorPosition);
        output.CursorPosition = GetNumberOfUTF8Chars(prefix);

        NSQLPureAST::IParseTree::TPtr tree = Parser_->Parse(input.Text);
        output.ParseTree = std::move(tree);

        return output;
    }

private:
    bool IsRecoverable(TCompletionInput input) const {
        TStringBuf s = input.Text;
        size_t i = input.CursorPosition;
        return (i < s.size() && IsWordBoundary(s[i]) || i == s.size());
    }

    NSQLPureAST::IParser::TPtr Parser_;
};

} // namespace

IParser::TPtr MakeParser(bool isAnsiLexer) {
    return MakeHolder<TParser>(NSQLPureAST::MakeParser(isAnsiLexer));
}

} // namespace NSQLComplete
