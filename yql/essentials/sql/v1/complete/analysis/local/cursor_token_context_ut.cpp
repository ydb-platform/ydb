#include "cursor_token_context.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(CursorTokenContextTests) {

    NSQLTranslation::ILexer::TPtr MakeLexer() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        return NSQLTranslationV1::MakeLexer(
            lexers, /* ansi = */ false, /* antlr4 = */ true,
            NSQLTranslationV1::ELexerFlavor::Pure);
    }

    TCursorTokenContext Context(TString input) {
        auto lexer = MakeLexer();
        TCursorTokenContext context;
        UNIT_ASSERT(GetCursorTokenContext(lexer, SharpedInput(input), context));
        return context;
    }

    Y_UNIT_TEST(Empty) {
        auto context = Context("");
        UNIT_ASSERT(context.Cursor.PrevTokenIndex.Empty());
        UNIT_ASSERT_VALUES_EQUAL(context.Cursor.NextTokenIndex, 0);
        UNIT_ASSERT_VALUES_EQUAL(context.Cursor.Position, 0);
        UNIT_ASSERT(context.Enclosing().Empty());
    }

    Y_UNIT_TEST(Blank) {
        UNIT_ASSERT(Context("# ").Enclosing().Empty());
        UNIT_ASSERT(Context(" #").Enclosing().Empty());
        UNIT_ASSERT(Context(" # ").Enclosing().Empty());
    }

    Y_UNIT_TEST(Enclosing) {
        UNIT_ASSERT(Context("se#").Enclosing().Defined());
        UNIT_ASSERT(Context("#se").Enclosing().Empty());
        UNIT_ASSERT(Context("`se`#").Enclosing().Empty());
        UNIT_ASSERT(Context("#`se`").Enclosing().Empty());
        UNIT_ASSERT(Context("`se`#`se`").Enclosing().Defined());
        UNIT_ASSERT(Context("\"se\"#\"se\"").Enclosing().Empty());
    }

} // Y_UNIT_TEST_SUITE(CursorTokenContextTests)
