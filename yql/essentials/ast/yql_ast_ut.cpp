#include "yql_ast.h"
#include "yql_ast_annotation.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/util.h>
#include <util/system/sanitizers.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TParseYqlAst) {
    constexpr TStringBuf TEST_PROGRAM =
        "(\n"
        "#comment\n"
        "(let mr_source (DataSource 'yamr 'cedar))\n"
        "(let x (Read! world mr_source (Key '('table (KeyString 'Input))) '('key 'value) '()))\n"
        "(let world (Left! x))\n"
        "(let table1 (Right! x))\n"
        "(let tresh (Int32 '100))\n"
        "(let table1low (Filter table1 (lambda '(item) (< (member item 'key) tresh))))\n"
        "(let mr_sink (DataSink 'yamr (quote cedar)))\n"
        "(let world (Write! world mr_sink (Key '('table (KeyString 'Output))) table1low '('('mode 'append))))\n"
        "(let world (Commit! world mr_sink))\n"
        "(return world)\n"
        ")";

    Y_UNIT_TEST(ParseAstTest) {
        TAstParseResult res = ParseAst(TEST_PROGRAM);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Root->IsList());
        UNIT_ASSERT(res.Issues.Empty());
    }

    Y_UNIT_TEST(ParseAstTestPerf) {
#ifdef WITH_VALGRIND
        const ui32 n = 1000;
#else
        const ui32 n = NSan::PlainOrUnderSanitizer(100000, 1000);
#endif
        auto t1 = TInstant::Now();
        for (ui32 i = 0; i < n; ++i) {
            TAstParseResult res = ParseAst(TEST_PROGRAM);
            UNIT_ASSERT(res.IsOk());
            UNIT_ASSERT(res.Root->IsList());
            UNIT_ASSERT(res.Issues.Empty());
        }
        auto t2 = TInstant::Now();
        Cout << t2 - t1 << Endl;
    }

    Y_UNIT_TEST(PrintAstTest) {
        TAstParseResult ast = ParseAst(TEST_PROGRAM);
        UNIT_ASSERT(ast.IsOk());

        TString printedProgram = ast.Root->ToString();
        UNIT_ASSERT(printedProgram.find('\n') == TString::npos);

        TAstParseResult parsedAst = ParseAst(printedProgram);
        UNIT_ASSERT(parsedAst.IsOk());
    }

    Y_UNIT_TEST(PrettyPrintAst) {
        const ui32 testFlags[] = {
            TAstPrintFlags::Default,
            TAstPrintFlags::PerLine,
            //TAstPrintFlags::ShortQuote, //-- generates invalid AST
            TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote
        };

        TAstParseResult ast = ParseAst(TEST_PROGRAM);
        UNIT_ASSERT(ast.IsOk());

        for (ui32 i = 0; i < Y_ARRAY_SIZE(testFlags); ++i) {
            ui32 prettyFlags = testFlags[i];

            TString printedProgram1 = ast.Root->ToString(prettyFlags);
            TAstParseResult parsedAst = ParseAst(printedProgram1);
            UNIT_ASSERT(parsedAst.IsOk());

            TString printedProgram2 = parsedAst.Root->ToString(prettyFlags);
            UNIT_ASSERT_STRINGS_EQUAL(printedProgram1, printedProgram2);
        }
    }

    Y_UNIT_TEST(AnnotatedAstPrint) {
        TMemoryPool pool(4096);
        TAstParseResult ast = ParseAst(TEST_PROGRAM, &pool);
        UNIT_ASSERT(ast.IsOk());

        TAstParseResult astWithPositions;
        astWithPositions.Root = AnnotatePositions(*ast.Root, pool);
        UNIT_ASSERT(!!astWithPositions.Root);

        TString sAnn = astWithPositions.Root->ToString();
        UNIT_ASSERT(false == sAnn.empty());

        TAstParseResult annRes = ParseAst(sAnn);
        UNIT_ASSERT(annRes.IsOk());

        TAstParseResult removedAnn;
        removedAnn.Root = RemoveAnnotations(*annRes.Root, pool);
        UNIT_ASSERT(!!removedAnn.Root);

        TString strOriginal = ast.Root->ToString();
        TString strAnnRemoved = removedAnn.Root->ToString();
        UNIT_ASSERT_VALUES_EQUAL(strOriginal, strAnnRemoved);

        astWithPositions.Root->GetChild(0)->SetContent("100:100", pool);

        TAstParseResult appliedPositionsAnn;
        appliedPositionsAnn.Root = ApplyPositionAnnotations(*astWithPositions.Root, 0, pool);
        UNIT_ASSERT(appliedPositionsAnn.Root);

        TAstParseResult removedAnn2;
        removedAnn2.Root = RemoveAnnotations(*appliedPositionsAnn.Root, pool);
        UNIT_ASSERT(removedAnn2.Root);
        UNIT_ASSERT_VALUES_EQUAL(removedAnn2.Root->GetPosition().Row, 100);
    }

    template <typename TCharType>
    void TestGoodArbitraryAtom(
                const TString& program,
                const TBasicStringBuf<TCharType>& expectedValue)
    {
        TAstParseResult ast = ParseAst(program);
        UNIT_ASSERT(ast.IsOk());
        UNIT_ASSERT_VALUES_EQUAL(ast.Root->GetChildrenCount(), 1);

        TAstNode* atom = ast.Root->GetChild(0);
        UNIT_ASSERT(atom->IsAtom());
        UNIT_ASSERT_STRINGS_EQUAL_C(
                    atom->GetContent(),
                    TString((char*)expectedValue.data(), expectedValue.size()),
                    program);
    }

    Y_UNIT_TEST(GoodArbitraryAtom) {
        TestGoodArbitraryAtom("(\"\")", TStringBuf());
        TestGoodArbitraryAtom("(\" 1 a 3 b \")", TStringBuf(" 1 a 3 b "));

        ui8 expectedHex[] = { 0xab, 'c', 'd', 0x00 };
        TestGoodArbitraryAtom("(\"\\xabcd\")", TBasicStringBuf<ui8>(expectedHex));
        TestGoodArbitraryAtom("(\" \\x3d \")", TStringBuf(" \x3d "));

        ui8 expectedOctal[] = { 056, '7', '8', 0x00 };
        TestGoodArbitraryAtom("(\"\\05678\")", TBasicStringBuf<ui8>(expectedOctal));
        TestGoodArbitraryAtom("(\" \\056 \")", TStringBuf(" \056 "));
        TestGoodArbitraryAtom("(\" \\177 \")", TStringBuf(" \177 "));
        TestGoodArbitraryAtom("(\" \\377 \")", TStringBuf(" \377 "));
        TestGoodArbitraryAtom("(\" \\477 \")", TStringBuf(" 477 "));

        {
            ui8 expected1[] = { 0x01, 0x00 };
            TestGoodArbitraryAtom("(\"\\u0001\")", TBasicStringBuf<ui8>(expected1));

            ui8 expected2[] = { 0xE1, 0x88, 0xB4, 0x00 };
            TestGoodArbitraryAtom("(\"\\u1234\")", TBasicStringBuf<ui8>(expected2));

            ui8 expected3[] = { 0xef, 0xbf, 0xbf, 0x00 };
            TestGoodArbitraryAtom("(\"\\uffff\")", TBasicStringBuf<ui8>(expected3));
        }

        {
            ui8 expected1[] = { 0x01, 0x00 };
            TestGoodArbitraryAtom("(\"\\U00000001\")", TBasicStringBuf<ui8>(expected1));

            ui8 expected2[] = { 0xf4, 0x8f, 0xbf, 0xbf, 0x00 };
            TestGoodArbitraryAtom("(\"\\U0010ffff\")", TBasicStringBuf<ui8>(expected2));
        }

        TestGoodArbitraryAtom("(\"\\t\")", TStringBuf("\t"));
        TestGoodArbitraryAtom("(\"\\n\")", TStringBuf("\n"));
        TestGoodArbitraryAtom("(\"\\r\")", TStringBuf("\r"));
        TestGoodArbitraryAtom("(\"\\b\")", TStringBuf("\b"));
        TestGoodArbitraryAtom("(\"\\f\")", TStringBuf("\f"));
        TestGoodArbitraryAtom("(\"\\a\")", TStringBuf("\a"));
        TestGoodArbitraryAtom("(\"\\v\")", TStringBuf("\v"));
    }

    void TestBadArbitraryAtom(
                const TString& program,
                const TString& expectedError)
    {
        TAstParseResult ast = ParseAst(program);
        UNIT_ASSERT(false == ast.IsOk());
        UNIT_ASSERT(false == !!ast.Root);
        UNIT_ASSERT(false == ast.Issues.Empty());
        UNIT_ASSERT_STRINGS_EQUAL(ast.Issues.begin()->GetMessage(), expectedError);
    }

    Y_UNIT_TEST(BadArbitraryAtom) {
        TestBadArbitraryAtom("(a\")", "Unexpected \"");
        TestBadArbitraryAtom("(\"++++\"11111)", "Unexpected end of \"");
        TestBadArbitraryAtom("(\"\\", "Expected escape sequence");
        TestBadArbitraryAtom("(\"\\\")", "Unexpected end of atom");
        TestBadArbitraryAtom("(\"abc)", "Unexpected end of atom");

        TestBadArbitraryAtom("(\"\\018\")", "Invalid octal value");
        TestBadArbitraryAtom("(\"\\01\")", "Invalid octal value");
        TestBadArbitraryAtom("(\"\\378\")", "Invalid octal value");

        TestBadArbitraryAtom("(\"\\x1g\")", "Invalid hexadecimal value");
        TestBadArbitraryAtom("(\"\\xf\")", "Invalid hexadecimal value");

        TestBadArbitraryAtom("(\"\\u\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\u1\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\u12\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\u123\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\ughij\")", "Invalid unicode value");

        TestBadArbitraryAtom("(\"\\U\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U11\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U1122\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U112233\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\Ughijklmn\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U00110000\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U00123456\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U00200000\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\Uffffffff\")", "Invalid unicode value");

        // surrogate range
        TestBadArbitraryAtom("(\"\\ud800\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\udfff\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U0000d800\")", "Invalid unicode value");
        TestBadArbitraryAtom("(\"\\U0000dfff\")", "Invalid unicode value");

        TestBadArbitraryAtom("(x\"ag\")", "Invalid binary value");
        TestBadArbitraryAtom("(x\"abc\")", "Invalid binary value");
        TestBadArbitraryAtom("(x\"abcd)", "Invalid binary value");
        TestBadArbitraryAtom("(x\"abcd", "Unexpected end of atom");
    }

    void ParseAndPrint(const TString& program, const TString& expected) {
        TAstParseResult ast = ParseAst(program);
        UNIT_ASSERT_C(ast.IsOk(), program);

        TString result = ast.Root->ToString();
        UNIT_ASSERT_STRINGS_EQUAL_C(result, expected, program);
    }

    Y_UNIT_TEST(ArbitraryAtomEscaping) {
        ParseAndPrint(
                    "(\"\\t\\n\\r\\b\\a\\f\\v\")",
                    "(\"\\t\\n\\r\\b\\a\\f\\v\")");

        ParseAndPrint("(\"\\u1234\")", "(\"\\u1234\")");
        ParseAndPrint("(\"\\u1234abcd\")", "(\"\\u1234abcd\")");
        ParseAndPrint("(\"\\177\")", "(\"\\x7F\")");
        ParseAndPrint("(\"\\377\")", "(\"\\xFF\")");

        ParseAndPrint(
                    "(\"тестовая строка\")",
                    "(\"\\u0442\\u0435\\u0441\\u0442\\u043E\\u0432\\u0430"
                    "\\u044F \\u0441\\u0442\\u0440\\u043E\\u043A\\u0430\")");

        ParseAndPrint("(\"\")", "(\"\")");
    }

    Y_UNIT_TEST(BinaryAtom) {
        ParseAndPrint("(x\"abcdef\")", "(x\"ABCDEF\")");
        ParseAndPrint("(x\"aBcDeF\")", "(x\"ABCDEF\")");
        ParseAndPrint("(x)", "(x)");
        ParseAndPrint("(x x)", "(x x)");
        ParseAndPrint("(x\"\" x)", "(x\"\" x)");
        ParseAndPrint("(x\"ab12cd\" x)", "(x\"AB12CD\" x)");
    }

    void ParseAndAdaptPrint(const TString& program, const TString& expected) {
        TAstParseResult ast = ParseAst(program);
        UNIT_ASSERT_C(ast.IsOk(), program);

        TString result = ast.Root->ToString(
                    TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine |
                    TAstPrintFlags::AdaptArbitraryContent);

        RemoveAll(result, '\n'); // for simplify expected string
        UNIT_ASSERT_STRINGS_EQUAL_C(result, expected, program);
    }

    Y_UNIT_TEST(AdaptArbitraryAtom) {
        ParseAndAdaptPrint("(\"test\")", "(test)");
        ParseAndAdaptPrint("(\"another test\")", "(\"another test\")");
        ParseAndAdaptPrint("(\"braces(in)test\")", "(\"braces(in)test\")");
        ParseAndAdaptPrint("(\"escaped\\u1234sequence\")", "(\"escaped\\u1234sequence\")");
        ParseAndAdaptPrint("(\"escaped\\x41sequence\")", "(escapedAsequence)");
        ParseAndAdaptPrint("(\"\")", "(\"\")");
    }

    void ParseError(const TString& program) {
        TAstParseResult ast = ParseAst(program);
        UNIT_ASSERT_C(!ast.IsOk(), program);
    }

    Y_UNIT_TEST(MultilineAtomTrivial) {
        TStringStream s;
        for (ui32 i = 4; i < 13; ++i) {
            TStringStream prog;
            prog << "(";
            for (ui32 j = 0; j < i; ++j) {
                prog << "@";
            }
            prog << ")";
            TAstParseResult ast = ParseAst(prog.Str());
            s << prog.Str() << " --> ";
            if (ast.IsOk()) {
                UNIT_ASSERT_VALUES_EQUAL(ast.Root->GetChildrenCount(), 1);

                TAstNode* atom = ast.Root->GetChild(0);
                UNIT_ASSERT(atom->IsAtom());
                UNIT_ASSERT(atom->GetFlags() & TNodeFlags::MultilineContent);
                s << "'" << atom->GetContent() << "'" << Endl;
            } else {
                s << "Error" << Endl;
            }
        }
        //~ Cerr << s.Str() << Endl;
        UNIT_ASSERT_NO_DIFF(
            "(@@@@) --> ''\n"
            "(@@@@@) --> '@'\n"
            "(@@@@@@) --> Error\n"
            "(@@@@@@@) --> Error\n"
            "(@@@@@@@@) --> '@@'\n"
            "(@@@@@@@@@) --> '@@@'\n"
            "(@@@@@@@@@@) --> Error\n"
            "(@@@@@@@@@@@) --> Error\n"
            "(@@@@@@@@@@@@) --> '@@@@'\n",
            s.Str()
        );
    }

    Y_UNIT_TEST(MultilineAtom) {
        TString s1 = "(@@multi \n"
                   "line \n"
                   "string@@)";
        ParseAndPrint(s1, s1);

        TString s2 = "(@@multi \n"
                   "l@ine \n"
                   "string@@)";
        ParseAndPrint(s2, s2);

        TString s3 = "(@@multi \n"
                   "l@@@ine \n"
                   "string@@)";
        ParseError(s3);

        TString s4 = "(@@multi \n"
                    "l@@@@ine \n"
                    "string@@)";
        ParseAndPrint(s4, s4);

        TString s5 = "(@@\n"
                     "one@\n"
                     "two@@@@\n"
                     "four@@@@@@@@\n"
                     "@@@@two\n"
                     "@one\n"
                     "@@)";

        TAstParseResult ast = ParseAst(s5);
        UNIT_ASSERT(ast.IsOk());
        UNIT_ASSERT_VALUES_EQUAL(ast.Root->GetChildrenCount(), 1);

        TAstNode* atom = ast.Root->GetChild(0);
        UNIT_ASSERT(atom->IsAtom());
        UNIT_ASSERT(atom->GetFlags() & TNodeFlags::MultilineContent);

        TString expected = "\n"
                          "one@\n"
                          "two@@\n"
                          "four@@@@\n"
                          "@@two\n"
                          "@one\n";
        UNIT_ASSERT_STRINGS_EQUAL(atom->GetContent(), expected);

        TString printResult = ast.Root->ToString();
        UNIT_ASSERT_STRINGS_EQUAL(s5, printResult);
    }

    Y_UNIT_TEST(UnicodePrettyPrint) {
        ParseAndAdaptPrint("(\"абв αβγ ﬡ\")", "(\"\\u0430\\u0431\\u0432 \\u03B1\\u03B2\\u03B3 \\uFB21\")");
    }

    Y_UNIT_TEST(SerializeQuotedEmptyAtom) {
        TMemoryPool pool(1024);
        TPosition pos(1, 1);
        TAstNode* empty = TAstNode::Quote(pos, pool, TAstNode::NewAtom(pos, "", pool));
        TString expected = "'\"\"";

        UNIT_ASSERT_STRINGS_EQUAL(empty->ToString(), expected);

        TString pretty = empty->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine |
                                         TAstPrintFlags::AdaptArbitraryContent);
        RemoveAll(pretty, '\n');
        UNIT_ASSERT_EQUAL(pretty, expected);

        pretty = empty->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine);
        RemoveAll(pretty, '\n');
        UNIT_ASSERT_EQUAL(pretty, expected);
    }
}

} // namespace NYql
