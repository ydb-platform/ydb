#include "yql_expr.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/hex.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TCompileYqlExpr) {

    static TAstParseResult ParseAstWithCheck(const TStringBuf& s) {
        TAstParseResult res = ParseAst(s);
        res.Issues.PrintTo(Cout);
        UNIT_ASSERT(res.IsOk());
        return res;
    }

    static void CompileExprWithCheck(TAstNode& root, TExprNode::TPtr& exprRoot, TExprContext& exprCtx, ui32 typeAnnotationIndex = Max<ui32>()) {
        const bool success = CompileExpr(root, exprRoot, exprCtx, nullptr, nullptr, typeAnnotationIndex != Max<ui32>(), typeAnnotationIndex);
        exprCtx.IssueManager.GetIssues().PrintTo(Cout);

        UNIT_ASSERT(success);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->GetState(), typeAnnotationIndex != Max<ui32>() ? TExprNode::EState::TypeComplete : TExprNode::EState::Initial);
    }

    static void CompileExprWithCheck(TAstNode& root, TLibraryCohesion& cohesion, TExprContext& exprCtx) {
        const bool success = CompileExpr(root, cohesion, exprCtx);
        exprCtx.IssueManager.GetIssues().PrintTo(Cout);

        UNIT_ASSERT(success);
    }

    static bool ParseAndCompile(const TString& program) {
        TAstParseResult astRes = ParseAstWithCheck(program);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        bool result = CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr);
        exprCtx.IssueManager.GetIssues().PrintTo(Cout);
        return result;
    }

    Y_UNIT_TEST(TestNoReturn1) {
        auto s = "(\n"
            ")\n";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestNoReturn2) {
        auto s = "(\n"
            "(let x 'y)\n"
            ")\n";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestExportInsteadOfReturn) {
        const auto s =
            "# library\n"
            "(\n"
            "  (let sqr (lambda '(x) (* x x)))\n"
            "  (export sqr)\n"
            ")\n"
        ;
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestLeftAfterReturn) {
        auto s = "(\n"
            "(return 'x)\n"
            "(let x 'y)\n"
            ")\n";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestReturn) {
        auto s = "(\n"
            "(return world)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::World);
    }

    Y_UNIT_TEST(TestExport) {
        auto s = "(\n"
            "(let X 'Y)\n"
            "(let ex '42)\n"
            "(export ex)\n"
            "(export X)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TLibraryCohesion cohesion;
        CompileExprWithCheck(*astRes.Root, cohesion, exprCtx);
        auto& exports = cohesion.Exports.Symbols(exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(2U, exports.size());
        UNIT_ASSERT_VALUES_EQUAL("42", exports["ex"]->Content());
        UNIT_ASSERT_VALUES_EQUAL("Y", exports["X"]->Content());
    }

    Y_UNIT_TEST(TestEmptyLib) {
        auto s = "(\n"
            "(let X 'Y)\n"
            "(let ex '42)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TLibraryCohesion cohesion;
        CompileExprWithCheck(*astRes.Root, cohesion, exprCtx);
        UNIT_ASSERT(cohesion.Exports.Symbols().empty());
        UNIT_ASSERT(cohesion.Imports.empty());
    }

    Y_UNIT_TEST(TestArbitraryAtom) {
        auto s = "(\n"
            "(let x '\"\\x01\\x23\\x45\\x67\\x89\\xAB\\xCD\\xEF\")"
            "(return x)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::Atom);
        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(exprRoot->Content()), "0123456789ABCDEF");
        UNIT_ASSERT(exprRoot->Flags() & TNodeFlags::ArbitraryContent);

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
        TAstNode* xValue = ast.Root->GetChild(0)->GetChild(1)->GetChild(1);
        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(TString(xValue->GetContent())), "0123456789ABCDEF");
        UNIT_ASSERT(xValue->GetFlags() & TNodeFlags::ArbitraryContent);
    }

    Y_UNIT_TEST(TestBinaryAtom) {
        auto s = "(\n"
            "(let x 'x\"FEDCBA9876543210\")"
            "(return x)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::Atom);
        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(exprRoot->Content()), "FEDCBA9876543210");
        UNIT_ASSERT(exprRoot->Flags() & TNodeFlags::BinaryContent);

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
        TAstNode* xValue = ast.Root->GetChild(0)->GetChild(2)->GetChild(1);
        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(TString(xValue->GetContent())), "FEDCBA9876543210");
        UNIT_ASSERT(xValue->GetFlags() & TNodeFlags::BinaryContent);
    }

    Y_UNIT_TEST(TestLet) {
        auto s = "(\n"
        "(let x 'y)\n"
        "(return x)\n"
        ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Content(), "y");
    }

    Y_UNIT_TEST(TestComplexQuote) {
        auto s = "(\n"
            "(let x 'a)\n"
            "(let y 'b)\n"
            "(let z (quote (x y)))\n"
            "(return z)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::List);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Content(), "a");
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(1)->Content(), "b");
    }

    Y_UNIT_TEST(TestEmptyReturn) {
        auto s = "(\n"
            "(return)\n"
            ")\n";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestManyReturn) {
        auto s = "(\n"
            "(return world world)\n"
            ")\n";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestUnknownFunction) {
        auto s = "(\n"
            "(let a '2)\n"
            "(let x (+ a '3))\n"
            "(return x)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Content(), "+");
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Content(), "2");
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(1)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(1)->Content(), "3");
    }

    Y_UNIT_TEST(TestReturnTwice) {
        auto s = "(\n"
            "(return)\n"
            "(return)\n"
            ")\n";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestDeclareNonTop) {
        const auto s = R"(
            (
            (let $1 (block '(
                (declare $param (DataType 'Uint32))
                (return $param)
            )))
            (return $1)
            )
        )";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestDeclareHideLet) {
        const auto s = R"(
            (
            (let $name (Uint32 '10))
            (declare $name (DataType 'Uint32))
            (return $name)
            )
        )";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestDeclareBadName) {
        const auto s = R"(
            (
            (declare $15 (DataType 'Uint32))
            (return $15)
            )
        )";
        UNIT_ASSERT(false == ParseAndCompile(s));
    }

    Y_UNIT_TEST(TestLetHideDeclare) {
        const auto s = R"(
            (
            (declare $name (DataType 'Uint32))
            (let $name (Uint32 '10))
            (return $name)
            )
        )";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Content(), "Uint32");
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Content(), "10");
    }

    Y_UNIT_TEST(TestDeclare) {
        const auto s = R"(
            (
            (declare $param (DataType 'Uint32))
            (return $param)
            )
        )";

        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->ChildrenSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Type(), TExprNode::Atom);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Content(), "$param");
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(1)->Type(), TExprNode::Callable);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(1)->Content(), "DataType");
    }
}

Y_UNIT_TEST_SUITE(TCompareExprTrees) {
    void CompileAndCompare(const TString& one, const TString& two, const std::pair<TPosition, TPosition> *const diffPositions = nullptr) {
        const auto progOne(ParseAst(one)), progTwo(ParseAst(two));
        UNIT_ASSERT(progOne.IsOk() && progTwo.IsOk());

        TExprContext ctxOne, ctxTwo;
        TExprNode::TPtr rootOne, rootTwo;

        UNIT_ASSERT(CompileExpr(*progOne.Root, rootOne, ctxOne, nullptr, nullptr));
        UNIT_ASSERT(CompileExpr(*progTwo.Root, rootTwo, ctxTwo, nullptr, nullptr));

        const TExprNode* diffOne = rootOne.Get();
        const TExprNode* diffTwo = rootTwo.Get();

        if (diffPositions) {
            UNIT_ASSERT(!CompareExprTrees(diffOne, diffTwo));
            UNIT_ASSERT_EQUAL(ctxOne.GetPosition(diffOne->Pos()), diffPositions->first);
            UNIT_ASSERT_EQUAL(ctxTwo.GetPosition(diffTwo->Pos()), diffPositions->second);
        } else
            UNIT_ASSERT(CompareExprTrees(diffOne, diffTwo));
    }

    Y_UNIT_TEST(BigGoodCompare) {
        const auto one = R"(
            (
            (let $1 world)
            (let $2 (DataSource '"yt" '"plato"))
            (let $3 (MrTableRange '"statbox/yql-log" '"2016-05-25" '"2016-06-01"))
            (let $4 '('table $3))
            (let $5 (Key $4))
            (let $6 '('"method" '"uri" '"login" '"user_agent" '"millis"))
            (let $7 '())
            (let $8 (Read! $1 $2 $5 $6 $7))
            (let $9 (Left! $8))
            (let $10 (DataSink 'result))
            (let $11 (Key))
            (let $12 (Right! $8))
            (let $13 (lambda '($111) (block '(
            (let $113 (Member $111 '"method"))
            (let $114 (String '"POST"))
            (let $115 (== $113 $114))
            (let $116 (Member $111 '"uri"))
            (let $117 (String '"/api/v2/operations"))
            (let $118 (== $116 $117))
            (let $119 (Udf '"String.HasPrefix"))
            (let $120 (Member $111 '"uri"))
            (let $121 (String '"/api/v2/tutorials/"))
            (let $122 (Apply $119 $120 $121))
            (let $123 (Or $118 $122))
            (let $124 (Udf '"String.HasPrefix"))
            (let $125 (Member $111 '"uri"))
            (let $126 (String '"/api/v2/queries/"))
            (let $127 (Apply $124 $125 $126))
            (let $128 (Or $123 $127))
            (let $129 (And $115 $128))
            (let $130 (Udf '"String.HasPrefix"))
            (let $131 (Member $111 '"uri"))
            (let $132 (String '"/api/v2/table_data_async"))
            (let $133 (Apply $130 $131 $132))
            (let $134 (Or $129 $133))
            (let $135 (Udf '"String.HasPrefix"))
            (let $136 (Member $111 '"login"))
            (let $137 (String '"robot-"))
            (let $138 (Apply $135 $136 $137))
            (let $139 (Not $138))
            (let $140 (And $134 $139))
            (let $141 (Bool 'false))
            (let $142 (Coalesce $140 $141))
            (return $142)
            ))))
            (let $14 (Filter $12 $13))
            (let $15 (lambda '($143) (block '(
            (let $145 (Struct))
            (let $146 (Member $143 '"login"))
            (let $147 (AddMember $145 '"login" $146))
            (let $148 (Udf '"String.HasPrefix"))
            (let $149 (Member $143 '"user_agent"))
            (let $150 (String '"YQL "))
            (let $151 (Apply $148 $149 $150))
            (let $152 (Bool 'false))
            (let $153 (Coalesce $151 $152))
            (let $154 (Udf '"String.SplitToList"))
            (let $155 (Member $143 '"user_agent"))
            (let $156 (String '" "))
            (let $157 (Apply $154 $155 $156))
            (let $158 (Int64 '"1"))
            (let $159 (SqlAccess 'dict $157 $158))
            (let $160 (String '"CLI"))
            (let $161 (== $159 $160))
            (let $162 (Bool 'false))
            (let $163 (Coalesce $161 $162))
            (let $164 (String '"CLI"))
            (let $165 (String '"API"))
            (let $166 (If $163 $164 $165))
            (let $167 (String '"Web UI"))
            (let $168 (If $153 $166 $167))
            (let $169 (AddMember $147 '"client_type" $168))
            (let $170 (Udf '"DateTime.ToDate"))
            (let $171 (Udf '"DateTime.StartOfWeek"))
            (let $172 (Udf '"DateTime.FromMilliSeconds"))
            (let $173 (Member $143 '"millis"))
            (let $174 (Cast $173 'Uint64))
            (let $175 (Apply $172 $174))
            (let $176 (Apply $171 $175))
            (let $177 (Apply $170 $176))
            (let $178 (String '""))
            (let $179 (Coalesce $177 $178))
            (let $180 (String '" - "))
            (let $181 (Concat $179 $180))
            (let $182 (Udf '"DateTime.ToDate"))
            (let $183 (Udf '"DateTime.StartOfWeek"))
            (let $184 (Udf '"DateTime.FromMilliSeconds"))
            (let $185 (Member $143 '"millis"))
            (let $186 (Cast $185 'Uint64))
            (let $187 (Apply $184 $186))
            (let $188 (Apply $183 $187))
            (let $189 (Udf '"DateTime.FromDays"))
            (let $190 (Int64 '"6"))
            (let $191 (Apply $189 $190))
            (let $192 (+ $188 $191))
            (let $193 (Apply $182 $192))
            (let $194 (String '""))
            (let $195 (Coalesce $193 $194))
            (let $196 (Concat $181 $195))
            (let $197 (AddMember $169 '"week" $196))
            (let $198 (AsList $197))
            (return $198)
            ))))
         )"
         R"(
            (let $16 (FlatMap $14 $15))
            (let $17 '('"client_type" '"week"))
            (let $18 (lambda '($199 $200) (block '(
            (let $202 (lambda '($205 $206 $207) (block '(
                (let $209 (ListItemType $205))
                (let $210 (lambda '($223) (block '(
                (let $212 (ListItemType $205))
                (let $213 (InstanceOf $212))
                (let $214 (Apply $206 $213))
                (let $215 (TypeOf $214))
                (let $216 (ListType $215))
                (let $217 (Apply $207 $216))
                (let $225 (NthArg '1 $217))
                (let $226 (Apply $206 $223))
                (let $227 (Apply $225 $226))
                (return $227)
                ))))
                (let $211 (lambda '($228 $229) (block '(
                (let $212 (ListItemType $205))
                (let $213 (InstanceOf $212))
                (let $214 (Apply $206 $213))
                (let $215 (TypeOf $214))
                (let $216 (ListType $215))
                (let $217 (Apply $207 $216))
                (let $231 (NthArg '2 $217))
                (let $232 (Apply $206 $228))
                (let $233 (Apply $231 $232 $229))
                (return $233)
                ))))
                (let $212 (ListItemType $205))
                (let $213 (InstanceOf $212))
                (let $214 (Apply $206 $213))
                (let $215 (TypeOf $214))
                (let $216 (ListType $215))
                (let $217 (Apply $207 $216))
                (let $218 (NthArg '3 $217))
                (let $219 (NthArg '4 $217))
                (let $220 (NthArg '5 $217))
                (let $221 (NthArg '6 $217))
                (let $222 (AggregationTraits $209 $210 $211 $218 $219 $220 $221))
                (return $222)
            ))))
            (let $203 (lambda '($234) (block '(
                (let $236 (ListItemType $234))
                (let $237 (lambda '($244) (block '(
                (let $246 (AggrCountInit $244))
                (return $246)
                ))))
                (let $238 (lambda '($247 $248) (block '(
                (let $250 (AggrCountUpdate $247 $248))
                (return $250)
                ))))
                (let $239 (lambda '($251) (block '(
                (return $251)
                ))))
                (let $240 (lambda '($253) (block '(
                (return $253)
                ))))
                (let $241 (lambda '($255 $256) (block '(
                (let $258 (+ $255 $256))
                (return $258)
                ))))
                (let $242 (lambda '($259) (block '(
                (return $259)
                ))))
                (let $243 (AggregationTraits $236 $237 $238 $239 $240 $241 $242))
                (return $243)
            ))))
            (let $204 (Apply $202 $199 $200 $203))
            (return $204)
            ))))
            (let $19 (TypeOf $16))
            (let $20 (ListItemType $19))
            (let $21 (StructMemberType $20 '"login"))
            (let $22 (ListType $21))
            (let $23 (lambda '($261) (block '(
            (return $261)
            ))))
            (let $24 (Apply $18 $22 $23))
            (let $25 '('Count1 $24 '"login"))
            (let $26 (TypeOf $16))
            (let $27 (lambda '($263) (block '(
            (let $265 (Void))
            (return $265)
            ))))
            (let $28 (Apply $18 $26 $27))
            (let $29 '('Count2 $28))
            (let $30 (TypeOf $16))
            (let $31 (lambda '($266) (block '(
            (let $268 (Void))
            (return $268)
            ))))
            (let $32 (Apply $18 $30 $31))
            (let $33 '('Count3 $32))
            (let $34 (TypeOf $16))
            (let $35 (ListItemType $34))
            (let $36 (StructMemberType $35 '"login"))
            (let $37 (ListType $36))
            (let $38 (lambda '($269) (block '(
            (return $269)
            ))))
            (let $39 (Apply $18 $37 $38))
            (let $40 '('Count4 $39 '"login"))
            (let $41 '($25 $29 $33 $40))
            (let $42 (Aggregate $16 $17 $41))
            (let $43 (lambda '($271) (block '(
            (let $273 (Struct))
            (let $274 (Member $271 '"week"))
            (let $275 (AddMember $273 '"week" $274))
            (let $276 (Member $271 '"client_type"))
            (let $277 (AddMember $275 '"client_type" $276))
            (let $278 (Member $271 'Count1))
            (let $279 (AddMember $277 '"users_count" $278))
            (let $280 (Member $271 'Count2))
            (let $281 (AddMember $279 '"operations_count" $280))
            (let $282 (Member $271 'Count3))
            (let $283 (Member $271 'Count4))
            (let $284 (/ $282 $283))
            (let $285 (AddMember $281 '"operations_per_user" $284))
            (let $286 (AsList $285))
            (return $286)
            ))))
            (let $44 (FlatMap $42 $43))
            (let $45 (Bool 'false))
            (let $46 (Bool 'false))
            (let $47 '($45 $46))
            (let $48 (lambda '($287) (block '(
            (let $289 (Member $287 '"week"))
            (let $290 (Member $287 '"users_count"))
            (let $291 '($289 $290))
            (return $291)
            ))))
            (let $49 (Sort $44 $47 $48))
            (let $50 '('type))
            (let $51 '('autoref))
            (let $52 '('"week" '"client_type" '"users_count" '"operations_count" '"operations_per_user"))
            (let $53 '('columns $52))
            (let $54 '($50 $51 $53))
            (let $55 (Write! $9 $10 $11 $49 $54))
            (let $56 (Commit! $55 $10))
            (let $57 (DataSource '"yt" '"plato"))
            (let $58 (MrTableRange '"statbox/yql-log" '"2016-05-25" '"2016-06-01"))
            (let $59 '('table $58))
            (let $60 (Key $59))
            (let $61 '('"method" '"uri" '"login" '"user_agent" '"millis"))
            (let $62 '())
            (let $63 (Read! $56 $57 $60 $61 $62))
            (let $64 (Left! $63))
            (let $65 (DataSink 'result))
            (let $66 (Key))
            (let $67 (Right! $63))
            (let $68 (lambda '($292) (block '(
            (let $294 (Member $292 '"method"))
            (let $295 (String '"POST"))
            (let $296 (== $294 $295))
            (let $297 (Member $292 '"uri"))
            (let $298 (String '"/api/v2/operations"))
            (let $299 (== $297 $298))
            (let $300 (Udf '"String.HasPrefix"))
            (let $301 (Member $292 '"uri"))
            (let $302 (String '"/api/v2/tutorials/"))
            (let $303 (Apply $300 $301 $302))
            (let $304 (Or $299 $303))
            (let $305 (Udf '"String.HasPrefix"))
            (let $306 (Member $292 '"uri"))
            (let $307 (String '"/api/v2/queries/"))
            (let $308 (Apply $305 $306 $307))
            (let $309 (Or $304 $308))
            (let $310 (And $296 $309))
            (let $311 (Udf '"String.HasPrefix"))
            (let $312 (Member $292 '"uri"))
            (let $313 (String '"/api/v2/table_data_async"))
            (let $314 (Apply $311 $312 $313))
            (let $315 (Or $310 $314))
            (let $316 (Udf '"String.HasPrefix"))
            (let $317 (Member $292 '"login"))
            (let $318 (String '"robot-"))
            (let $319 (Apply $316 $317 $318))
            (let $320 (Not $319))
            (let $321 (And $315 $320))
            (let $322 (Bool 'false))
            (let $323 (Coalesce $321 $322))
            (return $323)
            ))))
            (let $69 (Filter $67 $68))
            (let $70 (lambda '($324) (block '(
            (let $326 (Struct))
            (let $327 (Member $324 '"login"))
            (let $328 (AddMember $326 '"login" $327))
            (let $329 (Udf '"String.HasPrefix"))
            (let $330 (Member $324 '"user_agent"))
            (let $331 (String '"YQL "))
            (let $332 (Apply $329 $330 $331))
            (let $333 (Bool 'false))
            (let $334 (Coalesce $332 $333))
            (let $335 (Udf '"String.SplitToList"))
            (let $336 (Member $324 '"user_agent"))
            (let $337 (String '" "))
            (let $338 (Apply $335 $336 $337))
            (let $339 (Int64 '"1"))
            (let $340 (SqlAccess 'dict $338 $339))
            (let $341 (String '"CLI"))
            (let $342 (== $340 $341))
            (let $343 (Bool 'false))
            (let $344 (Coalesce $342 $343))
            (let $345 (String '"CLI"))
            (let $346 (String '"API"))
            (let $347 (If $344 $345 $346))
            (let $348 (String '"Web UI"))
            (let $349 (If $334 $347 $348))
            (let $350 (AddMember $328 '"client_type" $349))
            (let $351 (Udf '"DateTime.ToDate"))
            (let $352 (Udf '"DateTime.StartOfWeek"))
            (let $353 (Udf '"DateTime.FromMilliSeconds"))
            (let $354 (Member $324 '"millis"))
            (let $355 (Cast $354 'Uint64))
            (let $356 (Apply $353 $355))
            (let $357 (Apply $352 $356))
            (let $358 (Apply $351 $357))
            (let $359 (String '""))
            (let $360 (Coalesce $358 $359))
            (let $361 (String '" - "))
            (let $362 (Concat $360 $361))
            (let $363 (Udf '"DateTime.ToDate"))
            (let $364 (Udf '"DateTime.StartOfWeek"))
            (let $365 (Udf '"DateTime.FromMilliSeconds"))
            (let $366 (Member $324 '"millis"))
            (let $367 (Cast $366 'Uint64))
            (let $368 (Apply $365 $367))
            (let $369 (Apply $364 $368))
            (let $370 (Udf '"DateTime.FromDays"))
            (let $371 (Int64 '"6"))
            (let $372 (Apply $370 $371))
            (let $373 (+ $369 $372))
            (let $374 (Apply $363 $373))
            (let $375 (String '""))
            (let $376 (Coalesce $374 $375))
            (let $377 (Concat $362 $376))
            (let $378 (AddMember $350 '"week" $377))
            (let $379 (AsList $378))
            (return $379)
            ))))
            (let $71 (FlatMap $69 $70))
            (let $72 '('"week"))
            (let $73 (TypeOf $71))
            (let $74 (ListItemType $73))
            (let $75 (StructMemberType $74 '"login"))
            (let $76 (ListType $75))
            (let $77 (lambda '($380) (block '(
            (return $380)
            ))))
            (let $78 (Apply $18 $76 $77))
            (let $79 '('Count6 $78 '"login"))
            (let $80 (TypeOf $71))
            (let $81 (lambda '($382) (block '(
            (let $384 (Void))
            (return $384)
            ))))
            (let $82 (Apply $18 $80 $81))
            (let $83 '('Count7 $82))
            (let $84 (TypeOf $71))
            (let $85 (lambda '($385) (block '(
            (let $387 (Void))
            (return $387)
            ))))
            (let $86 (Apply $18 $84 $85))
            (let $87 '('Count8 $86))
            (let $88 (TypeOf $71))
            (let $89 (ListItemType $88))
            (let $90 (StructMemberType $89 '"login"))
            (let $91 (ListType $90))
            (let $92 (lambda '($388) (block '(
            (return $388)
            ))))
            (let $93 (Apply $18 $91 $92))
            (let $94 '('Count9 $93 '"login"))
            (let $95 '($79 $83 $87 $94))
            (let $96 (Aggregate $71 $72 $95))
            (let $97 (lambda '($390) (block '(
            (let $392 (Struct))
            (let $393 (Member $390 '"week"))
            (let $394 (AddMember $392 '"week" $393))
            (let $395 (Member $390 'Count6))
            (let $396 (AddMember $394 '"users_count" $395))
            (let $397 (Member $390 'Count7))
            (let $398 (AddMember $396 '"operations_count" $397))
            (let $399 (Member $390 'Count8))
            (let $400 (Member $390 'Count9))
            (let $401 (/ $399 $400))
            (let $402 (AddMember $398 '"operations_per_user" $401))
            (let $403 (AsList $402))
            (return $403)
            ))))
            (let $98 (FlatMap $96 $97))
            (let $99 (Bool 'false))
            (let $100 (lambda '($404) (block '(
            (let $406 (Member $404 '"week"))
            (return $406)
            ))))
            (let $101 (Sort $98 $99 $100))
            (let $102 '('type))
            (let $103 '('autoref))
            (let $104 '('"week" '"users_count" '"operations_count" '"operations_per_user"))
            (let $105 '('columns $104))
            (let $106 '($102 $103 $105))
            (let $107 (Write! $64 $65 $66 $101 $106))
            (let $108 (Commit! $107 $65))
            (let $109 (DataSink '"yt" '"plato"))
            (let $110 (Commit! $108 $109))
            (return $110)
            )
        )";

        const auto two = R"(
            (
            (let $1 (MrTableRange '"statbox/yql-log" '"2016-05-25" '"2016-06-01"))
            (let $2 '('"method" '"uri" '"login" '"user_agent" '"millis"))
            (let $3 (Read! world (DataSource '"yt" '"plato") (Key '('table $1)) $2 '()))
            (let $4 (DataSink 'result))
            (let $5 (FlatMap (Filter (Right! $3) (lambda '($36) (block '(
            (let $37 (Apply (Udf '"String.HasPrefix") (Member $36 '"uri") (String '"/api/v2/tutorials/")))
            (let $38 (Apply (Udf '"String.HasPrefix") (Member $36 '"uri") (String '"/api/v2/queries/")))
            (let $39 (Apply (Udf '"String.HasPrefix") (Member $36 '"uri") (String '"/api/v2/table_data_async")))
            (let $40 (Apply (Udf '"String.HasPrefix") (Member $36 '"login") (String '"robot-")))
            (return (Coalesce (And (Or (And (== (Member $36 '"method") (String '"POST")) (Or (Or (== (Member $36 '"uri") (String '"/api/v2/operations")) $37) $38)) $39) (Not $40)) (Bool 'false)))
            )))) (lambda '($41) (block '(
            (let $42 (AddMember (Struct) '"login" (Member $41 '"login")))
            (let $43 (Apply (Udf '"String.HasPrefix") (Member $41 '"user_agent") (String '"YQL ")))
            (let $44 (Apply (Udf '"String.SplitToList") (Member $41 '"user_agent") (String '" ")))
            (let $45 (SqlAccess 'dict $44 (Int64 '"1")))
            (let $46 (If (Coalesce (== $45 (String '"CLI")) (Bool 'false)) (String '"CLI") (String '"API")))
            (let $47 (If (Coalesce $43 (Bool 'false)) $46 (String '"Web UI")))
            (let $48 (AddMember $42 '"client_type" $47))
            (let $49 (AddMember $48 '"week" (Concat (Concat (Coalesce (Apply (Udf '"DateTime.ToDate") (Apply (Udf '"DateTime.StartOfWeek") (Apply (Udf '"DateTime.FromMilliSeconds") (Cast (Member $41 '"millis") 'Uint64)))) (String '"")) (String '" - ")) (Coalesce (Apply (Udf '"DateTime.ToDate") (+ (Apply (Udf '"DateTime.StartOfWeek") (Apply (Udf '"DateTime.FromMilliSeconds") (Cast (Member $41 '"millis") 'Uint64))) (Apply (Udf '"DateTime.FromDays") (Int64 '"6")))) (String '"")))))
            (return (AsList $49))
            )))))
            (let $6 (lambda '($50 $51) (block '(
            (let $52 (Apply (lambda '($53 $54 $55) (block '(
                (let $57 (Apply $55 (ListType (TypeOf (Apply $54 (InstanceOf (ListItemType $53)))))))
                (let $58 (AggregationTraits (ListItemType $53) (lambda '($59) (block '(
                (let $57 (Apply $55 (ListType (TypeOf (Apply $54 (InstanceOf (ListItemType $53)))))))
                (return (Apply (NthArg '1 $57) (Apply $54 $59)))
                ))) (lambda '($60 $61) (block '(
                (let $57 (Apply $55 (ListType (TypeOf (Apply $54 (InstanceOf (ListItemType $53)))))))
                (let $62 (Apply (NthArg '2 $57) (Apply $54 $60) $61))
                (return $62)
                ))) (NthArg '3 $57) (NthArg '4 $57) (NthArg '5 $57) (NthArg '6 $57)))
                (return $58)
            ))) $50 $51 (lambda '($63) (block '(
                (let $64 (AggregationTraits (ListItemType $63) (lambda '($65) (AggrCountInit $65)) (lambda '($66 $67) (AggrCountUpdate $66 $67)) (lambda '($68) $68) (lambda '($69) $69) (lambda '($70 $71) (+ $70 $71)) (lambda '($72) $72)))
                (return $64)
            )))))
            (return $52)
            ))))
            (let $7 (Apply $6 (ListType (StructMemberType (ListItemType (TypeOf $5)) '"login")) (lambda '($73) $73)))
            (let $8 '('Count1 $7 '"login"))
            (let $9 (Apply $6 (TypeOf $5) (lambda '($74) (Void))))
            (let $10 (Apply $6 (TypeOf $5) (lambda '($75) (Void))))
            (let $11 (Apply $6 (ListType (StructMemberType (ListItemType (TypeOf $5)) '"login")) (lambda '($76) $76)))
            (let $12 '('Count4 $11 '"login"))
            (let $13 '($8 '('Count2 $9) '('Count3 $10) $12))
            (let $14 (Aggregate $5 '('"client_type" '"week") $13))
            (let $15 (Sort (FlatMap $14 (lambda '($77) (block '(
            (let $78 (AddMember (Struct) '"week" (Member $77 '"week")))
            (let $79 (AddMember $78 '"client_type" (Member $77 '"client_type")))
            (let $80 (AddMember $79 '"users_count" (Member $77 'Count1)))
            (let $81 (AddMember $80 '"operations_count" (Member $77 'Count2)))
            (let $82 (AddMember $81 '"operations_per_user" (/ (Member $77 'Count3) (Member $77 'Count4))))
            (return (AsList $82))
            )))) '((Bool 'false) (Bool 'false)) (lambda '($83) '((Member $83 '"week") (Member $83 '"users_count")))))
            (let $16 '('"week" '"client_type" '"users_count" '"operations_count" '"operations_per_user"))
            (let $17 '('('type) '('autoref) '('columns $16)))
            (let $18 (Write! (Left! $3) $4 (Key) $15 $17))
            (let $19 (MrTableRange '"statbox/yql-log" '"2016-05-25" '"2016-06-01"))
            (let $20 '('"method" '"uri" '"login" '"user_agent" '"millis"))
            (let $21 (Read! (Commit! $18 $4) (DataSource '"yt" '"plato") (Key '('table $19)) $20 '()))
            (let $22 (DataSink 'result))
            (let $23 (FlatMap (Filter (Right! $21) (lambda '($84) (block '(
            (let $85 (Apply (Udf '"String.HasPrefix") (Member $84 '"uri") (String '"/api/v2/tutorials/")))
            (let $86 (Apply (Udf '"String.HasPrefix") (Member $84 '"uri") (String '"/api/v2/queries/")))
            (let $87 (Apply (Udf '"String.HasPrefix") (Member $84 '"uri") (String '"/api/v2/table_data_async")))
            (let $88 (Apply (Udf '"String.HasPrefix") (Member $84 '"login") (String '"robot-")))
            (return (Coalesce (And (Or (And (== (Member $84 '"method") (String '"POST")) (Or (Or (== (Member $84 '"uri") (String '"/api/v2/operations")) $85) $86)) $87) (Not $88)) (Bool 'false)))
            )))) (lambda '($89) (block '(
            (let $90 (AddMember (Struct) '"login" (Member $89 '"login")))
            (let $91 (Apply (Udf '"String.HasPrefix") (Member $89 '"user_agent") (String '"YQL ")))
            (let $92 (Apply (Udf '"String.SplitToList") (Member $89 '"user_agent") (String '" ")))
            (let $93 (SqlAccess 'dict $92 (Int64 '"1")))
            (let $94 (If (Coalesce (== $93 (String '"CLI")) (Bool 'false)) (String '"CLI") (String '"API")))
            (let $95 (If (Coalesce $91 (Bool 'false)) $94 (String '"Web UI")))
            (let $96 (AddMember $90 '"client_type" $95))
            (let $97 (AddMember $96 '"week" (Concat (Concat (Coalesce (Apply (Udf '"DateTime.ToDate") (Apply (Udf '"DateTime.StartOfWeek") (Apply (Udf '"DateTime.FromMilliSeconds") (Cast (Member $89 '"millis") 'Uint64)))) (String '"")) (String '" - ")) (Coalesce (Apply (Udf '"DateTime.ToDate") (+ (Apply (Udf '"DateTime.StartOfWeek") (Apply (Udf '"DateTime.FromMilliSeconds") (Cast (Member $89 '"millis") 'Uint64))) (Apply (Udf '"DateTime.FromDays") (Int64 '"6")))) (String '"")))))
            (return (AsList $97))
            )))))
            (let $24 (Apply $6 (ListType (StructMemberType (ListItemType (TypeOf $23)) '"login")) (lambda '($98) $98)))
            (let $25 '('Count6 $24 '"login"))
            (let $26 (Apply $6 (TypeOf $23) (lambda '($99) (Void))))
            (let $27 (Apply $6 (TypeOf $23) (lambda '($100) (Void))))
            (let $28 (Apply $6 (ListType (StructMemberType (ListItemType (TypeOf $23)) '"login")) (lambda '($101) $101)))
            (let $29 '('Count9 $28 '"login"))
            (let $30 '($25 '('Count7 $26) '('Count8 $27) $29))
            (let $31 (Aggregate $23 '('"week") $30))
            (let $32 (Sort (FlatMap $31 (lambda '($102) (block '(
            (let $103 (AddMember (Struct) '"week" (Member $102 '"week")))
            (let $104 (AddMember $103 '"users_count" (Member $102 'Count6)))
            (let $105 (AddMember $104 '"operations_count" (Member $102 'Count7)))
            (let $106 (AddMember $105 '"operations_per_user" (/ (Member $102 'Count8) (Member $102 'Count9))))
            (return (AsList $106))
            )))) (Bool 'false) (lambda '($107) (Member $107 '"week"))))
            (let $33 '('"week" '"users_count" '"operations_count" '"operations_per_user"))
            (let $34 '('('type) '('autoref) '('columns $33)))
            (let $35 (Write! (Left! $21) $22 (Key) $32 $34))
            (return (Commit! (Commit! $35 $22) (DataSink '"yt" '"plato")))
            )
        )";

        CompileAndCompare(one, two);
    }

    Y_UNIT_TEST(DiffrentAtoms) {
        const auto one = "((return (+ '4 (- '3 '2))))";
        const auto two = "((let x '3)\n(let y '1)\n(let z (- x y))\n(let r (+ '4 z))\n(return r))";

        const auto diff = std::make_pair(TPosition(23,1), TPosition(9,2));
        CompileAndCompare(one, two, &diff);
    }

    Y_UNIT_TEST(DiffrentLists) {
        const auto one = "((return '('7 '4 '('1 '3 '2))))";
        const auto two = "((let x '('1 '3))\n(let y '('7 '4 x))\n(return y))";

        const auto diff = std::make_pair(TPosition(20,1), TPosition(11,1));
        CompileAndCompare(one, two, &diff);
    }

    Y_UNIT_TEST(DiffrentCallables) {
        const auto one = "((return (- '4 (- '3 '2))))";
        const auto two = "((let x '3)\n(let y '2)\n(let z (- x y))\n(let r (+ '4 z))\n(return r))";

        const auto diff = std::make_pair(TPosition(11,1), TPosition(9,4));
        CompileAndCompare(one, two, &diff);
    }

    Y_UNIT_TEST(SwapArguments) {
        const auto one = "((let l (lambda '(x y) (+ x y)))\n(return (Apply l '7 '9)))";
        const auto two = "((return (Apply (lambda '(x y) (+ y x)) '7 '9)))";

        const auto diff = std::make_pair(TPosition(19,1), TPosition(29,1));
        CompileAndCompare(one, two, &diff);
    }
}

Y_UNIT_TEST_SUITE(TConvertToAst) {
    static TString CompileAndDisassemble(const TString& program, bool expectEqualExprs = true) {
        const auto astRes = ParseAst(program);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));
        UNIT_ASSERT(exprRoot);

        const auto convRes = ConvertToAst(*exprRoot, exprCtx, 0, true);
        UNIT_ASSERT(convRes.IsOk());

        TExprContext exprCtx2;
        TExprNode::TPtr exprRoot2;
        auto compileOk = CompileExpr(*convRes.Root, exprRoot2, exprCtx2, nullptr, nullptr);
        exprCtx2.IssueManager.GetIssues().PrintTo(Cout);
        UNIT_ASSERT(compileOk);
        UNIT_ASSERT(exprRoot2);
        const TExprNode* node = exprRoot.Get();
        const TExprNode* node2 = exprRoot2.Get();
        bool equal = CompareExprTrees(node, node2);
        UNIT_ASSERT(equal == expectEqualExprs);

        return convRes.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    }

    Y_UNIT_TEST(ManyLambdaWithCaptures) {
        const auto program = R"(
            (
            #comment
            (let mr_source (DataSource 'yt 'plato))
            (let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))
            (let world (Left! x))
            (let table1 (Right! x))
            (let table1int (FlatMap table1
                (lambda '(item) (block '(
                    (let intKey (FromString (Member item 'key) 'Int32))
                    (let keyDiv100 (FlatMap intKey (lambda '(x) (/ x (Int32 '100)))))
                    (let ret (Map keyDiv100 (lambda '(y) (block '(
                        (let r '(y (Member item 'value)))
                        (return r)
                    )))))
                    (return ret)
                )))
            ))
            (let table1intDebug (Map table1int (lambda '(it) (block '(
            (let s (Struct))
            (let s (AddMember s 'key (ToString (Nth it '0))))
            (let s (AddMember s 'subkey (String '.)))
            (let s (AddMember s 'value (Nth it '1)))
            (return s)
            )))))
            (let mr_sink (DataSink 'yt (quote plato)))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) table1intDebug '('('mode 'append))))
            (let world (Commit! world mr_sink))
            (return world)
            )
        )";
        CompileAndDisassemble(program);
    }

    Y_UNIT_TEST(LambdaWithCaptureArgumentOfTopLambda) {
        const auto program = R"(
                (
                (let mr_source (DataSource 'yt 'plato))
                (let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))
                (let world (Left! x))
                (let table1 (Right! x))
                (let table1low (FlatMap table1 (lambda '(item) (block '(
                  (let intValueOpt (FromString (Member item 'key) 'Int32))
                  (let ret (FlatMap intValueOpt (lambda '(item2) (block '(
                    (return (ListIf (< item2 (Int32 '100)) item))
                  )))))
                  (return ret)
                )))))
                (let res_sink (DataSink 'result))
                (let data (AsList (String 'x)))
                (let world (Write! world res_sink (Key) table1low '()))
                (let world (Commit! world res_sink))
                (return world)
                )
                )";
        CompileAndDisassemble(program);
    }

    Y_UNIT_TEST(LambdaWithCapture) {
        const auto program = R"(
            (
            (let conf (Configure! world (DataSource 'yt '"$all") '"Attr" '"mapjoinlimit" '"1"))
            (let dsr (DataSink 'result))
            (let dsy (DataSink 'yt 'plato))
            (let co '('key 'subkey 'value))
            (let data (DataSource 'yt 'plato))
            (let string (DataType 'String))
            (let ostr (OptionalType string))
            (let struct (StructType '('key ostr) '('subkey ostr) '('value ostr)))
            (let scheme '('('"scheme" struct)))
            (let temp (MrTempTable dsy '"tmp/bb686f68-2245bd5f-2318fa4e-1" scheme))
            (let str (lambda '(arg) (Just (AsStruct '('key (Just (Member arg 'key))) '('subkey (Just (Member arg 'subkey))) '('value (Just (Member arg 'value)))))))
            (let map (MrMap! world dsy (Key '('table (String 'Input1))) co '() data temp '() str))
            (let tt (MrTempTable dsy '"tmp/7ae6459a-7382d1e7-7935c08e-2" scheme))
            (let map2 (MrMap! world dsy (Key '('table (String 'Input2))) co '() data tt '() str))
            (let s2 (StructType '('"a.key" string) '('"a.subkey" ostr) '('"a.value" ostr) '('"b.key" string) '('"b.subkey" ostr) '('"b.value" ostr)))
            (let mtt3 (MrTempTable dsy '"tmp/ecfc6738-59d47572-b9936849-3" '('('"scheme" s2))))
            (let tuple '('('"take" (Uint64 '"101"))))
            (let lmap (MrLMap! (Sync! map map2) dsy temp co '() data mtt3 '('('"limit" '(tuple))) (lambda '(arg) (block '(
                (let read (MrReadTable! world data tt co '()))
                (let key '('"key"))
                (let wtf '('"Hashed" '"One" '"Compact" '('"ItemsCount" '"4")))
                (let dict (ToDict (FilterNullMembers (MrTableContent read '()) key) (lambda '(arg) (Member arg '"key")) (lambda '(x) x) wtf))
                (let acols '('"key" '"a.key" '"subkey" '"a.subkey" '"value" '"a.value"))
                (let bcols '('"key" '"b.key" '"subkey" '"b.subkey" '"value" '"b.value"))
                (return (MapJoinCore arg dict 'Inner key acols bcols))
            )))))
            (let cols '('"a.key" '"a.subkey" '"a.value" '"b.key" '"b.subkey" '"b.value"))
            (let res (ResPull! conf dsr (Key) (Right! (MrReadTable! lmap data mtt3 cols tuple)) '('('type)) 'yt))
            (return (Commit! res dsr))
            )
        )";

        const auto disassembled = CompileAndDisassemble(program);
        UNIT_ASSERT(TString::npos != disassembled.find("'('key 'subkey 'value)"));
        UNIT_ASSERT_EQUAL(disassembled.find("'('key 'subkey 'value)"), disassembled.rfind("'('key 'subkey 'value)"));
    }

    Y_UNIT_TEST(ManyLambdasWithCommonCapture) {
        const auto program = R"(
            (
            (let c42 (+ (Int64 '40) (Int64 '2)))
            (let c100 (Int64 '100))
            (let l0 (lambda '(x) (- (* x c42) c42)))
            (let l1 (lambda '(y) (Apply l0 y)))
            (let l2 (lambda '(z) (+ (* c42) (Apply l0 c42))))
            (return (* (Apply l1 c100)(Apply l2 c100)))
            )
        )";

        const auto disassembled = CompileAndDisassemble(program);
        UNIT_ASSERT(TString::npos != disassembled.find("(+ (Int64 '40) (Int64 '2))"));
        UNIT_ASSERT_EQUAL(disassembled.find("(+ (Int64 '40) (Int64 '2))"), disassembled.rfind("(+ (Int64 '40) (Int64 '2))"));
    }

    Y_UNIT_TEST(CapturedUseInTopLevelAfrerLambda) {
        const auto program = R"(
            (
            (let $1 (DataSink 'result))
            (let $2 (DataSink '"yt" '"plato"))
            (let $3 (DataSource '"yt" '"plato"))
            (let $4 (TupleType (DataType 'Int64) (DataType 'Uint64)))
            (let $5 (MrTempTable $2 '"tmp/ecfc6738-59d47572-b9936849-3" '('('"scheme" (StructType '('"key" (DataType 'String)) '('"value" (StructType '('Avg1 (OptionalType $4)) '('Avg2 $4))))))))
            (let $6 (MrMapCombine! world $2 (Key '('table (String '"Input"))) '('"key" '"subkey") '() $3 $5 '() (lambda '($13) (Just (AsStruct '('"key" (Cast (Member $13 '"key") 'Int64)) '('"sub" (Unwrap (Cast (Member $13 '"subkey") 'Int64)))))) (lambda '($14) (Uint32 '"0")) (lambda '($15 $16) (block '(
            (let $18 (Uint64 '1))
            (let $17 (IfPresent (Member $16 '"key") (lambda '($19) (Just '($19 $18))) (Nothing (OptionalType (TupleType (DataType 'Int64) (DataType 'Uint64))))))
            (return (AsStruct '('Avg1 $17) '('Avg2 '((Member $16 '"sub") $18))))
            ))) (lambda '($20 $21 $22) (block '(
            (let $23 (IfPresent (Member $21 '"key") (lambda '($28) (block '(
                (let $29 (Uint64 '1))
                (return (Just '($28 $29)))
            ))) (Nothing (OptionalType (TupleType (DataType 'Int64) (DataType 'Uint64))))))
            (let $24 (IfPresent (Member $22 'Avg1) (lambda '($26) (IfPresent (Member $21 '"key") (lambda '($27) (Just '((+ (Nth $26 '0) $27) (Inc (Nth $26 '1))))) (Just $26))) $23))
            (let $25 (Member $22 'Avg2))
            (return (AsStruct '('Avg1 $24) '('Avg2 '((+ (Nth $25 '0) (Member $21 '"sub")) (Inc (Nth $25 '1))))))
            ))) (lambda '($30 $31) (Just (AsStruct '('"value" $31) '('"key" (String '"")))))))
            (return $6)
            )
        )";

        CompileAndDisassemble(program);
    }

    Y_UNIT_TEST(SelectCommonAncestor) {
        const auto program = R"(
        (
        (let $1 (DataSink 'result))
        (let $2 (DataSink '"yt" '"plato"))
        (let $3 '('"key" '"value"))
        (let $4 (DataSource '"yt" '"plato"))
        (let $5 (DataType 'String))
        (let $6 (OptionalType $5))
        (let $7 (MrTempTable $2 '"tmp/41c7eb81-87a9f8b6-70daa714-11" '('('"scheme" (StructType '('"key" $5) '('"value" (StructType '('Histogram0 $6) '('Histogram1 $6))))))))
        (let $8 (Udf 'Histogram.AdaptiveWardHistogram_Create (Void) (VoidType) '"" (CallableType '() '((ResourceType '"Histogram.AdaptiveWard")) '((DataType 'Double)) '((DataType 'Double)) '((DataType 'Uint32)))))
        (let $9 (Double '1.0))
        (let $10 (Cast (Int32 '"1") 'Uint32))
        (let $11 (Double '1.0))
        (let $12 (Cast (Int32 '"1000000") 'Uint32))
        (let $13 (MrMapCombine! world $2 (Key '('table (String '"Input"))) $3 '() $4 $7 '() (lambda '($21) (Just $21)) (lambda '($22) (Uint32 '"0")) (lambda '($23 $24) (AsStruct '('Histogram0 (FlatMap (Cast (Member $24 '"key") 'Double) (lambda '($25) (block '(
        (let $26 '((DataType 'Double)))
        (let $27 (CallableType '() '((ResourceType '"Histogram.AdaptiveWard")) $26 $26 '((DataType 'Uint32))))
        (let $28 '((Unwrap (Cast (Member $24 '"key") 'Double)) $9 $10))
        (return (Just (NamedApply $8 $28 (AsStruct) (Uint32 '"0"))))
        ))))) '('Histogram1 (FlatMap (Cast (Member $24 '"value") 'Double) (lambda '($29) (block '(
        (let $30 '((Unwrap (Cast (Member $24 '"value") 'Double)) $11 $12))
        (return (Just (NamedApply $8 $30 (AsStruct) (Uint32 '"1"))))
        ))))))) (lambda '($31 $32 $33) (block '(
        (let $34 (Udf 'Histogram.AdaptiveWardHistogram_AddValue (Void) (VoidType) '"" (CallableType '() '((ResourceType '"Histogram.AdaptiveWard")) '((ResourceType '"Histogram.AdaptiveWard")) '((DataType 'Double)) '((DataType 'Double)))))
        (let $35 (Uint32 '"0"))
        (let $36 (IfPresent (Member $33 'Histogram0) (lambda '($39) (block '(
            (let $40 (Cast (Member $32 '"key") 'Double))
            (let $41 '((ResourceType '"Histogram.AdaptiveWard")))
            (let $42 '((DataType 'Double)))
            (let $43 (CallableType '() $41 $41 $42 $42))
            (let $44 '($39 (Unwrap $40) $9))
            (let $45 (NamedApply $34 $44 (AsStruct) $35))
            (return (Just (If (Exists $40) $45 $39)))
        ))) (FlatMap (Cast (Member $32 '"key") 'Double) (lambda '($46) (block '(
            (let $47 '((Unwrap (Cast (Member $32 '"key") 'Double)) $9 $10))
            (return (Just (NamedApply $8 $47 (AsStruct) $35)))
        ))))))
        (let $37 (Uint32 '"1"))
        (let $38 (IfPresent (Member $33 'Histogram1) (lambda '($48) (block '(
            (let $49 (Cast (Member $32 '"value") 'Double))
            (let $50 '($48 (Unwrap $49) $11))
            (let $51 (NamedApply $34 $50 (AsStruct) $37))
            (return (Just (If (Exists $49) $51 $48)))
        ))) (FlatMap (Cast (Member $32 '"value") 'Double) (lambda '($52) (block '(
            (let $53 '((Unwrap (Cast (Member $32 '"value") 'Double)) $11 $12))
            (return (Just (NamedApply $8 $53 (AsStruct) $37)))
        ))))))
        (return (AsStruct '('Histogram0 $36) '('Histogram1 $38)))
        ))) (lambda '($54 $55) (block '(
        (let $56 (lambda '($57) (block '(
            (let $58 (CallableType '() '((DataType 'String)) '((ResourceType '"Histogram.AdaptiveWard"))))
            (let $59 (Udf 'Histogram.AdaptiveWardHistogram_Serialize (Void) (VoidType) '"" $58))
            (return (Just (Apply $59 $57)))
        ))))
        (return (Just (AsStruct '('"value" (AsStruct '('Histogram0 (FlatMap (Member $55 'Histogram0) $56)) '('Histogram1 (FlatMap (Member $55 'Histogram1) $56)))) '('"key" (String '"")))))
        )))))
        (let $14 (DataType 'Double))
        (let $15 (OptionalType (StructType '('"Bins" (ListType (StructType '('"Frequency" $14) '('"Position" $14)))) '('"Max" $14) '('"Min" $14) '('"WeightsSum" $14))))
        (let $16 (MrTempTable $2 '"tmp/b6d6c3ee-30bb3e55-ea0c48bc-12" '('('"scheme" (StructType '('"key_histogram" $15) '('"value_histogram" $15))))))
        (let $17 (MrReduce! $13 $2 $7 $3 '() $4 $16 '('('"reduceBy" '('"key"))) (lambda '($60 $61) (block '(
        (let $67 (Udf 'Histogram.AdaptiveWardHistogram_Deserialize (Void) (VoidType) '"" (CallableType '() '((ResourceType '"Histogram.AdaptiveWard")) '((DataType 'String)) '((DataType 'Uint32)))))
        (let $62 (lambda '($68) (block '(
            (let $69 (CallableType '() '((ResourceType '"Histogram.AdaptiveWard")) '((DataType 'String)) '((DataType 'Uint32))))
            (return (Just (Apply $67 $68 $10)))
        ))))
        (let $63 (lambda '($70) (Just (Apply $67 $70 $12))))
        (let $64 (Fold1 (FlatMap $61 (lambda '($65) (Just (Member $65 '"value")))) (lambda '($66) (block '(
            (return (AsStruct '('Histogram0 (FlatMap (Member $66 'Histogram0) $62)) '('Histogram1 (FlatMap (Member $66 'Histogram1) $63))))
        ))) (lambda '($71 $72) (block '(
            (let $73 (lambda '($76 $77) (block '(
            (let $78 '((ResourceType '"Histogram.AdaptiveWard")))
            (let $79 (CallableType '() $78 $78 $78))
            (let $80 (Udf 'Histogram.AdaptiveWardHistogram_Merge (Void) (VoidType) '"" $79))
            (return (Apply $80 $76 $77))
            ))))
            (let $74 (OptionalReduce (FlatMap (Member $71 'Histogram0) $62) (Member $72 'Histogram0) $73))
            (let $75 (OptionalReduce (FlatMap (Member $71 'Histogram1) $63) (Member $72 'Histogram1) $73))
            (return (AsStruct '('Histogram0 $74) '('Histogram1 $75)))
        )))))
        (return (FlatMap $64 (lambda '($81) (block '(
            (let $82 (lambda '($83) (block '(
            (let $84 (DataType 'Double))
            (let $85 (CallableType '() '((StructType '('"Bins" (ListType (StructType '('"Frequency" $84) '('"Position" $84)))) '('"Max" $84) '('"Min" $84) '('"WeightsSum" $84))) '((ResourceType '"Histogram.AdaptiveWard"))))
            (let $86 (Udf 'Histogram.AdaptiveWardHistogram_GetResult (Void) (VoidType) '"" $85))
            (return (Just (Apply $86 $83)))
            ))))
            (return (AsList (AsStruct '('"key_histogram" (FlatMap (Member $81 'Histogram0) $82)) '('"value_histogram" (FlatMap (Member $81 'Histogram1) $82)))))
        )))))
        )))))
        (let $18 '('"key_histogram" '"value_histogram"))
        (let $19 '('('type) '('autoref) '('columns $18)))
        (let $20 (ResPull! world $1 (Key) (Right! (MrReadTable! $17 $4 $16 $18 '())) $19 '"yt"))
        (return (Commit! (Commit! $20 $1) $2 '('('"epoch" '"1"))))
        )
        )";

        CompileAndDisassemble(program);
    }

    Y_UNIT_TEST(Parameters) {
        const auto program = R"(
        (
        (let $nameType (OptionalType (DataType 'String)))
        (let $1 (Read! world (DataSource '"kikimr" '"local_ut") (Key '('table (String '"tmp/table"))) (Void) '()))
        (let $2 (DataSink 'result))
        (let $5 (Write! (Left! $1) $2 (Key) (FlatMap (Filter (Right! $1)
            (lambda '($9) (Coalesce (And
                (== (Member $9 '"Group") (Parameter '"$Group" (DataType 'Uint32)))
                (== (Member $9 '"Name") (Parameter '"$Name" $nameType))) (Bool 'false))))
            (lambda '($10) (AsList $10))) '('('type) '('autoref))))
        (let $6 (Read! (Commit! $5 $2) (DataSource '"kikimr" '"local_ut")
            (Key '('table (String '"tmp/table"))) (Void) '()))
        (let $7 (DataSink 'result))
        (let $8 (Write! (Left! $6) $7 (Key) (FlatMap (Filter (Right! $6)
            (lambda '($11) (Coalesce (And
                (== (Member $11 '"Group") (+ (Parameter '"$Group" (DataType 'Uint32)) (Int32 '"1")))
                (== (Member $11 '"Name") (Coalesce (Parameter '"$Name" $nameType)
                    (String '"Empty")))) (Bool 'false))))
            (lambda '($12) (AsList $12))) '('('type) '('autoref))))
        (return (Commit! $8 $7))
        )
        )";

        const auto disassembled = CompileAndDisassemble(program);
        UNIT_ASSERT(TString::npos != disassembled.find("(declare $Group (DataType 'Uint32))"));
        UNIT_ASSERT(TString::npos != disassembled.find("(declare $Name (OptionalType (DataType 'String)))"));
    }

    Y_UNIT_TEST(ParametersDifferentTypes) {
        const auto program = R"(
        (
        (let $1 (Read! world (DataSource '"kikimr" '"local_ut") (Key '('table (String '"tmp/table"))) (Void) '()))
        (let $2 (DataSink 'result))
        (let $5 (Write! (Left! $1) $2 (Key) (FlatMap (Filter (Right! $1)
            (lambda '($9) (Coalesce (And
                (== (Member $9 '"Group") (Parameter '"$Group" (DataType 'Uint32)))
                (== (Member $9 '"Name") (Parameter '"$Name" (OptionalType (DataType 'String))))) (Bool 'false))))
            (lambda '($10) (AsList $10))) '('('type) '('autoref))))
        (let $6 (Read! (Commit! $5 $2) (DataSource '"kikimr" '"local_ut")
            (Key '('table (String '"tmp/table"))) (Void) '()))
        (let $7 (DataSink 'result))
        (let $8 (Write! (Left! $6) $7 (Key) (FlatMap (Filter (Right! $6)
            (lambda '($11) (Coalesce (And
                (== (Member $11 '"Group") (+ (Parameter '"$Group" (DataType 'Uint32)) (Int32 '"1")))
                (== (Member $11 '"Name") (Coalesce (Parameter '"$Name" (OptionalType (DataType 'Int32)))
                    (String '"Empty")))) (Bool 'false))))
            (lambda '($12) (AsList $12))) '('('type) '('autoref))))
        (return (Commit! $8 $7))
        )
        )";

        const auto disassembled = CompileAndDisassemble(program, false);
        UNIT_ASSERT(TString::npos != disassembled.find("(declare $Group (DataType 'Uint32))"));
        UNIT_ASSERT(TString::npos != disassembled.find("(declare $Name (OptionalType (DataType 'String)))"));
    }
}

} // namespace NYql
