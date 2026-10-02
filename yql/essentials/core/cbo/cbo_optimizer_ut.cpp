#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>

#include <yql/essentials/parser/pg_wrapper/interface/optimizer.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(CboOptimizer) {

Y_UNIT_TEST(InputToString) {
    IOptimizer::TRel rel1 = {.Rows = 100000, .TotalCost = 1000000, .TargetVars = {{'a'}}};
    IOptimizer::TRel rel2 = {.Rows = 1000000, .TotalCost = 9000009, .TargetVars = {{'b'}}};
    IOptimizer::TRel rel3 = {.Rows = 10000, .TotalCost = 9009, .TargetVars = {{'c'}}};
    IOptimizer::TInput input = {.Rels = {rel1, rel2, rel3}, .EqClasses = {}, .Left = {}, .Right = {}};

    input.EqClasses.emplace_back(IOptimizer::TEq{
        {{1, 1}, {2, 1}, {3, 1}}});

    auto str = input.ToString();

    TString expected = R"__(Rels: [{rows: 100000,cost: 1000000,vars: [a]},
{rows: 1000000,cost: 9000009,vars: [b]},
{rows: 10000,cost: 9009,vars: [c]}]
EqClasses: [[a,b,c]]
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, str);
}

Y_UNIT_TEST(OutputToString) {
    IOptimizer::TOutput output;
    auto str = output.ToString();

    TString expected = R"__(Rows: 0.00
TotalCost: 0.00
{
}
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, str);
}

Y_UNIT_TEST(InputNormalize) {
    IOptimizer::TRel rel1 = {.Rows = 100000, .TotalCost = 1000000, .TargetVars = {{'a'}}};
    IOptimizer::TRel rel2 = {.Rows = 1000000, .TotalCost = 9000009, .TargetVars = {{'b'}}};
    IOptimizer::TRel rel3 = {.Rows = 10000, .TotalCost = 9009, .TargetVars = {{'c'}}};
    IOptimizer::TInput input = {.Rels = {rel1, rel2, rel3}, .EqClasses = {}, .Left = {}, .Right = {}};

    input.EqClasses.emplace_back(IOptimizer::TEq{
        {{1, 1}, {2, 1}}});
    input.EqClasses.emplace_back(IOptimizer::TEq{
        {{2, 1}, {3, 1}}});

    TString expected = R"__(Rels: [{rows: 100000,cost: 1000000,vars: [a]},
{rows: 1000000,cost: 9000009,vars: [b]},
{rows: 10000,cost: 9009,vars: [c]}]
EqClasses: [[a,b],[b,c]]
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, input.ToString());

    input.Normalize();

    expected = R"__(Rels: [{rows: 100000,cost: 1000000,vars: [a]},
{rows: 1000000,cost: 9000009,vars: [b]},
{rows: 10000,cost: 9009,vars: [c]}]
EqClasses: [[a,b,c]]
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, input.ToString());

    IOptimizer::TRel rel4 = {.Rows = 10001, .TotalCost = 9009, .TargetVars = {{'d'}}};
    IOptimizer::TInput input2 = {.Rels = {rel1, rel2, rel3, rel4}, .EqClasses = {}, .Left = {}, .Right = {}};
    input2.EqClasses.emplace_back(IOptimizer::TEq{
        {{1, 1}, {2, 1}}});
    input2.EqClasses.emplace_back(IOptimizer::TEq{
        {{4, 1}, {3, 1}}});

    expected = R"__(Rels: [{rows: 100000,cost: 1000000,vars: [a]},
{rows: 1000000,cost: 9000009,vars: [b]},
{rows: 10000,cost: 9009,vars: [c]},
{rows: 10001,cost: 9009,vars: [d]}]
EqClasses: [[a,b],[d,c]]
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, input2.ToString());

    input2.Normalize();

    expected = R"__(Rels: [{rows: 100000,cost: 1000000,vars: [a]},
{rows: 1000000,cost: 9000009,vars: [b]},
{rows: 10000,cost: 9009,vars: [c]},
{rows: 10001,cost: 9009,vars: [d]}]
EqClasses: [[a,b],[c,d]]
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, input2.ToString());
}

} // Y_UNIT_TEST_SUITE(CboOptimizer)
