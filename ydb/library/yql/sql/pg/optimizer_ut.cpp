#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/optimizer.h>

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/thread_inits.h>
}

using namespace NYql;

Y_UNIT_TEST_SUITE(PgOptimizer) {

Y_TEST_HOOK_BEFORE_RUN(InitTest) {
    pg_thread_init();
}

Y_UNIT_TEST(PgJoinSearch2Rels) {
    IOptimizer::TRel rel1 = {100000, 1000000, {{'a'}}};
    IOptimizer::TRel rel2 = {1000000, 9000009, {{'b'}}};
    IOptimizer::TInput input = {.Rels={rel1, rel2}};

    input.EqClasses.emplace_back(IOptimizer::TEq {
        {{1, 1}, {2, 1}}
    });

    auto log = [](const TString& str) {
        Cerr << str << "\n";
    };

    auto optimizer = std::unique_ptr<IOptimizer>(MakePgOptimizerInternal(input, log));

    auto res = optimizer->JoinSearch();
    auto resStr = res.ToString(false);
    Cerr << resStr;
    TString expected = R"__({
 Inner Join
 Loop Strategy
 Rels: [1,2]
 Op: b = a
 {
  Node
  Rels: [2]
 }
 {
  Node
  Rels: [1]
 }
}
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, resStr);
    UNIT_ASSERT(res.TotalCost > 0);
    UNIT_ASSERT(res.Rows > 0);
}

Y_UNIT_TEST(PgJoinSearch2RelsLeft) {
    IOptimizer::TRel rel1 = {100000, 1000000, {{'a'}}};
    IOptimizer::TRel rel2 = {1000000, 9000009, {{'b'}}};
    IOptimizer::TInput input = {.Rels={rel1, rel2}};

    input.EqClasses.emplace_back(IOptimizer::TEq {
        {{1, 1}}
    });
    input.EqClasses.emplace_back(IOptimizer::TEq {
        {{2, 1}}
    });
    input.Left.emplace_back(
        IOptimizer::TEq {
        {{1, 1}, {2, 1}}
        }
    );

    auto log = [](const TString& str) {
        Cerr << str << "\n";
    };

    auto optimizer = std::unique_ptr<IOptimizer>(MakePgOptimizerInternal(input, log));

    auto res = optimizer->JoinSearch();
    auto resStr = res.ToString(false);
    Cerr << resStr;
    TString expected = R"__({
 Left Join
 Loop Strategy
 Rels: [1,2]
 Op: a = b
 {
  Node
  Rels: [1]
 }
 {
  Node
  Rels: [2]
 }
}
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, resStr);
    UNIT_ASSERT(res.TotalCost > 0);
    UNIT_ASSERT(res.Rows > 0);
}

Y_UNIT_TEST(PgJoinSearch2RelsRight) {
    IOptimizer::TRel rel1 = {100000, 1000000, {{'a'}}};
    IOptimizer::TRel rel2 = {1000000, 9000009, {{'b'}}};
    IOptimizer::TInput input = {.Rels={rel1, rel2}};

    input.EqClasses.emplace_back(IOptimizer::TEq {
        {{1, 1}}
    });
    input.EqClasses.emplace_back(IOptimizer::TEq {
        {{2, 1}}
    });
    input.Right.emplace_back(
        IOptimizer::TEq {
        {{1, 1}, {2, 1}}
        }
    );

    auto log = [](const TString& str) {
        Cerr << str << "\n";
    };

    auto optimizer = std::unique_ptr<IOptimizer>(MakePgOptimizerInternal(input, log));

    auto res = optimizer->JoinSearch();
    auto resStr = res.ToString(false);
    Cerr << resStr;
    TString expected = R"__({
 Left Join
 Loop Strategy
 Rels: [1,2]
 Op: b = a
 {
  Node
  Rels: [2]
 }
 {
  Node
  Rels: [1]
 }
}
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, resStr);
    UNIT_ASSERT(res.TotalCost > 0);
    UNIT_ASSERT(res.Rows > 0);
}

Y_UNIT_TEST(PgJoinSearch3Rels) {
    IOptimizer::TRel rel1 = {100000, 1000000, {{'a'}}};
    IOptimizer::TRel rel2 = {1000000, 9000009, {{'b'}}};
    IOptimizer::TRel rel3 = {10000, 9009, {{'c'}}};
    IOptimizer::TInput input = {.Rels={rel1, rel2, rel3}};

    input.EqClasses.emplace_back(IOptimizer::TEq {
        {{1, 1}, {2, 1}, {3, 1}}
    });

    auto log = [](const TString& str) {
        Cerr << str << "\n";
    };

    auto optimizer = std::unique_ptr<IOptimizer>(MakePgOptimizerInternal(input, log));
    auto res = optimizer->JoinSearch();
    auto resStr = res.ToString(false);
    Cerr << resStr;
    TString expected = R"__({
 Inner Join
 Hash Strategy
 Rels: [1,2,3]
 Op: a = b
 {
  Inner Join
  Loop Strategy
  Rels: [1,3]
  Op: a = c
  {
   Node
   Rels: [1]
  }
  {
   Node
   Rels: [3]
  }
 }
 {
  Node
  Rels: [2]
 }
}
)__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, resStr);
    UNIT_ASSERT(res.TotalCost > 0);
    UNIT_ASSERT(res.Rows > 0);
}

} // Y_UNIT_TEST_SUITE(PgOptimizer)
