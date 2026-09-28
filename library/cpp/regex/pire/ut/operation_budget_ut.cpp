#include <library/cpp/regex/pire/pire.h>
#include <library/cpp/regex/pire/pire/operation_budget.h>
#include <library/cpp/regex/pire/pire/approx_matching.h>
#include <library/cpp/testing/unittest/registar.h>

#include <limits>
#include <sstream>
#include <thread>

namespace {
    Pire::Scanner Compile(const char* regexp) {
        return Pire::Lexer(regexp).Parse().Compile<Pire::Scanner>();
    }

    template<class Scanner>
    void CheckColdRuntime() {
        const auto scanner = Pire::Lexer("a").Parse().Compile<Scanner>();
        Pire::ScopedOperationBudget budget(0);
        UNIT_ASSERT(!scanner.Empty());
        UNIT_ASSERT_VALUES_EQUAL(scanner.RegexpsCount(), 1);
        const Scanner empty;
        UNIT_ASSERT(empty.Empty());
        UNIT_ASSERT_VALUES_EQUAL(budget.Remaining(), 0);
        UNIT_ASSERT_EXCEPTION(Compile("b"), Pire::BudgetExceeded);
    }
}

Y_UNIT_TEST_SUITE(PireOperationBudget) {
    SIMPLE_UNIT_FORKED_TEST(ColdRuntimeDoesNotSpendBudget) {
        // A fresh process prevents other tests from warming up Null singletons.
        CheckColdRuntime<Pire::Scanner>();
        CheckColdRuntime<Pire::SimpleScanner>();
        CheckColdRuntime<Pire::SlowScanner>();
        CheckColdRuntime<Pire::CapturingScanner>();
    }

    Y_UNIT_TEST(InputPreparationIsFree) {
        const TString pattern(1000000, 'a');
        Pire::ScopedOperationBudget budget(0);
        Pire::Lexer lexer(pattern);
        UNIT_ASSERT_VALUES_EQUAL(budget.Remaining(), 0);
        UNIT_ASSERT_EXCEPTION(lexer.Parse(), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(InputIteratorPreparationIsFree) {
        std::istringstream input("abcdef");
        Pire::Lexer lexer;
        Pire::ScopedOperationBudget budget(0);
        using Iter = std::istreambuf_iterator<char>;
        lexer.Assign(Iter(input), Iter());
        UNIT_ASSERT_VALUES_EQUAL(input.peek(), std::char_traits<char>::eof());
        UNIT_ASSERT_VALUES_EQUAL(budget.Remaining(), 0);
        UNIT_ASSERT_EXCEPTION(lexer.Parse(), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(ExactBoundaryAndSharedBudget) {
        Pire::Fsm first;
        Pire::Fsm second;
        Pire::ScopedOperationBudget budget(2);
        first.Append('a');
        UNIT_ASSERT_VALUES_EQUAL(budget.Remaining(), 0);
        UNIT_ASSERT(first.Connected(0, 1, 'a'));
        UNIT_ASSERT_EXCEPTION(second.Append('b'), Pire::BudgetExceeded);
        UNIT_ASSERT_VALUES_EQUAL(second.Size(), 1);
    }

    Y_UNIT_TEST(BulkChargePrecedesAllocation) {
        Pire::Fsm fsm;
        Pire::ScopedOperationBudget budget(10);
        UNIT_ASSERT_EXCEPTION(fsm.Resize(std::numeric_limits<size_t>::max()), Pire::BudgetExceeded);
        UNIT_ASSERT_VALUES_EQUAL(fsm.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(budget.Remaining(), 0);
        UNIT_ASSERT_EXCEPTION(fsm.Append('a'), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(BulkProductDoesNotOverflow) {
        Pire::Fsm fsm;
        fsm.Append('a');
        Pire::ScopedOperationBudget budget(std::numeric_limits<size_t>::max());
        UNIT_ASSERT_EXCEPTION(fsm * std::numeric_limits<size_t>::max(), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(PlainFsmCopiesAreFree) {
        const auto fsm = Pire::Lexer("[a-z]{20}").Parse();
        Pire::ScopedOperationBudget budget(0);
        Pire::Fsm target;
        Pire::Fsm copy(fsm);
        target = fsm;
        UNIT_ASSERT_VALUES_EQUAL(copy.Size(), fsm.Size());
        UNIT_ASSERT_VALUES_EQUAL(target.Size(), fsm.Size());
        UNIT_ASSERT_VALUES_EQUAL(budget.Remaining(), 0);
        UNIT_ASSERT_EXCEPTION(target.Append('b'), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(ApproximateExpansionUsesCommonBudgetChecks) {
        const auto fsm = Pire::Lexer("ab").Parse();
        Pire::ScopedOperationBudget budget(100);
        UNIT_ASSERT_EXCEPTION(Pire::CreateApproxFsm(fsm, 10000), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(NestedScopeRestoresRemainder) {
        Pire::Fsm fsm;
        Pire::ScopedOperationBudget outer(4);
        fsm.Append('a');
        {
            Pire::ScopedOperationBudget inner(2);
            fsm.Append('b');
            UNIT_ASSERT_VALUES_EQUAL(inner.Remaining(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(outer.Remaining(), 2);
        fsm.Append('c');
        UNIT_ASSERT_EXCEPTION(fsm.Append('d'), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(ExceptionRestoresUnlimitedBudget) {
        {
            Pire::ScopedOperationBudget outer(10);
            try {
                Pire::ScopedOperationBudget inner(0);
                Compile("a");
                UNIT_FAIL("Expected exhausted budget");
            } catch (const Pire::BudgetExceeded&) {
                UNIT_ASSERT_VALUES_EQUAL(outer.Remaining(), 10);
            }
        }
        const auto scanner = Compile("a{3}");
        UNIT_ASSERT(Pire::Runner(scanner).Run("aaa"));
        UNIT_ASSERT(!Pire::Runner(scanner).Run("aa"));
    }

    Y_UNIT_TEST(ThreadLocalIsolation) {
        bool accepted = false;
        Pire::ScopedOperationBudget budget(0);
        std::thread worker([&] {
            const auto scanner = Compile("abc");
            accepted = Pire::Runner(scanner).Run("abc");
        });
        worker.join();
        UNIT_ASSERT(accepted);
        UNIT_ASSERT_EXCEPTION(Compile("abc"), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(ExpensiveFamiliesAreRejected) {
        for (const char* pattern : {"a{100000001}", "((a{50}){50}){50}",
                "(abc|def|ghi){10000}", ".{250}", "[^a]{200}",
                ".*a.{100}", ".*a...................."}) {
            Pire::ScopedOperationBudget budget(10000);
            UNIT_ASSERT_EXCEPTION(Compile(pattern), Pire::BudgetExceeded);
        }
    }

    Y_UNIT_TEST(EpsilonRemovalUsesBudget) {
        auto fsm = Pire::Lexer("(a?|b?)*").Parse();
        Pire::ScopedOperationBudget budget(1);
        UNIT_ASSERT_EXCEPTION(fsm.RemoveEpsilons(), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(DeterminizationUsesBudget) {
        auto fsm = Pire::Lexer(".*a.{12}").Parse();
        fsm.RemoveEpsilons();
        Pire::ScopedOperationBudget budget(10000);
        UNIT_ASSERT_EXCEPTION(fsm.Determine(), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(MinimizationUsesBudget) {
        auto fsm = Pire::Lexer("(abc|def){10}").Parse();
        UNIT_ASSERT(fsm.Determine());
        Pire::ScopedOperationBudget budget(1);
        UNIT_ASSERT_EXCEPTION(fsm.Minimize(), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(DenseNfaScannerUsesBudget) {
        Pire::Fsm fsm;
        fsm.Resize(300);
        for (size_t from = 0; from < fsm.Size(); ++from)
            for (size_t to = 0; to < fsm.Size(); ++to)
                fsm.Connect(from, to, 'a');
        // Enough for letter partitioning and the table, but not its 90000 targets.
        Pire::ScopedOperationBudget budget(100000);
        UNIT_ASSERT_EXCEPTION((Pire::SlowScanner{fsm, false, false}), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(LexerRangeUsesBudgetAndKeepsExceptionType) {
        Pire::ScopedOperationBudget budget(100);
        UNIT_ASSERT_EXCEPTION(Compile("[\\x{0000}-\\x{ffff}]"), Pire::BudgetExceeded);
    }

    Y_UNIT_TEST(AcceptedLanguageIsUnchanged) {
        Pire::ScopedOperationBudget budget(10000000);
        for (const char* pattern : {"(ab){2,3}", "(a{1}b){2,3}", "(ab|xy){2,3}"}) {
            const auto scanner = Compile(pattern);
            UNIT_ASSERT(Pire::Runner(scanner).Run("abab"));
            UNIT_ASSERT(Pire::Runner(scanner).Run("ababab"));
            UNIT_ASSERT(!Pire::Runner(scanner).Run("ab"));
            UNIT_ASSERT(!Pire::Runner(scanner).Run("abababab"));
        }
    }

    Y_UNIT_TEST(RuntimeMatchingDoesNotSpendBudget) {
        const auto scanner = Compile("a+");
        Pire::ScopedOperationBudget budget(0);
        UNIT_ASSERT(Pire::Runner(scanner).Run("aaa"));
        UNIT_ASSERT(!Pire::Runner(scanner).Run("bbb"));
    }

    Y_UNIT_TEST(EveryParserFailurePointIsSafe) {
        // ASan/LSan exercises exceptions while yacc owns both tokens and FSMs.
        for (const char* pattern : {"(ab|cd){2,4}", "[^a]{2}", "[a-z]", "(a?b?)*"}) {
            for (size_t limit = 0; limit < 400; ++limit) {
                try {
                    Pire::ScopedOperationBudget budget(limit);
                    Compile(pattern);
                } catch (const Pire::BudgetExceeded&) {
                }
            }
        }
    }
}
