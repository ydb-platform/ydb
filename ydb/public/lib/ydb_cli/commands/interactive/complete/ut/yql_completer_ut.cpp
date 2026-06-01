#include <ydb/public/lib/ydb_cli/commands/interactive/complete/yql_completer.h>
#include <ydb/public/lib/ydb_cli/common/tx_mode_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/string/cast.h>

namespace NYdb::NConsoleClient {

namespace {

// Build a composite completer that has only the TCL completer enabled
// (no slash commands, no YQL completer). This is enough to exercise the
// context-aware TCL logic in isolation.
IYQLCompleter::TPtr MakeTclOnlyCompleter() {
    TCompositeCompleterConfig cfg;
    auto fullForms = GetTransactionControlCompletions();
    cfg.TclCommands.assign(fullForms.begin(), fullForms.end());
    return MakeYQLCompositeCompleter(cfg);
}

THashSet<std::string> AsSet(const THints& hints) {
    THashSet<std::string> set;
    for (const auto& h : hints) {
        set.insert(h);
    }
    return set;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TclCompleter) {

    Y_UNIT_TEST(SuggestsBeginOnPrefix) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEG";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 3);
        UNIT_ASSERT(AsSet(hints).contains("BEGIN"));
        // Must not propose tail words yet.
        UNIT_ASSERT(!AsSet(hints).contains("TRANSACTION"));
        UNIT_ASSERT(!AsSet(hints).contains("WORK"));
    }

    Y_UNIT_TEST(SuggestsBeginOnPrefixLowerCase) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "beg";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 3);
        UNIT_ASSERT(AsSet(hints).contains("BEGIN"));
    }

    Y_UNIT_TEST(SuggestsStartOnPrefix) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "STA";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 3);
        UNIT_ASSERT(AsSet(hints).contains("START"));
    }

    Y_UNIT_TEST(NextWordAfterBeginSpace) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEGIN ";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 0);
        const auto set = AsSet(hints);
        UNIT_ASSERT(set.contains("TRANSACTION"));
        UNIT_ASSERT(set.contains("WORK"));
        UNIT_ASSERT(set.contains("serializable-rw"));
        UNIT_ASSERT(set.contains("read-committed-rw"));
        UNIT_ASSERT(set.contains("snapshot-ro"));
        UNIT_ASSERT(set.contains("snapshot-rw"));
        // online-ro / stale-ro are NOT supported as interactive modes.
        UNIT_ASSERT(!set.contains("online-ro"));
        UNIT_ASSERT(!set.contains("stale-ro"));
    }

    Y_UNIT_TEST(NextWordAfterBeginPartialT) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEGIN T";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 1);
        const auto set = AsSet(hints);
        UNIT_ASSERT(set.contains("TRANSACTION"));
        UNIT_ASSERT(!set.contains("WORK"));
        UNIT_ASSERT(!set.contains("serializable-rw"));
    }

    Y_UNIT_TEST(NextWordAfterBeginPartialS) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEGIN s";
        // Replxx may pass only the current word as prefix; full line is in text.
        auto hints = completer->ApplyLight(text, "s", contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 1);
        const auto set = AsSet(hints);
        UNIT_ASSERT(set.contains("serializable-rw"));
        UNIT_ASSERT(set.contains("snapshot-ro"));
        UNIT_ASSERT(set.contains("snapshot-rw"));
        UNIT_ASSERT(!set.contains("read-committed-rw"));
    }

    Y_UNIT_TEST(NextWordAfterBeginTransactionSpace) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEGIN TRANSACTION ";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 0);
        const auto set = AsSet(hints);
        UNIT_ASSERT(set.contains("serializable-rw"));
        UNIT_ASSERT(set.contains("read-committed-rw"));
        UNIT_ASSERT(set.contains("snapshot-ro"));
        UNIT_ASSERT(set.contains("snapshot-rw"));
        UNIT_ASSERT(!set.contains("online-ro"));
        UNIT_ASSERT(!set.contains("stale-ro"));
        UNIT_ASSERT(!set.contains("TRANSACTION"));
        UNIT_ASSERT(!set.contains("WORK"));
    }

    Y_UNIT_TEST(NextWordAfterStartTransactionSpace) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "START TRANSACTION ";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 0);
        const auto set = AsSet(hints);
        UNIT_ASSERT(set.contains("serializable-rw"));
        UNIT_ASSERT(set.contains("snapshot-ro"));
        // Repeated phrase must NOT be proposed.
        UNIT_ASSERT(!set.contains("START TRANSACTION serializable-rw"));
        UNIT_ASSERT(!set.contains("TRANSACTION"));
    }

    Y_UNIT_TEST(NoOnlineRoSuggestion) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEGIN o";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        // No mode starts with 'o' anymore (online-ro removed).
        UNIT_ASSERT(hints.empty());
    }

    Y_UNIT_TEST(NoInconsistentReadsModifier) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        for (auto text : {TString("BEGIN snapshot-ro "), TString("BEGIN snapshot-rw ")}) {
            auto hints = completer->ApplyLight(text, std::string(text), contextLen);
            const auto set = AsSet(hints);
            UNIT_ASSERT(!set.contains("INCONSISTENT"));
        }
    }

    Y_UNIT_TEST(CommitForms) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        {
            const TString text = "COM";
            auto hints = completer->ApplyLight(text, std::string(text), contextLen);
            UNIT_ASSERT_VALUES_EQUAL(contextLen, 3);
            UNIT_ASSERT(AsSet(hints).contains("COMMIT"));
        }
        {
            const TString text = "COMMIT ";
            auto hints = completer->ApplyLight(text, std::string(text), contextLen);
            UNIT_ASSERT_VALUES_EQUAL(contextLen, 0);
            const auto set = AsSet(hints);
            UNIT_ASSERT(set.contains("TRANSACTION"));
            UNIT_ASSERT(set.contains("WORK"));
        }
    }

    Y_UNIT_TEST(RollbackForms) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        {
            const TString text = "ROL";
            auto hints = completer->ApplyLight(text, std::string(text), contextLen);
            UNIT_ASSERT_VALUES_EQUAL(contextLen, 3);
            UNIT_ASSERT(AsSet(hints).contains("ROLLBACK"));
        }
        {
            const TString text = "ROLLBACK ";
            auto hints = completer->ApplyLight(text, std::string(text), contextLen);
            const auto set = AsSet(hints);
            UNIT_ASSERT(set.contains("TRANSACTION"));
            UNIT_ASSERT(set.contains("WORK"));
        }
    }

    Y_UNIT_TEST(EndForms) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "END ";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        const auto set = AsSet(hints);
        UNIT_ASSERT(set.contains("TRANSACTION"));
        UNIT_ASSERT(!set.contains("WORK"));
    }

    Y_UNIT_TEST(NoTclSuggestionsForUnrelatedInput) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "SELECT * FROM";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT(hints.empty());
    }

    Y_UNIT_TEST(NoTclSuggestionsForEmptyInput) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT(hints.empty());
    }

    Y_UNIT_TEST(NoTclSuggestionsAfterUnknownMode) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "BEGIN nosuchmode ";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT(hints.empty());
    }

    Y_UNIT_TEST(LeadingWhitespaceHandled) {
        auto completer = MakeTclOnlyCompleter();
        int contextLen = -1;
        const TString text = "   BEG";
        auto hints = completer->ApplyLight(text, std::string(text), contextLen);
        UNIT_ASSERT_VALUES_EQUAL(contextLen, 3);
        UNIT_ASSERT(AsSet(hints).contains("BEGIN"));
    }
}

} // namespace NYdb::NConsoleClient
