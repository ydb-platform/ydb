#include <ydb/public/lib/ydb_cli/common/tx_mode_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

namespace NYdb::NConsoleClient {

Y_UNIT_TEST_SUITE(TxModeUtils) {

    // ------------------------------------------------------------------
    // GetSupportedTxModeNames / GetTxModeNamesForHelp
    // ------------------------------------------------------------------

    Y_UNIT_TEST(SupportedNamesContainExpectedModes) {
        const auto names = GetSupportedTxModeNames();
        const THashSet<TStringBuf> set(names.begin(), names.end());
        UNIT_ASSERT(set.contains("serializable-rw"));
        UNIT_ASSERT(set.contains("snapshot-ro"));
        UNIT_ASSERT(set.contains("snapshot-rw"));
        UNIT_ASSERT(set.contains("read-committed-rw"));
        // online-ro and stale-ro are not interactive-able.
        UNIT_ASSERT(!set.contains("online-ro"));
        UNIT_ASSERT(!set.contains("stale-ro"));
    }

    Y_UNIT_TEST(HelpStringMatchesSupportedNames) {
        const TString help = GetTxModeNamesForHelp();
        UNIT_ASSERT(help.Contains("serializable-rw"));
        UNIT_ASSERT(help.Contains("read-committed-rw"));
        UNIT_ASSERT(!help.Contains("online-ro"));
        UNIT_ASSERT(!help.Contains("stale-ro"));
    }

    // ------------------------------------------------------------------
    // ParseTxModeName
    // ------------------------------------------------------------------

    Y_UNIT_TEST(ParseModeAcceptsCanonicalNames) {
        UNIT_ASSERT(ParseTxModeName("serializable-rw"));
        UNIT_ASSERT(ParseTxModeName("snapshot-ro"));
        UNIT_ASSERT(ParseTxModeName("snapshot-rw"));
        UNIT_ASSERT(ParseTxModeName("read-committed-rw"));
    }

    Y_UNIT_TEST(ParseModeIsCaseInsensitive) {
        UNIT_ASSERT(ParseTxModeName("Serializable-RW"));
        UNIT_ASSERT(ParseTxModeName("SNAPSHOT-RO"));
    }

    Y_UNIT_TEST(ParseModeRejectsOneShotModes) {
        UNIT_ASSERT(!ParseTxModeName("online-ro"));
        UNIT_ASSERT(!ParseTxModeName("stale-ro"));
    }

    Y_UNIT_TEST(ParseModeRejectsUnknown) {
        UNIT_ASSERT(!ParseTxModeName(""));
        UNIT_ASSERT(!ParseTxModeName("garbage"));
        UNIT_ASSERT(!ParseTxModeName("read-committed"));
    }

    // ------------------------------------------------------------------
    // ParseBeginTransactionIsolation
    // ------------------------------------------------------------------

    Y_UNIT_TEST(BeginParsesAllPrefixes) {
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("begin"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN TRANSACTION"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN WORK"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("START TRANSACTION"));
    }

    Y_UNIT_TEST(BeginAllowsTrailingSemicolon) {
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN;"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN TRANSACTION;"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN snapshot-ro;"));
    }

    Y_UNIT_TEST(BeginParsesMode) {
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN serializable-rw"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN TRANSACTION snapshot-ro"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("START TRANSACTION read-committed-rw"));
    }

    Y_UNIT_TEST(BeginRejectsOneShotModes) {
        // online-ro and stale-ro are not openable interactively even though
        // they are valid YDB tx modes.
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN online-ro"));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN stale-ro"));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN online-ro INCONSISTENT READS"));
    }

    Y_UNIT_TEST(BeginRejectsUnknownMode) {
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN garbage-mode"));
    }

    Y_UNIT_TEST(BeginRejectsTrailingTokens) {
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN serializable-rw extra"));
        // The PostgreSQL-style ISOLATION LEVEL form is intentionally rejected.
        UNIT_ASSERT(!ParseBeginTransactionIsolation(
            "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"));
    }

    Y_UNIT_TEST(BeginRejectsNonBeginInput) {
        UNIT_ASSERT(!ParseBeginTransactionIsolation(""));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("SELECT 1"));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGINNING"));
    }

    // ------------------------------------------------------------------
    // ExtractBeginModeToken / IsOneShotOnlyTxMode
    // ------------------------------------------------------------------

    Y_UNIT_TEST(ExtractBeginModeToken) {
        UNIT_ASSERT_VALUES_EQUAL(*ExtractBeginModeToken("BEGIN online-ro"), "online-ro");
        UNIT_ASSERT_VALUES_EQUAL(*ExtractBeginModeToken("begin Stale-RO"), "stale-ro");
        UNIT_ASSERT_VALUES_EQUAL(*ExtractBeginModeToken("BEGIN TRANSACTION snapshot-ro"), "snapshot-ro");
        UNIT_ASSERT_VALUES_EQUAL(*ExtractBeginModeToken("START TRANSACTION serializable-rw"), "serializable-rw");
        UNIT_ASSERT(!ExtractBeginModeToken("BEGIN"));
        UNIT_ASSERT(!ExtractBeginModeToken("BEGIN TRANSACTION"));
        UNIT_ASSERT(!ExtractBeginModeToken("SELECT 1"));
    }

    Y_UNIT_TEST(IsOneShotOnlyTxMode) {
        UNIT_ASSERT(IsOneShotOnlyTxMode("online-ro"));
        UNIT_ASSERT(IsOneShotOnlyTxMode("ONLINE-RO"));
        UNIT_ASSERT(IsOneShotOnlyTxMode("stale-ro"));
        UNIT_ASSERT(!IsOneShotOnlyTxMode("serializable-rw"));
        UNIT_ASSERT(!IsOneShotOnlyTxMode("snapshot-ro"));
        UNIT_ASSERT(!IsOneShotOnlyTxMode("snapshot-rw"));
        UNIT_ASSERT(!IsOneShotOnlyTxMode("read-committed-rw"));
        UNIT_ASSERT(!IsOneShotOnlyTxMode("garbage"));
        UNIT_ASSERT(!IsOneShotOnlyTxMode(""));
    }

    // ------------------------------------------------------------------
    // IsBegin / IsCommit / IsRollback predicates
    // ------------------------------------------------------------------

    Y_UNIT_TEST(IsBeginCommand) {
        UNIT_ASSERT(IsBeginCommand("BEGIN"));
        UNIT_ASSERT(IsBeginCommand("begin transaction"));
        UNIT_ASSERT(IsBeginCommand("Begin Work serializable-rw"));
        UNIT_ASSERT(IsBeginCommand("START TRANSACTION"));
        UNIT_ASSERT(IsBeginCommand("BEGIN;"));
        UNIT_ASSERT(!IsBeginCommand(""));
        UNIT_ASSERT(!IsBeginCommand("SELECT 1"));
        UNIT_ASSERT(!IsBeginCommand("BEGINNING"));
        UNIT_ASSERT(!IsBeginCommand("START "));  // START alone is not BEGIN
    }

    Y_UNIT_TEST(IsCommitCommand) {
        UNIT_ASSERT(IsCommitCommand("COMMIT"));
        UNIT_ASSERT(IsCommitCommand("commit transaction"));
        UNIT_ASSERT(IsCommitCommand("COMMIT WORK;"));
        UNIT_ASSERT(IsCommitCommand("END"));
        UNIT_ASSERT(IsCommitCommand("END TRANSACTION"));
        UNIT_ASSERT(!IsCommitCommand("COMMIT FOO"));
        UNIT_ASSERT(!IsCommitCommand("END WORK"));  // END WORK is not a thing
    }

    Y_UNIT_TEST(IsRollbackCommand) {
        UNIT_ASSERT(IsRollbackCommand("ROLLBACK"));
        UNIT_ASSERT(IsRollbackCommand("rollback work"));
        UNIT_ASSERT(IsRollbackCommand("ROLLBACK TRANSACTION;"));
        UNIT_ASSERT(!IsRollbackCommand("ROLLBACK FOO"));
    }
}

} // namespace NYdb::NConsoleClient
