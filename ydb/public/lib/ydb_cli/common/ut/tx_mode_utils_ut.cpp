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
        UNIT_ASSERT_VALUES_EQUAL(names.size(), 4u);
        const THashSet<TStringBuf> set(names.begin(), names.end());
        UNIT_ASSERT(set.contains("serializable-rw"));
        UNIT_ASSERT(set.contains("snapshot-ro"));
        UNIT_ASSERT(set.contains("snapshot-rw"));
        UNIT_ASSERT(set.contains("read-committed-rw"));
    }

    Y_UNIT_TEST(HelpStringMatchesSupportedNames) {
        const TString help = GetTxModeNamesForHelp();
        UNIT_ASSERT(help.Contains("serializable-rw"));
        UNIT_ASSERT(help.Contains("snapshot-ro"));
        UNIT_ASSERT(help.Contains("snapshot-rw"));
        UNIT_ASSERT(help.Contains("read-committed-rw"));
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
    }

    Y_UNIT_TEST(BeginAllowsTrailingSemicolon) {
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN;"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN TRANSACTION;"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN snapshot-ro;"));
    }

    Y_UNIT_TEST(BeginParsesMode) {
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN serializable-rw"));
        UNIT_ASSERT(ParseBeginTransactionIsolation("BEGIN TRANSACTION snapshot-ro"));
    }

    Y_UNIT_TEST(BeginRejectsUnknownMode) {
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN garbage-mode"));
    }

    Y_UNIT_TEST(BeginRejectsTrailingTokens) {
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN serializable-rw extra"));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGIN snapshot-ro extra"));
    }

    Y_UNIT_TEST(BeginRejectsNonBeginInput) {
        UNIT_ASSERT(!ParseBeginTransactionIsolation(""));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("SELECT 1"));
        UNIT_ASSERT(!ParseBeginTransactionIsolation("BEGINNING"));
    }

    // ------------------------------------------------------------------
    // ExtractBeginModeToken / HasTrailingBeginTokens
    // ------------------------------------------------------------------

    Y_UNIT_TEST(ExtractBeginModeToken) {
        UNIT_ASSERT_VALUES_EQUAL(*ExtractBeginModeToken("BEGIN snapshot-ro"), "snapshot-ro");
        UNIT_ASSERT_VALUES_EQUAL(
            *ExtractBeginModeToken("BEGIN TRANSACTION read-committed-rw"),
            "read-committed-rw");
        UNIT_ASSERT_VALUES_EQUAL(*ExtractBeginModeToken("BEGIN garbage-mode"), "garbage-mode");
        UNIT_ASSERT(!ExtractBeginModeToken("BEGIN"));
        UNIT_ASSERT(!ExtractBeginModeToken("BEGIN TRANSACTION"));
        UNIT_ASSERT(!ExtractBeginModeToken("SELECT 1"));
    }

    Y_UNIT_TEST(HasTrailingBeginTokens) {
        UNIT_ASSERT(HasTrailingBeginTokens("BEGIN serializable-rw extra"));
        UNIT_ASSERT(HasTrailingBeginTokens("BEGIN snapshot-ro extra"));
        UNIT_ASSERT(!HasTrailingBeginTokens("BEGIN serializable-rw"));
        UNIT_ASSERT(!HasTrailingBeginTokens("BEGIN snapshot-ro"));
        UNIT_ASSERT(!HasTrailingBeginTokens("BEGIN"));
        UNIT_ASSERT(!HasTrailingBeginTokens("SELECT 1"));
    }

    // ------------------------------------------------------------------
    // IsBegin / IsCommit / IsRollback predicates
    // ------------------------------------------------------------------

    Y_UNIT_TEST(IsBeginCommand) {
        UNIT_ASSERT(IsBeginCommand("BEGIN"));
        UNIT_ASSERT(IsBeginCommand("begin transaction"));
        UNIT_ASSERT(IsBeginCommand("Begin Transaction serializable-rw"));
        UNIT_ASSERT(IsBeginCommand("BEGIN;"));
        UNIT_ASSERT(!IsBeginCommand(""));
        UNIT_ASSERT(!IsBeginCommand("SELECT 1"));
        UNIT_ASSERT(!IsBeginCommand("BEGINNING"));
    }

    Y_UNIT_TEST(IsCommitCommand) {
        UNIT_ASSERT(IsCommitCommand("COMMIT"));
        UNIT_ASSERT(IsCommitCommand("commit transaction"));
        UNIT_ASSERT(!IsCommitCommand("COMMIT FOO"));
    }

    Y_UNIT_TEST(IsRollbackCommand) {
        UNIT_ASSERT(IsRollbackCommand("ROLLBACK"));
        UNIT_ASSERT(IsRollbackCommand("ROLLBACK TRANSACTION;"));
        UNIT_ASSERT(!IsRollbackCommand("ROLLBACK FOO"));
    }
}

} // namespace NYdb::NConsoleClient
