#include <ydb/public/lib/ydb_cli/common/tabbed_table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NYdb::NConsoleClient {

namespace {

NYdb::NScheme::TSchemeEntry MakeEntry(const std::string& name, NYdb::NScheme::ESchemeEntryType type = NYdb::NScheme::ESchemeEntryType::Table) {
    NYdb::NScheme::TSchemeEntry entry;
    entry.Name = name;
    entry.Type = type;
    return entry;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(AdaptiveTabbedTableTests) {
    Y_UNIT_TEST(EmptyEntriesProducesNoOutput) {
        std::vector<NYdb::NScheme::TSchemeEntry> entries;
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        table.Print(ss);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "");
    }

    Y_UNIT_TEST(SingleEntryPrintsNameAndNewline) {
        std::vector<NYdb::NScheme::TSchemeEntry> entries = {MakeEntry("only")};
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        table.Print(ss);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "only\n");
    }

    Y_UNIT_TEST(MultipleShortEntriesOnSingleLine) {
        // In a non-tty test environment the terminal width is unknown, so all
        // entries are laid out on one line padded to the minimum column width.
        std::vector<NYdb::NScheme::TSchemeEntry> entries = {
            MakeEntry("a"),
            MakeEntry("b"),
            MakeEntry("c"),
        };
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        table.Print(ss);

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "a  b  c\n");
    }

    Y_UNIT_TEST(LongerNamesArePaddedToColumnWidth) {
        std::vector<NYdb::NScheme::TSchemeEntry> entries = {
            MakeEntry("alpha"),
            MakeEntry("b"),
            MakeEntry("c"),
        };
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        table.Print(ss);

        // First column expands to fit "alpha" plus separator; last column has no trailing padding.
        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "alpha  b  c\n");
    }

    Y_UNIT_TEST(OutStreamOperatorRoutesToPrint) {
        std::vector<NYdb::NScheme::TSchemeEntry> entries = {MakeEntry("x"), MakeEntry("y")};
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        ss << table;

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "x  y\n");
    }

    Y_UNIT_TEST(EmptyEntriesOutStreamOperatorProducesNoOutput) {
        // Regression: before the fix Print() would Y_VALIDATE on ColumnCount==0
        // (or otherwise misbehave) when given an empty entries vector. The Out<>
        // specialization for TAdaptiveTabbedTable must not crash either.
        std::vector<NYdb::NScheme::TSchemeEntry> entries;
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        ss << table;

        UNIT_ASSERT_STRINGS_EQUAL(ss.Str(), "");
    }

    Y_UNIT_TEST(DifferentEntryTypesAreAllPrinted) {
        std::vector<NYdb::NScheme::TSchemeEntry> entries = {
            MakeEntry("dir1", NYdb::NScheme::ESchemeEntryType::Directory),
            MakeEntry("tbl1", NYdb::NScheme::ESchemeEntryType::Table),
            MakeEntry("topic1", NYdb::NScheme::ESchemeEntryType::Topic),
        };
        TAdaptiveTabbedTable table(entries);

        TStringStream ss;
        table.Print(ss);

        const TString out = ss.Str();
        UNIT_ASSERT(out.Contains("dir1"));
        UNIT_ASSERT(out.Contains("tbl1"));
        UNIT_ASSERT(out.Contains("topic1"));
        UNIT_ASSERT(out.EndsWith("\n"));
    }
}

} // namespace NYdb::NConsoleClient
