#include "recursive_remove.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NConsoleClient;
using namespace NScheme;

namespace NYdb::NScheme {

bool operator==(const TSchemeEntry& lhs, const TSchemeEntry& rhs) {
    return std::make_pair(lhs.Name, lhs.Type) == std::make_pair(rhs.Name, rhs.Type);
}

}

Y_UNIT_TEST_SUITE(RecursiveRemoveTests) {

TSchemeEntry CreateSchemeEntry(ESchemeEntryType type) {
    TSchemeEntry entry;
    entry.Type = type;
    entry.Name = TStringBuilder() << type;
    return entry;
}

Y_UNIT_TEST(SecondPass) {
    TVector<TSchemeEntry> entries = {
        { CreateSchemeEntry(ESchemeEntryType::Table) },
        { CreateSchemeEntry(ESchemeEntryType::ExternalDataSource) },
        { CreateSchemeEntry(ESchemeEntryType::ExternalTable) }
    };

    TVector<TSchemeEntry> removedEntries;
    NInternal::TRemover remover = [&removedEntries](const TSchemeEntry& entry) {
        removedEntries.emplace_back(entry);
        return TStatus(EStatus::SUCCESS, {});
    };
    NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, std::nullopt);

    std::swap(entries[1], entries[2]);
    UNIT_ASSERT_VALUES_EQUAL(removedEntries, entries);
}

}
