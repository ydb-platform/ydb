#include "recursive_remove.h"

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

using namespace NYdb;
using namespace NConsoleClient;
using namespace NScheme;

namespace NYdb::NScheme {

bool operator==(const TSchemeEntry& lhs, const TSchemeEntry& rhs) {
    return std::make_pair(lhs.Name, lhs.Type) == std::make_pair(rhs.Name, rhs.Type);
}

}

Y_UNIT_TEST_SUITE(RecursiveRemoveTests) {

TSchemeEntry CreateSchemeEntry(ESchemeEntryType type, const TString& name = {}) {
    TSchemeEntry entry;
    entry.Type = type;
    entry.Name = name ? name : TStringBuilder() << type;
    return entry;
}

Y_UNIT_TEST(SecondPass) {
    // Table and ExternalTable are in pass1 (no dependents).
    // ExternalDataSource is in pass2 (might have dependents: ExternalTable references it).
    // Invariants to verify:
    //   1. Each entry is removed exactly once.
    //   2. All pass-1 objects are removed before any pass-2 object.
    TVector<TSchemeEntry> entries = {
        { CreateSchemeEntry(ESchemeEntryType::Table) },
        { CreateSchemeEntry(ESchemeEntryType::ExternalDataSource) },
        { CreateSchemeEntry(ESchemeEntryType::ExternalTable) }
    };

    std::mutex mu;
    TVector<TSchemeEntry> pass1Removed, pass2Removed;
    bool pass2Started = false;

    NInternal::TRemover remover = [&](const TSchemeEntry& entry) {
        std::lock_guard lock(mu);
        if (NInternal::MightHaveDependents(entry.Type)) {
            pass2Started = true;
            pass2Removed.emplace_back(entry);
        } else {
            UNIT_ASSERT_C(!pass2Started, "pass-1 object removed after pass-2 started: " << entry.Name);
            pass1Removed.emplace_back(entry);
        }
        return TStatus(EStatus::SUCCESS, {});
    };

    NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr);

    // Each entry removed exactly once
    UNIT_ASSERT_VALUES_EQUAL(pass1Removed.size() + pass2Removed.size(), entries.size());

    TVector<TSchemeEntry> expectedPass1 = {
        CreateSchemeEntry(ESchemeEntryType::Table),
        CreateSchemeEntry(ESchemeEntryType::ExternalTable),
    };
    TVector<TSchemeEntry> expectedPass2 = {
        CreateSchemeEntry(ESchemeEntryType::ExternalDataSource),
    };

    // Sort both vectors by name for order-independent comparison
    auto byName = [](const TSchemeEntry& a, const TSchemeEntry& b) { return a.Name < b.Name; };
    std::sort(pass1Removed.begin(), pass1Removed.end(), byName);
    std::sort(expectedPass1.begin(), expectedPass1.end(), byName);

    UNIT_ASSERT_VALUES_EQUAL(pass1Removed, expectedPass1);
    UNIT_ASSERT_VALUES_EQUAL(pass2Removed, expectedPass2);
}

Y_UNIT_TEST(DepthOrdering) {
    // Directories must be deleted deepest-first: /a/b/c before /a/b before /a.
    // All three are in pass2 (MightHaveDependents).
    TVector<TSchemeEntry> entries = {
        CreateSchemeEntry(ESchemeEntryType::Directory, "/a"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/a/b"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/a/b/c"),
    };

    std::mutex mu;
    TVector<TString> removedOrder;

    NInternal::TRemover remover = [&](const TSchemeEntry& entry) {
        std::lock_guard lock(mu);
        removedOrder.push_back(TString(entry.Name));
        return TStatus(EStatus::SUCCESS, {});
    };

    NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(removedOrder.size(), 3u);
    // Deepest path must appear before its ancestors
    auto pos = [&](const TString& name) {
        return std::find(removedOrder.begin(), removedOrder.end(), name) - removedOrder.begin();
    };
    UNIT_ASSERT_C(pos("/a/b/c") < pos("/a/b"), "/a/b/c must be removed before /a/b");
    UNIT_ASSERT_C(pos("/a/b") < pos("/a"), "/a/b must be removed before /a");
}

Y_UNIT_TEST(ParallelRemovesAllEntries) {
    // Verify that all entries are removed exactly once when running in parallel.
    const size_t count = 50;
    TVector<TSchemeEntry> entries;
    entries.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        entries.push_back(CreateSchemeEntry(ESchemeEntryType::Table, TStringBuilder() << "table_" << i));
    }

    std::mutex mu;
    TVector<TString> removedNames;

    NInternal::TRemover remover = [&](const TSchemeEntry& entry) {
        std::lock_guard lock(mu);
        removedNames.push_back(TString(entry.Name));
        return TStatus(EStatus::SUCCESS, {});
    };

    NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr);

    UNIT_ASSERT_VALUES_EQUAL(removedNames.size(), count);

    // Each entry removed exactly once
    std::sort(removedNames.begin(), removedNames.end());
    UNIT_ASSERT_VALUES_EQUAL(std::unique(removedNames.begin(), removedNames.end()), removedNames.end());
}

}
