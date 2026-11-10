#include "recursive_remove.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>
#include <util/string/join.h>

#include <condition_variable>
#include <mutex>

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
    entry.Name = name ? name : TString(TStringBuilder() << type);
    return entry;
}

TVector<TSchemeEntry> CreateTables(const TString& directory, size_t count) {
    TVector<TSchemeEntry> tables;
    for (size_t i = 0; i < count; ++i) {
        tables.push_back(CreateSchemeEntry(ESchemeEntryType::Table, TStringBuilder() << directory << "/table_" << i));
    }
    return tables;
}

TStatus Success() {
    return TStatus(EStatus::SUCCESS, {});
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
    auto status = RemovePathsRecursive(entries.begin(), entries.end(), remover, TRemoveDirectoryRecursiveSettings().Threads(1));
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    std::swap(entries[1], entries[2]);
    UNIT_ASSERT_VALUES_EQUAL(removedEntries, entries);
}

Y_UNIT_TEST(BackupCollectionAfterChildren) {
    TVector<TSchemeEntry> entries = {
        CreateSchemeEntry(ESchemeEntryType::Table),
        CreateSchemeEntry(ESchemeEntryType::Directory),
        CreateSchemeEntry(ESchemeEntryType::BackupCollection),
    };

    TVector<TSchemeEntry> removedEntries;
    NInternal::TRemover remover = [&removedEntries](const TSchemeEntry& entry) {
        removedEntries.emplace_back(entry);
        return TStatus(EStatus::SUCCESS, {});
    };
    auto status = RemovePathsRecursive(entries.begin(), entries.end(), remover, TRemoveDirectoryRecursiveSettings().Threads(1));

    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(removedEntries, entries);
}

Y_UNIT_TEST(ChildrenBeforeParent) {
    TVector<TSchemeEntry> entries = {
        CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir/a"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir/a/b"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir/a/b/empty"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir/c"),
        CreateSchemeEntry(ESchemeEntryType::Topic, "/Root/dir/a/topic"),
        CreateSchemeEntry(ESchemeEntryType::ExternalDataSource, "/Root/dir/a/b/data_source"),
        CreateSchemeEntry(ESchemeEntryType::ExternalTable, "/Root/dir/c/external_table"),
        CreateSchemeEntry(ESchemeEntryType::ColumnStore, "/Root/dir/c/store"),
        CreateSchemeEntry(ESchemeEntryType::ColumnTable, "/Root/dir/c/store/column_table_1"),
        CreateSchemeEntry(ESchemeEntryType::ColumnTable, "/Root/dir/c/store/column_table_2"),
        CreateSchemeEntry(ESchemeEntryType::BackupCollection, "/Root/dir/c/collection"),
        CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir/c/collection/backup"),
        CreateSchemeEntry(ESchemeEntryType::Table, "/Root/dir/c/collection/backup/table"),
    };
    for (const auto& directory : {"/Root/dir", "/Root/dir/a", "/Root/dir/a/b", "/Root/dir/c"}) {
        for (auto& table : CreateTables(directory, 10)) {
            entries.push_back(std::move(table));
        }
    }

    std::mutex mutex;
    TSet<TString> notRemoved;
    for (const auto& entry : entries) {
        notRemoved.insert(TString(entry.Name));
    }
    TVector<TString> violations;
    NInternal::TRemover remover = [&](const TSchemeEntry& entry) {
        const TString name(entry.Name);
        {
            std::lock_guard lock(mutex);
            UNIT_ASSERT_C(notRemoved.contains(name), "removed twice: " << name);
            for (const auto& other : notRemoved) {
                if (other.StartsWith(name + "/")) {
                    violations.push_back(TStringBuilder() << name << " is removed before its descendant " << other);
                }
            }
            if (entry.Type == ESchemeEntryType::ExternalDataSource && notRemoved.contains("/Root/dir/c/external_table")) {
                violations.push_back(TStringBuilder() << name << " is removed before the dependent external table");
            }
        }
        // give other threads a chance to run at the same time
        Sleep(TDuration::MilliSeconds(1));
        std::lock_guard lock(mutex);
        notRemoved.erase(name);
        return Success();
    };

    const auto result = NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr, 16);

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_C(violations.empty(), JoinSeq("\n", violations));
    UNIT_ASSERT_C(notRemoved.empty(), "not removed: " << JoinSeq(", ", notRemoved));
}

Y_UNIT_TEST(UsesRequestedNumberOfThreads) {
    const ui32 threads = 4;
    const auto entries = CreateTables("/Root/dir", 20);

    std::mutex mutex;
    std::condition_variable allThreadsActive;
    ui32 active = 0;
    ui32 maxActive = 0;
    NInternal::TRemover remover = [&](const TSchemeEntry&) {
        std::unique_lock lock(mutex);
        ++active;
        maxActive = std::max(maxActive, active);
        allThreadsActive.notify_all();
        // the first calls wait for each other, so all threads become active at the same time
        allThreadsActive.wait_for(lock, std::chrono::seconds(30), [&] { return maxActive == threads; });
        --active;
        return Success();
    };

    const auto result = NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr, threads);

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(maxActive, threads);
}

Y_UNIT_TEST(PromptBeforeEveryRemovalIsSequential) {
    const auto entries = CreateTables("/Root/dir", 20);

    std::mutex mutex;
    ui32 active = 0;
    ui32 maxActive = 0;
    NInternal::TRemover remover = [&](const TSchemeEntry&) {
        {
            std::lock_guard lock(mutex);
            ++active;
            maxActive = std::max(maxActive, active);
        }
        Sleep(TDuration::MilliSeconds(1));
        std::lock_guard lock(mutex);
        --active;
        return Success();
    };

    const auto settings = TRemoveDirectoryRecursiveSettings()
        .Prompt(ERecursiveRemovePrompt::Always)
        .Threads(16);
    const auto result = RemovePathsRecursive(entries.begin(), entries.end(), remover, settings);

    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(maxActive, 1);
}

Y_UNIT_TEST(StopsOnError) {
    TVector<TSchemeEntry> entries = CreateTables("/Root/dir", 20);
    entries.push_back(CreateSchemeEntry(ESchemeEntryType::Directory, "/Root/dir"));

    std::mutex mutex;
    TSet<TString> removed;
    NInternal::TRemover remover = [&](const TSchemeEntry& entry) {
        if (entry.Name == "/Root/dir/table_5") {
            return TStatus(EStatus::SCHEME_ERROR, {});
        }
        std::lock_guard lock(mutex);
        removed.insert(TString(entry.Name));
        return Success();
    };

    const auto result = NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr, 4);

    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    UNIT_ASSERT(!removed.contains("/Root/dir"));
}

Y_UNIT_TEST(RethrowsRemoverException) {
    const auto entries = CreateTables("/Root/dir", 20);

    NInternal::TRemover remover = [&](const TSchemeEntry& entry) {
        if (entry.Name == "/Root/dir/table_5") {
            ythrow yexception() << "remover failed";
        }
        return Success();
    };

    UNIT_ASSERT_EXCEPTION_CONTAINS(
        NInternal::RemovePathsRecursive(entries.begin(), entries.end(), remover, nullptr, 4),
        yexception,
        "remover failed"
    );
}

}
