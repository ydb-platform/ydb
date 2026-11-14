#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

TVector<TString> IndexImplTablesFromDirectory(
    const NScheme::TListDirectoryResult& list,
    const NTable::TIndexDescription& index);

} // namespace NYdb::NConsoleClient

namespace NYdb::NConsoleClient {
namespace {

using namespace NScheme;
using namespace NTable;

TListDirectoryResult MakeList(EStatus status, std::vector<TSchemeEntry> children = {}) {
    return TListDirectoryResult(TStatus(status, NYdb::NIssue::TIssues{}), TSchemeEntry{}, std::move(children));
}

TSchemeEntry Child(const char* name, ESchemeEntryType type) {
    TSchemeEntry entry;
    entry.Name = name;
    entry.Type = type;
    return entry;
}

Y_UNIT_TEST_SUITE(ListObjectsIndexImplTables) {

Y_UNIT_TEST(DirectoryReadErrorIsNotAnEmptyListing) {
    const TIndexDescription index("idx", EIndexType::GlobalSync, {"id"});
    bool thrown = false;
    try {
        IndexImplTablesFromDirectory(MakeList(EStatus::UNAVAILABLE), index);
    } catch (const NStatusHelpers::TYdbErrorException&) {
        thrown = true;
    }
    UNIT_ASSERT(thrown);
}

Y_UNIT_TEST(EmptyDirectoryUsesAssumedNames) {
    const TIndexDescription sync("idx", EIndexType::GlobalSync, {"id"});
    const TVector<TString> syncNames = IndexImplTablesFromDirectory(MakeList(EStatus::SUCCESS), sync);
    UNIT_ASSERT_VALUES_EQUAL(syncNames.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(syncNames[0], "indexImplTable");

    const TIndexDescription vector("vec", EIndexType::GlobalVectorKMeansTree, {"embedding"});
    const TVector<TString> vectorNames = IndexImplTablesFromDirectory(MakeList(EStatus::SUCCESS), vector);
    UNIT_ASSERT_VALUES_EQUAL(vectorNames.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(vectorNames[0], "indexImplLevelTable");
    UNIT_ASSERT_VALUES_EQUAL(vectorNames[1], "indexImplPostingTable");

    const TIndexDescription prefixed("vec", EIndexType::GlobalVectorKMeansTree, {"tenant", "embedding"});
    const TVector<TString> prefixedNames = IndexImplTablesFromDirectory(MakeList(EStatus::SUCCESS), prefixed);
    UNIT_ASSERT_VALUES_EQUAL(prefixedNames.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(prefixedNames[2], "indexImplPrefixTable");
}

Y_UNIT_TEST(ListedTablesAreUsedAsIs) {
    const TIndexDescription index("idx", EIndexType::GlobalSync, {"id"});
    std::vector<TSchemeEntry> children = {
        Child("indexImplPostingTable", ESchemeEntryType::Table),
        Child("indexImplTable0build", ESchemeEntryType::Table),
        Child("indexImplTable1build", ESchemeEntryType::Table),
        Child("docsrowidsrc", ESchemeEntryType::Table),
        Child("notes", ESchemeEntryType::Directory),
        Child("indexImplLevelTable", ESchemeEntryType::Unknown),
    };
    const TVector<TString> names = IndexImplTablesFromDirectory(MakeList(EStatus::SUCCESS, std::move(children)), index);
    UNIT_ASSERT_VALUES_EQUAL(names.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(names[0], "indexImplPostingTable");
    UNIT_ASSERT_VALUES_EQUAL(names[1], "indexImplLevelTable");
}

Y_UNIT_TEST(OnlyTransientChildrenFallBackToAssumedNames) {
    const TIndexDescription index("idx", EIndexType::GlobalSync, {"id"});
    std::vector<TSchemeEntry> children = {
        Child("indexImplTable0build", ESchemeEntryType::Table),
    };
    const TVector<TString> names = IndexImplTablesFromDirectory(MakeList(EStatus::SUCCESS, std::move(children)), index);
    UNIT_ASSERT_VALUES_EQUAL(names.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(names[0], "indexImplTable");
}

} // Y_UNIT_TEST_SUITE(ListObjectsIndexImplTables)

} // namespace
} // namespace NYdb::NConsoleClient
