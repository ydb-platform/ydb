#include <ydb/core/tx/schemeshard/schemeshard_database_relative_path.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

namespace {

TString ValueOf(const TConclusion<TDatabaseRelativePath>& conclusion) {
    UNIT_ASSERT_C(conclusion.IsSuccess(),
        conclusion.IsFail() ? conclusion.GetErrorMessage() : TString("unexpected failure"));
    return TString(conclusion.GetResult().Value());
}

} // namespace

Y_UNIT_TEST_SUITE(TDatabaseRelativePathTest) {

Y_UNIT_TEST(RootMapsToSlash) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Db");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/");
    UNIT_ASSERT(conclusion.GetResult().IsRoot());
}

Y_UNIT_TEST(NestedPath) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Db/dir/table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/table");
    UNIT_ASSERT(!conclusion.GetResult().IsRoot());
}

Y_UNIT_TEST(DirectChild) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Db/table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/table");
}

Y_UNIT_TEST(TrailingSeparatorCanonicalizes) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Db/dir/table/");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/table");
}

Y_UNIT_TEST(RepeatedSeparatorsCanonicalize) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Db//dir///table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/table");
}

Y_UNIT_TEST(SiblingDatabaseIsNotInside) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Db2/table");
    UNIT_ASSERT(conclusion.IsFail());
}

Y_UNIT_TEST(PathOutsideDatabaseRejected) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db", "/Root/Other/table");
    UNIT_ASSERT(conclusion.IsFail());
}

Y_UNIT_TEST(FromWorkingDirAndNameJoins) {
    auto conclusion = TDatabaseRelativePath::FromWorkingDirAndName("/Root/Db", "/Root/Db/dir", "table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/table");
}

Y_UNIT_TEST(FromWorkingDirAndNameForNonexistentPath) {
    auto conclusion = TDatabaseRelativePath::FromWorkingDirAndName("/Root/Db", "/Root/Db/dir", "newtable");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/newtable");
}

Y_UNIT_TEST(FromWorkingDirAndNameHandlesTrailingSlashWorkingDir) {
    auto conclusion = TDatabaseRelativePath::FromWorkingDirAndName("/Root/Db", "/Root/Db/dir/", "table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/table");
}

// A create may carry a relative path rather than a leaf -- MkDir "DirB/DirC" under
// /MyRoot/DirA -- so a name with a separator is split into components, not rejected.
// This mirrors what SplitIntoTransactions does to the WorkingDir/Name boundary.
Y_UNIT_TEST(NameWithSeparatorSplitsIntoComponents) {
    auto conclusion = TDatabaseRelativePath::FromWorkingDirAndName("/Root/Db", "/Root/Db/dir", "sub/table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/sub/table");
}

// The database root is not inside a deeper database. Guards the component walk against
// underflowing when the candidate is an ancestor rather than a descendant.
Y_UNIT_TEST(PathAboveDatabaseRejected) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db/sub", "/Root/Db");
    UNIT_ASSERT(conclusion.IsFail());
}

// The database root itself arrives from the wire and may be non-canonical.
Y_UNIT_TEST(DatabaseRootWithTrailingSlash) {
    auto conclusion = TDatabaseRelativePath::FromAbsolute("/Root/Db/", "/Root/Db/dir/table");
    UNIT_ASSERT_VALUES_EQUAL(ValueOf(conclusion), "/dir/table");
}

}
