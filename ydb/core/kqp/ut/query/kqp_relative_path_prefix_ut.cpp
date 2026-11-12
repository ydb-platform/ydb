#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpRelativePathPrefix) {
    Y_UNIT_TEST(LiteralRelativePath) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto directory = kikimr.GetSchemeClient().MakeDirectory("/Root/folder").ExtractValueSync();
        UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());

        auto create = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/folder/items` (id Uint64 NOT NULL, PRIMARY KEY (id));
        )").ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

        auto write = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/folder/items` (id) VALUES (1u);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        auto read = session.ExecuteDataQuery(R"(
            PRAGMA RelativePathPrefix = "folder";
            SELECT id FROM items;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
        CompareYson("[[1u]]", FormatResultSetYson(read.GetResultSet(0)));
    }
}

} // namespace NKikimr::NKqp
