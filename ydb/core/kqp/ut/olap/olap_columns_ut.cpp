#include "helpers/get_value.h"
#include "helpers/local.h"
#include "helpers/query_executor.h"
#include "helpers/typed_local.h"
#include "helpers/writer.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(OlapTestt) {
    Y_UNIT_TEST(TableSinkWithOlapStore) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/test_table`
            (
                WatchID Int64 NOT NULL,
                CounterID Int32 NOT NULL,
                'URL LLL' Text NOT NULL,
                Age Int16 NOT NULL,
                Sex Int16 NOT NULL,
                PRIMARY KEY (CounterID, WatchID)
            )
            PARTITION BY HASH(WatchID)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), true, result.GetIssues().ToString());
        
    }
}

}   // namespace NKikimr::NKqp