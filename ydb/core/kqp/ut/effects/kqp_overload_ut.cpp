#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/scope.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpOverload) {

    Y_UNIT_TEST(OverloadedImmediate) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);

        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto writeSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        Y_UNUSED(runtime);
    }
}
}
}
