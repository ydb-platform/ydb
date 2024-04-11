#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;

void CreateStarSampleTable(TSession session, size_t cnt) {
    TString createCentralTable = "CREATE TABLE `/Root/table_0` (";

    for (size_t i = 0; i < cnt; ++i) {
        createCentralTable
            .append("id").append(ToString(i)).append( " Int32, ");
    }

    createCentralTable
        .append("PRIMARY KEY (id0));");

    UNIT_ASSERT(session.ExecuteSchemeQuery(createCentralTable).GetValueSync().IsSuccess());

    for (size_t i = 1; i < cnt; ++i) {
        TString createEdgeTable;
        createEdgeTable
            .append("CREATE TABLE `/Root/table_").append(ToString(i)).append("` (")
            .append("id").append(ToString(i)).append(" Int32, ")
            .append("PRIMARY KEY (id").append(ToString(i)).append(");");
        UNIT_ASSERT(session.ExecuteSchemeQuery(createEdgeTable).GetValue().IsSuccess());
    }
}

void ExecuteStarJoin(TSession session, size_t cnt) {
    TString joinString 
        = "SELECT * FROM `/Root/table_0` as t0";
    for (size_t i = 1; i < cnt; ++i) {
        joinString
            .append("JOIN `/Root/table_").append(ToString(i)).append("` as t").append(ToString(i)).append(" ")
            .append("ON t0.id = t").append(ToString(i)).append(".id");
    }

    UNIT_ASSERT(session.ExecuteSchemeQuery(joinString).GetValue().IsSuccess());
}

static TKikimrRunner GetKikimrWithJoinSettings(){
    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;
   
    setting.SetName("CostBasedOptimizationLevel");
    setting.SetValue("2");
    settings.push_back(setting);

    setting.SetName("OptEnableConstantFolding");
    setting.SetValue("true");
    settings.push_back(setting);

    return TKikimrRunner(settings);
}

Y_UNIT_TEST_SUITE(Lmao) {
    Y_UNIT_TEST(StarJoin) {
        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateStarSampleTable(session, 15);
        ExecuteStarJoin(session, 15);
    }
}