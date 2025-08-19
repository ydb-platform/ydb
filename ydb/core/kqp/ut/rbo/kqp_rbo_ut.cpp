#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/interface/codec.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <util/system/env.h>

#include <ctime>


extern "C" {
#include "catalog/pg_type_d.h"
}

namespace {
    struct TPgTypeTestSpec {
        ui32 TypeId;
        bool IsKey;
        std::function<TString(size_t)> TextIn, TextOut;
        std::function<TString(TString)> ArrayPrint = [] (auto s) { return Sprintf("{%s,%s}", s.c_str(), s.c_str()); };
    };

    struct TPgTypeCoercionTestSpec {
        ui32 TypeId;
        TString TypeMod;
        bool ShouldPass;
        std::function<TString()> TextIn, TextOut;
        std::function<TString(TString)> ArrayPrint = [] (auto s) { return Sprintf("{%s,%s}", s.c_str(), s.c_str()); };
    };
}

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

double TimeQuery(NKikimr::NKqp::TKikimrRunner& kikimr, TString query, int nIterations) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    clock_t the_time;
    double elapsed_time;
    the_time = clock();

    for (int i=0; i<nIterations; i++) {
        //session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        session.ExplainDataQuery(query).GetValueSync();
    }

    elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
    return elapsed_time / nIterations;        
}

double TimeQuery(TString schema, TString query, int nIterations) {
    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
    TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    session.ExecuteSchemeQuery(schema).GetValueSync();

    clock_t the_time;
    double elapsed_time;
    the_time = clock();

    for (int i=0; i<nIterations; i++) {
        //session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        session.ExplainDataQuery(query).GetValueSync();
    }

    elapsed_time = double(clock() - the_time) / CLOCKS_PER_SEC;
    return elapsed_time / nIterations;        
}

Y_UNIT_TEST_SUITE(KqpRbo) {

    Y_UNIT_TEST(Select) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_pg
            SELECT 1 as "a", 2 as "b";
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Filter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,	
	            name	String,
                primary key(id)	
            );
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT id as "id2" FROM foo WHERE name = 'some_name';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CrossFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,	
	            name	String,
                primary key(id)	
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,	
	            lastname	String,
                primary key(id)	
            );
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE name = 'some_name';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(PredicatePushdownLeftJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
	            b String,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT t1.a FROM t1 left join t2 on t1.a = t2.a where t2.b = 'some_string';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT t1.a FROM t1 left join t2 on t1.a = t2.a where t2.b IS NULL;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(LeftJoinToKqpOpJoin) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
	            b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
	            b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t3` (
                a Int64 NOT NULL,
	            b String,
                c Int64,
                primary key(a)
            );

            CREATE TABLE `/Root/t4` (
                a Int64 NOT NULL,
	            b String,
                c Int64,
                primary key(a)
            );
        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT t1.a FROM t1 left join t2 on t1.a = t2.a where t2.b = 'some_string';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT t1.a FROM t1 left join t2 on t1.a = t2.a left join t3 on t2.a = t3.a where t2.b = 'some_string';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT t1.a FROM t1 left join t2 on t1.a = t2.a left join t3 on t2.a = t3.a left join t4 on t3.a = t4.a and t4.c = t2.c and t1.c = t4.c where t2.b = 'some_string';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(JoinFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,	
	            name	String,
                primary key(id)	
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,	
	            lastname	String,
                primary key(id)	
            );

        )").GetValueSync();

        db = kikimr.GetTableClient();
        auto session2 = db.CreateSession().GetValueSync().GetSession();

        auto result = session2.ExecuteDataQuery(R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE f.id = bar.id and name = 'some_name';
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Bench_Select) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));

        auto time = TimeQuery(kikimr, R"(
                --!syntax_pg
                SELECT 1 as "a", 2 as "b";
            )", 10);

        Cout << "Time per query: " << time;     

        //UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Bench_Filter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,	
	            name	String,
                primary key(id)	
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT id as "id2" FROM foo WHERE name = 'some_name';
        )",10);

        Cout << "Time per query: " << time;     
    }

    Y_UNIT_TEST(Bench_CrossFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,	
	            name	String,
                primary key(id)	
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,	
	            lastname	String,
                primary key(id)	
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE name = 'some_name';
        )", 10);

        Cout << "Time per query: " << time;     
    }

    Y_UNIT_TEST(Bench_JoinFilter) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/foo` (
                id	Int64	NOT NULL,	
	            name	String,
                primary key(id)	
            );

            CREATE TABLE `/Root/bar` (
                id	Int64	NOT NULL,	
	            lastname	String,
                primary key(id)	
            );
        )").GetValueSync();

        auto time = TimeQuery(kikimr, R"(
            --!syntax_pg
            SET TablePathPrefix = "/Root/";
            SELECT f.id as "id2" FROM foo AS f, bar WHERE f.id = bar.id and name = 'some_name';
        )", 10);

        Cout << "Time per query: " << time;     
    }

    Y_UNIT_TEST(Bench_10Joins) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        TKikimrRunner kikimr(NKqp::TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schema = R"(
CREATE TABLE `/Root/foo_0` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_1` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_2` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_3` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_4` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_5` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_6` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_7` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_8` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    

    CREATE TABLE `/Root/foo_9` (
    id Int64 NOT NULL,
    join_id Int64,
    primary key(id)
    );
    )";

        auto query = R"(
            --!syntax_pg
     SET TablePathPrefix = "/Root/";

     SELECT foo_0.id as "id2"
     FROM foo_0, foo_1, foo_2, foo_3, foo_4, foo_5, foo_6, foo_7, foo_8, foo_9
     WHERE foo_0.join_id = foo_1.id AND foo_0.join_id = foo_2.id AND foo_0.join_id = foo_3.id AND foo_0.join_id = foo_4.id AND foo_0.join_id = foo_5.id AND foo_0.join_id = foo_6.id AND foo_0.join_id = foo_7.id AND foo_0.join_id = foo_8.id AND foo_0.join_id = foo_9.id;
     
    )";

        auto time = TimeQuery(schema, query, 10);

        Cout << "Time per query: " << time;     
    }
}

} // namespace NKqp
} // namespace NKikimr
