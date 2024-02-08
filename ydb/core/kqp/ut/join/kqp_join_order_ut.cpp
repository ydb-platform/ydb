#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

/**
 * A basic join order test. We define 5 tables sharing the same
 * key attribute and construct various full clique join queries
*/
static void CreateSampleTable(TSession session) {
    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/R` (
            id Int32 not null,
            payload1 String,
            ts Date,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/S` (
            id Int32 not null,
            payload2 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/T` (
            id Int32 not null,
            payload3 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/U` (
            id Int32 not null,
            payload4 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/V` (
            id Int32 not null,
            payload5 String,
            PRIMARY KEY (id)
        );
    )").GetValueSync().IsSuccess());

        UNIT_ASSERT(session.ExecuteDataQuery(R"(

        REPLACE INTO `/Root/R` (id, payload1, ts) VALUES
            (1, "blah", CAST("1998-12-01" AS Date) );

        REPLACE INTO `/Root/S` (id, payload2) VALUES
            (1, "blah");

        REPLACE INTO `/Root/T` (id, payload3) VALUES
            (1, "blah");

        REPLACE INTO `/Root/U` (id, payload4) VALUES
            (1, "blah");

        REPLACE INTO `/Root/V` (id, payload5) VALUES
            (1, "blah");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync().IsSuccess());

    UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
    CREATE TABLE `/Root/customer` (
    c_acctbal Double,
    c_address String,
    c_comment String,
    c_custkey Int32, -- Identifier
    c_mktsegment String ,
    c_name String ,
    c_nationkey Int32 , -- FK to N_NATIONKEY
    c_phone String ,
    PRIMARY KEY (c_custkey)
)
;

CREATE TABLE `/Root/lineitem` (
    l_comment String ,
    l_commitdate Date ,
    l_discount Double , -- it should be Decimal(12, 2)
    l_extendedprice Double , -- it should be Decimal(12, 2)
    l_linenumber Int32 ,
    l_linestatus String ,
    l_orderkey Int32 , -- FK to O_ORDERKEY
    l_partkey Int32 , -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity Double , -- it should be Decimal(12, 2)
    l_receiptdate Date ,
    l_returnflag String ,
    l_shipdate Date ,
    l_shipinstruct String ,
    l_shipmode String ,
    l_suppkey Int32 , -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax Double , -- it should be Decimal(12, 2)
    PRIMARY KEY (l_orderkey, l_linenumber)
)
;

CREATE TABLE `/Root/nation` (
    n_comment String ,
    n_name String ,
    n_nationkey Int32 , -- Identifier
    n_regionkey Int32 , -- FK to R_REGIONKEY
    PRIMARY KEY(n_nationkey)
)
;

CREATE TABLE `/Root/orders` (
    o_clerk String ,
    o_comment String ,
    o_custkey Int32 , -- FK to C_CUSTKEY
    o_orderdate Date ,
    o_orderkey Int32 , -- Identifier
    o_orderpriority String ,
    o_orderstatus String ,
    o_shippriority Int32 ,
    o_totalprice Double , -- it should be Decimal(12, 2)
    PRIMARY KEY (o_orderkey)
)
;

CREATE TABLE `/Root/part` (
    p_brand String ,
    p_comment String ,
    p_container String ,
    p_mfgr String ,
    p_name String ,
    p_partkey Int32 , -- Identifier
    p_retailprice Double , -- it should be Decimal(12, 2)
    p_size Int32 ,
    p_type String ,
    PRIMARY KEY(p_partkey)
)
;

CREATE TABLE `/Root/partsupp` (
    ps_availqty Int32 ,
    ps_comment String ,
    ps_partkey Int32 , -- FK to P_PARTKEY
    ps_suppkey Int32 , -- FK to S_SUPPKEY
    ps_supplycost Double , -- it should be Decimal(12, 2)
    PRIMARY KEY(ps_partkey, ps_suppkey)
)
;

CREATE TABLE `/Root/region` (
    r_comment String ,
    r_name String ,
    r_regionkey Int32 , -- Identifier
    PRIMARY KEY(r_regionkey)
)
;

CREATE TABLE `/Root/supplier` (
    s_acctbal Double , -- it should be Decimal(12, 2)
    s_address String ,
    s_comment String ,
    s_name String ,
    s_nationkey Int32 , -- FK to N_NATIONKEY
    s_phone String ,
    s_suppkey Int32 , -- Identifier
    PRIMARY KEY(s_suppkey)
)
;)").GetValueSync().IsSuccess());

}

static TKikimrRunner GetKikimrWithJoinSettings(){
    TVector<NKikimrKqp::TKqpSetting> settings;

    NKikimrKqp::TKqpSetting setting;
   
    setting.SetName("CostBasedOptimizationLevel");
    setting.SetValue("1");
    settings.push_back(setting);

    setting.SetName("OptEnableConstantFolding");
    setting.SetValue("true");
    settings.push_back(setting);

    return TKikimrRunner(settings);
}


Y_UNIT_TEST_SUITE(KqpJoinOrder) {
    Y_UNIT_TEST(FiveWayJoin) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
            )");

            auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            //NJson::TJsonValue plan;
            //NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            //Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FourWayJoinLeftFirst) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  LEFT JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
            )");

            auto result = session.ExecuteDataQuery(query,TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            //NJson::TJsonValue plan;
            //NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            //Cout << result.GetPlan();
        }
    }

     Y_UNIT_TEST(FiveWayJoinWithPreds) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithComplexPreds) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND ( S.payload2  || T.payload3 = U.payload4 )
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithComplexPreds2) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE (R.payload1 || V.payload5 = 'blah') AND U.payload4 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithPredsAndEquiv) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND R.id = 1
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FourWayJoinWithPredsAndEquivAndLeft) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  LEFT JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'blah' AND V.payload5 = 'blah' AND R.id = 1
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithConstantFold) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'bl' || 'ah' AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(FiveWayJoinWithConstantFoldOpt) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                  INNER JOIN
                     `/Root/S` as S
                  ON R.id = S.id
                  INNER JOIN
                     `/Root/T` as T
                  ON S.id = T.id
                  INNER JOIN
                     `/Root/U` as U
                  ON T.id = U.id
                  INNER JOIN
                     `/Root/V` as V
                  ON U.id = V.id
                WHERE R.payload1 = 'bl' || Cast(1 as String?) AND V.payload5 = 'blah'
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(DatetimeConstantFold) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
                SELECT *
                FROM `/Root/R` as R
                WHERE CAST(R.ts AS Timestamp) = (CAST('1998-12-01' AS Date) - Interval("P100D"))
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(TPCH21) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$n = select n_nationkey from `/Root/nation`
where n_name = 'EGYPT';

$s = select s_name, s_suppkey from `/Root/supplier` as supplier
join $n as nation
on supplier.s_nationkey = nation.n_nationkey;

$l = select l_suppkey, l_orderkey from `/Root/lineitem`
where l_receiptdate > l_commitdate;

$j1 = select s_name, l_suppkey, l_orderkey from $l as l1
join $s as supplier
on l1.l_suppkey = supplier.s_suppkey;

-- exists
$j2 = select l1.l_orderkey as l_orderkey, l1.l_suppkey as l_suppkey, l1.s_name as s_name, l2.l_receiptdate as l_receiptdate, l2.l_commitdate as l_commitdate from $j1 as l1
join `/Root/lineitem` as l2
on l1.l_orderkey = l2.l_orderkey
where l2.l_suppkey <> l1.l_suppkey;

$j2_1 = select s_name, l1.l_suppkey as l_suppkey, l1.l_orderkey as l_orderkey from $j1 as l1
left semi join $j2 as l2
on l1.l_orderkey = l2.l_orderkey;

-- not exists
$j2_2 = select l_orderkey from $j2 where l_receiptdate > l_commitdate;

$j3 = select s_name, l_suppkey, l_orderkey from $j2_1 as l1
left only join $j2_2 as l3
on l1.l_orderkey = l3.l_orderkey;

$j4 = select s_name from $j3 as l1
join `/Root/orders` as orders
on orders.o_orderkey = l1.l_orderkey
where o_orderstatus = 'F';

select s_name,
    count(*) as numwait from $j4
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;)");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

       Y_UNIT_TEST(TPCH5) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    o.o_orderkey as o_orderkey,
    o.o_orderdate as o_orderdate,
    c.c_nationkey as c_nationkey
from
    `/Root/customer` as c
join
    `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount,
    l.l_suppkey as l_suppkey
from
    $join1 as j
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j.o_orderkey
);

$join3 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    s.s_nationkey as s_nationkey
from
    $join2 as j
join
    `/Root/supplier` as s
on
    j.l_suppkey = s.s_suppkey
);
$join4 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    n.n_regionkey as n_regionkey,
    n.n_name as n_name
from
    $join3 as j
join
    `/Root/nation` as n
on
    j.s_nationkey = n.n_nationkey
    and j.c_nationkey = n.n_nationkey
);
$join5 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    j.n_regionkey as n_regionkey,
    j.n_name as n_name,
    r.r_name as r_name
from
    $join4 as j
join
    `/Root/region` as r
on
    j.n_regionkey = r.r_regionkey
);
$border = Date('1995-01-01');
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    $join5
where
    r_name = 'AFRICA'
    and CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < ($border + Interval('P365D'))
group by
    n_name
order by
    revenue desc;

            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(TPCH10) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(

-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1993-12-01");
$join1 = (
select
    c.c_custkey as c_custkey,
    c.c_name as c_name,
    c.c_acctbal as c_acctbal,
    c.c_address as c_address,
    c.c_phone as c_phone,
    c.c_comment as c_comment,
    c.c_nationkey as c_nationkey,
    o.o_orderkey as o_orderkey
from
    `/Root/customer` as c
join
    `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
where
    cast(o.o_orderdate as timestamp) >= $border and
    cast(o.o_orderdate as timestamp) < ($border + Interval("P90D"))
);
$join2 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount
from
    $join1 as j
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j.o_orderkey
where
    l.l_returnflag = 'R'
);
$join3 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    n.n_name as n_name
from
    $join2 as j
join
    `/Root/nation` as n
on
    n.n_nationkey = j.c_nationkey
);
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    $join3
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
            )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }
    }

    Y_UNIT_TEST(TPCH11) {

        auto kikimr = GetKikimrWithJoinSettings();
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSampleTable(session);

        /* join with parameters */
        {
            const TString query = Q_(R"(

-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    ps.ps_partkey as ps_partkey,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_availqty as ps_availqty,
    s.s_nationkey as s_nationkey
from
    `/Root/partsupp` as ps
join
    `/Root/supplier` as s
on
    ps.ps_suppkey = s.s_suppkey
);
$join2 = (
select
    j.ps_partkey as ps_partkey,
    j.ps_supplycost as ps_supplycost,
    j.ps_availqty as ps_availqty,
    j.s_nationkey as s_nationkey
from
    $join1 as j
join
    `/Root/nation` as n
on
    n.n_nationkey = j.s_nationkey
where
    n.n_name = 'CANADA'
);
$threshold = (
select
    sum(ps_supplycost * ps_availqty) * 0.0001000000 as threshold
from
    $join2
);
$values = (
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    $join2
group by
    ps_partkey
);

select
    v.ps_partkey as ps_partkey,
    v.value as value
from
    $values as v
cross join
    $threshold as t
where
    v.value > t.threshold
order by
    value desc;
    )");

            auto result = session.ExplainDataQuery(query).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            NJson::TJsonValue plan;
            NJson::ReadJsonTree(result.GetPlan(), &plan, true);
            Cout << result.GetPlan();
        }        
    }
}
}
}
