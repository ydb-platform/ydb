#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/grpc.pb.h>
#include <ydb/core/protos/grpc.grpc.pb.h>
#include <ydb/core/kqp/tests/tpch/lib/tpch_runner.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>


namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

class KqpTpch : public NUnitTest::TTestBase {
public:
    void SetUp() override {
        TFileInput e{"ydb_endpoint.txt"};
        TString endpoint = e.ReadLine();

        TFileInput d{"ydb_database.txt"};
        Database = d.ReadLine();

        Driver.Reset(new TDriver(TDriverConfig().SetEndpoint(endpoint)));
        Tpch.Reset(new NTpch::TTpchRunner(*Driver, Database));

        if (!InitDone) {
            ConfigureTableService(endpoint);

            Tpch->UploadBundledData(2, false);

            InitDone = true;
        }
    }

    void TearDown() override {
        Tpch.Destroy();
        Driver->Stop(true);
    }

    void ConfigureTableService(const TString& endpoint) {
        NYdbGrpc::TGRpcClientLow grpcClient;
        auto grpcContext = grpcClient.CreateContext();

        NYdbGrpc::TGRpcClientConfig grpcConfig{endpoint};
        auto grpc = grpcClient.CreateGRpcServiceConnection<NKikimrClient::TGRpcServer>(grpcConfig);

        NKikimrClient::TConsoleRequest request;
        auto* action = request.MutableConfigureRequest()->MutableActions()->Add();
        auto* configItem = action->MutableAddConfigItem()->MutableConfigItem();
        configItem->SetKind(NKikimrConsole::TConfigItem::TableServiceConfigItem);
        auto* rm = configItem->MutableConfig()->MutableTableServiceConfig()->MutableResourceManager();
        rm->SetChannelBufferSize(10ul << 20);
        rm->SetMkqlLightProgramMemoryLimit(100ul << 20);
        rm->SetMkqlHeavyProgramMemoryLimit(100ul << 20);
        rm->SetQueryMemoryLimit(20ul << 30);
        rm->SetPublishStatisticsIntervalSec(0);

        TAtomic done = 0;
        grpc->DoRequest<NKikimrClient::TConsoleRequest, NKikimrClient::TConsoleResponse>(
            request,
            [&done](NYdbGrpc::TGrpcStatus&& status, NKikimrClient::TConsoleResponse&& response) {
                if (status.Ok()) {
                    if (response.GetStatus().code() != Ydb::StatusIds::SUCCESS) {
                        AtomicSet(done, 3);
                        return;
                    }
                    if (response.GetConfigureResponse().GetStatus().code() != Ydb::StatusIds::SUCCESS) {
                        AtomicSet(done, 4);
                        return;
                    }
                    AtomicSet(done, 1);
                } else {
                    Cerr << "status: {" << status.Msg << ", " << status.InternalError << ", "
                         << status.GRpcStatusCode << "}" << Endl;
                    Cerr << response.Utf8DebugString() << Endl;
                    AtomicSet(done, 2);
                }
            },
            &NKikimrClient::TGRpcServer::Stub::AsyncConsoleRequest,
            {},
            grpcContext.get());

        while (AtomicGet(done) == 0) {
            ::Sleep(TDuration::Seconds(1));
        }
        grpcContext.reset();
        grpcClient.Stop(true);

        UNIT_ASSERT_EQUAL(done, 1);
    }

    UNIT_TEST_SUITE(KqpTpch);
    UNIT_TEST(Query01);
    UNIT_TEST(Query02);
    UNIT_TEST(Query03);
    UNIT_TEST(Query04);
    UNIT_TEST(Query05);
    UNIT_TEST(Query06);
    UNIT_TEST(Query07);
    UNIT_TEST(Query08);
    UNIT_TEST(Query09);
    UNIT_TEST(Query10);
    UNIT_TEST(Query11);
    UNIT_TEST(Query12);
    UNIT_TEST(Query13);
    UNIT_TEST(Query14);
    UNIT_TEST(Query15);
    UNIT_TEST(Query16);
    UNIT_TEST(Query17);
    UNIT_TEST(Query18);
    UNIT_TEST(Query19);
    UNIT_TEST(Query20);
    UNIT_TEST(Query21);
    UNIT_TEST(Query22);
    UNIT_TEST_SUITE_END();

private:
    void Query01();
    void Query02();
    void Query03();
    void Query04();
    void Query05();
    void Query06();
    void Query07();
    void Query08();
    void Query09();
    void Query10();
    void Query11();
    void Query12();
    void Query13();
    void Query14();
    void Query15();
    void Query16();
    void Query17();
    void Query18();
    void Query19();
    void Query20();
    void Query21();
    void Query22();

private:
    TString Database;
    THolder<TDriver> Driver;
    THolder<NTpch::TTpchRunner> Tpch;
    bool InitDone = false;
};
UNIT_TEST_SUITE_REGISTRATION(KqpTpch);


static TValueParser&& DropOptional(TValueParser&& parser) {
    if (parser.GetKind() == TTypeParser::ETypeKind::Optional) {
        parser.OpenOptional();
    }
    return std::move(parser);
}

#define UNWRAP_VALUE(parser, column, type) DropOptional(TValueParser{parser.GetValue(#column)}).Get ## type ()
#define UNWRAP_UTF8(parser, column) UNWRAP_VALUE(parser, column, Utf8)
#define UNWRAP_STRING(parser, column) UNWRAP_VALUE(parser, column, String)
#define UNWRAP_DOUBLE(parser, column) UNWRAP_VALUE(parser, column, Double)
#define UNWRAP_UINT16(parser, column) UNWRAP_VALUE(parser, column, Uint16)
#define UNWRAP_INT32(parser, column) UNWRAP_VALUE(parser, column, Int32)
#define UNWRAP_UINT32(parser, column) UNWRAP_VALUE(parser, column, Uint32)
#define UNWRAP_INT64(parser, column) UNWRAP_VALUE(parser, column, Int64)
#define UNWRAP_UINT64(parser, column) UNWRAP_VALUE(parser, column, Uint64)
#define UNWRAP_DATE(parser, column) UNWRAP_VALUE(parser, column, Date)

#define AS_DATE(date) TInstant::ParseIso8601( #date )

static constexpr double PRESICION = 1e-6;

#define ASSERT_RESULT(it, result) \
    do { \
        Cerr << "-- result --" << Endl; \
        size_t rowIndex = 0; \
        for (;;) { \
            Cerr << "rowIndex: " << rowIndex << Endl; \
            auto streamPart = it.ReadNext().GetValueSync(); \
            if (!streamPart.IsSuccess()) { \
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString()); \
                break; \
            } \
            if (streamPart.HasResultSet()) { \
                NYdb::TResultSetParser parser{streamPart.GetResultSet()}; \
                while (parser.TryNextRow()) { \
                    result[rowIndex].AssertEquals(parser); \
                    ++rowIndex; \
                } \
            } \
        } \
        UNIT_ASSERT_EQUAL_C(result.size(), rowIndex, "expected " << result.size() << " rows, got " << rowIndex); \
    } while (0)

void KqpTpch::Query01() {
    auto it = Tpch->RunQuery01();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString l_returnflag;
        TString l_linestatus;
        double sum_qty = 0;
        double sum_base_price = 0;
        double sum_disc_price = 0;
        double sum_charge = 0;
        double avg_qty = 0;
        double avg_price = 0;
        double avg_disc = 0;
        ui64 count_order = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL(UNWRAP_UTF8(parser, l_returnflag), l_returnflag);
            UNIT_ASSERT_EQUAL(UNWRAP_UTF8(parser, l_linestatus), l_linestatus);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, sum_qty), sum_qty, PRESICION);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, sum_base_price), sum_base_price, PRESICION);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, sum_disc_price), sum_disc_price, PRESICION);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, sum_charge), sum_charge, PRESICION);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, avg_qty), avg_qty, PRESICION);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, avg_price), avg_price, PRESICION);
            UNIT_ASSERT_DOUBLES_EQUAL(UNWRAP_DOUBLE(parser, avg_disc), avg_disc, PRESICION);
            UNIT_ASSERT_EQUAL(UNWRAP_UINT64(parser, count_order), count_order);
        }
    };

    TVector<TResultRow> result = {
        {"A", "F", 37474.00, 37569624.64, 35676192.0970, 37101416.222424, 25.3545331529093369, 25419.231826792963, 0.05086603518267929635, 1478},
        {"N", "F", 1041.00,  1041301.07,  999060.8980,   1036450.802280,  27.3947368421052632, 27402.659736842105, 0.04289473684210526316, 38},
        {"N", "O", 77372.00, 77592631.43, 73758104.0931, 76702028.450392, 25.5184696569920844, 25591.237279023747, 0.04971635883905013193, 3032},
        {"R", "F", 36511.00, 36570841.24, 34738472.8758, 36169060.112193, 25.0590253946465340, 25100.096938915580, 0.05002745367192862045, 1457}
    };

    ASSERT_RESULT(it, result);
}

#define TEST_GENERIC_IMPL(number) \
    void KqpTpch::Query ## number () { \
        auto it = Tpch->RunQuery(1##number - 100); \
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString()); \
        size_t rowIndex = 0; \
        for (;;) { \
            auto streamPart = it.ReadNext().GetValueSync(); \
            if (!streamPart.IsSuccess()) { \
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString()); \
                break; \
            } \
            auto result = streamPart.ExtractPart(); \
            auto columns = result.GetColumnsMeta(); \
            NYdb::TResultSetParser parser{result}; \
            while (parser.TryNextRow()) { \
                for (size_t i = 0; i < columns.size(); ++i) { \
                    if (i == 0) { \
                        Cerr << "#" << rowIndex << ": "; \
                    } else { \
                        Cerr << ", "; \
                    } \
                    auto value = parser.GetValue(i); \
                    TValueParser vp(value); \
                    bool opt = false; \
                    if (vp.GetKind() == TTypeParser::ETypeKind::Optional) { \
                        vp.OpenOptional(); \
                        opt = true; \
                    } \
                    if (vp.GetKind() == TTypeParser::ETypeKind::Primitive && vp.GetPrimitiveType() == EPrimitiveType::Date) { \
                        Cerr << columns[i].Name << ": " << (opt ? "[" : "") << vp.GetDate().FormatGmTime("%Y-%m-%d") << (opt ? "]" : ""); \
                    } else { \
                        Cerr << columns[i].Name << ": " << FormatValueYson(value); \
                    } \
                } \
                Cerr << Endl; \
                ++rowIndex; \
            } \
        } \
    }

void KqpTpch::Query02() {
    auto it = Tpch->RunQuery02();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        double s_acctbal = 0;
        TString s_name;
        TString n_name;
        ui32 p_partkey = 0;
        TString p_mfgr;
        TString s_address;
        TString s_phone;
        TString s_comment;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, s_acctbal), s_acctbal, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, s_acctbal) << " != " << s_acctbal);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_name), s_name,
                    ": " << UNWRAP_UTF8(parser, s_name) << " != " << s_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, n_name), n_name,
                    ": " << UNWRAP_UTF8(parser, n_name) << " != " << n_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, p_partkey), p_partkey,
                    ": " << UNWRAP_UINT32(parser, p_partkey) << " != " << p_partkey);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, p_mfgr), p_mfgr,
                    ": " << UNWRAP_UTF8(parser, p_mfgr) << " != " << p_mfgr);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_address), s_address,
                    ": " << UNWRAP_UTF8(parser, s_address) << " != " << s_address);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_phone), s_phone,
                    ": " << UNWRAP_UTF8(parser, s_phone) << " != " << s_phone);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_comment), s_comment,
                    ": " << UNWRAP_UTF8(parser, s_comment) << " != " << s_comment);
        }
    };

    TVector<TResultRow> result = {
        {6820.35, "Supplier#000000007", "UNITED KINGDOM", 55, "Manufacturer#2", "s,4TicNGB4uO6PaSqNBUq", "33-990-965-2201", "s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit"}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query03() {
    auto it = Tpch->RunQuery03();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        ui32 l_orderkey = 0;
        double revenue = 0;
        TInstant o_orderdate;
        i32 o_shippriority = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, l_orderkey), l_orderkey,
                    ": " << UNWRAP_UINT32(parser, l_orderkey) << " != " << l_orderkey);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, revenue), revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, revenue) << " != " << revenue);
            UNIT_ASSERT_EQUAL_C(UNWRAP_DATE(parser, o_orderdate).Days(), o_orderdate.Days(),
                    ": " << UNWRAP_DATE(parser, o_orderdate) << " != " << o_orderdate);
            UNIT_ASSERT_EQUAL_C(UNWRAP_INT32(parser, o_shippriority), o_shippriority,
                    ": " << UNWRAP_INT32(parser, o_shippriority) << " != " << o_shippriority);
        }
    };

    TVector<TResultRow> result = {
        {4960, 145461.147,  AS_DATE(1995-02-26), 0},
        {3814, 125940.863,  AS_DATE(1995-02-22), 0},
        {2053, 121426.6978, AS_DATE(1995-02-07), 0},
        {4134, 121167.5858, AS_DATE(1995-01-12), 0},
        {4227, 87250.2119,  AS_DATE(1995-02-24), 0},
        {1830, 79553.5194,  AS_DATE(1995-02-23), 0},
        {5312, 61757.3752,  AS_DATE(1995-02-24), 0},
        {4707, 57177.8158,  AS_DATE(1995-02-27), 0},
        {3330, 42826.931,   AS_DATE(1994-12-19), 0},
        {3110, 29371.8645,  AS_DATE(1994-12-17), 0}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query04() {
    auto it = Tpch->RunQuery04();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString o_orderpriority;
        ui64 order_count = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, o_orderpriority), o_orderpriority,
                    ": " << UNWRAP_UTF8(parser, o_orderpriority) << " != " << o_orderpriority);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT64(parser, order_count), order_count,
                    ": " << UNWRAP_UINT64(parser, order_count) << " != " << order_count);
        }
    };

    TVector<TResultRow> result = {
        {"1-URGENT", 27},
        {"2-HIGH", 28},
        {"3-MEDIUM", 33},
        {"4-NOT SPECIFIED", 30},
        {"5-LOW", 45}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query05() {
    auto it = Tpch->RunQuery05();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString n_name;
        double revenue = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, n_name), n_name,
                    ": " << UNWRAP_UTF8(parser, n_name) << " != " << n_name);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, revenue), revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, revenue) << " != " << revenue);
        }
    };

    TVector<TResultRow> result = {
        {"MOROCCO",  127381.751},
        {"ETHIOPIA", 58323.2012}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query06() {
    auto it = Tpch->RunQuery06();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        double revenue = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, revenue), revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, revenue) << " != " << revenue);
        }
    };

    TVector<TResultRow> result = {
        {111758.1314}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query07() {
    auto it = Tpch->RunQuery07();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString supp_nation;
        TString cust_nation;
        ui32 l_year = 0;
        double revenue = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, supp_nation), supp_nation,
                    ": " << UNWRAP_UTF8(parser, supp_nation) << " != " << supp_nation);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, cust_nation), cust_nation,
                    ": " << UNWRAP_UTF8(parser, cust_nation) << " != " << cust_nation);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT16(parser, l_year), l_year,
                    ": " << UNWRAP_UINT16(parser, l_year) << " != " << l_year);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, revenue), revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, revenue) << " != " << revenue);
        }
    };

    TVector<TResultRow> result = {
        {"ETHIOPIA", "CANADA",  1995, 44157.1086},
        {"ETHIOPIA", "CANADA",  1996, 265995.1567},
        {"IRAN",     "ALGERIA", 1995, 162547.627},
        {"IRAN",     "ALGERIA", 1996, 95093.3287}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query08() {
    auto it = Tpch->RunQuery08();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        ui32 o_year = 0;
        double mkt_share = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT16(parser, o_year), o_year,
                    ": " << UNWRAP_UINT16(parser, o_year) << " != " << o_year);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, mkt_share), mkt_share, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, mkt_share) << " != " << mkt_share);
        }
    };

    TVector<TResultRow> result = {
        {1993, 1},
        {1995, 1},
        {1996, 0},
        {1997, 0}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query09() {
    auto it = Tpch->RunQuery09();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString nation;
        ui32 o_year = 0;
        double sum_profit = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, nation), nation,
                    ": " << UNWRAP_UTF8(parser, nation) << " != " << nation);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT16(parser, o_year), o_year,
                    ": " << UNWRAP_UINT16(parser, o_year) << " != " << o_year);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, sum_profit), sum_profit, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, sum_profit) << " != " << sum_profit);
        }
    };

    TVector<TResultRow> result = {
        {"ARGENTINA", 1998, 35655.2436},
        {"ARGENTINA", 1997, 65.224},
        {"ARGENTINA", 1996, 40372.8496},
        {"ARGENTINA", 1995, 76727.2035},
        {"ARGENTINA", 1994, 33573.9354},
        {"ARGENTINA", 1993, 78199.6228},
        {"ARGENTINA", 1992, 28918.1563},
        {"ETHIOPIA",  1998, 5508.114},
        {"ETHIOPIA",  1997, 38724.1072},
        {"ETHIOPIA",  1996, 6014.9643}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query10() {
    auto it = Tpch->RunQuery10();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        ui32 c_custkey = 0;
        TString c_name;
        double revenue = 0;
        double c_acctbal = 0;
        TString n_name;
        TString c_address;
        TString c_phone;
        TString c_comment;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, c_custkey), c_custkey,
                    ": " << UNWRAP_UINT32(parser, c_custkey) << " != " << c_custkey);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, c_name), c_name,
                    ": " << UNWRAP_UTF8(parser, c_name) << " != " << c_name);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, revenue), revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, revenue) << " != " << revenue);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, c_acctbal), c_acctbal, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, c_acctbal) << " != " << c_acctbal);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, n_name), n_name,
                    ": " << UNWRAP_UTF8(parser, n_name) << " != " << n_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, c_address), c_address,
                    ": " << UNWRAP_UTF8(parser, c_address) << " != " << c_address);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, c_phone), c_phone,
                    ": " << UNWRAP_UTF8(parser, c_phone) << " != " << c_phone);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, c_comment), c_comment,
                    ": " << UNWRAP_UTF8(parser, c_comment) << " != " << c_comment);
        }
    };

    TVector<TResultRow> result = {
        {43,  "Customer#000000043", 157641.557,  9904.28, "ROMANIA",      "ouSbjHk8lh5fKX3zGso3ZSIj9Aa3PoaFd", "29-316-665-2897", "ial requests: carefully pending foxes detect quickly. carefully final courts cajole quickly. carefully"},
        {80,  "Customer#000000080", 151534.6602, 7383.53, "ALGERIA",      "K,vtXp8qYB ",                       "10-267-172-7101", "tect among the dependencies. bold accounts engage closely even pinto beans. ca"},
        {94,  "Customer#000000094", 145771.9832, 5500.11, "INDONESIA",    "IfVNIN9KtkScJ9dUjK3Pg5gY1aFeaXewwf", "19-953-499-8833", "latelets across the bold, final requests sleep according to the fluffily bold accounts. unusual deposits amon"},
        {70,  "Customer#000000070", 123700.08,   4867.52, "RUSSIA",       "mFowIuhnHjp2GjCiYYavkW kUwOjIaTCQ", "32-828-107-2832", "fter the special asymptotes. ideas after the unusual frets cajole quickly regular pinto be"},
        {2,   "Customer#000000002", 121173.0081, 121.65,  "JORDAN",       "XSTf4,NCwDVaWNe6tEgvwfmRchLXak", "23-768-687-3665", "l accounts. blithely ironic theodolites integrate boldly: caref"},
        {109, "Customer#000000109", 116304.0452, -716.10, "MOZAMBIQUE",   "OOOkYBgCMzgMQXUmkocoLb56rfrdWp2NE2c", "26-992-422-8153", "es. fluffily final dependencies sleep along the blithely even pinto beans. final deposits haggle furiously furiou"},
        {100, "Customer#000000100", 113840.4294, 9889.89, "SAUDI ARABIA", "fptUABXcmkC5Wx", "30-749-445-4907", "was furiously fluffily quiet deposits. silent, pending requests boost against "},
        {131, "Customer#000000131", 108556.82,   8595.53, "IRAQ",         "jyN6lAjb1FtH10rMC,XzlWyCBrg75", "21-840-210-3572", "jole special packages. furiously final dependencies about the furiously speci"},
        {58,  "Customer#000000058", 103790.5932, 6478.46, "JORDAN",       "g9ap7Dk1Sv9fcXEWjpMYpBZIRUohi T", "23-244-493-2508", "ideas. ironic ideas affix furiously express, final instructions. regular excuses use quickly e"},
        {97,  "Customer#000000097", 99194.2317,  2164.48, "PERU",         "OApyejbhJG,0Iw3j rd1M", "27-588-919-5638", "haggle slyly. bold, special ideas are blithely above the thinly bold theo"},
        {101, "Customer#000000101", 90564.6478,  7470.96, "BRAZIL",       "sMmL2rNeHDltovSm Y", "12-514-298-3699", " sleep. pending packages detect slyly ironic pack"},
        {52,  "Customer#000000052", 85250.331,   5630.28, "IRAQ",         "7 QOqGqqSy9jfV51BC71jcHJSD0", "21-186-284-5998", "ic platelets use evenly even accounts. stealthy theodolites cajole furiou"},
        {46,  "Customer#000000046", 81936.6367,  5744.59, "FRANCE",       "eaTXWWm10L9", "16-357-681-2007", "ctions. accounts sleep furiously even requests. regular, regular accounts cajole blithely around the final pa"},
        {59,  "Customer#000000059", 71312.3586,  3458.60, "ARGENTINA",    "zLOCP0wh92OtBihgspOGl4", "11-355-584-3112", "ously final packages haggle blithely after the express deposits. furiou"},
        {8,   "Customer#000000008", 67088.8134,  6819.74, "PERU",         "I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5", "27-147-574-9335", "among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide"},
        {49,  "Customer#000000049", 66789.796,   4573.94, "IRAN",         "cNgAeX7Fqrdf7HQN9EwjUa4nxT,68L FKAxzl", "20-908-631-4424", "nusual foxes! fluffily pending packages maintain to the regular "},
        {26,  "Customer#000000026", 66653.8296,  5182.05, "RUSSIA",       "8ljrc5ZeMl7UciP", "32-363-455-4837", "c requests use furiously ironic requests. slyly ironic dependencies us"},
        {77,  "Customer#000000077", 65603.604,   1738.87, "PERU",         "4tAE5KdMFGD4byHtXF92vx", "27-269-357-4674", "uffily silent requests. carefully ironic asymptotes among the ironic hockey players are carefully bli"},
        {136, "Customer#000000136", 64024.4532,  -842.39, "GERMANY",      "QoLsJ0v5C1IQbh,DS1", "17-501-210-4726", "ackages sleep ironic, final courts. even requests above the blithely bold requests g"},
        {140, "Customer#000000140", 63799.0648,  9963.15, "EGYPT",        "XRqEPiKgcETII,iOLDZp5jA", "14-273-885-6505", "ies detect slyly ironic accounts. slyly ironic theodolites hag"}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query11() {
    auto it = Tpch->RunQuery11();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        ui32 ps_partkey = 0;
        double value = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, ps_partkey), ps_partkey,
                    ": " << UNWRAP_UINT32(parser, ps_partkey) << " != " << ps_partkey);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, value), value, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, value) << " != " << value);
        }
    };

    TVector<TResultRow> result = {
        {111, 9382317.55},
        {146, 8547518.19},
        {8,   7450810.66},
        {124, 6405861.96},
        {175, 5904243.46},
        {176, 5123186.57},
        {75,  4790042.88},
        {24,  4690023.8},
        {166, 4649900.92},
        {168, 4620574.93}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query12() {
    auto it = Tpch->RunQuery12();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString l_shipmode;
        i64 high_line_count = 0;
        i64 low_line_count = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, l_shipmode), l_shipmode,
                    ": " << UNWRAP_UTF8(parser, l_shipmode) << " != " << l_shipmode);
            UNIT_ASSERT_EQUAL_C(UNWRAP_INT64(parser, high_line_count), high_line_count,
                    ": " << UNWRAP_INT64(parser, high_line_count) << " != " << high_line_count);
            UNIT_ASSERT_EQUAL_C(UNWRAP_INT64(parser, low_line_count), low_line_count,
                    ": " << UNWRAP_INT64(parser, low_line_count) << " != " << low_line_count);
        }
    };

    TVector<TResultRow> result = {
        {"AIR", 8, 13},
        {"RAIL", 6, 11}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query13() {
    auto it = Tpch->RunQuery13();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        ui64 c_count = 0;
        ui64 custdist = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT64(parser, c_count), c_count,
                    ": " << UNWRAP_UINT64(parser, c_count) << " != " << c_count);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT64(parser, custdist), custdist,
                    ": " << UNWRAP_UINT64(parser, custdist) << " != " << custdist);
        }
    };

    TVector<TResultRow> result = {
        {0,  50},
        {17, 8},
        {16, 7},
        {22, 6},
        {14, 6},
        {11, 6},
        {20, 5},
        {12, 5},
        {10, 5},
        {9,  5},
        {7,  5},
        {23, 4},
        {21, 4},
        {15, 4},
        {13, 4},
        {4,  4},
        {26, 3},
        {19, 3},
        {8,  3},
        {6,  3},
        {18, 2},
        {5,  2},
        {30, 1},
        {29, 1},
        {28, 1},
        {25, 1},
        {24, 1},
        {3,  1}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query14() {
    auto it = Tpch->RunQuery14();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        double promo_revenue = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, promo_revenue), promo_revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, promo_revenue) << " != " << promo_revenue);
        }
    };

    TVector<TResultRow> result = {
        {17.5159015276780114}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query15() {
    auto it = Tpch->RunQuery15();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        ui32 s_suppkey = 0;
        TString s_name;
        TString s_address;
        TString s_phone;
        double total_revenue = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, s_suppkey), s_suppkey,
                    ": " << UNWRAP_UINT32(parser, s_suppkey) << " != " << s_suppkey);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_name), s_name,
                    ": " << UNWRAP_UTF8(parser, s_name) << " != " << s_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_address), s_address,
                    ": " << UNWRAP_UTF8(parser, s_address) << " != " << s_address);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_phone), s_phone,
                    ": " << UNWRAP_UTF8(parser, s_phone) << " != " << s_phone);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, total_revenue), total_revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, total_revenue) << " != " << total_revenue);
        }
    };

    TVector<TResultRow> result = {
        {5, "Supplier#000000005", "Gcdm2rJRzl5qlTVzc", "21-151-690-3663", 740551.2744}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query16() {
    auto it = Tpch->RunQuery16();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString p_brand;
        TString p_type;
        i32 p_size = 0;
        ui64 supplier_cnt = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, p_brand), p_brand,
                    ": " << UNWRAP_UTF8(parser, p_brand) << " != " << p_brand);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, p_type), p_type,
                    ": " << UNWRAP_UTF8(parser, p_type) << " != " << p_type);
            UNIT_ASSERT_EQUAL_C(UNWRAP_INT32(parser, p_size), p_size,
                    ": " << UNWRAP_INT32(parser, p_size) << " != " << p_size);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT64(parser, supplier_cnt), supplier_cnt,
                    ": " << UNWRAP_UINT64(parser, supplier_cnt) << " != " << supplier_cnt);
        }
    };

    TVector<TResultRow> result = {
        {"Brand#11", "ECONOMY ANODIZED STEEL",    26, 4},
        {"Brand#11", "PROMO ANODIZED STEEL",      10, 4},
        {"Brand#13", "SMALL POLISHED STEEL",      28, 4},
        {"Brand#13", "STANDARD POLISHED TIN",     10, 4},
        {"Brand#14", "SMALL ANODIZED TIN",        26, 4},
        {"Brand#21", "MEDIUM ANODIZED TIN",       8,  4},
        {"Brand#21", "SMALL BRUSHED NICKEL",      28, 4},
        {"Brand#21", "SMALL BURNISHED STEEL",     11, 4},
        {"Brand#23", "SMALL PLATED NICKEL",       26, 4},
        {"Brand#24", "PROMO PLATED STEEL",        4,  4},
        {"Brand#31", "MEDIUM PLATED BRASS",       28, 4},
        {"Brand#32", "LARGE ANODIZED STEEL",      26, 4},
        {"Brand#32", "STANDARD BURNISHED NICKEL", 10, 4},
        {"Brand#33", "ECONOMY ANODIZED TIN",      4,  4},
        {"Brand#33", "ECONOMY POLISHED TIN",      11, 4},
        {"Brand#33", "LARGE POLISHED COPPER",     28, 4},
        {"Brand#33", "STANDARD PLATED BRASS",     26, 4},
        {"Brand#34", "SMALL PLATED BRASS",        14, 4},
        {"Brand#34", "STANDARD BRUSHED COPPER",   11, 4},
        {"Brand#43", "PROMO POLISHED BRASS",      19, 4},
        {"Brand#44", "PROMO PLATED BRASS",        28, 4},
        {"Brand#44", "SMALL PLATED COPPER",       19, 4},
        {"Brand#45", "MEDIUM BRUSHED NICKEL",     4,  4},
        {"Brand#52", "LARGE POLISHED COPPER",     4,  4},
        {"Brand#52", "SMALL BURNISHED NICKEL",    14, 4},
        {"Brand#53", "PROMO PLATED NICKEL",       28, 4},
        {"Brand#54", "LARGE BRUSHED COPPER",      28, 4},
        {"Brand#55", "STANDARD POLISHED BRASS",   10, 4},
        {"Brand#13", "LARGE BRUSHED STEEL",       8,  2},
        {"Brand#13", "SMALL BRUSHED NICKEL",      19, 2},
        {"Brand#43", "ECONOMY ANODIZED BRASS",    11, 2},
        {"Brand#43", "MEDIUM ANODIZED BRASS",     14, 2},
        {"Brand#53", "STANDARD BRUSHED TIN",      10, 2},
        {"Brand#24", "MEDIUM PLATED STEEL",       19, 1},
        {"Brand#31", "MEDIUM ANODIZED COPPER",    11, 1},
        {"Brand#44", "ECONOMY POLISHED TIN",      4,  1},
        {"Brand#45", "SMALL ANODIZED NICKEL",     26, 1}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query17() {
    auto it = Tpch->RunQuery17();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        double avg_yearly = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, avg_yearly), avg_yearly, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, avg_yearly) << " != " << avg_yearly);
        }
    };

    TVector<TResultRow> result = {
        {614.9542857142857143}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query18() {
    auto it = Tpch->RunQuery18();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString c_name;
        ui32 c_custkey = 0;
        ui32 o_orderkey = 0;
        TInstant o_orderdate;
        double o_totalprice = 0;
        double sum_l_quantity = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, c_name), c_name,
                    ": " << UNWRAP_UTF8(parser, c_name) << " != " << c_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, c_custkey), c_custkey,
                    ": " << UNWRAP_UINT32(parser, c_custkey) << " != " << c_custkey);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT32(parser, o_orderkey), o_orderkey,
                    ": " << UNWRAP_UINT32(parser, o_orderkey) << " != " << o_orderkey);
            UNIT_ASSERT_EQUAL_C(UNWRAP_DATE(parser, o_orderdate).Days(), o_orderdate.Days(),
                    ": " << UNWRAP_DATE(parser, o_orderdate) << " != " << o_orderdate);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, o_totalprice), o_totalprice, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, o_totalprice) << " != " << o_totalprice);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, sum_l_quantity), sum_l_quantity, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, sum_l_quantity) << " != " << sum_l_quantity);
        }
    };

    TVector<TResultRow> result = {
        {"Customer#000000070", 70, 2567, AS_DATE(1998-02-27), 263411.29, 266},
        {"Customer#000000010", 10, 4421, AS_DATE(1997-04-04), 258779.02, 255},
        {"Customer#000000082", 82, 3460, AS_DATE(1995-10-03), 245976.74, 254},
        {"Customer#000000068", 68, 2208, AS_DATE(1995-05-01), 245388.06, 256}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query19() {
    auto it = Tpch->RunQuery19();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        double revenue = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, revenue), revenue, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, revenue) << " != " << revenue);
        }
    };

    TVector<TResultRow> result = {
        {122618.9902}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query20() {
    auto it = Tpch->RunQuery20();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString s_name;
        TString s_address;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_name), s_name,
                    ": " << UNWRAP_UTF8(parser, s_name) << " != " << s_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_address), s_address,
                    ": " << UNWRAP_UTF8(parser, s_address) << " != " << s_address);
        }
    };

    TVector<TResultRow> result = {
        {"Supplier#000000001", " N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ"}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query21() {
    auto it = Tpch->RunQuery21();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString s_name;
        ui64 numwait = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_UTF8(parser, s_name), s_name,
                    ": " << UNWRAP_UTF8(parser, s_name) << " != " << s_name);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT64(parser, numwait), numwait,
                    ": " << UNWRAP_UINT64(parser, numwait) << " != " << numwait);
        }
    };

    TVector<TResultRow> result = {
        {"Supplier#000000009", 18}
    };

    ASSERT_RESULT(it, result);
}

void KqpTpch::Query22() {
    auto it = Tpch->RunQuery22();

    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

    struct TResultRow {
        TString cntrycode;
        ui64 numcust = 0;
        double totacctbal = 0;

        void AssertEquals(const TResultSetParser& parser) {
            UNIT_ASSERT_EQUAL_C(UNWRAP_STRING(parser, cntrycode), cntrycode,
                    ": " << UNWRAP_STRING(parser, cntrycode) << " != " << cntrycode);
            UNIT_ASSERT_EQUAL_C(UNWRAP_UINT64(parser, numcust), numcust,
                    ": " << UNWRAP_UINT64(parser, numcust) << " != " << numcust);
            UNIT_ASSERT_DOUBLES_EQUAL_C(UNWRAP_DOUBLE(parser, totacctbal), totacctbal, PRESICION,
                    ": " << UNWRAP_DOUBLE(parser, totacctbal) << " != " << totacctbal);
        }
    };

    TVector<TResultRow> result = {
        {"13", 1, 5679.84},
        {"15", 2, 14624.84},
        {"19", 2, 17120.35},
        {"26", 1, 7354.23},
        {"32", 1, 6505.26}
    };

    ASSERT_RESULT(it, result);
}

} // namespace NKikimr::NKqp
