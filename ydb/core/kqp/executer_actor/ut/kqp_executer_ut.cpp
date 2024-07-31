#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/dq/common/dq_value.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYdb::NTable;

namespace {

[[maybe_unused]]
NKqpProto::TKqpPhyTx BuildTxPlan(const TString& sql, TIntrusivePtr<IKqpGateway> gateway,
    const TKikimrConfiguration::TPtr& config, NActors::TActorId* actorSystem)
{
    auto cluster = TString(DefaultKikimrClusterName);

    TExprContext moduleCtx;
    IModuleResolver::TPtr moduleResolver;
    UNIT_ASSERT(GetYqlDefaultModuleResolver(moduleCtx, moduleResolver));

    auto qp = CreateKqpHost(gateway, cluster, "/Root", config, moduleResolver, NYql::IHTTPGateway::Make(), nullptr, nullptr, NKikimrConfig::TQueryServiceConfig(), Nothing(), nullptr, nullptr, false, false, nullptr, actorSystem, nullptr);
    auto result = qp->SyncPrepareDataQuery(sql, IKqpHost::TPrepareSettings());
    result.Issues().PrintTo(Cerr);
    UNIT_ASSERT(result.Success());

    auto& phyQuery = result.PreparedQuery.GetPhysicalQuery();
    UNIT_ASSERT(phyQuery.TransactionsSize() == 1);
    return phyQuery.GetTransactions(0);
}

[[maybe_unused]]
TIntrusivePtr<IKqpGateway> MakeIcGateway(const TKikimrRunner& kikimr) {
    auto actorSystem = kikimr.GetTestServer().GetRuntime()->GetAnyNodeActorSystem();
    return CreateKikimrIcGateway(TString(DefaultKikimrClusterName), "/Root", TKqpGatewaySettings(),
        actorSystem, kikimr.GetTestServer().GetRuntime()->GetNodeId(0),
        TAlignedPagePoolCounters(), kikimr.GetTestServer().GetSettings().AppConfig->GetQueryServiceConfig());
}

[[maybe_unused]]
TKikimrParamsMap GetParamsMap(const NYdb::TParams& params) {
    TKikimrParamsMap paramsMap;

    auto paramValues = params.GetValues();
    for (auto& pair : paramValues) {
        Ydb::TypedValue protoParam;
        protoParam.mutable_type()->CopyFrom(NYdb::TProtoAccessor::GetProto(pair.second.GetType()));
        protoParam.mutable_value()->CopyFrom(NYdb::TProtoAccessor::GetProto(pair.second));

        NKikimrMiniKQL::TParams mkqlParam;
        ConvertYdbTypeToMiniKQLType(protoParam.type(), *mkqlParam.MutableType());
        ConvertYdbValueToMiniKQLValue(protoParam.type(), protoParam.value(), *mkqlParam.MutableValue());

        paramsMap.insert(std::make_pair(pair.first, mkqlParam));
    }

    return paramsMap;
}

[[maybe_unused]]
TKqpParamsRefMap GetParamRefsMap(const TKikimrParamsMap& paramsMap) {
    TKqpParamsRefMap refsMap;

    for (auto& pair : paramsMap) {
        refsMap.emplace(std::make_pair(pair.first, NDq::TMkqlValueRef(pair.second)));
    }

    return refsMap;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpExecuter) {
    // TODO: Test shard write shuffle.
    /*
    Y_UNIT_TEST(BlindWriteDistributed) {
        TKikimrRunner kikimr;
        auto gateway = MakeIcGateway(kikimr);

        TExprContext ctx;
        auto tx = BuildTxPlan(R"(
            DECLARE $items AS 'List<Struct<Key:Uint64?, Text:String?>>';

            $itemsSource = (
                SELECT Item.Key AS Key, Item.Text AS Text
                FROM (SELECT $items AS List) FLATTEN BY List AS Item
            );

            UPSERT INTO [Root/EightShard]
            SELECT * FROM $itemsSource;
        )", gateway, ctx, kikimr.GetTestServer().GetRuntime()->GetAnyNodeActorSystem());

        LogTxPlan(kikimr, tx);

        auto db = kikimr.GetTableClient();
        auto params = db.GetParamsBuilder()
            .AddParam("$items")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(205)
                    .AddMember("Text")
                        .OptionalString("New")
                    .EndStruct()
                .AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(505)
                    .AddMember("Text")
                        .OptionalString("New")
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        auto paramsMap = GetParamsMap(std::move(params));

        IKqpGateway::TExecPhysicalRequest request;
        request.Transactions.emplace_back(tx.Ref(), GetParamRefsMap(paramsMap));

        auto txResult = gateway->ExecutePhysical(std::move(request)).GetValueSync();
        UNIT_ASSERT(txResult.Success());

        UNIT_ASSERT_VALUES_EQUAL(txResult.ExecuterResult.GetStats().GetAffectedShards(), 2);

        auto session = db.CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [Root/EightShard] WHERE Text = "New" ORDER BY Key;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        CompareYson(R"(
            [
                [#;[205u];["New"]];
                [#;[505u];["New"]]
            ]
        )", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }
    */
}

} // namspace NKqp
} // namespace NKikimr
