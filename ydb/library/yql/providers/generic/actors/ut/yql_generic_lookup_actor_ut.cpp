#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <ydb/library/yql/providers/generic/actors/yql_generic_lookup_actor.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/connector_client_mock.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/test_creds.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_lookup_actor.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>
#include <ydb/library/yql/utils/log/log.h>

using namespace NYql::NConnector;
using namespace NYql::NConnector::NTest;
using namespace NYql;
using namespace NActors;

Y_UNIT_TEST_SUITE(GenericProviderLookupActor) {

    //Simple actor to call IDqAsyncLookupSource::AsyncLookup from an actor system's thread
    class TCallLookupActor: public TActorBootstrapped<TCallLookupActor> {
    public:
        TCallLookupActor(
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
            NYql::NDq::IDqAsyncLookupSource* lookupSource,
            NKikimr::NMiniKQL::TUnboxedValueVector&& keysToLookUp)
            : Alloc(alloc)
            , LookupSource(lookupSource)
            , KeysToLookUp(std::move(keysToLookUp))
        {
        }

        void Bootstrap() {
            LookupSource->AsyncLookup(std::move(KeysToLookUp));
            auto guard = Guard(*Alloc);
            KeysToLookUp.clear();
            KeysToLookUp.shrink_to_fit();
        }

    private:
        static constexpr char ActorName[] = "TEST";

    private:
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        NYql::NDq::IDqAsyncLookupSource* LookupSource;
        NKikimr::NMiniKQL::TUnboxedValueVector KeysToLookUp;
    };

    Y_UNIT_TEST(Lookup) {
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
        NKikimr::NMiniKQL::TMemoryUsageInfo memUsage("TestMemUsage");
        NKikimr::NMiniKQL::THolderFactory holderFactory(alloc->Ref(), memUsage);
        NKikimr::NMiniKQL::TTypeEnvironment typeEnv(*alloc);
        NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);

        auto loggerConfig = NYql::NProto::TLoggingConfig();
        loggerConfig.set_allcomponentslevel(::NYql::NProto::TLoggingConfig_ELevel::TLoggingConfig_ELevel_TRACE);
        NYql::NLog::InitLogger(loggerConfig, false);

        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto edge = runtime.AllocateEdgeActor();

        NYql::NConnector::NApi::TDataSourceInstance dsi;
        dsi.Setkind(NYql::NConnector::NApi::EDataSourceKind::YDB);
        dsi.mutable_endpoint()->Sethost("some_host");
        dsi.mutable_endpoint()->Setport(2135);
        dsi.Setdatabase("some_db");
        dsi.Setuse_tls(true);
        dsi.set_protocol(::NYql::NConnector::NApi::EProtocol::NATIVE);
        auto token = dsi.mutable_credentials() -> mutable_token();
        token->Settype("IAM");
        token->Setvalue("TEST_TOKEN");

        auto connectorMock = std::make_shared<NYql::NConnector::NTest::TConnectorClientMock>();

        // clang-format off
        // step 1: ListSplits
        connectorMock->ExpectListSplits()
            .Select()
                .DataSourceInstance(dsi)
                .What()
                    .Column("id", Ydb::Type::UINT64)
                    .NullableColumn("optional_id", Ydb::Type::UINT64)
                    .NullableColumn("string_value", Ydb::Type::STRING)
                    .Done()
                .Table("lookup_test")
                .Where()
                    .Filter()
                        .Disjunction()
                            .Operand()
                                .Conjunction()
                                    .Operand().Equal().Column("id").Value<ui64>(0).Done().Done()
                                    .Operand().Equal().Column("optional_id").OptionalValue<ui64>(100).Done().Done()
                                    .Done()
                                .Done()
                            .Operand()
                                .Conjunction()
                                    .Operand().Equal().Column("id").Value<ui64>(1).Done().Done()
                                    .Operand().Equal().Column("optional_id").OptionalValue<ui64>(101).Done().Done()
                                    .Done()
                                .Done()
                            .Operand()
                                .Conjunction()
                                    .Operand().Equal().Column("id").Value<ui64>(2).Done().Done()
                                    .Operand().Equal().Column("optional_id").OptionalValue<ui64>(102).Done().Done()
                                    .Done()
                                .Done()
                            .Done()
                        .Done()
                    .Done()
                .Done()
            .MaxSplitCount(1)
            .Result()
                .AddResponse(NewSuccess())
                    .Description("Actual split info is not important")
        ;

        connectorMock->ExpectReadSplits()
            .DataSourceInstance(dsi)
            .Split()
                .Description("Actual split info is not important")
                .Done()
            .Result()
                .AddResponse(
                    MakeRecordBatch(
                        MakeArray<arrow::UInt64Builder, ui64>("id", {0, 1, 2}, arrow::uint64()),
                        MakeArray<arrow::UInt64Builder, ui64>("optional_id", {100, 101, 103}, arrow::uint64()), //the last value is intentially wrong
                        MakeArray<arrow::StringBuilder, std::string>("string_value", {"a", "b", "c"}, arrow::utf8())
                    ),
                    NewSuccess()
                )    
        ;
        // clang-format on

        NYql::Generic::TLookupSource lookupSourceSettings;
        *lookupSourceSettings.mutable_data_source_instance() = dsi;
        lookupSourceSettings.Settable("lookup_test");
        lookupSourceSettings.SetServiceAccountId("testsaid");
        lookupSourceSettings.SetServiceAccountIdSignature("fake_signature");

        google::protobuf::Any packedLookupSource;
        Y_ABORT_UNLESS(packedLookupSource.PackFrom(lookupSourceSettings));

        NKikimr::NMiniKQL::TStructTypeBuilder keyTypeBuilder{typeEnv};
        keyTypeBuilder.Add("id", typeBuilder.NewDataType(NUdf::EDataSlot::Uint64, false));
        keyTypeBuilder.Add("optional_id", typeBuilder.NewDataType(NUdf::EDataSlot::Uint64, true));
        NKikimr::NMiniKQL::TStructTypeBuilder outputypeBuilder{typeEnv};
        outputypeBuilder.Add("string_value", typeBuilder.NewDataType(NUdf::EDataSlot::String, true));

        auto guard = Guard(*alloc.get());

        auto [lookupSource, actor] = NYql::NDq::CreateGenericLookupActor(
            connectorMock,
            std::make_shared<NTestCreds::TSecuredServiceAccountCredentialsFactory>(),
            edge,
            alloc,
            std::move(lookupSourceSettings),
            keyTypeBuilder.Build(),
            outputypeBuilder.Build(),
            typeEnv,
            holderFactory,
            1'000'000);
        runtime.Register(actor);

        NKikimr::NMiniKQL::TUnboxedValueVector keys;
        for (size_t i = 0; i != 3; ++i) {
            NUdf::TUnboxedValue* keyItems;
            auto key = holderFactory.CreateDirectArrayHolder(2, keyItems);
            keyItems[0] = NUdf::TUnboxedValuePod(ui64(i));
            keyItems[1] = NUdf::TUnboxedValuePod(ui64(100 + i));
            keys.push_back(std::move(key));
        }

        guard.Release(); //let actors use alloc

        auto callLookupActor = new TCallLookupActor(alloc, lookupSource, std::move(keys));
        runtime.Register(callLookupActor);

        auto ev = runtime.GrabEdgeEventRethrow<NYql::NDq::IDqAsyncLookupSource::TEvLookupResult>(edge);
        auto guard2 = Guard(*alloc.get());
        NKikimr::NMiniKQL::TKeyPayloadPairVector lookupResult = std::move(ev->Get()->Data);

        UNIT_ASSERT_EQUAL(3, lookupResult.size());
        {
            auto& [k, v] = lookupResult[0];
            UNIT_ASSERT_EQUAL(0, k.GetElement(0).Get<ui64>());
            UNIT_ASSERT_EQUAL(100, k.GetElement(1).Get<ui64>());
            NUdf::TUnboxedValue val = v.GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("a"));
        }
        {
            auto& [k, v] = lookupResult[1];
            UNIT_ASSERT_EQUAL(1, k.GetElement(0).Get<ui64>());
            UNIT_ASSERT_EQUAL(101, k.GetElement(1).Get<ui64>());
            NUdf::TUnboxedValue val = v.GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("b"));
        }
        {
            auto& [k, v] = lookupResult[2];
            UNIT_ASSERT_EQUAL(2, k.GetElement(0).Get<ui64>());
            UNIT_ASSERT_EQUAL(102, k.GetElement(1).Get<ui64>());
            //this key was not found and reported as empty
            UNIT_ASSERT(!v);
        }
    }

} //Y_UNIT_TEST_SUITE(GenericProviderLookupActor)