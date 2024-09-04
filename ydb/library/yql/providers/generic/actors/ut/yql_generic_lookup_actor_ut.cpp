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
using namespace NActors;

Y_UNIT_TEST_SUITE(GenericProviderLookupActor) {

    NYql::NUdf::TUnboxedValue CreateStructValue(NKikimr::NMiniKQL::THolderFactory& holderFactory, std::initializer_list<ui64> members) {
        NYql::NUdf::TUnboxedValue* items;
        NYql::NUdf::TUnboxedValue result = holderFactory.CreateDirectArrayHolder(members.size(), items);
        for (size_t i = 0; i != members.size(); ++i) {
            items[i] = NYql::NUdf::TUnboxedValuePod{*(members.begin() + i)};
        }
        return result;
    }

    // Simple actor to call IDqAsyncLookupSource::AsyncLookup from an actor system's thread
    class TCallLookupActor: public TActorBootstrapped<TCallLookupActor> {
    public:
        TCallLookupActor(
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
            NYql::NDq::IDqAsyncLookupSource* lookupSource,
            NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap&& request)
            : Alloc(alloc)
            , LookupSource(lookupSource)
            , Request(std::move(request))
        {
        }

        void Bootstrap() {
            LookupSource->AsyncLookup(std::move(Request));
        }

    private:
        static constexpr char ActorName[] = "TEST";

    private:
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        NYql::NDq::IDqAsyncLookupSource* LookupSource;
        NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap Request;
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
                                    .Operand().Equal().Column("id").Value<ui64>(2).Done().Done()
                                    .Operand().Equal().Column("optional_id").OptionalValue<ui64>(102).Done().Done()
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
                                    .Operand().Equal().Column("id").Value<ui64>(0).Done().Done()
                                    .Operand().Equal().Column("optional_id").OptionalValue<ui64>(100).Done().Done()
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
                        MakeArray<arrow::UInt64Builder, uint64_t>("id", {0, 1, 2}, arrow::uint64()),
                        MakeArray<arrow::UInt64Builder, uint64_t>("optional_id", {100, 101, 103}, arrow::uint64()), // the last value is intentially wrong
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
        keyTypeBuilder.Add("id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, false));
        keyTypeBuilder.Add("optional_id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, true));
        NKikimr::NMiniKQL::TStructTypeBuilder outputypeBuilder{typeEnv};
        outputypeBuilder.Add("string_value", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::String, true));

        auto guard = Guard(*alloc.get());
        auto keyTypeHelper = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TKeyTypeHelper>(keyTypeBuilder.Build());

        auto [lookupSource, actor] = NYql::NDq::CreateGenericLookupActor(
            connectorMock,
            std::make_shared<NYql::NTestCreds::TSecuredServiceAccountCredentialsFactory>(),
            edge,
            alloc,
            keyTypeHelper,
            std::move(lookupSourceSettings),
            keyTypeBuilder.Build(),
            outputypeBuilder.Build(),
            typeEnv,
            holderFactory,
            1'000'000);
        runtime.Register(actor);

        NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap request(3, keyTypeHelper->GetValueHash(), keyTypeHelper->GetValueEqual());
        for (size_t i = 0; i != 3; ++i) {
            NYql::NUdf::TUnboxedValue* keyItems;
            auto key = holderFactory.CreateDirectArrayHolder(2, keyItems);
            keyItems[0] = NYql::NUdf::TUnboxedValuePod(ui64(i));
            keyItems[1] = NYql::NUdf::TUnboxedValuePod(ui64(100 + i));
            request.emplace(std::move(key), NYql::NUdf::TUnboxedValue{});
        }

        guard.Release(); // let actors use alloc

        auto callLookupActor = new TCallLookupActor(alloc, lookupSource, std::move(request));
        runtime.Register(callLookupActor);

        auto ev = runtime.GrabEdgeEventRethrow<NYql::NDq::IDqAsyncLookupSource::TEvLookupResult>(edge);
        auto guard2 = Guard(*alloc.get());
        auto lookupResult = std::move(ev->Get()->Result);

        UNIT_ASSERT_EQUAL(3, lookupResult.size());
        {
            const auto* v = lookupResult.FindPtr(CreateStructValue(holderFactory, {0, 100}));
            UNIT_ASSERT(v);
            NYql::NUdf::TUnboxedValue val = v->GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("a"));
        }
        {
            const auto* v = lookupResult.FindPtr(CreateStructValue(holderFactory, {1, 101}));
            UNIT_ASSERT(v);
            NYql::NUdf::TUnboxedValue val = v->GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("b"));
        }
        {
            const auto* v = lookupResult.FindPtr(CreateStructValue(holderFactory, {2, 102}));
            UNIT_ASSERT(v);
            UNIT_ASSERT(!*v);
        }
    }

} // Y_UNIT_TEST_SUITE(GenericProviderLookupActor)
