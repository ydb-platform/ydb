#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <ydb/library/yql/providers/generic/actors/yql_generic_lookup_actor.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/connector_client_mock.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/test_creds.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_lookup_actor.h>
#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/utils/log/proto/logger_config.pb.h>
#include <yql/essentials/utils/log/log.h>

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
            const NActors::TActorId& lookupActor,
            std::shared_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap> request)
            : Alloc(alloc)
            , LookupActor(lookupActor)
            , Request(request)
        {
        }

        void Bootstrap() {
            auto ev = new NYql::NDq::IDqAsyncLookupSource::TEvLookupRequest(Request);
            TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(LookupActor, SelfId(), ev));
        }

        void PassAway() override {
            auto guard = Guard(*Alloc);
            Request.reset();
        }

        ~TCallLookupActor() {
            PassAway();
        }

    private:
        static constexpr char ActorName[] = "TEST";

    private:
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        const NActors::TActorId LookupActor;
        std::shared_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap> Request;
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

        NYql::TGenericDataSourceInstance dsi;
        dsi.Setkind(NYql::EGenericDataSourceKind::YDB);
        dsi.mutable_endpoint()->Sethost("some_host");
        dsi.mutable_endpoint()->Setport(2135);
        dsi.Setdatabase("some_db");
        dsi.Setuse_tls(true);
        dsi.set_protocol(::NYql::EGenericProtocol::NATIVE);
        auto token = dsi.mutable_credentials()->mutable_token();
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
            .Filtering(NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_MANDATORY)
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
            nullptr,
            alloc,
            keyTypeHelper,
            std::move(lookupSourceSettings),
            keyTypeBuilder.Build(),
            outputypeBuilder.Build(),
            typeEnv,
            holderFactory,
            1'000'000);
        auto lookupActor = runtime.Register(actor);

        auto request = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap>(3, keyTypeHelper->GetValueHash(), keyTypeHelper->GetValueEqual());
        for (size_t i = 0; i != 3; ++i) {
            NYql::NUdf::TUnboxedValue* keyItems;
            auto key = holderFactory.CreateDirectArrayHolder(2, keyItems);
            keyItems[0] = NYql::NUdf::TUnboxedValuePod(ui64(i));
            keyItems[1] = NYql::NUdf::TUnboxedValuePod(ui64(100 + i));
            request->emplace(std::move(key), NYql::NUdf::TUnboxedValue{});
        }

        guard.Release(); // let actors use alloc

        auto callLookupActor = new TCallLookupActor(alloc, lookupActor, request);
        runtime.Register(callLookupActor);

        auto ev = runtime.GrabEdgeEventRethrow<NYql::NDq::IDqAsyncLookupSource::TEvLookupResult>(edge);
        auto guard2 = Guard(*alloc.get());
        auto lookupResult = ev->Get()->Result.lock();
        UNIT_ASSERT(lookupResult);

        UNIT_ASSERT_EQUAL(3, lookupResult->size());
        {
            const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {0, 100}));
            UNIT_ASSERT(v);
            NYql::NUdf::TUnboxedValue val = v->GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("a"));
        }
        {
            const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {1, 101}));
            UNIT_ASSERT(v);
            NYql::NUdf::TUnboxedValue val = v->GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("b"));
        }
        {
            const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {2, 102}));
            UNIT_ASSERT(v);
            UNIT_ASSERT(!*v);
        }
    }

    Y_UNIT_TEST(LookupWithErrors) {
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
        NKikimr::NMiniKQL::TMemoryUsageInfo memUsage("TestMemUsage");
        NKikimr::NMiniKQL::THolderFactory holderFactory(alloc->Ref(), memUsage);
        NKikimr::NMiniKQL::TTypeEnvironment typeEnv(*alloc);
        NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);

        auto loggerConfig = NYql::NProto::TLoggingConfig();
        loggerConfig.set_allcomponentslevel(::NYql::NProto::TLoggingConfig_ELevel::TLoggingConfig_ELevel_TRACE);
        NYql::NLog::InitLogger(loggerConfig, false);

        TTestActorRuntimeBase runtime(1, 1, true);
        runtime.Initialize();
        auto edge = runtime.AllocateEdgeActor();

        NYql::TGenericDataSourceInstance dsi;
        dsi.Setkind(NYql::EGenericDataSourceKind::YDB);
        dsi.mutable_endpoint()->Sethost("some_host");
        dsi.mutable_endpoint()->Setport(2135);
        dsi.Setdatabase("some_db");
        dsi.Setuse_tls(true);
        dsi.set_protocol(::NYql::EGenericProtocol::NATIVE);
        auto token = dsi.mutable_credentials()->mutable_token();
        token->Settype("IAM");
        token->Setvalue("TEST_TOKEN");

        auto connectorMock = std::make_shared<NYql::NConnector::NTest::TConnectorClientMock>();

        // clang-format off
        // step 1: ListSplits
        {
            ::testing::InSequence seq;
            for (grpc::StatusCode readStatus : { grpc::StatusCode::UNAVAILABLE, grpc::StatusCode::OK }) {
                for (grpc::StatusCode listStatus : { grpc::StatusCode::UNAVAILABLE, readStatus == grpc::StatusCode::OK ? grpc::StatusCode::DEADLINE_EXCEEDED : grpc::StatusCode::UNAVAILABLE, grpc::StatusCode::OK }) {
                    auto listBuilder = connectorMock->ExpectListSplits();
                    listBuilder
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
                    ;
                    if (listStatus != grpc::StatusCode::OK) {
                        listBuilder
                            .Status(NYdbGrpc::TGrpcStatus(listStatus, "Mocked Error"))
                        ;
                        continue;
                    }
                    listBuilder
                        .Result()
                            .AddResponse(NewSuccess())
                                .Description("Actual split info is not important")
                    ;
                }

                auto readBuilder = connectorMock->ExpectReadSplits();
                readBuilder
                    .DataSourceInstance(dsi)
                    .Filtering(NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_MANDATORY)
                    .Split()
                        .Description("Actual split info is not important")
                        .Done()
                ;
                if (readStatus != grpc::StatusCode::OK) {
                    readBuilder
                        .Status(NYdbGrpc::TGrpcStatus(readStatus, "Mocked Error"))
                    ;
                    continue;
                }
                readBuilder
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
            }
        }
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
            nullptr,
            alloc,
            keyTypeHelper,
            std::move(lookupSourceSettings),
            keyTypeBuilder.Build(),
            outputypeBuilder.Build(),
            typeEnv,
            holderFactory,
            1'000'000);
        auto lookupActor = runtime.Register(actor);

        auto request = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap>(3, keyTypeHelper->GetValueHash(), keyTypeHelper->GetValueEqual());
        for (size_t i = 0; i != 3; ++i) {
            NYql::NUdf::TUnboxedValue* keyItems;
            auto key = holderFactory.CreateDirectArrayHolder(2, keyItems);
            keyItems[0] = NYql::NUdf::TUnboxedValuePod(ui64(i));
            keyItems[1] = NYql::NUdf::TUnboxedValuePod(ui64(100 + i));
            request->emplace(std::move(key), NYql::NUdf::TUnboxedValue{});
        }

        guard.Release(); // let actors use alloc

        auto callLookupActor = new TCallLookupActor(alloc, lookupActor, request);
        runtime.Register(callLookupActor);

        auto ev = runtime.GrabEdgeEventRethrow<NYql::NDq::IDqAsyncLookupSource::TEvLookupResult>(edge);
        auto guard2 = Guard(*alloc.get());
        auto lookupResult = ev->Get()->Result.lock();
        UNIT_ASSERT(lookupResult);

        UNIT_ASSERT_EQUAL(3, lookupResult->size());
        {
            const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {0, 100}));
            UNIT_ASSERT(v);
            NYql::NUdf::TUnboxedValue val = v->GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("a"));
        }
        {
            const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {1, 101}));
            UNIT_ASSERT(v);
            NYql::NUdf::TUnboxedValue val = v->GetElement(0);
            UNIT_ASSERT(val.AsStringRef() == TStringBuf("b"));
        }
        {
            const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {2, 102}));
            UNIT_ASSERT(v);
            UNIT_ASSERT(!*v);
        }
    }

} // Y_UNIT_TEST_SUITE(GenericProviderLookupActor)
