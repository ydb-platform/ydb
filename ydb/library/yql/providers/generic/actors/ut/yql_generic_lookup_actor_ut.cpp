#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <ydb/library/yql/providers/generic/actors/yql_generic_lookup_actor.h>

#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/testlib/common/test_utils.h>
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
        using TBase = TActorBootstrapped<TCallLookupActor>;
        using TLookupActorFactory = std::function<std::pair<NActors::IActor*, std::shared_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap>>(const NActors::TActorId&, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>&, NKikimr::NMiniKQL::TTypeEnvironment&)>;
        using TCallback = std::function<void(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>&, NYql::NDq::IDqAsyncLookupSource::TEvLookupResult::TPtr&)>;
    public:
        TCallLookupActor(
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>&& alloc,
            TLookupActorFactory&& lookupActorFactory,
            TCallback&& callback,
            const NActors::TActorId& edge,
            size_t fullscanLimit = 0)
            : Alloc(std::move(alloc))
            , TypeEnv(std::make_shared<NKikimr::NMiniKQL::TTypeEnvironment>(*Alloc))
            , LookupActorFactory(std::move(lookupActorFactory))
            , Callback(std::move(callback))
            , Edge(edge)
            , FullscanLimit(fullscanLimit)
        {

        }

        void Bootstrap() {
            Become(&TCallLookupActor::StateFunc);
            auto guard = Guard(*Alloc);
            NActors::IActor* actor;
            std::tie(actor, Request) = LookupActorFactory(SelfId(), Alloc, *TypeEnv);
            LookupActorFactory = {};
            LookupActor = RegisterWithSameMailbox(actor);
            auto ev = new NYql::NDq::IDqAsyncLookupSource::TEvLookupRequest(Request, FullscanLimit);
            TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(LookupActor, SelfId(), ev));
        }

    private:
        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NYql::NDq::IDqAsyncLookupSource::TEvLookupResult, Handle);
            }
        }

        void Handle(NYql::NDq::IDqAsyncLookupSource::TEvLookupResult::TPtr& ev) {
            Callback(Alloc, ev);
            {
                auto guard = Guard(*Alloc);
                Request.reset();
                Callback = {};
            }
            Send(LookupActor, new NActors::TEvents::TEvPoison());
            Send(Edge, new NActors::TEvents::TEvWakeup());
        }

        void Free() {
            if (Alloc) {
                auto guard = Guard(*Alloc);
                Request.reset();
                TypeEnv.reset();
            }
            Alloc.reset();
        }

    public:
        void PassAway() override {
            Free();
            TBase::PassAway();
        }

        ~TCallLookupActor() {
            Free();
        }

    private:
        static constexpr char ActorName[] = "TEST";

    private:
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        std::shared_ptr<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap> Request;
        std::shared_ptr<NKikimr::NMiniKQL::TTypeEnvironment> TypeEnv;
        NActors::TActorId LookupActor;
        TLookupActorFactory LookupActorFactory;
        TCallback Callback;
        NActors::TActorId Edge;
        size_t FullscanLimit;
    };

    Y_UNIT_TEST_QUAD(Lookup, MultiMatches, Fullscan) {
        NKikimr::NMiniKQL::TMemoryUsageInfo memUsage("TestMemUsage");
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
        NKikimr::NMiniKQL::THolderFactory holderFactory(alloc->Ref(), memUsage);
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
        token->Setvalue("token_value");

        auto connectorMock = std::make_shared<NYql::NConnector::NTest::TConnectorClientMock>();

        constexpr size_t fullscanLimit = Fullscan ? 3 : 0;
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
                .Where(/*skip=*/Fullscan)
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
                .Limit(/*skip=*/!Fullscan)
                    .Limit(fullscanLimit)
                .Done()
            .Done()
            .MaxSplitCount(1)
            .Result()
                .AddResponse(NewSuccess())
                    .Description("Actual split info is not important")
        ;

        connectorMock->ExpectReadSplits()
            .DataSourceInstance(dsi)
            .Filtering(Fullscan ? NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_OPTIONAL : NYql::NConnector::NApi::TReadSplitsRequest::FILTERING_MANDATORY)
            .Split()
                .Description("Actual split info is not important")
                .Done()
            .Result()
                .AddResponse(
                    MakeRecordBatch(
                        MakeArray<arrow::UInt64Builder, uint64_t>("id", {0, 1, 1, 2}, arrow::uint64()),
                        MakeArray<arrow::UInt64Builder, uint64_t>("optional_id", {100, 101, 101, 103}, arrow::uint64()), // the last value is intentionally wrong
                        MakeArray<arrow::StringBuilder, std::string>("string_value", {"a", "b", "ba", "c"}, arrow::utf8())
                    ),
                    NewSuccess()
                )
        ;
        // clang-format on

        NYql::Generic::TLookupSource lookupSourceSettings;
        *lookupSourceSettings.mutable_data_source_instance() = dsi;
        lookupSourceSettings.Settable("lookup_test");
        lookupSourceSettings.SetTokenName("test_token");

        google::protobuf::Any packedLookupSource;
        Y_ABORT_UNLESS(packedLookupSource.PackFrom(lookupSourceSettings));

        auto lookupActorFactory = [&holderFactory, connectorMock = std::move(connectorMock), lookupSourceSettings = std::move(lookupSourceSettings)](const NActors::TActorId& caller, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>& alloc, NKikimr::NMiniKQL::TTypeEnvironment& typeEnv) mutable {
            NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);

            NKikimr::NMiniKQL::TStructTypeBuilder keyTypeBuilder{typeEnv};
            keyTypeBuilder.Add("id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, false));
            keyTypeBuilder.Add("optional_id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, true));
            NKikimr::NMiniKQL::TStructTypeBuilder outputypeBuilder{typeEnv};
            outputypeBuilder.Add("string_value", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::String, true));

            auto guard = Guard(*alloc.get());
            auto keyTypeHelper = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TKeyTypeHelper>(keyTypeBuilder.Build());

            auto [lookupSource, actor] = NYql::NDq::CreateGenericLookupActor(
                std::move(connectorMock),
                NKikimr::NKqp::NFederatedQueryTest::CreateCredentialsFactory("token_value"),
                caller,
                nullptr,
                alloc,
                keyTypeHelper,
                std::move(lookupSourceSettings),
                keyTypeBuilder.Build(),
                outputypeBuilder.Build(),
                typeEnv,
                holderFactory,
                1'000'000,
                {{"test_token", "{\"token\": \"token_value\"}"}},
                MultiMatches);

            auto request = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap>(3, keyTypeHelper->GetValueHash(), keyTypeHelper->GetValueEqual());
            if (!Fullscan) {
                for (size_t i = 0; i != 3; ++i) {
                    NYql::NUdf::TUnboxedValue* keyItems;
                    auto key = holderFactory.CreateDirectArrayHolder(2, keyItems);
                    keyItems[0] = NYql::NUdf::TUnboxedValuePod(ui64(i));
                    keyItems[1] = NYql::NUdf::TUnboxedValuePod(ui64(100 + i));
                    request->emplace(std::move(key), NYql::NUdf::TUnboxedValue{});
                }
            }

            return std::pair { actor, std::move(request) };
        };
        auto callback = [&holderFactory](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>& alloc,
                NYql::NDq::IDqAsyncLookupSource::TEvLookupResult::TPtr& ev) {
            auto guard2 = Guard(*alloc.get());
            auto lookupResult = ev->Get()->Result.lock();
            UNIT_ASSERT(lookupResult);

            UNIT_ASSERT_EQUAL(Fullscan ? 2 : 3, lookupResult->size());
            UNIT_ASSERT_EQUAL(ev->Get()->FullscanLimit, fullscanLimit);
            UNIT_ASSERT_EQUAL(ev->Get()->ResultRows, Fullscan ? 3 : 4);
            if (!MultiMatches) {
                return;
            }
            {
                const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {0, 100}));
                UNIT_ASSERT(v);
                UNIT_ASSERT_VALUES_EQUAL(v->GetListLength(), 1);
                auto ptr = v->GetElements();
                UNIT_ASSERT(ptr);
                {
                    auto val = ptr->GetElement(0);
                    UNIT_ASSERT_VALUES_EQUAL(TString(val.AsStringRef()), TStringBuf("a"));
                    ++ptr;
                }
            }
            {
                const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {1, 101}));
                UNIT_ASSERT(v);
                UNIT_ASSERT_VALUES_EQUAL(v->GetListLength(), 2);
                auto ptr = v->GetElements();
                UNIT_ASSERT(ptr);
                {
                    auto val = ptr->GetElement(0);
                    UNIT_ASSERT_VALUES_EQUAL(TString(val.AsStringRef()), TStringBuf("b"));
                    ++ptr;
                }
                {
                    auto val = ptr->GetElement(0);
                    UNIT_ASSERT_VALUES_EQUAL(TString(val.AsStringRef()), TStringBuf("ba"));
                    ++ptr;
                }
            }
            if (!Fullscan) {
                const auto* v = lookupResult->FindPtr(CreateStructValue(holderFactory, {2, 102}));
                UNIT_ASSERT(v);
                UNIT_ASSERT(!*v);
            }
        };

        auto callLookupActor = new TCallLookupActor(std::move(alloc), std::move(lookupActorFactory), std::move(callback), edge, fullscanLimit);
        runtime.Register(callLookupActor);
        runtime.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(edge);
    }

    Y_UNIT_TEST(LookupWithErrors) {
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
        NKikimr::NMiniKQL::TMemoryUsageInfo memUsage("TestMemUsage");
        NKikimr::NMiniKQL::THolderFactory holderFactory(alloc->Ref(), memUsage);

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
        token->Setvalue("token_value");

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
                                MakeArray<arrow::UInt64Builder, uint64_t>("optional_id", {100, 101, 103}, arrow::uint64()), // the last value is intentionally wrong
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
        lookupSourceSettings.SetTokenName("test_token");

        google::protobuf::Any packedLookupSource;
        Y_ABORT_UNLESS(packedLookupSource.PackFrom(lookupSourceSettings));

        auto lookupActorFactory = [&holderFactory, connectorMock = std::move(connectorMock), lookupSourceSettings = std::move(lookupSourceSettings)](const NActors::TActorId& caller, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>& alloc, NKikimr::NMiniKQL::TTypeEnvironment& typeEnv) mutable {
            NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);

            NKikimr::NMiniKQL::TStructTypeBuilder keyTypeBuilder{typeEnv};
            keyTypeBuilder.Add("id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, false));
            keyTypeBuilder.Add("optional_id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, true));
            NKikimr::NMiniKQL::TStructTypeBuilder outputypeBuilder{typeEnv};
            outputypeBuilder.Add("string_value", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::String, true));

            auto guard = Guard(*alloc.get());
            auto keyTypeHelper = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TKeyTypeHelper>(keyTypeBuilder.Build());

            auto [lookupSource, actor] = NYql::NDq::CreateGenericLookupActor(
                std::move(connectorMock),
                NKikimr::NKqp::NFederatedQueryTest::CreateCredentialsFactory("token_value"),
                caller,
                nullptr,
                alloc,
                keyTypeHelper,
                std::move(lookupSourceSettings),
                keyTypeBuilder.Build(),
                outputypeBuilder.Build(),
                typeEnv,
                holderFactory,
                1'000'000,
                {{"test_token", "{\"token\": \"token_value\"}"}});

            auto request = std::make_shared<NYql::NDq::IDqAsyncLookupSource::TUnboxedValueMap>(3, keyTypeHelper->GetValueHash(), keyTypeHelper->GetValueEqual());
            for (size_t i = 0; i != 3; ++i) {
                NYql::NUdf::TUnboxedValue* keyItems;
                auto key = holderFactory.CreateDirectArrayHolder(2, keyItems);
                keyItems[0] = NYql::NUdf::TUnboxedValuePod(ui64(i));
                keyItems[1] = NYql::NUdf::TUnboxedValuePod(ui64(100 + i));
                request->emplace(std::move(key), NYql::NUdf::TUnboxedValue{});
            }

            return std::pair { actor, std::move(request) };
        };
        auto callback = [&holderFactory](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc>& alloc,
                NYql::NDq::IDqAsyncLookupSource::TEvLookupResult::TPtr& ev) {
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
        };

        auto callLookupActor = new TCallLookupActor(std::move(alloc), std::move(lookupActorFactory), std::move(callback), edge);
        runtime.Register(callLookupActor);
        runtime.GrabEdgeEventRethrow<NActors::TEvents::TEvWakeup>(edge);
    }

} // Y_UNIT_TEST_SUITE(GenericProviderLookupActor)
