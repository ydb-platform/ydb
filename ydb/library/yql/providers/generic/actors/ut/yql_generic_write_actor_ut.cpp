#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <ydb/library/yql/providers/generic/actors/yql_generic_write_actor.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <yql/essentials/utils/log/proto/logger_config.pb.h>
#include <yql/essentials/utils/log/log.h>

#include <arrow/api.h>

using namespace NYql;
using namespace NYql::NDq;
using namespace NActors;
using namespace NKikimr::NMiniKQL;

namespace {

    // Fake connector client used by the generic write actor test. It resolves the target
    // table schema synchronously (as the YT-native client does) and captures every
    // WriteRows call by deserializing the Arrow IPC payload into a record batch so the
    // test can assert on the written content. Read entrypoints are unused here.
    class TFakeWriteClient: public NConnector::IClient {
    public:
        NConnector::TDescribeTableAsyncResult DescribeTable(
            const NConnector::NApi::TDescribeTableRequest&, TDuration = {}) override {
            NConnector::NApi::TDescribeTableResponse response;
            auto* schema = response.mutable_schema();
            {
                auto* column = schema->add_columns();
                column->set_name("id");
                column->mutable_type()->set_type_id(Ydb::Type::UINT64);
            }
            {
                auto* column = schema->add_columns();
                column->set_name("payload");
                column->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
            }

            NConnector::TResult<NConnector::NApi::TDescribeTableResponse> result;
            result.Status = NYdbGrpc::TGrpcStatus(); // Ok
            result.Response = std::move(response);
            return NThreading::MakeFuture(std::move(result));
        }

        NConnector::TListSplitsStreamIteratorAsyncResult ListSplits(
            const NConnector::NApi::TListSplitsRequest&, TDuration = {}) override {
            ythrow yexception() << "ListSplits is not expected in the write test";
        }

        NConnector::TReadSplitsStreamIteratorAsyncResult ReadSplits(
            const NConnector::NApi::TReadSplitsRequest&, TDuration = {}) override {
            ythrow yexception() << "ReadSplits is not expected in the write test";
        }

        void WriteRows(const NConnector::NApi::TSchema& /*schema*/,
                       const TString& table,
                       const TString& arrowIpcStreaming,
                       const NYql::TGenericDataSourceInstance& dataSourceInstance) override {
            Table = table;
            Cluster = dataSourceInstance.yt_options().cluster();

            auto serializer = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
            auto deserialized = serializer->Deserialize(arrowIpcStreaming);
            Y_ENSURE(deserialized.ok(), deserialized.status().ToString());
            Batches.push_back(deserialized.ValueOrDie());
        }

        TString Table;
        TString Cluster;
        TVector<std::shared_ptr<arrow::RecordBatch>> Batches;
    };

    struct TFakeCallbacks: public IDqComputeActorAsyncOutput::ICallbacks {
        void ResumeExecution(EResumeSource) override {
        }

        void OnAsyncOutputError(ui64, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode) override {
            Error = issues.ToOneLineString();
        }

        void OnAsyncOutputStateSaved(TSinkState&&, ui64, const NDqProto::TCheckpoint&) override {
        }

        void OnAsyncOutputFinished(ui64) override {
            Finished = true;
        }

        TString Error;
        bool Finished = false;
    };

    std::shared_ptr<arrow::Array> MakeUInt64Array(const TVector<ui64>& values) {
        arrow::UInt64Builder builder;
        Y_ABORT_UNLESS(builder.Reserve(values.size()).ok());
        for (auto v : values) {
            Y_ABORT_UNLESS(builder.Append(v).ok());
        }
        std::shared_ptr<arrow::Array> array;
        Y_ABORT_UNLESS(builder.Finish(&array).ok());
        return array;
    }

    std::shared_ptr<arrow::Array> MakeStringArray(const TVector<TString>& values) {
        arrow::StringBuilder builder;
        Y_ABORT_UNLESS(builder.Reserve(values.size()).ok());
        for (const auto& v : values) {
            Y_ABORT_UNLESS(builder.Append(v.data(), v.size()).ok());
        }
        std::shared_ptr<arrow::Array> array;
        Y_ABORT_UNLESS(builder.Finish(&array).ok());
        return array;
    }

    // Drives the generic write actor from an actor-system thread: constructs it via the
    // factory, waits for its Bootstrap (which resolves the schema) to run, then feeds a
    // single wide/block batch through SendData and signals the edge actor.
    class TCallWriteActor: public TActorBootstrapped<TCallWriteActor> {
        using TBase = TActorBootstrapped<TCallWriteActor>;

    public:
        TCallWriteActor(
            std::shared_ptr<TScopedAlloc> alloc,
            THolderFactory& holderFactory,
            TTypeEnvironment& typeEnv,
            const IFunctionRegistry& functionRegistry,
            std::shared_ptr<TFakeWriteClient> client,
            TFakeCallbacks* callbacks,
            const TActorId& edge)
            : Alloc_(std::move(alloc))
            , HolderFactory_(holderFactory)
            , TypeEnv_(typeEnv)
            , FunctionRegistry_(functionRegistry)
            , Client_(std::move(client))
            , Callbacks_(callbacks)
            , Edge_(edge)
        {
        }

        void Bootstrap() {
            Become(&TCallWriteActor::StateFunc);

            auto guard = Guard(*Alloc_);

            Generic::TSink sink;
            auto* dsi = sink.mutable_data_source_instance();
            dsi->set_kind(NYql::EGenericDataSourceKind::YT);
            dsi->mutable_yt_options()->set_cluster("test_cluster");
            sink.set_table("//tmp/write_test");
            sink.SetTokenName("test_token");

            // Row type is a struct{id: Uint64, payload: Optional<String>}. MiniKQL orders
            // struct members by name, so the wide block columns arrive as (id, payload).
            TTypeBuilder typeBuilder(TypeEnv_);
            TStructTypeBuilder structBuilder(TypeEnv_);
            structBuilder.Add("id", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, false));
            structBuilder.Add("payload", typeBuilder.NewDataType(NYql::NUdf::EDataSlot::String, true));
            sink.SetRowType(NCommon::WriteTypeToYson(structBuilder.Build()));

            auto [output, actor] = CreateGenericWriteActor(
                TypeEnv_,
                FunctionRegistry_,
                Client_,
                std::move(sink),
                /*outputIndex=*/0,
                TCollectStatsLevel::None,
                TTxId(),
                {{"test_token", "{\"token\": \"token_value\"}"}},
                Callbacks_,
                NKikimr::NKqp::NFederatedQueryTest::CreateCredentialsFactory("token_value"));

            AsyncOutput_ = output;
            WriteActor_ = RegisterWithSameMailbox(actor);

            // The write actor's Bootstrap is enqueued on this shared mailbox before the
            // wakeup below, so by the time we handle it the schema is resolved.
            Send(SelfId(), new TEvents::TEvWakeup());
        }

    private:
        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                cFunc(TEvents::TEvWakeup::EventType, DoSendData);
            }
        }

        void DoSendData() {
            auto guard = Guard(*Alloc_);

            auto idArray = MakeUInt64Array({1, 2, 3});
            auto payloadArray = MakeStringArray({"a", "b", "c"});

            NYql::NUdf::TUnboxedValue values[3];
            values[0] = HolderFactory_.CreateArrowBlock(arrow::Datum(idArray));
            values[1] = HolderFactory_.CreateArrowBlock(arrow::Datum(payloadArray));
            values[2] = MakeBlockCount(HolderFactory_, 3);

            TTypeBuilder typeBuilder(TypeEnv_);
            NKikimr::NMiniKQL::TType* elemType = typeBuilder.NewDataType(NYql::NUdf::EDataSlot::Uint64, false);
            NKikimr::NMiniKQL::TType* elems[3] = {elemType, elemType, elemType};
            auto* multiType = NKikimr::NMiniKQL::TMultiType::Create(3, elems, TypeEnv_);

            TUnboxedValueBatch batch(multiType);
            batch.PushRow(values, 3);

            AsyncOutput_->SendData(std::move(batch), 0, Nothing(), /*finished=*/true);

            // The write actor is driven synchronously via SendData (no message handlers),
            // so we do not poison it here: STRICT_STFUNC_EXC would treat TEvPoison as an
            // unexpected message. The test runtime tears the actor down at shutdown.
            Send(Edge_, new TEvents::TEvWakeup());
        }

    public:
        void PassAway() override {
            TBase::PassAway();
        }

    private:
        static constexpr char ActorName[] = "TEST_WRITE_CALLER";

        std::shared_ptr<TScopedAlloc> Alloc_;
        THolderFactory& HolderFactory_;
        TTypeEnvironment& TypeEnv_;
        const IFunctionRegistry& FunctionRegistry_;
        std::shared_ptr<TFakeWriteClient> Client_;
        TFakeCallbacks* Callbacks_;
        TActorId Edge_;

        IDqComputeActorAsyncOutput* AsyncOutput_ = nullptr;
        TActorId WriteActor_;
    };

} // namespace

Y_UNIT_TEST_SUITE(GenericProviderWriteActor) {

    Y_UNIT_TEST(WriteBlockAppendsRows) {
        auto alloc = std::make_shared<TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), true, false);
        TMemoryUsageInfo memUsage("TestMemUsage");
        THolderFactory holderFactory(alloc->Ref(), memUsage);
        TTypeEnvironment typeEnv(*alloc);

        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());

        auto loggerConfig = NYql::NProto::TLoggingConfig();
        loggerConfig.set_allcomponentslevel(::NYql::NProto::TLoggingConfig_ELevel::TLoggingConfig_ELevel_TRACE);
        NYql::NLog::InitLogger(loggerConfig, false);

        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto edge = runtime.AllocateEdgeActor();

        auto client = std::make_shared<TFakeWriteClient>();
        TFakeCallbacks callbacks;

        auto* caller = new TCallWriteActor(
            alloc,
            holderFactory,
            typeEnv,
            *functionRegistry,
            client,
            &callbacks,
            edge);

        runtime.Register(caller);
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(edge);

        UNIT_ASSERT_C(callbacks.Error.empty(), callbacks.Error);
        UNIT_ASSERT(callbacks.Finished);

        UNIT_ASSERT_VALUES_EQUAL(client->Table, "//tmp/write_test");
        UNIT_ASSERT_VALUES_EQUAL(client->Cluster, "test_cluster");
        UNIT_ASSERT_VALUES_EQUAL(client->Batches.size(), 1u);

        auto recordBatch = client->Batches[0];
        UNIT_ASSERT_VALUES_EQUAL(recordBatch->num_columns(), 2);
        UNIT_ASSERT_VALUES_EQUAL(recordBatch->num_rows(), 3);

        UNIT_ASSERT_VALUES_EQUAL(recordBatch->schema()->field(0)->name(), "id");
        UNIT_ASSERT_VALUES_EQUAL(recordBatch->schema()->field(1)->name(), "payload");

        auto idArray = std::static_pointer_cast<arrow::UInt64Array>(recordBatch->column(0));
        UNIT_ASSERT_VALUES_EQUAL(idArray->Value(0), 1u);
        UNIT_ASSERT_VALUES_EQUAL(idArray->Value(1), 2u);
        UNIT_ASSERT_VALUES_EQUAL(idArray->Value(2), 3u);

        auto payloadArray = std::static_pointer_cast<arrow::StringArray>(recordBatch->column(1));
        UNIT_ASSERT_VALUES_EQUAL(payloadArray->GetString(0), "a");
        UNIT_ASSERT_VALUES_EQUAL(payloadArray->GetString(1), "b");
        UNIT_ASSERT_VALUES_EQUAL(payloadArray->GetString(2), "c");
    }

} // Y_UNIT_TEST_SUITE(GenericProviderWriteActor)
