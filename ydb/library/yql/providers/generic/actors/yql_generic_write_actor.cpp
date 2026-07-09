#include "yql_generic_credentials_provider.h"
#include "yql_generic_write_actor.h"

#include <arrow/api.h>

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

    using namespace NActors;
    using namespace NKikimr::NMiniKQL;

    // TGenericWriteActor is the generic-provider write (sink) DQ actor. It consumes rows
    // in the wide/block (Arrow) representation used by the generic provider, serializes
    // each incoming block to Arrow IPC streaming and appends the rows to the target table
    // through the connector client's WriteRows entrypoint (currently the YT-native client).
    class TGenericWriteActor: public TActorBootstrapped<TGenericWriteActor>, public IDqComputeActorAsyncOutput {
    public:
        TGenericWriteActor(
            NConnector::IClient::TPtr client,
            TGenericCredentialsProvider::TPtr tokenProvider,
            Generic::TSink&& sink,
            TVector<TString>&& columnNames,
            ui64 outputIndex,
            TCollectStatsLevel statsLevel,
            const TTxId& txId,
            IDqComputeActorAsyncOutput::ICallbacks* callbacks)
            : OutputIndex_(outputIndex)
            , TxId_(txId)
            , Client_(std::move(client))
            , TokenProvider_(std::move(tokenProvider))
            , Sink_(std::move(sink))
            , ColumnNames_(std::move(columnNames))
            , Callbacks_(callbacks)
        {
            EgressStats_.Level = statsLevel;
        }

        void Bootstrap() {
            Cerr << "YTDBG WriteActor::Bootstrap outputIndex=" << OutputIndex_ << Endl;
            Become(&TGenericWriteActor::StateFunc);

            // Prepare the data source instance with credentials filled in for the YT client.
            DataSourceInstance_ = Sink_.data_source_instance();
            if (TokenProvider_) {
                const auto err = TokenProvider_->FillCredentials(DataSourceInstance_);
                if (err) {
                    return OnFatalError(TIssue(TStringBuilder() << "Failed to fill credentials: " << err));
                }
            }

            // The target table schema is resolved at compile time (via DescribeTable
            // in the metadata transformer) and carried in the sink settings, so the
            // write actor does not re-describe the table at runtime.
            Schema_ = Sink_.schema();
            SchemaReady_ = true;
        }

        static constexpr char ActorName[] = "GENERIC_WRITE_ACTOR";

    private:
        // clang-format off
        STRICT_STFUNC_EXC(StateFunc,
                          , ExceptionFunc(std::exception, HandleException)
        )
        // clang-format on

        void HandleException(const std::exception& e) {
            OnFatalError(TIssue(TStringBuilder() << "Internal error. Got unexpected exception: " << e.what()));
        }

        void OnFatalError(TIssue issue) {
            Callbacks_->OnAsyncOutputError(
                OutputIndex_,
                TIssues{std::move(issue)},
                NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR);
        }

        // Serializes a single wide block batch to Arrow IPC streaming, matching the
        // format produced by the connector read path (ReadSplits).
        TString SerializeBlock(NUdf::TUnboxedValue* values, ui32 width) {
            YQL_ENSURE(width, "Expected non zero width for block output");
            YQL_ENSURE(width - 1 == ColumnNames_.size(),
                       "Block width " << (width - 1) << " does not match column count " << ColumnNames_.size());

            std::vector<std::shared_ptr<arrow::Field>> fields;
            std::vector<std::shared_ptr<arrow::Array>> columns;
            fields.reserve(width - 1);
            columns.reserve(width - 1);

            for (ui32 i = 0; i < width - 1; ++i) {
                auto array = TArrowBlock::From(values[i]).GetDatum().make_array();
                fields.push_back(arrow::field(std::string(ColumnNames_[i]), array->type()));
                columns.push_back(std::move(array));
            }

            const auto numRows =
                TArrowBlock::From(values[width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

            auto recordBatch = arrow::RecordBatch::Make(arrow::schema(fields), numRows, std::move(columns));

            auto serializer = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
            return serializer->SerializeFull(recordBatch);
        }

        // IDqComputeActorAsyncOutput
        void SendData(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64,
                      const TMaybe<NDqProto::TCheckpoint>&, bool finished) override {
            Cerr << "YTDBG WriteActor::SendData finished=" << finished
                 << " isWide=" << batch.IsWide() << " rowCount=" << batch.RowCount() << Endl;
            YQL_ENSURE(SchemaReady_, "Schema is not ready");
            YQL_ENSURE(batch.IsWide(), "Expected wide input for generic block sink");

            EgressStats_.Resume();

            batch.ForEachRowWide([&](NUdf::TUnboxedValue* values, ui32 width) {
                const auto arrowIpc = SerializeBlock(values, width);

                Client_->WriteRows(Schema_, Sink_.table(), arrowIpc, DataSourceInstance_);

                EgressStats_.Bytes += arrowIpc.size();
                EgressStats_.Chunks++;
                EgressStats_.Splits++;
            });

            batch.clear();

            if (finished) {
                Finished_ = true;
                Callbacks_->OnAsyncOutputFinished(OutputIndex_);
            }
        }

        ui64 GetOutputIndex() const override {
            return OutputIndex_;
        }

        i64 GetFreeSpace() const override {
            Cerr << "YTDBG WriteActor::GetFreeSpace" << Endl;
            return std::numeric_limits<i64>::max();
        }

        const TDqAsyncStats& GetEgressStats() const override {
            return EgressStats_;
        }

        void CommitState(const NDqProto::TCheckpoint&) override {
        }

        void LoadState(const TSinkState&) override {
        }

        void PassAway() override { // Is called from Compute Actor
            YQL_CLOG(INFO, ProviderGeneric) << "PassAway :: final egress stats"
                                            << ": bytes " << EgressStats_.Bytes
                                            << ", rows " << EgressStats_.Rows
                                            << ", chunks " << EgressStats_.Chunks;
            TActorBootstrapped<TGenericWriteActor>::PassAway();
        }

    private:
        const ui64 OutputIndex_;
        const TTxId TxId_;
        TDqAsyncStats EgressStats_;

        NConnector::IClient::TPtr Client_;
        TGenericCredentialsProvider::TPtr TokenProvider_;
        Generic::TSink Sink_;
        TVector<TString> ColumnNames_;
        IDqComputeActorAsyncOutput::ICallbacks* const Callbacks_;

        NYql::TGenericDataSourceInstance DataSourceInstance_;
        NConnector::NApi::TSchema Schema_;
        bool SchemaReady_ = false;
        bool Finished_ = false;
    };

    std::pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*>
    CreateGenericWriteActor(
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
        NConnector::IClient::TPtr genericClient,
        Generic::TSink&& sink,
        ui64 outputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        const THashMap<TString, TString>& secureParams,
        NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory)
    {
        const auto& dsi = sink.data_source_instance();
        Cerr << "YTDBG CreateGenericWriteActor kind=" << NYql::EGenericDataSourceKind_Name(dsi.kind())
             << " table=" << sink.table() << Endl;
        YQL_CLOG(INFO, ProviderGeneric) << "Creating write actor with params:"
                                        << " kind=" << NYql::EGenericDataSourceKind_Name(dsi.kind())
                                        << ", table=" << sink.table();

        // Parse the serialized (YSON) row type into the list of column names. The wide block
        // columns arrive in struct member order, so we preserve that order for the arrow
        // record batch field names (which the connector client maps to table columns).
        const auto programBuilder = std::make_unique<TProgramBuilder>(typeEnv, functionRegistry);
        const TStringBuf rowTypeYson(sink.GetRowType());
        TStringStream error;
        const auto* rowType = NCommon::ParseTypeFromYson(rowTypeYson, *programBuilder, error);
        YQL_ENSURE(rowType, "Failed to parse row type: " << rowTypeYson << ", reason: " << error.Str());
        YQL_ENSURE(rowType->IsStruct(), "Row type is not a struct");
        const auto* structType = static_cast<const TStructType*>(rowType);

        TVector<TString> columnNames;
        columnNames.reserve(structType->GetMembersCount());
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            columnNames.emplace_back(structType->GetMemberName(i));
        }

        auto tokenProvider = CreateGenericCredentialsProvider(
            secureParams.Value(sink.GetTokenName(), ""),
            credentialsFactory);

        const auto actor = new TGenericWriteActor(
            std::move(genericClient),
            std::move(tokenProvider),
            std::move(sink),
            std::move(columnNames),
            outputIndex,
            statsLevel,
            txId,
            callbacks);

        return {actor, actor};
    }

} // namespace NYql::NDq
