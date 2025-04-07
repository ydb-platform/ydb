#include "yql_generic_base_actor.h"
#include "yql_generic_read_actor.h"
#include "yql_generic_token_provider.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/proto/partition.pb.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NDq {

    using namespace NActors;

    namespace {

        template <typename T>
        T ExtractFromConstFuture(const NThreading::TFuture<T>& f) {
            // We want to avoid making a copy of data stored in a future.
            // But there is no direct way to extract data from a const future
            // So, we make a copy of the future, that is cheap. Then, extract the value from this copy.
            // It destructs the value in the original future, but this trick is legal and documented here:
            // https://docs.yandex-team.ru/arcadia-cpp/cookbook/concurrency
            return NThreading::TFuture<T>(f).ExtractValueSync();
        }

    } // namespace

    class TGenericReadActor: public TGenericBaseActor<TGenericReadActor>, public IDqComputeActorAsyncInput {
    public:
        TGenericReadActor(
            ui64 inputIndex,
            TCollectStatsLevel statsLevel,
            NConnector::IClient::TPtr client,
            TGenericTokenProvider::TPtr tokenProvider,
            NGeneric::TSource&& source,
            const NActors::TActorId& computeActorId,
            const NKikimr::NMiniKQL::THolderFactory& holderFactory,
            TVector<NGeneric::TPartition>&& partitions)
            : InputIndex_(inputIndex)
            , ComputeActorId_(computeActorId)
            , Client_(std::move(client))
            , TokenProvider_(std::move(tokenProvider))
            , Partitions_(std::move(partitions))
            , HolderFactory_(holderFactory)
            , Source_(source)
        {
            IngressStats_.Level = statsLevel;
        }

        void Bootstrap() {
            Become(&TGenericReadActor::StateFunc);
            auto issue = InitSplitsReading();
            if (issue) {
                return NotifyComputeActorWithIssue(
                    TActivationContext::ActorSystem(),
                    ComputeActorId_,
                    InputIndex_,
                    std::move(*issue));
            };
        }

        static constexpr char ActorName[] = "GENERIC_READ_ACTOR";

    private:
        // clang-format off
        STRICT_STFUNC(StateFunc,
                      hFunc(TEvReadSplitsIterator, Handle);
                      hFunc(TEvReadSplitsPart, Handle);
                      hFunc(TEvReadSplitsFinished, Handle);
        )
        // clang-format on

        // ReadSplits
        TMaybe<TIssue> InitSplitsReading() {
            YQL_CLOG(DEBUG, ProviderGeneric) << "Start splits reading";

            if (Partitions_.empty()) {
                YQL_CLOG(WARN, ProviderGeneric) << "Got empty list of partitions";
                ReadSplitsFinished_ = true;
                NotifyComputeActorWithData();
                return Nothing();
            }

            // Prepare ReadSplits request. For the sake of simplicity,
            // all the splits from all partitions will be packed into a single ReadSplits call.
            // There's a lot of space for the optimizations here.
            NConnector::NApi::TReadSplitsRequest request;
            request.set_format(NConnector::NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
            request.set_filtering(NConnector::NApi::TReadSplitsRequest::FILTERING_OPTIONAL);

            for (const auto& partition : Partitions_) {
                request.mutable_splits()->Reserve(request.splits().size() + partition.splits().size());

                for (const auto& srcSplit : partition.splits()) {
                    auto dstSplit = request.add_splits();

                    // Take actual SQL request from the source, because it contains predicates
                    *dstSplit->mutable_select() = Source_.select();

                    // Take split description from task params
                    dstSplit->set_description(srcSplit.description());

                    // Assign actual IAM token to a split
                    auto error = TokenProvider_->MaybeFillToken(*dstSplit->mutable_select()->mutable_data_source_instance());
                    if (error) {
                        return TIssue(std::move(error));
                    }
                }
            }

            // Start streaming
            Client_->ReadSplits(request).Subscribe(
                [actorSystem = TActivationContext::ActorSystem(),
                 selfId = SelfId(),
                 computeActorId = ComputeActorId_,
                 inputIndex = InputIndex_](
                    const NConnector::TReadSplitsStreamIteratorAsyncResult& future) {
                    AwaitIterator<
                        NConnector::TReadSplitsStreamIteratorAsyncResult,
                        TEvReadSplitsIterator>(
                        actorSystem, selfId, computeActorId, inputIndex, future);
                });

            return Nothing();
        }

        void Handle(TEvReadSplitsIterator::TPtr& ev) {
            ReadSplitsIterator_ = std::move(ev->Get()->Iterator);

            AwaitNextStreamItem<NConnector::IReadSplitsStreamIterator,
                                TEvReadSplitsPart,
                                TEvReadSplitsFinished>(ReadSplitsIterator_);
        }

        void Handle(TEvReadSplitsPart::TPtr& ev) {
            auto& response = ev->Get()->Response;
            YQL_CLOG(TRACE, ProviderGeneric) << "Handle :: EvReadSplitsPart :: event handling started"
                                             << ": part_size=" << response.arrow_ipc_streaming().size();

            if (!NConnector::IsSuccess(response)) {
                return NotifyComputeActorWithError(
                    TActivationContext::ActorSystem(),
                    ComputeActorId_,
                    InputIndex_,
                    response.error());
            }

            YQL_ENSURE(response.arrow_ipc_streaming().size(), "empty data");

            // Preserve stream message to return it to ComputeActor later
            LastReadSplitsResponse_ = std::move(ev->Get()->Response);
            UpdateIngressStats();
            NotifyComputeActorWithData();

            YQL_CLOG(TRACE, ProviderGeneric) << "Handle :: EvReadSplitsPart :: event handling finished";
        }

        void Handle(TEvReadSplitsFinished::TPtr& ev) {
            const auto& status = ev->Get()->Status;

            YQL_CLOG(TRACE, ProviderGeneric) << "Handle :: EvReadSplitsFinished :: event handling started: " << status.ToDebugString();

            // Server sent EOF, no more data is expected, so ask compute actor to come for the last time
            if (NConnector::GrpcStatusEndOfStream(status)) {
                YQL_CLOG(DEBUG, ProviderGeneric) << "Handle :: EvReadSplitsFinished :: last message was reached, finish data reading";
                ReadSplitsFinished_ = true;
                return NotifyComputeActorWithData();
            }

            // Server temporary failure
            if (NConnector::GrpcStatusNeedsRetry(status)) {
                YQL_CLOG(WARN, ProviderGeneric) << "Handle :: EvReadSplitsFinished :: you should retry your operation due to '"
                                                << status.ToDebugString() << "' error";
                // TODO: retry
            }

            return NotifyComputeActorWithError(
                TActivationContext::ActorSystem(),
                ComputeActorId_,
                InputIndex_,
                NConnector::ErrorFromGRPCStatus(status));
        }

        template <typename TIterator, typename TEventPart, typename TEventFinished>
        void AwaitNextStreamItem(const typename TIterator::TPtr& iterator) {
            YQL_ENSURE(iterator, "iterator was not initialized");

            iterator->ReadNext().Subscribe(
                [actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const typename TIterator::TResult& asyncResult) {
                    auto result = ExtractFromConstFuture(asyncResult);
                    if (result.Status.Ok()) {
                        YQL_ENSURE(result.Response, "empty response");
                        auto ev = new TEventPart(std::move(*result.Response));
                        actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                    } else {
                        auto ev = new TEventFinished(std::move(result.Status));
                        actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                    }
                });
        }

        template <typename TAsyncResult, typename TIteratorEvent>
        static void AwaitIterator(TActorSystem* actorSystem, TActorId selfId, TActorId computeActorId, ui64 inputIndex, const TAsyncResult& asyncResult) {
            auto result = ExtractFromConstFuture(asyncResult);
            if (result.Status.Ok()) {
                YQL_ENSURE(result.Iterator, "uninitialized iterator");
                auto ev = new TIteratorEvent(std::move(result.Iterator));
                actorSystem->Send(new NActors::IEventHandle(selfId, {}, ev));
            } else {
                NotifyComputeActorWithError(
                    actorSystem,
                    computeActorId,
                    inputIndex,
                    NConnector::ErrorFromGRPCStatus(result.Status));
            }
        }

        void UpdateIngressStats() {
            if (LastReadSplitsResponse_->has_stats()) {
                IngressStats_.Bytes += LastReadSplitsResponse_->stats().bytes();
                IngressStats_.Rows += LastReadSplitsResponse_->stats().rows();
            } else {
                // Delete this branch with fallback behavior after YQ-2347 is deployed
                IngressStats_.Bytes += LastReadSplitsResponse_->ByteSizeLong();
            }
            IngressStats_.Chunks++;
            IngressStats_.Resume();
        }

        void NotifyComputeActorWithData() {
            Send(ComputeActorId_, new TEvNewAsyncInputDataArrived(InputIndex_));
        }

        static void NotifyComputeActorWithError(
            TActorSystem* actorSystem,
            NActors::TActorId computeActorId,
            ui64 inputIndex,
            const NConnector::NApi::TError& error) {
            actorSystem->Send(computeActorId,
                              new TEvAsyncInputError(
                                  inputIndex,
                                  NConnector::ErrorToIssues(error),
                                  NConnector::ErrorToDqStatus(error)));
            return;
        }

        static void NotifyComputeActorWithIssue(
            TActorSystem* actorSystem,
            NActors::TActorId computeActorId,
            ui64 inputIndex,
            TIssue issue) {
            actorSystem->Send(computeActorId,
                              new TEvAsyncInputError(
                                  inputIndex,
                                  TIssues{std::move(issue)},
                                  NDqProto::StatusIds::StatusCode::StatusIds_StatusCode_INTERNAL_ERROR));
            return;
        }

        i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer,
                              TMaybe<TInstant>&,
                              bool& finished,
                              i64 /*freeSpace*/) final {
            YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");

            YQL_CLOG(TRACE, ProviderGeneric) << "GetAsyncInputData :: start";

            // Stream is finished
            if (!LastReadSplitsResponse_) {
                finished = ReadSplitsFinished_;

                if (finished) {
                    YQL_CLOG(INFO, ProviderGeneric) << "GetAsyncInputData :: data reading finished";
                }

                IngressStats_.TryPause();
                return 0;
            }

            // Stream is not finished, process next message
            NUdf::TUnboxedValue value;
            ui64 total = 0;

            // It's very important to fill UV columns in the alphabet order,
            // paying attention to the scalar field containing block length.
            TVector<TString> fieldNames;
            std::transform(Source_.select().what().items().cbegin(),
                           Source_.select().what().items().cend(),
                           std::back_inserter(fieldNames),
                           [](const auto& item) { return item.column().name(); });

            fieldNames.push_back(std::string(BlockLengthColumnName));
            std::sort(fieldNames.begin(), fieldNames.end());
            std::map<TStringBuf, std::size_t> fieldNameOrder;
            for (std::size_t i = 0; i < fieldNames.size(); i++) {
                fieldNameOrder[fieldNames[i]] = i;
            }

            // TODO: avoid copying from Protobuf to Arrow
            auto batch = NConnector::ReadSplitsResponseToArrowRecordBatch(*LastReadSplitsResponse_);

            total += NUdf::GetSizeOfArrowBatchInBytes(*batch);

            NUdf::TUnboxedValue* structItems = nullptr;
            auto structObj = ArrowRowContainerCache_.NewArray(HolderFactory_, fieldNames.size(), structItems);
            for (int i = 0; i < batch->num_columns(); ++i) {
                const auto& columnName = batch->schema()->field(i)->name();
                const auto ix = fieldNameOrder[columnName];
                structItems[ix] = HolderFactory_.CreateArrowBlock(arrow::Datum(batch->column(i)));
            }

            structItems[fieldNameOrder[BlockLengthColumnName]] = HolderFactory_.CreateArrowBlock(
                arrow::Datum(std::make_shared<arrow::UInt64Scalar>(batch->num_rows())));
            value = structObj;

            buffer.emplace_back(std::move(value));

            // freeSpace -= size;
            LastReadSplitsResponse_ = std::nullopt;

            // TODO: check it, because in S3 the generic cache clearing happens only when LastFileWasProcessed:
            // https://a.yandex-team.ru/arcadia/ydb/library/yql/providers/s3/actors/yql_s3_read_actor.cpp?rev=r11543410#L2497
            ArrowRowContainerCache_.Clear();

            // Request server for the next data block
            AwaitNextStreamItem<NConnector::IReadSplitsStreamIterator,
                                TEvReadSplitsPart,
                                TEvReadSplitsFinished>(ReadSplitsIterator_);
            finished = false;

            YQL_CLOG(TRACE, ProviderGeneric) << "GetAsyncInputData :: bytes obtained = " << total;

            return total;
        }

        // IActor & IDqComputeActorAsyncInput
        void PassAway() override { // Is called from Compute Actor
            YQL_CLOG(INFO, ProviderGeneric) << "PassAway :: final ingress stats"
                                            << ": bytes " << IngressStats_.Bytes
                                            << ", rows " << IngressStats_.Rows
                                            << ", chunks " << IngressStats_.Chunks;
            TActorBootstrapped<TGenericReadActor>::PassAway();
        }

        void SaveState(const NDqProto::TCheckpoint&, TSourceState&) final {
        }

        void LoadState(const TSourceState&) final {
        }

        void CommitState(const NDqProto::TCheckpoint&) final {
        }

        ui64 GetInputIndex() const final {
            return InputIndex_;
        }

        const TDqAsyncStats& GetIngressStats() const override {
            return IngressStats_;
        }

    private:
        const ui64 InputIndex_;
        TDqAsyncStats IngressStats_;
        const NActors::TActorId ComputeActorId_;

        NConnector::IClient::TPtr Client_;
        TGenericTokenProvider::TPtr TokenProvider_;

        TVector<NGeneric::TPartition> Partitions_;

        NConnector::IReadSplitsStreamIterator::TPtr ReadSplitsIterator_;
        std::optional<NConnector::NApi::TReadSplitsResponse> LastReadSplitsResponse_;
        bool ReadSplitsFinished_ = false;

        NKikimr::NMiniKQL::TPlainContainerCache ArrowRowContainerCache_;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
        NGeneric::TSource Source_;
    };

    void ExtractPartitionsFromParams(
        TVector<NGeneric::TPartition>& partitions,
        const THashMap<TString, TString>& taskParams, // partitions are here in v1
        const TVector<TString>& readRanges            // partitions are here in v2
    ) {
        if (!readRanges.empty()) {
            for (const auto& readRange : readRanges) {
                NGeneric::TPartition partition;
                YQL_ENSURE(partition.ParseFromString(readRange), "Failed to parse partition from read ranges");
                partitions.emplace_back(std::move(partition));
            }
        } else {
            const auto& iter = taskParams.find(GenericProviderName);
            if (iter != taskParams.end()) {
                NGeneric::TPartition partition;
                TStringInput input(iter->first);
                YQL_ENSURE(partition.ParseFromString(iter->second), "Failed to parse partition from task params");
                partitions.emplace_back(std::move(partition));
            }
        }

        Y_ENSURE(!partitions.empty(), "partitions must not be empty");
    }

    std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>
    CreateGenericReadActor(NConnector::IClient::TPtr genericClient,
                           NGeneric::TSource&& source,
                           ui64 inputIndex,
                           TCollectStatsLevel statsLevel,
                           const THashMap<TString, TString>& /*secureParams*/,
                           ui64 taskId,
                           const THashMap<TString, TString>& taskParams,
                           const TVector<TString>& readRanges,
                           const NActors::TActorId& computeActorId,
                           ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
                           const NKikimr::NMiniKQL::THolderFactory& holderFactory)
    {
        TVector<NGeneric::TPartition> partitions;
        ExtractPartitionsFromParams(partitions, taskParams, readRanges);

        const auto dsi = source.select().data_source_instance();
        YQL_CLOG(INFO, ProviderGeneric) << "Creating read actor with params:"
                                        << " kind=" << NYql::EGenericDataSourceKind_Name(dsi.kind())
                                        << ", endpoint=" << dsi.endpoint().ShortDebugString()
                                        << ", database=" << dsi.database()
                                        << ", use_tls=" << ToString(dsi.use_tls())
                                        << ", protocol=" << NYql::EGenericProtocol_Name(dsi.protocol())
                                        << ", task_id=" << taskId
                                        << ", partitions_count=" << partitions.size();

        // FIXME: strange piece of logic - authToken is created but not used:
        // https://a.yandex-team.ru/arcadia/ydb/library/yql/providers/clickhouse/actors/yql_ch_read_actor.cpp?rev=r11550199#L140
        /*
        const auto token = secureParams.Value(params.token(), TString{});
        const auto credentialsProviderFactory =
            CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token);
        const auto authToken = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
        const auto one = token.find('#'), two = token.rfind('#');
        YQL_ENSURE(one != TString::npos && two != TString::npos && one < two, "Bad token format:" << token);
        */

        auto tokenProvider = CreateGenericTokenProvider(
            source.GetToken(),
            source.GetServiceAccountId(),
            source.GetServiceAccountIdSignature(),
            credentialsFactory);

        const auto actor = new TGenericReadActor(
            inputIndex,
            statsLevel,
            genericClient,
            std::move(tokenProvider),
            std::move(source),
            computeActorId,
            holderFactory,
            std::move(partitions));

        return {actor, actor};
    }

} // namespace NYql::NDq
