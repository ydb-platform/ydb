#include "yql_generic_base_actor.h"
#include "yql_generic_read_actor.h"
#include "yql_generic_token_provider.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

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
            Generic::TSource&& source,
            const NActors::TActorId& computeActorId,
            const NKikimr::NMiniKQL::THolderFactory& holderFactory)
            : InputIndex_(inputIndex)
            , ComputeActorId_(computeActorId)
            , Client_(std::move(client))
            , TokenProvider_(std::move(tokenProvider))
            , HolderFactory_(holderFactory)
            , Source_(source)
        {
            IngressStats_.Level = statsLevel;
        }

        void Bootstrap() {
            Become(&TGenericReadActor::StateFunc);
            InitSplitsListing();
        }

        static constexpr char ActorName[] = "GENERIC_READ_ACTOR";

    private:
        // TODO: make two different states
        // clang-format off
        STRICT_STFUNC(StateFunc,
                      hFunc(TEvListSplitsIterator, Handle);
                      hFunc(TEvListSplitsPart, Handle);
                      hFunc(TEvListSplitsFinished, Handle);
                      hFunc(TEvReadSplitsIterator, Handle);
                      hFunc(TEvReadSplitsPart, Handle);
                      hFunc(TEvReadSplitsFinished, Handle);
        )
        // clang-format on

        // ListSplits

        void InitSplitsListing() {
            YQL_CLOG(DEBUG, ProviderGeneric) << "Start splits listing";

            // Prepare request
            NConnector::NApi::TListSplitsRequest request;
            NConnector::NApi::TSelect select = Source_.select(); // copy TSelect from source
            TokenProvider_->MaybeFillToken(*select.mutable_data_source_instance());
            *request.mutable_selects()->Add() = std::move(select);

            // Initialize stream
            Client_->ListSplits(request).Subscribe(
                [actorSystem = TActivationContext::ActorSystem(),
                 selfId = SelfId(),
                 computeActorId = ComputeActorId_,
                 inputIndex = InputIndex_](
                    const NConnector::TListSplitsStreamIteratorAsyncResult& future) {
                    AwaitIterator<
                        NConnector::TListSplitsStreamIteratorAsyncResult,
                        TEvListSplitsIterator>(
                        actorSystem, selfId, computeActorId, inputIndex, future);
                });
        }

        void Handle(TEvListSplitsIterator::TPtr& ev) {
            ListSplitsIterator_ = std::move(ev->Get()->Iterator);

            AwaitNextStreamItem<NConnector::IListSplitsStreamIterator,
                                TEvListSplitsPart,
                                TEvListSplitsFinished>(ListSplitsIterator_);
        }

        void Handle(TEvListSplitsPart::TPtr& ev) {
            auto& response = ev->Get()->Response;
            YQL_CLOG(TRACE, ProviderGeneric) << "Handle :: EvListSplitsPart :: event handling started"
                                             << ": splits_size=" << response.splits().size();

            if (!NConnector::IsSuccess(response)) {
                return NotifyComputeActorWithError(
                    TActivationContext::ActorSystem(),
                    ComputeActorId_,
                    InputIndex_,
                    response.error());
            }

            // Save splits for the further usage
            Splits_.insert(
                Splits_.end(),
                std::move_iterator(response.mutable_splits()->begin()),
                std::move_iterator(response.mutable_splits()->end()));

            // ask for next stream message
            AwaitNextStreamItem<NConnector::IListSplitsStreamIterator,
                                TEvListSplitsPart,
                                TEvListSplitsFinished>(ListSplitsIterator_);

            YQL_CLOG(TRACE, ProviderGeneric) << "Handle :: EvListSplitsPart :: event handling finished";
        }

        void Handle(TEvListSplitsFinished::TPtr& ev) {
            const auto& status = ev->Get()->Status;

            YQL_CLOG(TRACE, ProviderGeneric) << "Handle :: EvListSplitsFinished :: event handling started: ";

            // Server sent EOF, now we are ready to start splits reading
            if (NConnector::GrpcStatusEndOfStream(status)) {
                YQL_CLOG(DEBUG, ProviderGeneric) << "Handle :: EvListSplitsFinished :: last message was reached, start data reading";
                return InitSplitsReading();
            }

            // Server temporary failure
            if (NConnector::GrpcStatusNeedsRetry(status)) {
                YQL_CLOG(WARN, ProviderGeneric) << "Handle :: EvListSplitsFinished :: you should retry your operation due to '"
                                                << status.ToDebugString() << "' error";
                // TODO: retry
            }

            return NotifyComputeActorWithError(
                TActivationContext::ActorSystem(),
                ComputeActorId_,
                InputIndex_,
                NConnector::ErrorFromGRPCStatus(status));
        }

        // ReadSplits
        void InitSplitsReading() {
            YQL_CLOG(DEBUG, ProviderGeneric) << "Start splits reading";

            if (Splits_.empty()) {
                YQL_CLOG(WARN, ProviderGeneric) << "Accumulated empty list of splits";
                ReadSplitsFinished_ = true;
                return NotifyComputeActorWithData();
            }

            // Prepare request
            NConnector::NApi::TReadSplitsRequest request;
            request.set_format(NConnector::NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
            request.mutable_splits()->Reserve(Splits_.size());

            std::for_each(
                Splits_.cbegin(), Splits_.cend(),
                [&](const NConnector::NApi::TSplit& split) {
                    NConnector::NApi::TSplit splitCopy = split;
                    TokenProvider_->MaybeFillToken(*splitCopy.mutable_select()->mutable_data_source_instance());
                    *request.mutable_splits()->Add() = std::move(split);
                });

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
            const NActors::TActorId computeActorId,
            const ui64 inputIndex,
            const NConnector::NApi::TError& error) {
            actorSystem->Send(computeActorId,
                              new TEvAsyncInputError(
                                  inputIndex,
                                  NConnector::ErrorToIssues(error),
                                  NConnector::ErrorToDqStatus(error)));
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
        NConnector::IListSplitsStreamIterator::TPtr ListSplitsIterator_;
        TVector<NConnector::NApi::TSplit> Splits_; // accumulated list of table splits
        NConnector::IReadSplitsStreamIterator::TPtr ReadSplitsIterator_;
        std::optional<NConnector::NApi::TReadSplitsResponse> LastReadSplitsResponse_;
        bool ReadSplitsFinished_ = false;

        NKikimr::NMiniKQL::TPlainContainerCache ArrowRowContainerCache_;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
        Generic::TSource Source_;
    };

    std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>
    CreateGenericReadActor(NConnector::IClient::TPtr genericClient,
                           Generic::TSource&& source,
                           ui64 inputIndex,
                           TCollectStatsLevel statsLevel,
                           const THashMap<TString, TString>& /*secureParams*/,
                           const THashMap<TString, TString>& /*taskParams*/,
                           const NActors::TActorId& computeActorId,
                           ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
                           const NKikimr::NMiniKQL::THolderFactory& holderFactory)
    {
        const auto dsi = source.select().data_source_instance();
        YQL_CLOG(INFO, ProviderGeneric) << "Creating read actor with params:"
                                        << " kind=" << NYql::NConnector::NApi::EDataSourceKind_Name(dsi.kind())
                                        << ", endpoint=" << dsi.endpoint().ShortDebugString()
                                        << ", database=" << dsi.database()
                                        << ", use_tls=" << ToString(dsi.use_tls())
                                        << ", protocol=" << NYql::NConnector::NApi::EProtocol_Name(dsi.protocol());

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

        // Obtain token to access remote data source if necessary
        // TODO: partitioning is not implemented now, but this code will be useful for the further research:
        /*
        TStringBuilder part;
        if (const auto taskParamsIt = taskParams.find(GenericProviderName); taskParamsIt != taskParams.cend()) {
            Generic::TRange range;
            TStringInput input(taskParamsIt->second);
            range.Load(&input);
            if (const auto& r = range.GetRange(); !r.empty())
                part << ' ' << r;
        }
        part << ';';
        */

        auto tokenProvider = CreateGenericTokenProvider(
            source.GetToken(),
            source.GetServiceAccountId(), source.GetServiceAccountIdSignature(),
            credentialsFactory);

        const auto actor = new TGenericReadActor(
            inputIndex,
            statsLevel,
            genericClient,
            std::move(tokenProvider),
            std::move(source),
            computeActorId,
            holderFactory);

        return {actor, actor};
    }

} // namespace NYql::NDq
