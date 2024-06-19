#include "yql_generic_lookup_actor.h"
#include "yql_generic_token_provider.h"
#include "yql_generic_base_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/utils.h>
#include <ydb/library/yql/providers/generic/proto/range.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

namespace NYql::NDq {

    using namespace NActors;

    namespace {

        const NKikimr::NMiniKQL::TStructType* MergeStructTypes(const NKikimr::NMiniKQL::TTypeEnvironment& env, const NKikimr::NMiniKQL::TStructType* t1, const NKikimr::NMiniKQL::TStructType* t2) {
            Y_ABORT_UNLESS(t1);
            Y_ABORT_UNLESS(t2);
            NKikimr::NMiniKQL::TStructTypeBuilder resultTypeBuilder{env};
            for (ui32 i = 0; i != t1->GetMembersCount(); ++i) {
                resultTypeBuilder.Add(t1->GetMemberName(i), t1->GetMemberType(i));
            }
            for (ui32 i = 0; i != t2->GetMembersCount(); ++i) {
                resultTypeBuilder.Add(t2->GetMemberName(i), t2->GetMemberType(i));
            }
            return resultTypeBuilder.Build();
        }

        template <typename T>
        T ExtractFromConstFuture(const NThreading::TFuture<T>& f) {
            //We want to avoid making a copy of data stored in a future.
            //But there is no direct way to extract data from a const future5
            //So, we make a copy of the future, that is cheap. Then, extract the value from this copy.
            //It destructs the value in the original future, but this trick is legal and documented here:
            //https://docs.yandex-team.ru/arcadia-cpp/cookbook/concurrency
            return NThreading::TFuture<T>(f).ExtractValueSync();
        }

    } // namespace

    class TGenericLookupActor
        : public NYql::NDq::IDqAsyncLookupSource,
          public TGenericBaseActor<TGenericLookupActor> {
        using TBase = TGenericBaseActor<TGenericLookupActor>;

    public:
        TGenericLookupActor(
            NConnector::IClient::TPtr connectorClient,
            TGenericTokenProvider::TPtr tokenProvider,
            NActors::TActorId&& parentId,
            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
            std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
            NYql::Generic::TLookupSource&& lookupSource,
            const NKikimr::NMiniKQL::TStructType* keyType,
            const NKikimr::NMiniKQL::TStructType* payloadType,
            const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
            const NKikimr::NMiniKQL::THolderFactory& holderFactory,
            const size_t maxKeysInRequest)
            : Connector(connectorClient)
            , TokenProvider(std::move(tokenProvider))
            , ParentId(std::move(parentId))
            , Alloc(alloc)
            , KeyTypeHelper(keyTypeHelper)
            , LookupSource(std::move(lookupSource))
            , KeyType(keyType)
            , PayloadType(payloadType)
            , SelectResultType(MergeStructTypes(typeEnv, keyType, payloadType))
            , HolderFactory(holderFactory)
            , ColumnDestinations(CreateColumnDestination())
            , MaxKeysInRequest(maxKeysInRequest)
            , Request(
                  0,
                  KeyTypeHelper->GetValueHash(),
                  KeyTypeHelper->GetValueEqual())
        {
        }

        ~TGenericLookupActor() {
            auto guard = Guard(*Alloc);
            KeyTypeHelper.reset();
            TKeyTypeHelper empty;
            Request = IDqAsyncLookupSource::TUnboxedValueMap(0, empty.GetValueHash(), empty.GetValueEqual());
        }

        void Bootstrap() {
            auto dsi = LookupSource.data_source_instance();
            YQL_CLOG(INFO, ProviderGeneric) << "New generic proivider lookup source actor(ActorId=" << SelfId() << ") for"
                                            << " kind=" << NYql::NConnector::NApi::EDataSourceKind_Name(dsi.kind())
                                            << ", endpoint=" << dsi.endpoint().ShortDebugString()
                                            << ", database=" << dsi.database()
                                            << ", use_tls=" << ToString(dsi.use_tls())
                                            << ", protocol=" << NYql::NConnector::NApi::EProtocol_Name(dsi.protocol())
                                            << ", table=" << LookupSource.table();
            Become(&TGenericLookupActor::StateFunc);
        }

        static constexpr char ActorName[] = "GENERIC_PROVIDER_LOOKUP_ACTOR";

    private: //IDqAsyncLookupSource
        size_t GetMaxSupportedKeysInRequest() const override {
            return MaxKeysInRequest;
        }
        void AsyncLookup(IDqAsyncLookupSource::TUnboxedValueMap&& request) override {
            auto guard = Guard(*Alloc);
            CreateRequest(std::move(request));
        }

    private: //events
        STRICT_STFUNC(StateFunc,
                      hFunc(TEvListSplitsIterator, Handle);
                      hFunc(TEvListSplitsPart, Handle);
                      hFunc(TEvReadSplitsIterator, Handle);
                      hFunc(TEvReadSplitsPart, Handle);
                      hFunc(TEvReadSplitsFinished, Handle);
                      hFunc(TEvError, Handle);
                      hFunc(NActors::TEvents::TEvPoison, Handle);)

        void Handle(TEvListSplitsIterator::TPtr ev) {
            auto& iterator = ev->Get()->Iterator;
            iterator->ReadNext().Subscribe(
                [actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const NConnector::TAsyncResult<NConnector::NApi::TListSplitsResponse>& asyncResult) {
                    YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << selfId << " Got TListSplitsResponse from Connector";
                    auto result = ExtractFromConstFuture(asyncResult);
                    if (result.Status.Ok()) {
                        Y_ABORT_UNLESS(result.Response);
                        auto ev = new TEvListSplitsPart(std::move(*result.Response));
                        actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                    } else {
                        SendError(actorSystem, selfId, result.Status);
                    }
                });
        }

        void Handle(TEvListSplitsPart::TPtr ev) {
            auto response = ev->Get()->Response;
            Y_ABORT_UNLESS(response.splits_size() == 1);
            auto& split = response.splits(0);
            NConnector::NApi::TReadSplitsRequest readRequest;
            *readRequest.mutable_data_source_instance() = GetDataSourceInstanceWithToken();
            *readRequest.add_splits() = split;
            readRequest.Setformat(NConnector::NApi::TReadSplitsRequest_EFormat::TReadSplitsRequest_EFormat_ARROW_IPC_STREAMING);
            Connector->ReadSplits(readRequest).Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const NConnector::TReadSplitsStreamIteratorAsyncResult& asyncResult) {
                YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << selfId << " Got ReadSplitsStreamIterator from Connector";
                auto result = ExtractFromConstFuture(asyncResult);
                if (result.Status.Ok()) {
                    auto ev = new TEvReadSplitsIterator(std::move(result.Iterator));
                    actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                } else {
                    SendError(actorSystem, selfId, result.Status);
                }
            });
        }

        void Handle(TEvReadSplitsIterator::TPtr ev) {
            ReadSplitsIterator = ev->Get()->Iterator;
            ReadNextData();
        }

        void Handle(TEvReadSplitsPart::TPtr ev) {
            ProcessReceivedData(ev->Get()->Response);
            ReadNextData();
        }

        void Handle(TEvReadSplitsFinished::TPtr) {
            FinalizeRequest();
        }

        void Handle(TEvError::TPtr) {
            FinalizeRequest();
        }

        void Handle(NActors::TEvents::TEvPoison::TPtr) {
            PassAway();
        }

    private:
        void CreateRequest(IDqAsyncLookupSource::TUnboxedValueMap&& request) {
            YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << SelfId() << " Got LookupRequest for " << request.size() << " keys";
            Y_ABORT_IF(InProgress);
            Y_ABORT_IF(request.size() == 0 || request.size() > MaxKeysInRequest);
            Request = std::move(request);
            NConnector::NApi::TListSplitsRequest splitRequest;
            *splitRequest.add_selects() = CreateSelect();
            splitRequest.Setmax_split_count(1);
            Connector->ListSplits(splitRequest).Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const NConnector::TListSplitsStreamIteratorAsyncResult& asyncResult) {
                auto result = ExtractFromConstFuture(asyncResult);
                if (result.Status.Ok()) {
                    YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << selfId << " Got TListSplitsStreamIterator";
                    Y_ABORT_UNLESS(result.Iterator, "Uninitialized iterator");
                    auto ev = new TEvListSplitsIterator(std::move(result.Iterator));
                    actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                } else {
                    SendError(actorSystem, selfId, result.Status);
                }
            });
        }

        void ReadNextData() {
            ReadSplitsIterator->ReadNext().Subscribe(
                [actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const NConnector::TAsyncResult<NConnector::NApi::TReadSplitsResponse>& asyncResult) {
                    auto result = ExtractFromConstFuture(asyncResult);
                    if (result.Status.Ok()) {
                        YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << selfId << " Got DataChunk";
                        Y_ABORT_UNLESS(result.Response);
                        auto& response = *result.Response;
                        if (NConnector::IsSuccess(response)) {
                            auto ev = new TEvReadSplitsPart(std::move(response));
                            actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                        } else {
                            SendError(actorSystem, selfId, response.Geterror());
                        }
                    } else if (NConnector::GrpcStatusEndOfStream(result.Status)) {
                        YQL_CLOG(DEBUG, ProviderGeneric) << "ActorId=" << selfId << " Got EOF";
                        auto ev = new TEvReadSplitsFinished(std::move(result.Status));
                        actorSystem->Send(new NActors::IEventHandle(selfId, selfId, ev));
                    } else {
                        SendError(actorSystem, selfId, result.Status);
                    }
                });
        }

        void ProcessReceivedData(const NConnector::NApi::TReadSplitsResponse& resp) {
            Y_ABORT_UNLESS(resp.payload_case() == NConnector::NApi::TReadSplitsResponse::PayloadCase::kArrowIpcStreaming);
            auto guard = Guard(*Alloc);
            NKikimr::NArrow::NSerialization::TSerializerContainer deser = NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer(); //todo move to class' member
            const auto& data = deser->Deserialize(resp.arrow_ipc_streaming());
            Y_ABORT_UNLESS(data.ok());
            const auto& value = data.ValueOrDie();
            Y_ABORT_UNLESS(static_cast<ui32>(value->num_columns()) == ColumnDestinations.size());
            std::vector<NKikimr::NMiniKQL::TUnboxedValueVector> columns(ColumnDestinations.size());
            for (size_t i = 0; i != columns.size(); ++i) {
                Y_ABORT_UNLESS(value->column_name(i) == (ColumnDestinations[i].first == EColumnDestination::Key ? KeyType : PayloadType)->GetMemberName(ColumnDestinations[i].second));
                columns[i] = NArrow::ExtractUnboxedValues(value->column(i), SelectResultType->GetMemberType(i), HolderFactory);
            }

            auto height = columns[0].size();
            for (size_t i = 0; i != height; ++i) {
                NUdf::TUnboxedValue* keyItems;
                NUdf::TUnboxedValue key = HolderFactory.CreateDirectArrayHolder(KeyType->GetMembersCount(), keyItems);
                NUdf::TUnboxedValue* outputItems;
                NUdf::TUnboxedValue output = HolderFactory.CreateDirectArrayHolder(PayloadType->GetMembersCount(), outputItems);
                for (size_t j = 0; j != columns.size(); ++j) {
                    (ColumnDestinations[j].first == EColumnDestination::Key ? keyItems : outputItems)[ColumnDestinations[j].second] = columns[j][i];
                }
                if (auto* v = Request.FindPtr(key)) {
                    *v = std::move(output); //duplicates will be overwritten
                }
            }
        }

        void FinalizeRequest() {
            YQL_CLOG(DEBUG, ProviderGeneric) << "Sending lookup results for " << Request.size() << " keys";
            auto guard = Guard(*Alloc);
            auto ev = new IDqAsyncLookupSource::TEvLookupResult(Alloc, std::move(Request));
            TActivationContext::ActorSystem()->Send(new NActors::IEventHandle(ParentId, SelfId(), ev));
            LookupResult = {};
            ReadSplitsIterator = {};
            InProgress = false;
        }

        static void SendError(NActors::TActorSystem* actorSystem, const NActors::TActorId& selfId, const NConnector::NApi::TError& error) {
            YQL_CLOG(ERROR, ProviderGeneric) << "ActorId=" << selfId << " Got GrpcError from Connector:" << error.Getmessage();
            actorSystem->Send(
                selfId,
                new TEvError(std::move(error)));
        }

        static void SendError(NActors::TActorSystem* actorSystem, const NActors::TActorId& selfId, const NYdbGrpc::TGrpcStatus& status) {
            SendError(actorSystem, selfId, NConnector::ErrorFromGRPCStatus(status));
        }

    private:
        enum class EColumnDestination {
            Key,
            Output
        };

        std::vector<std::pair<EColumnDestination, size_t>> CreateColumnDestination() {
            THashMap<TStringBuf, size_t> keyColumns;
            for (ui32 i = 0; i != KeyType->GetMembersCount(); ++i) {
                keyColumns[KeyType->GetMemberName(i)] = i;
            }
            THashMap<TStringBuf, size_t> outputColumns;
            for (ui32 i = 0; i != PayloadType->GetMembersCount(); ++i) {
                outputColumns[PayloadType->GetMemberName(i)] = i;
            }

            std::vector<std::pair<EColumnDestination, size_t>> result(SelectResultType->GetMembersCount());
            for (size_t i = 0; i != result.size(); ++i) {
                if (const auto* p = keyColumns.FindPtr(SelectResultType->GetMemberName(i))) {
                    result[i] = {EColumnDestination::Key, *p};
                } else if (const auto* p = outputColumns.FindPtr(SelectResultType->GetMemberName(i))) {
                    result[i] = {EColumnDestination::Output, *p};
                } else {
                    Y_ABORT();
                }
            }
            return result;
        }

        NYql::NConnector::NApi::TDataSourceInstance GetDataSourceInstanceWithToken() const {
            auto dsi = LookupSource.data_source_instance();
            //Note: returned token may be stale and we have no way to check or recover here
            //Consider to redesign ICredentialsProvider
            TokenProvider->MaybeFillToken(dsi);
            return dsi;
        }

        NConnector::NApi::TSelect CreateSelect() {
            NConnector::NApi::TSelect select;
            *select.mutable_data_source_instance() = GetDataSourceInstanceWithToken();

            for (ui32 i = 0; i != SelectResultType->GetMembersCount(); ++i) {
                auto c = select.mutable_what()->add_items()->mutable_column();
                c->Setname((TString(SelectResultType->GetMemberName(i))));
                ExportTypeToProto(SelectResultType->GetMemberType(i), *c->mutable_type());
            }

            select.mutable_from()->Settable(LookupSource.table());

            NConnector::NApi::TPredicate_TDisjunction disjunction;
            for (const auto& [k, _] : Request) {
                NConnector::NApi::TPredicate_TConjunction conjunction;
                for (ui32 c = 0; c != KeyType->GetMembersCount(); ++c) {
                    NConnector::NApi::TPredicate_TComparison eq;
                    eq.Setoperation(NConnector::NApi::TPredicate_TComparison_EOperation::TPredicate_TComparison_EOperation_EQ);
                    eq.mutable_left_value()->Setcolumn(TString(KeyType->GetMemberName(c)));
                    auto rightTypedValue = eq.mutable_right_value()->mutable_typed_value();
                    ExportTypeToProto(KeyType->GetMemberType(c), *rightTypedValue->mutable_type());
                    ExportValueToProto(KeyType->GetMemberType(c), k.GetElement(c), *rightTypedValue->mutable_value());
                    *conjunction.mutable_operands()->Add()->mutable_comparison() = eq;
                }
                *disjunction.mutable_operands()->Add()->mutable_conjunction() = conjunction;
            }
            *select.mutable_where()->mutable_filter_typed()->mutable_disjunction() = disjunction;
            return select;
        }

    private:
        NConnector::IClient::TPtr Connector;
        TGenericTokenProvider::TPtr TokenProvider;
        const NActors::TActorId ParentId;
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        std::shared_ptr<TKeyTypeHelper> KeyTypeHelper;
        const NYql::Generic::TLookupSource LookupSource;
        const NKikimr::NMiniKQL::TStructType* const KeyType;
        const NKikimr::NMiniKQL::TStructType* const PayloadType;
        const NKikimr::NMiniKQL::TStructType* const SelectResultType; //columns from KeyType + PayloadType
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        const std::vector<std::pair<EColumnDestination, size_t>> ColumnDestinations;
        const size_t MaxKeysInRequest;
        std::atomic_bool InProgress;
        IDqAsyncLookupSource::TUnboxedValueMap Request;
        NConnector::IReadSplitsStreamIterator::TPtr ReadSplitsIterator; //TODO move me to TEvReadSplitsPart
        NKikimr::NMiniKQL::TKeyPayloadPairVector LookupResult;
    };

    std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateGenericLookupActor(
        NConnector::IClient::TPtr connectorClient,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        NActors::TActorId parentId,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
        NYql::Generic::TLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest)
    {
        auto tokenProvider = NYql::NDq::CreateGenericTokenProvider(lookupSource.GetToken(), lookupSource.GetServiceAccountId(), lookupSource.GetServiceAccountIdSignature(), credentialsFactory);
        auto guard = Guard(*alloc);
        const auto actor = new TGenericLookupActor(
            connectorClient,
            std::move(tokenProvider),
            std::move(parentId),
            alloc,
            keyTypeHelper,
            std::move(lookupSource),
            keyType,
            payloadType,
            typeEnv,
            holderFactory,
            maxKeysInRequest);
        return {actor, actor};
    }

} // namespace NYql::NDq
