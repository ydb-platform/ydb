#include "yql_generic_read_actor.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/proto/range.pb.h>
#include <ydb/library/yql/public/udf/arrow/util.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/providers/generic/connector/api/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/error.h>

namespace NYql::NDq {

    using namespace NActors;

    namespace {

        struct TEvPrivate {
            // Event ids
            enum EEv: ui32 {
                EvBegin = EventSpaceBegin(TEvents::ES_PRIVATE),

                EvReadResult = EvBegin,
                EvReadError,

                EvEnd
            };

            static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

            // Events
            struct TEvReadResult: public TEventLocal<TEvReadResult, EvReadResult> {
                TEvReadResult(const Connector::ReadSplitsResult::TPtr& result)
                    : Result(result)
                {
                }

                Connector::ReadSplitsResult::TPtr Result;
            };

            struct TEvReadError: public TEventLocal<TEvReadError, EvReadError> {
                TEvReadError(TIssues&& error)
                    : Error(std::move(error))
                {
                }
                TIssues Error;
            };
        };

    } // namespace

    class TGenericReadActor: public TActorBootstrapped<TGenericReadActor>, public IDqComputeActorAsyncInput {
    public:
        TGenericReadActor(ui64 inputIndex, Connector::IClient::TPtr genericClient, const NYql::Connector::API::Select& select,
                          const NYql::Connector::API::DataSourceInstance& dataSourceInstance,
                          const NActors::TActorId& computeActorId, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
            : InputIndex(inputIndex)
            , ComputeActorId(computeActorId)
            , ActorSystem(TActivationContext::ActorSystem())
            , ConnectorClient(genericClient)
            , HolderFactory(holderFactory)
            , Select(select)
            , DataSourceInstance(dataSourceInstance)
        {
        }

        void Bootstrap() {
            Become(&TGenericReadActor::StateFunc);

            Connector::API::ListSplitsRequest listSplitsRequest;
            listSplitsRequest.mutable_selects()->Add()->CopyFrom(Select);
            listSplitsRequest.mutable_data_source_instance()->CopyFrom(DataSourceInstance);

            auto listSplitsResult = ConnectorClient->ListSplits(listSplitsRequest);
            if (!Connector::ErrorIsSuccess(listSplitsResult->Error)) {
                YQL_CLOG(ERROR, ProviderGeneric) << "ListSplits failure" << listSplitsResult->Error.DebugString();
                ActorSystem->Send(new IEventHandle(
                    SelfId(), TActorId(), new TEvPrivate::TEvReadError(Connector::ErrorToIssues(listSplitsResult->Error))));
                return;
            }

            YQL_CLOG(INFO, ProviderGeneric) << "ListSplits succeess, total splits: " << listSplitsResult->Splits.size();

            Connector::API::ReadSplitsRequest readSplitsRequest;
            readSplitsRequest.set_format(Connector::API::ReadSplitsRequest::ARROW_IPC_STREAMING);
            readSplitsRequest.mutable_splits()->Reserve(listSplitsResult->Splits.size());
            std::for_each(
                listSplitsResult->Splits.cbegin(), listSplitsResult->Splits.cend(),
                [&](const Connector::API::Split& split) { readSplitsRequest.mutable_splits()->Add()->CopyFrom(split); });
            readSplitsRequest.mutable_data_source_instance()->CopyFrom(DataSourceInstance);

            auto readSplitsResult = ConnectorClient->ReadSplits(readSplitsRequest);
            if (!Connector::ErrorIsSuccess(listSplitsResult->Error)) {
                YQL_CLOG(ERROR, ProviderGeneric) << "ReadSplits failure" << readSplitsResult->Error.DebugString();
                ActorSystem->Send(new IEventHandle(
                    SelfId(), TActorId(), new TEvPrivate::TEvReadError(Connector::ErrorToIssues(listSplitsResult->Error))));
                return;
            }

            YQL_CLOG(INFO, ProviderGeneric) << "ReadSplits succeess, total batches: "
                                            << readSplitsResult->RecordBatches.size();

            ActorSystem->Send(new IEventHandle(SelfId(), TActorId(), new TEvPrivate::TEvReadResult(readSplitsResult)));
        }

        static constexpr char ActorName[] = "Generic_READ_ACTOR";

    private:
        void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {
        }
        void LoadState(const NDqProto::TSourceState&) final {
        }
        void CommitState(const NDqProto::TCheckpoint&) final {
        }
        ui64 GetInputIndex() const final {
            return InputIndex;
        }

        STRICT_STFUNC(StateFunc,
                      hFunc(TEvPrivate::TEvReadResult, Handle);
                      hFunc(TEvPrivate::TEvReadError, Handle);)

        i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished,
                              i64 /*freeSpace*/) final {
            YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");
            if (Result) {
                NUdf::TUnboxedValue value;

                ui64 total = 0;

                for (const auto& batch : Result->RecordBatches) {
                    total += NUdf::GetSizeOfArrowBatchInBytes(*batch);

                    YQL_CLOG(TRACE, ProviderGeneric) << "Converting arrow::RecordBatch into NUdf::UnboxedValue:\n"
                                                     << batch->ToString();

                    // It's very important to fill UV column in the alphabet order,
                    // paying attention to the scalar field containing block length.
                    auto fieldNames = batch->schema()->field_names();
                    fieldNames.push_back(std::string(BlockLengthColumnName));
                    std::sort(fieldNames.begin(), fieldNames.end());
                    std::map<std::string, std::size_t> fieldNameOrder;
                    for (std::size_t i = 0; i < fieldNames.size(); i++) {
                        fieldNameOrder[fieldNames[i]] = i;
                    }

                    NUdf::TUnboxedValue* structItems = nullptr;
                    auto structObj = ArrowRowContainerCache.NewArray(HolderFactory, 1 + batch->num_columns(), structItems);
                    for (int i = 0; i < batch->num_columns(); ++i) {
                        const auto& columnName = batch->schema()->field(i)->name();
                        const auto ix = fieldNameOrder[columnName];
                        structItems[ix] = HolderFactory.CreateArrowBlock(arrow::Datum(batch->column(i)));
                    }

                    structItems[fieldNameOrder[std::string(BlockLengthColumnName)]] = HolderFactory.CreateArrowBlock(
                        arrow::Datum(std::make_shared<arrow::UInt64Scalar>(batch->num_rows())));
                    value = structObj;

                    buffer.emplace_back(std::move(value));
                }

                // freeSpace -= size;
                finished = true;
                Result.reset();

                // TODO: check it, because in S3 the generic cache clearing happens only when LastFileWasProcessed:
                // https://a.yandex-team.ru/arcadia/ydb/library/yql/providers/s3/actors/yql_s3_read_actor.cpp?rev=r11543410#L2497
                ArrowRowContainerCache.Clear();

                return total;
            }

            return 0LL;
        }

        void Handle(TEvPrivate::TEvReadResult::TPtr& evReadResult) {
            Result = evReadResult->Get()->Result;
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        }

        void Handle(TEvPrivate::TEvReadError::TPtr& result) {
            Send(ComputeActorId,
                 new TEvAsyncInputError(InputIndex, result->Get()->Error, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        }

        // IActor & IDqComputeActorAsyncInput
        void PassAway() override { // Is called from Compute Actor
            TActorBootstrapped<TGenericReadActor>::PassAway();
        }

        const ui64 InputIndex;
        const NActors::TActorId ComputeActorId;

        TActorSystem* const ActorSystem;

        // Changed:
        Connector::IClient::TPtr ConnectorClient;
        Connector::ReadSplitsResult::TPtr Result;
        NKikimr::NMiniKQL::TPlainContainerCache ArrowRowContainerCache;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        const NYql::Connector::API::Select Select;
        const NYql::Connector::API::DataSourceInstance DataSourceInstance;
    };

    std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>
    CreateGenericReadActor(Connector::IClient::TPtr genericClient, Generic::TSource&& params, ui64 inputIndex,
                           const THashMap<TString, TString>& /*secureParams*/,
                           const THashMap<TString, TString>& /*taskParams*/, const NActors::TActorId& computeActorId,
                           ISecuredServiceAccountCredentialsFactory::TPtr /*credentialsFactory*/,
                           const NKikimr::NMiniKQL::THolderFactory& holderFactory)
    {
        YQL_CLOG(DEBUG, ProviderGeneric) << "Creating read actor with params: " << params.ShortDebugString();

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

        const auto actor = new TGenericReadActor(inputIndex, genericClient, params.select(), params.data_source_instance(),
                                                 computeActorId, holderFactory);
        return {actor, actor};
    }

} // namespace NYql::NDq
