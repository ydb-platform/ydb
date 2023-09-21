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
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
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
                TEvReadResult(const NConnector::TReadSplitsResult::TPtr& result)
                    : Result(result)
                {
                }

                NConnector::TReadSplitsResult::TPtr Result;
            };

            struct TEvReadError: public TEventLocal<TEvReadError, EvReadError> {
                TEvReadError(const NYql::NConnector::NApi::TError& error)
                    : Error(error)
                {
                }
                NYql::NConnector::NApi::TError Error;
            };
        };

    } // namespace

    class TGenericReadActor: public TActorBootstrapped<TGenericReadActor>, public IDqComputeActorAsyncInput {
    public:
        TGenericReadActor(ui64 inputIndex, NConnector::IClient::TPtr genericClient, const NConnector::NApi::TSelect& select,
                          const NConnector::NApi::TDataSourceInstance& dataSourceInstance,
                          const NActors::TActorId& computeActorId, const NKikimr::NMiniKQL::THolderFactory& holderFactory)
            : InputIndex_(inputIndex)
            , ComputeActorId_(computeActorId)
            , ActorSystem_(TActivationContext::ActorSystem())
            , ConnectorClient_(genericClient)
            , HolderFactory_(holderFactory)
            , Select_(select)
            , DataSourceInstance_(dataSourceInstance)
        {
        }

        void Bootstrap() {
            Become(&TGenericReadActor::StateFunc);

            NConnector::NApi::TListSplitsRequest listSplitsRequest;
            listSplitsRequest.mutable_selects()->Add()->CopyFrom(Select_);

            auto listSplitsResult = ConnectorClient_->ListSplits(listSplitsRequest);
            if (!NConnector::ErrorIsSuccess(listSplitsResult->Error)) {
                YQL_CLOG(ERROR, ProviderGeneric) << "ListSplits failure: " << listSplitsResult->Error.DebugString();
                ActorSystem_->Send(new IEventHandle(
                    SelfId(), TActorId(), new TEvPrivate::TEvReadError(listSplitsResult->Error)));
                return;
            }

            YQL_CLOG(INFO, ProviderGeneric) << "ListSplits success, total splits: " << listSplitsResult->Splits.size();

            NConnector::NApi::TReadSplitsRequest readSplitsRequest;
            readSplitsRequest.set_format(NConnector::NApi::TReadSplitsRequest::ARROW_IPC_STREAMING);
            readSplitsRequest.mutable_splits()->Reserve(listSplitsResult->Splits.size());
            std::for_each(
                listSplitsResult->Splits.cbegin(), listSplitsResult->Splits.cend(),
                [&](const NConnector::NApi::TSplit& split) { readSplitsRequest.mutable_splits()->Add()->CopyFrom(split); });
            readSplitsRequest.mutable_data_source_instance()->CopyFrom(DataSourceInstance_);

            auto readSplitsResult = ConnectorClient_->ReadSplits(readSplitsRequest);
            if (!NConnector::ErrorIsSuccess(readSplitsResult->Error)) {
                YQL_CLOG(ERROR, ProviderGeneric) << "ReadSplits failure: " << readSplitsResult->Error.DebugString();
                ActorSystem_->Send(new IEventHandle(
                    SelfId(), TActorId(), new TEvPrivate::TEvReadError(readSplitsResult->Error)));
                return;
            }

            YQL_CLOG(INFO, ProviderGeneric) << "ReadSplits success, total batches: "
                                            << readSplitsResult->RecordBatches.size();

            ActorSystem_->Send(new IEventHandle(SelfId(), TActorId(), new TEvPrivate::TEvReadResult(readSplitsResult)));
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
            return InputIndex_;
        }

        STRICT_STFUNC(StateFunc,
                      hFunc(TEvPrivate::TEvReadResult, Handle);
                      hFunc(TEvPrivate::TEvReadError, Handle);)

        i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished,
                              i64 /*freeSpace*/) final {
            YQL_ENSURE(!buffer.IsWide(), "Wide stream is not supported");
            if (Result_) {
                NUdf::TUnboxedValue value;

                ui64 total = 0;

                // It's very important to fill UV columns in the alphabet order,
                // paying attention to the scalar field containing block length.
                TVector<TString> fieldNames;
                std::transform(Select_.what().items().cbegin(), Select_.what().items().cend(),
                               std::back_inserter(fieldNames), [](const auto& item) { return item.column().name(); });

                fieldNames.push_back(std::string(BlockLengthColumnName));
                std::sort(fieldNames.begin(), fieldNames.end());
                std::map<TStringBuf, std::size_t> fieldNameOrder;
                for (std::size_t i = 0; i < fieldNames.size(); i++) {
                    fieldNameOrder[fieldNames[i]] = i;
                }

                for (const auto& batch : Result_->RecordBatches) {
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
                }

                // freeSpace -= size;
                finished = true;
                Result_.reset();

                // TODO: check it, because in S3 the generic cache clearing happens only when LastFileWasProcessed:
                // https://a.yandex-team.ru/arcadia/ydb/library/yql/providers/s3/actors/yql_s3_read_actor.cpp?rev=r11543410#L2497
                ArrowRowContainerCache_.Clear();

                return total;
            }

            return 0LL;
        }

        void Handle(TEvPrivate::TEvReadResult::TPtr& evReadResult) {
            Result_ = evReadResult->Get()->Result;
            Send(ComputeActorId_, new TEvNewAsyncInputDataArrived(InputIndex_));
        }

        void Handle(TEvPrivate::TEvReadError::TPtr& result) {
            Send(ComputeActorId_,
                 new TEvAsyncInputError(
                     InputIndex_,
                     NConnector::ErrorToIssues(result->Get()->Error),
                     NConnector::ErrorToDqStatus(result->Get()->Error)));
        }

        // IActor & IDqComputeActorAsyncInput
        void PassAway() override { // Is called from Compute Actor
            TActorBootstrapped<TGenericReadActor>::PassAway();
        }

        const ui64 InputIndex_;
        const NActors::TActorId ComputeActorId_;

        TActorSystem* const ActorSystem_;

        // Changed:
        NConnector::IClient::TPtr ConnectorClient_;
        NConnector::TReadSplitsResult::TPtr Result_;
        NKikimr::NMiniKQL::TPlainContainerCache ArrowRowContainerCache_;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
        const NYql::NConnector::NApi::TSelect Select_;
        const NYql::NConnector::NApi::TDataSourceInstance DataSourceInstance_;
    };

    std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>
    CreateGenericReadActor(NConnector::IClient::TPtr genericClient, Generic::TSource&& params, ui64 inputIndex,
                           const THashMap<TString, TString>& /*secureParams*/,
                           const THashMap<TString, TString>& /*taskParams*/, const NActors::TActorId& computeActorId,
                           ISecuredServiceAccountCredentialsFactory::TPtr /*credentialsFactory*/,
                           const NKikimr::NMiniKQL::THolderFactory& holderFactory)
    {
        const auto dsi = params.select().data_source_instance();
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

        const auto actor = new TGenericReadActor(
            inputIndex,
            genericClient,
            params.select(),
            dsi,
            computeActorId,
            holderFactory);

        return {actor, actor};
    }

} // namespace NYql::NDq
