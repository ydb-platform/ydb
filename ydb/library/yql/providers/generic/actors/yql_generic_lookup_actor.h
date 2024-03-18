#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>

namespace NYql::NDq {

    namespace NGenericProviderLookupActorEvents { //TODO move to some common header to define interface for LookupSource in any provider
        enum EEventIds: ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_USERSPACE),
            EvLookupRequest = EvBegin,
            EvLookupResult,
            EvEnd
        };
        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_USERSPACE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvLookupRequest: NActors::TEventLocal<TEvLookupRequest, EvLookupRequest> {
            TEvLookupRequest(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, NKikimr::NMiniKQL::TUnboxedValueVector&& keys)
                : Alloc(alloc)
                , Keys(std::move(keys))
            {
            }
            ~TEvLookupRequest() {
                auto guard = Guard(*Alloc.get());
                Keys = NKikimr::NMiniKQL::TUnboxedValueVector{};
            }

            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
            NKikimr::NMiniKQL::TUnboxedValueVector Keys;
        };

        struct TEvLookupResult: NActors::TEventLocal<TEvLookupResult, EvLookupResult> {
            TEvLookupResult(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, NKikimr::NMiniKQL::TKeyPayloadPairVector&& data)
                : Alloc(alloc)
                , Data(std::move(data))
            {
            }
            ~TEvLookupResult() {
                auto guard = Guard(*Alloc.get());
                Data = NKikimr::NMiniKQL::TKeyPayloadPairVector{};
            }

            std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
            NKikimr::NMiniKQL::TKeyPayloadPairVector Data;
        };
    } // namespace NGenericProviderLookupActorEvents

    NActors::IActor*
    CreateGenericLookupActor(
        NConnector::IClient::TPtr connectorClient,
        const TString& serviceAccountId,
        const TString& serviceAccountSignature,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        NActors::TActorId&& parentId,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        NYql::NConnector::NApi::TDataSourceInstance&& dataSource,
        TString&& table,
        const NKikimr::NMiniKQL::TStructType* lookupKeyType,
        const NKikimr::NMiniKQL::TStructType* resultType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest);

} // namespace NYql::NDq
