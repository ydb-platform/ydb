#pragma once
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYql::NDq {

    template <typename TDerived, typename TEvState = std::monostate>
    class TGenericBaseActor: public NActors::TActorBootstrapped<TDerived> {
    protected: // Events
        // Event ids
        enum EEventIds: ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvListSplitsIterator = EvBegin,
            EvListSplitsPart,
            EvListSplitsFinished,
            EvReadSplitsIterator,
            EvReadSplitsPart,
            EvReadSplitsFinished,
            EvError,
            EvRetry,
            EvEnd
        };

        static_assert(EEventIds::EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvListSplitsIterator: NActors::TEventLocal<TEvListSplitsIterator, EvListSplitsIterator> {
            explicit TEvListSplitsIterator(NConnector::IListSplitsStreamIterator::TPtr&& iterator)
                : Iterator(std::move(iterator))
            {
            }

            NConnector::IListSplitsStreamIterator::TPtr Iterator;
            TEvState State;
        };

        struct TEvListSplitsPart: NActors::TEventLocal<TEvListSplitsPart, EvListSplitsPart> {
            explicit TEvListSplitsPart(NConnector::NApi::TListSplitsResponse&& response)
                : Response(std::move(response))
            {
            }

            NConnector::NApi::TListSplitsResponse Response;
            TEvState State;
        };

        struct TEvListSplitsFinished: NActors::TEventLocal<TEvListSplitsFinished, EvListSplitsFinished> {
            explicit TEvListSplitsFinished(NYdbGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {
            }

            NYdbGrpc::TGrpcStatus Status;
            TEvState State;
        };

        struct TEvReadSplitsIterator: NActors::TEventLocal<TEvReadSplitsIterator, EvReadSplitsIterator> {
            explicit TEvReadSplitsIterator(NConnector::IReadSplitsStreamIterator::TPtr&& iterator)
                : Iterator(std::move(iterator))
            {
            }

            NConnector::IReadSplitsStreamIterator::TPtr Iterator;
            TEvState State;
        };

        struct TEvReadSplitsPart: NActors::TEventLocal<TEvReadSplitsPart, EvReadSplitsPart> {
            explicit TEvReadSplitsPart(NConnector::NApi::TReadSplitsResponse&& response)
                : Response(std::move(response))
            {
            }

            NConnector::NApi::TReadSplitsResponse Response;
            TEvState State;
        };

        struct TEvReadSplitsFinished: NActors::TEventLocal<TEvReadSplitsFinished, EvReadSplitsFinished> {
            explicit TEvReadSplitsFinished(NYdbGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {
            }

            NYdbGrpc::TGrpcStatus Status;
            TEvState State;
        };

        struct TEvError: NActors::TEventLocal<TEvError, EvError> {
            explicit TEvError(const NConnector::NApi::TError& error)
                : Error(error)
            {
            }

            NConnector::NApi::TError Error;
            TEvState State;
        };

    protected: // TODO move common logic here
    };

} // namespace NYql::NDq
