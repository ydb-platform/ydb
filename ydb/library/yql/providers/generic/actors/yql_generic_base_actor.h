#pragma once
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYql::NDq {

    template <typename TDerived>
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
            EvEnd
        };

        static_assert(EEventIds::EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvListSplitsIterator: NActors::TEventLocal<TEvListSplitsIterator, EvListSplitsIterator> {
            explicit TEvListSplitsIterator(NConnector::IListSplitsStreamIterator::TPtr&& iterator)
                : Iterator(std::move(iterator))
            {
            }

            NConnector::IListSplitsStreamIterator::TPtr Iterator;
        };

        struct TEvListSplitsPart: NActors::TEventLocal<TEvListSplitsPart, EvListSplitsPart> {
            explicit TEvListSplitsPart(NConnector::NApi::TListSplitsResponse&& response)
                : Response(std::move(response))
            {
            }

            NConnector::NApi::TListSplitsResponse Response;
        };

        struct TEvListSplitsFinished: NActors::TEventLocal<TEvListSplitsFinished, EvListSplitsFinished> {
            explicit TEvListSplitsFinished(NYdbGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {
            }

            NYdbGrpc::TGrpcStatus Status;
        };

        struct TEvReadSplitsIterator: NActors::TEventLocal<TEvReadSplitsIterator, EvReadSplitsIterator> {
            explicit TEvReadSplitsIterator(NConnector::IReadSplitsStreamIterator::TPtr&& iterator)
                : Iterator(std::move(iterator))
            {
            }

            NConnector::IReadSplitsStreamIterator::TPtr Iterator;
        };

        struct TEvReadSplitsPart: NActors::TEventLocal<TEvReadSplitsPart, EvReadSplitsPart> {
            explicit TEvReadSplitsPart(NConnector::NApi::TReadSplitsResponse&& response)
                : Response(std::move(response))
            {
            }

            NConnector::NApi::TReadSplitsResponse Response;
        };

        struct TEvReadSplitsFinished: NActors::TEventLocal<TEvReadSplitsFinished, EvReadSplitsFinished> {
            explicit TEvReadSplitsFinished(NYdbGrpc::TGrpcStatus&& status)
                : Status(std::move(status))
            {
            }

            NYdbGrpc::TGrpcStatus Status;
        };

        struct TEvError: NActors::TEventLocal<TEvError, EvError> {
            explicit TEvError(const NConnector::NApi::TError& error)
                : Error(error)
            {
            }

            NConnector::NApi::TError Error;
        };

    protected: // TODO move common logic here
    };

} // namespace NYql::NDq
