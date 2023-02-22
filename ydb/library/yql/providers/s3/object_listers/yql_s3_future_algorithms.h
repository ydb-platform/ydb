#pragma once

#include "yql_s3_list.h"

#include <library/cpp/threading/future/future.h>

#include <memory>
#include <variant>
#include <vector>

namespace NYql {

enum class EAggregationAction : ui8 { Proceed = 0, Stop = 1 };

template<typename State, typename Increment>
using TAggregationOperation =
    std::function<EAggregationAction(State& state, Increment&& increment)>;

template<typename State>
using TAggregationErrorHandler =
    std::function<EAggregationAction(State& state, const std::exception& exception)>;

namespace NDetails {

template<typename State>
struct TAggregationState {
    State Acc;
    TAggregationOperation<State, NS3Lister::TListResult> IncrementHandler;
    TAggregationErrorHandler<State> ErrorHandler;
    std::shared_ptr<NS3Lister::IS3Lister> Lister;
    NThreading::TPromise<State> Promise;

    TAggregationState(
        State&& acc,
        TAggregationOperation<State, NS3Lister::TListResult>&& incrementHandler,
        TAggregationErrorHandler<State>&& errorHandler,
        std::shared_ptr<NS3Lister::IS3Lister> lister,
        NThreading::TPromise<State> promise)
        : Acc(std::move(acc))
        , IncrementHandler(std::move(incrementHandler))
        , ErrorHandler(std::move(errorHandler))
        , Lister(std::move(lister))
        , Promise(std::move(promise)) { }
};

template<typename State>
void SubscribeCallback(
    const NThreading::TFuture<NS3Lister::TListResult>& strategyListingResult,
    std::shared_ptr<TAggregationState<State>> state) {
    EAggregationAction nextAction;

    try {
        NS3Lister::TListResult listingResult = strategyListingResult.GetValue();
        nextAction = state->IncrementHandler(state->Acc, std::move(listingResult));
    } catch (const std::exception& e) {
        nextAction = state->ErrorHandler(state->Acc, e);
    }

    if (nextAction == EAggregationAction::Proceed && state->Lister->HasNext()) {
        auto nextFuture = state->Lister->Next();
        nextFuture.Subscribe(
            [state = std::move(state)](
                const NThreading::TFuture<NS3Lister::TListResult>& listingResult) mutable {
                SubscribeCallback(listingResult, std::move(state));
            });
    } else {
        state->Promise.SetValue(std::move(state->Acc));
    }
}

} // namespace

template<typename State>
NThreading::TFuture<State> AccumulateWithEarlyStop(
    std::shared_ptr<NS3Lister::IS3Lister> lister,
    typename std::remove_reference<State>::type&& initialState,
    TAggregationOperation<State, NS3Lister::TListResult>&& incrementHandler,
    TAggregationErrorHandler<State>&& errorHandler) {
    auto state = std::make_shared<NDetails::TAggregationState<State>>(
        std::move(initialState),
        std::move(incrementHandler),
        std::move(errorHandler),
        std::move(lister),
        NThreading::NewPromise<State>());

    auto promise = state->Promise;

    if (state->Lister->HasNext()) {
        auto nextFuture = state->Lister->Next();
        nextFuture.Subscribe(
            [state = std::move(state)](
                const NThreading::TFuture<NS3Lister::TListResult>& listingResult) mutable {
                SubscribeCallback(listingResult, std::move(state));
            });
    } else {
        state->Promise.SetValue(std::move(state->Acc));
    }

    return promise.GetFuture();
}

} // namespace NYql
