#ifndef FLUENT_LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion.
#include "log.h"
#include "fluent_log.h" // it makes CLion happy
#endif
#undef FLUENT_LOG_INL_H_

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
TOneShotFluentLogEventImpl<TParent>::TOneShotFluentLogEventImpl(
    TStatePtr state,
    const NLogging::TLogger& logger,
    NLogging::ELogLevel level)
    : TBase(state->GetConsumer())
    , State_(std::move(state))
    , Logger_(&logger)
    , Level_(level)
{
    for (const auto& [key, value] : logger.GetStructuredTags()) {
        (*this).Item(key).Value(value);
    }
}

template <class TParent>
NYTree::TFluentYsonBuilder::TAny<TOneShotFluentLogEventImpl<TParent>&&> TOneShotFluentLogEventImpl<TParent>::Item(TStringBuf key)
{
    this->Consumer->OnKeyedItem(key);
    return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
}

template <class TParent>
TOneShotFluentLogEventImpl<TParent>::~TOneShotFluentLogEventImpl()
{
    if (State_ && *Logger_) {
        LogStructuredEvent(*Logger_, State_->GetValue(), Level_);
    }
}

template <class TParent>
template <class T, class... TExtraArgs>
typename TOneShotFluentLogEventImpl<TParent>::TThis& TOneShotFluentLogEventImpl<TParent>::OptionalItem(TStringBuf key, const T& optionalValue, TExtraArgs&&... extraArgs)
{
    using NYTree::Serialize;

    if (optionalValue) {
        this->Consumer->OnKeyedItem(key);
        Serialize(optionalValue, this->Consumer, std::forward<TExtraArgs>(extraArgs)...);
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
