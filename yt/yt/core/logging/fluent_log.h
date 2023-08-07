#pragma once

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TOneShotFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TOneShotFluentLogEventImpl, TParent, NYTree::TFluentMap>
{
public:
    using TThis = TOneShotFluentLogEventImpl;
    using TBase = NYTree::TFluentYsonBuilder::TFluentFragmentBase<NLogging::TOneShotFluentLogEventImpl, TParent, NYTree::TFluentMap>;
    using TStatePtr = TIntrusivePtr<NYTree::TFluentYsonWriterState>;

    TOneShotFluentLogEventImpl(TStatePtr state, const NLogging::TLogger& logger, NLogging::ELogLevel level);
    TOneShotFluentLogEventImpl(TOneShotFluentLogEventImpl&& other) = default;
    TOneShotFluentLogEventImpl(const TOneShotFluentLogEventImpl& other) = delete;

    ~TOneShotFluentLogEventImpl();

    TOneShotFluentLogEventImpl& operator=(TOneShotFluentLogEventImpl&& other) = default;
    TOneShotFluentLogEventImpl& operator=(const TOneShotFluentLogEventImpl& other) = delete;

    // TODO(max42): why these two methods must be re-implemented here? Maybe it is enough to replace TFluentYsonVoid with TFluentMap below?

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(TStringBuf key);

    template <class T, class... TExtraArgs>
    TThis& OptionalItem(TStringBuf key, const T& optionalValue, TExtraArgs&&... extraArgs);

private:
    TStatePtr State_;
    const NLogging::TLogger* Logger_;
    NLogging::ELogLevel Level_;
};

////////////////////////////////////////////////////////////////////////////////

using TOneShotFluentLogEvent = TOneShotFluentLogEventImpl<NYTree::TFluentYsonVoid>;

TOneShotFluentLogEvent LogStructuredEventFluently(const NLogging::TLogger& logger, NLogging::ELogLevel level);

TOneShotFluentLogEvent LogStructuredEventFluentlyToNowhere();

////////////////////////////////////////////////////////////////////////////////

class TStructuredLogBatcher
{
public:
    explicit TStructuredLogBatcher(
        TLogger logger,
        i64 maxBatchSize = 10_KBs,
        ELogLevel level = ELogLevel::Info);

    using TFluent = decltype(NYTree::BuildYsonListFragmentFluently(nullptr).Item());

    TFluent AddItemFluently();

    ~TStructuredLogBatcher();

private:
    const NLogging::TLogger Logger;
    const i64 MaxBatchSize_;
    const ELogLevel Level_;

    TString BatchYson_;
    TStringOutput BatchOutputStream_{BatchYson_};
    NYson::TYsonWriter BatchYsonWriter_{
        &BatchOutputStream_,
        NYson::EYsonFormat::Binary,
        NYson::EYsonType::ListFragment};
    int BatchItemCount_ = 0;

    void Flush();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define FLUENT_LOG_INL_H_
#include "fluent_log-inl.h"
#undef FLUENT_LOG_INL_H_
