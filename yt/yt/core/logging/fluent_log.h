#pragma once

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TOneShotFluentLogEvent
    : public NYTree::TFluentYsonBuilder::TFluentMapFragmentBase<NYTree::TFluentYsonVoid, TOneShotFluentLogEvent&&>
{
public:
    using TThis = TOneShotFluentLogEvent;
    using TBase = NYTree::TFluentYsonBuilder::TFluentMapFragmentBase<NYTree::TFluentYsonVoid, TThis&&>;
    using TStatePtr = TIntrusivePtr<NYTree::TFluentYsonWriterState>;

    TOneShotFluentLogEvent(TStatePtr state, const NLogging::TLogger& logger, NLogging::ELogLevel level);
    TOneShotFluentLogEvent(TOneShotFluentLogEvent&& other) = default;
    TOneShotFluentLogEvent(const TOneShotFluentLogEvent& other) = delete;

    ~TOneShotFluentLogEvent();

    TOneShotFluentLogEvent& operator=(TOneShotFluentLogEvent&& other) = default;
    TOneShotFluentLogEvent& operator=(const TOneShotFluentLogEvent& other) = delete;

private:
    TStatePtr State_;
    const NLogging::TLogger* Logger_;
    NLogging::ELogLevel Level_;
};

////////////////////////////////////////////////////////////////////////////////

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
