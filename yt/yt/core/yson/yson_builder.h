#pragma once

#include "public.h"

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

struct IYsonBuilder
{
    YT_DEFINE_STRONG_TYPEDEF(TCheckpoint, int);

    virtual ~IYsonBuilder() = default;

    virtual IYsonConsumer* GetConsumer() = 0;
    virtual TCheckpoint CreateCheckpoint() = 0;
    virtual void RestoreCheckpoint(TCheckpoint checkpoint) = 0;

    IYsonConsumer* operator->();
};

////////////////////////////////////////////////////////////////////////////////

class TYsonStringBuilder
    : public IYsonBuilder
{
public:
    TYsonStringBuilder(
        NYson::EYsonFormat format = NYson::EYsonFormat::Binary,
        NYson::EYsonType type = NYson::EYsonType::Node,
        bool enableRaw = true);

    NYson::IYsonConsumer* GetConsumer() override;
    IYsonBuilder::TCheckpoint CreateCheckpoint() override;
    void RestoreCheckpoint(IYsonBuilder::TCheckpoint checkpoint) override;

    NYson::TYsonString GetYsonString() const;
    NYson::TYsonString Flush();
    bool IsEmpty();

private:
    TString ValueString_;
    TStringOutput Output_;
    const std::unique_ptr<NYson::IFlushableYsonConsumer> Writer_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TType>
concept CStatefulYsonConsumer = std::derived_from<TType, NYson::IYsonConsumer> && requires (TType value) {
    { value.GetState() } -> std::same_as<typename TType::TState>;
    value.SetState(value.GetState());
};

////////////////////////////////////////////////////////////////////////////////

template <CStatefulYsonConsumer TYsonConsumer>
class TYsonBuilder
    : public IYsonBuilder
{
public:
    template <class ...TArgs>
    TYsonBuilder(IYsonBuilder& builder, TArgs... args);

    IYsonConsumer* GetConsumer() override;
    IYsonBuilder::TCheckpoint CreateCheckpoint() override;
    void RestoreCheckpoint(TCheckpoint checkpoint) override;

private:
    using TConsumerState = typename TYsonConsumer::TState;

    IYsonBuilder& Builder_;
    TYsonConsumer Consumer_;
    std::vector<std::pair<TCheckpoint, TConsumerState>> States_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define YSON_BUILDER_INL_H_
#include "yson_builder-inl.h"
#undef YSON_BUILDER_INL_H_
