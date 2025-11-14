#ifndef YSON_BUILDER_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_builder.h"
// For the sake of sane code completion.
#include "yson_builder.h"
#endif

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <CStatefulYsonConsumer TYsonConsumer>
template <class ...TArgs>
TYsonBuilder<TYsonConsumer>::TYsonBuilder(IYsonBuilder& builder, TArgs... args)
    : Builder_(builder)
    , Consumer_(builder.GetConsumer(), std::forward<TArgs>(args)...)
{ }

template <CStatefulYsonConsumer TYsonConsumer>
IYsonConsumer* TYsonBuilder<TYsonConsumer>::GetConsumer()
{
    return &Consumer_;
}

template <CStatefulYsonConsumer TYsonConsumer>
IYsonBuilder::TCheckpoint TYsonBuilder<TYsonConsumer>::CreateCheckpoint()
{
    States_.push_back({Builder_.CreateCheckpoint(), Consumer_.GetState()});
    return TCheckpoint{static_cast<int>(std::ssize(States_)) - 1};
}

template <CStatefulYsonConsumer TYsonConsumer>
void TYsonBuilder<TYsonConsumer>::RestoreCheckpoint(IYsonBuilder::TCheckpoint checkpoint)
{
    const auto& state = States_[checkpoint.Underlying()];
    Builder_.RestoreCheckpoint(state.first);
    Consumer_.SetState(state.second);
    States_.resize(checkpoint.Underlying() + 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
