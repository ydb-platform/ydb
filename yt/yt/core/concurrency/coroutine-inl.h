#ifndef COROUTINE_INL_H_
#error "Direct inclusion of this file is not allowed, include coroutine.h"
// For the sake of sane code completion.
#include "coroutine.h"
#endif
#undef COROUTINE_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <CInvocable<void()> TBody>
TCoroutineBase::TCoroutineBase(TBody body, EExecutionStackKind stackKind)
    : CoroutineStack_(CreateExecutionStack(stackKind))
{
    TTrampoLine trampoline(&body, this);

    std::construct_at(
        &CoroutineContext,
        TContClosure{
            .TrampoLine = &trampoline,
            .Stack = TArrayRef(static_cast<char*>(CoroutineStack_->GetStack()), CoroutineStack_->GetSize())
        });

    Resume();
}

template <CInvocable<void()> TBody>
TCoroutineBase::TTrampoLine<TBody>::TTrampoLine(TBody* body, TCoroutineBase* owner)
    : Body_(body)
    , Owner_(owner)
{ }

template <CInvocable<void()> TBody>
void TCoroutineBase::TTrampoLine<TBody>::DoRun()
{
    // Move/Copy stuff on stack frame.
    auto* owner = Owner_;
    bool abandoned = false;

    {
        auto body = std::move(*Body_);

        try {
            owner->Suspend();
        } catch (TCoroutineAbandonedException) {
            abandoned = true;
        }

        // Actual execution.
        if (!abandoned) {
            try {
                body();
            } catch (TCoroutineAbandonedException) {
            } catch (...) {
                owner->CoroutineException_ = std::current_exception();
            }
        }
    }

    owner->State_ = EState::Completed;
    owner->Suspend();

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template <class TCallee, class TCaller, class TArguments, size_t... Indexes>
void Invoke(
    TCallee& callee,
    TCaller& caller,
    TArguments&& arguments,
    std::index_sequence<Indexes...>)
{
    callee(
        caller,
        std::get<Indexes>(std::forward<TArguments>(arguments))...);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
template <class TCallee>
CInvocable<void()> auto TCoroutine<R(TArgs...)>::MakeBody(TCallee&& callee)
{
    return [this, callee = std::forward<TCallee>(callee)] {
        try {
            NDetail::Invoke(
                callee,
                *this,
                std::move(Arguments_),
                std::make_index_sequence<sizeof...(TArgs)>());
            Result_.reset();
        } catch (...) {
            Result_.reset();
            throw;
        }
    };
}

template <class R, class... TArgs>
template <class TCallee>
TCoroutine<R(TArgs...)>::TCoroutine(TCallee&& callee, const EExecutionStackKind stackKind)
    : NDetail::TCoroutineBase(MakeBody(std::forward<TCallee>(callee)), stackKind)
{
    static_assert(CInvocable<TCallee, void(TCoroutine<R(TArgs...)>&, TArgs...)>);
}

template <class R, class... TArgs>
template <class... TParams>
const std::optional<R>& TCoroutine<R(TArgs...)>::Run(TParams&& ... params)
{
    static_assert(sizeof...(TParams) == sizeof...(TArgs),
        "TParams<> and TArgs<> have different length");
    Arguments_ = std::tuple(std::forward<TParams>(params)...);
    Resume();
    return Result_;
}

template <class R, class... TArgs>
template <class Q>
typename TCoroutine<R(TArgs...)>::TArguments&& TCoroutine<R(TArgs...)>::Yield(Q&& result)
{
    Result_ = std::forward<Q>(result);
    Suspend();
    return std::move(Arguments_);
}

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
template <class TCallee>
CInvocable<void()> auto TCoroutine<void(TArgs...)>::MakeBody(TCallee&& callee)
{
    return [this, callee = std::forward<TCallee>(callee)] {
        try {
            NDetail::Invoke(
                callee,
                *this,
                std::move(Arguments_),
                std::make_index_sequence<sizeof...(TArgs)>());
            Result_ = false;
        } catch (...) {
            Result_ = false;
            throw;
        }
    };
}

template <class... TArgs>
template <class TCallee>
TCoroutine<void(TArgs...)>::TCoroutine(TCallee&& callee, const EExecutionStackKind stackKind)
    : NDetail::TCoroutineBase(MakeBody(std::forward<TCallee>(callee)), stackKind)
{
    static_assert(CInvocable<TCallee, void(TCoroutine<void(TArgs...)>&, TArgs...)>);
}

template <class... TArgs>
template <class... TParams>
bool TCoroutine<void(TArgs...)>::Run(TParams&& ... params)
{
    static_assert(sizeof...(TParams) == sizeof...(TArgs),
        "TParams<> and TArgs<> have different length");
    Arguments_ = std::tuple(std::forward<TParams>(params)...);
    Resume();
    return Result_;
}

template <class... TArgs>
typename TCoroutine<void(TArgs...)>::TArguments&& TCoroutine<void(TArgs...)>::Yield()
{
    Result_ = true;
    Suspend();
    return std::move(Arguments_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
