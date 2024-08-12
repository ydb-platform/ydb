#pragma once

#include "public.h"
#include "execution_stack.h"

#include <yt/yt/core/actions/callback.h>

#include <util/system/context.h>

#include <optional>
#include <type_traits>

#ifdef _win_
#undef Yield
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TCoroutineBase
{
public:
    TCoroutineBase(const TCoroutineBase& other) = delete;
    TCoroutineBase& operator=(const TCoroutineBase& other) = delete;

    ~TCoroutineBase();

    bool IsCompleted() const noexcept;

protected:
    template <CInvocable<void()> TBody>
    explicit TCoroutineBase(TBody body, EExecutionStackKind stackKind);

    void Resume();
    void Suspend();

private:
    enum class EState
    {
        Running,
        Abandoned,
        Completed,
    };

    std::shared_ptr<TExecutionStack> CoroutineStack_;

    TExceptionSafeContext CallerContext_;

    // We have to delay initialization of this object until the body
    // of ctor.
    union {
        TExceptionSafeContext CoroutineContext;
    };

    EState State_ = EState::Running;
    struct TCoroutineAbandonedException
    { };

    std::exception_ptr CoroutineException_;

    // NB(arkady-e1ppa): We make a "proxy-trampoline"
    // which DoRun consist of two parts:
    // 1) Move relevant stuff on stack frame: in this case it is TBody object.
    // 2) Execute the actual body.
    // This way we save up space in the coroutine itself
    // and eliminate type-erasure. If this class was more
    // popular it would make sense to move the rest of the fields
    // to the stack at the cost of much worse readability.
    template <CInvocable<void()> TBody>
    class TTrampoLine
        : public ITrampoLine
    {
    public:
        explicit TTrampoLine(TBody* body, TCoroutineBase* owner);

        void DoRun() override;

    private:
        TBody* Body_;
        TCoroutineBase* Owner_;
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
class TCoroutine<R(TArgs...)>
    : public NDetail::TCoroutineBase
{
public:
    using TArguments = std::tuple<TArgs...>;

    TCoroutine() = default;

    // NB(arkady-e1ppa): clang can't parse out-of-line
    // definition with concepts which refer to the class
    // name, aliases or variables. That's why it is commented out.
    template <class TCallee>
    // requires CInvocable<TCallee, void(TCoroutine<R(TArgs...)>&, TArgs...)>
    TCoroutine(
        TCallee&& callee,
        const EExecutionStackKind stackKind = EExecutionStackKind::Small);

    template <class... TParams>
    const std::optional<R>& Run(TParams&&... params);

    template <class Q>
    TArguments&& Yield(Q&& result);

private:
    TArguments Arguments_;
    std::optional<R> Result_;

    template <class TCallee>
    CInvocable<void()> auto MakeBody(TCallee&& callee);
};

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
class TCoroutine<void(TArgs...)>
    : public NDetail::TCoroutineBase
{
public:
    using TArguments = std::tuple<TArgs...>;

    TCoroutine() = default;

    // NB(arkady-e1ppa): clang can't parse out-of-line
    // definition with concepts which refer to the class
    // name, aliases or variables. That's why it is commented out.
    template <class TCallee>
    // requires CInvocable<TCallee, void(TCoroutine<R(TArgs...)>&, TArgs...)>
    TCoroutine(
        TCallee&& callee,
        const EExecutionStackKind stackKind = EExecutionStackKind::Small);

    template <class... TParams>
    bool Run(TParams&&... params);

    TArguments&& Yield();

private:
    TArguments Arguments_;
    bool Result_ = false;

    template <class TCallee>
    CInvocable<void()> auto MakeBody(TCallee&& callee);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define COROUTINE_INL_H_
#include "coroutine-inl.h"
#undef COROUTINE_INL_H_
