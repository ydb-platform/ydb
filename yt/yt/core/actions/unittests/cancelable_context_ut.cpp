#include <gtest/gtest.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/invoker_util.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TError MakeCancelationError(std::string message)
{
    return TError(NYT::EErrorCode::Canceled, std::move(message), TError::DisableFormat);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCancelableContextTest, NotCanceledByDefault)
{
    auto context = New<TCancelableContext>();
    EXPECT_FALSE(context->IsCanceled());
}

TEST(TCancelableContextTest, Cancel)
{
    auto context = New<TCancelableContext>();
    context->Cancel(MakeCancelationError("Boo"));

    EXPECT_TRUE(context->IsCanceled());
    EXPECT_EQ(context->GetCancelationError().GetCode(), NYT::EErrorCode::Canceled);
    EXPECT_EQ(context->GetCancelationError().GetMessage(), "Boo");
}

TEST(TCancelableContextTest, CancelIsIdempotent)
{
    auto context = New<TCancelableContext>();
    context->Cancel(MakeCancelationError("First"));
    context->Cancel(MakeCancelationError("Second"));

    // The first error wins.
    EXPECT_EQ(context->GetCancelationError().GetMessage(), "First");
}

TEST(TCancelableContextTest, SubscribeBeforeCancel)
{
    auto context = New<TCancelableContext>();

    int fired = 0;
    TError observedError;
    context->SubscribeCanceled(BIND([&] (const TError& error) {
        ++fired;
        observedError = error;
    }));

    EXPECT_EQ(fired, 0);

    context->Cancel(MakeCancelationError("Boo"));
    EXPECT_EQ(fired, 1);
    EXPECT_EQ(observedError.GetMessage(), "Boo");

    // Handlers are fired at most once.
    context->Cancel(MakeCancelationError("Again"));
    EXPECT_EQ(fired, 1);
}

TEST(TCancelableContextTest, SubscribeAfterCancelFiresImmediately)
{
    auto context = New<TCancelableContext>();
    context->Cancel(MakeCancelationError("Boo"));

    int fired = 0;
    context->SubscribeCanceled(BIND([&] (const TError&) {
        ++fired;
    }));
    EXPECT_EQ(fired, 1);
}

TEST(TCancelableContextTest, Unsubscribe)
{
    auto context = New<TCancelableContext>();

    int firedA = 0;
    int firedB = 0;
    auto handlerA = BIND([&] (const TError&) { ++firedA; });
    auto handlerB = BIND([&] (const TError&) { ++firedB; });

    context->SubscribeCanceled(handlerA);
    context->SubscribeCanceled(handlerB);
    context->UnsubscribeCanceled(handlerA);

    context->Cancel(MakeCancelationError("Boo"));

    // The unsubscribed handler must not fire; the other one must.
    EXPECT_EQ(firedA, 0);
    EXPECT_EQ(firedB, 1);
}

TEST(TCancelableContextTest, UnsubscribeAfterCancel)
{
    auto context = New<TCancelableContext>();

    int fired = 0;
    auto handler = BIND([&] (const TError&) { ++fired; });
    context->SubscribeCanceled(handler);

    context->Cancel(MakeCancelationError("Boo"));
    EXPECT_EQ(fired, 1);

    // Unsubscribing after the context already fired must be a harmless no-op --
    // this is exactly what WaitUntilSet's cleanup does after a context cancel.
    context->UnsubscribeCanceled(handler);
    EXPECT_EQ(fired, 1);
}

TEST(TCancelableContextTest, ResubscribeAfterUnsubscribe)
{
    auto context = New<TCancelableContext>();

    int fired = 0;
    auto handler = BIND([&] (const TError&) { ++fired; });

    context->SubscribeCanceled(handler);
    context->UnsubscribeCanceled(handler);
    context->SubscribeCanceled(handler);

    context->Cancel(MakeCancelationError("Boo"));

    // The re-subscribed handler fires exactly once.
    EXPECT_EQ(fired, 1);
}

TEST(TCancelableContextTest, PropagateToContext)
{
    auto parent = New<TCancelableContext>();
    auto child = New<TCancelableContext>();
    parent->PropagateTo(child);

    EXPECT_FALSE(child->IsCanceled());
    parent->Cancel(MakeCancelationError("Boo"));

    EXPECT_TRUE(child->IsCanceled());
    EXPECT_EQ(child->GetCancelationError().GetMessage(), "Boo");
}

TEST(TCancelableContextTest, PropagateToAlreadyCanceledContext)
{
    auto parent = New<TCancelableContext>();
    parent->Cancel(MakeCancelationError("Boo"));

    auto child = New<TCancelableContext>();
    parent->PropagateTo(child);

    // Propagating from an already-canceled context cancels the target at once.
    EXPECT_TRUE(child->IsCanceled());
    EXPECT_EQ(child->GetCancelationError().GetMessage(), "Boo");
}

TEST(TCancelableContextTest, PropagateToFuture)
{
    auto context = New<TCancelableContext>();
    auto promise = NewPromise<void>();
    auto future = promise.ToFuture();
    context->PropagateTo(future);

    EXPECT_FALSE(future.IsSet());
    context->Cancel(MakeCancelationError("Boo"));

    ASSERT_TRUE(future.IsSet());
    EXPECT_EQ(future.TryGet()->GetCode(), NYT::EErrorCode::Canceled);
}

TEST(TCancelableContextTest, InvokerRunsWhenNotCanceled)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(GetSyncInvoker());

    bool ran = false;
    invoker->Invoke(BIND([&] { ran = true; }));
    EXPECT_TRUE(ran);
}

TEST(TCancelableContextTest, InvokerSkipsWhenCanceled)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(GetSyncInvoker());
    context->Cancel(MakeCancelationError("Boo"));

    bool ran = false;
    invoker->Invoke(BIND([&] { ran = true; }));
    EXPECT_FALSE(ran);
}

TEST(TCancelableContextTest, InvokerInstallsCurrentContext)
{
    auto context = New<TCancelableContext>();
    auto invoker = context->CreateInvoker(GetSyncInvoker());

    TCancelableContext* observed = nullptr;
    invoker->Invoke(BIND([&] {
        observed = TryGetCurrentCancelableContext();
    }));
    EXPECT_EQ(observed, context.Get());

    // The slot is restored once the callback returns.
    EXPECT_FALSE(TryGetCurrentCancelableContext());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCurrentCancelableContextTest, NoneByDefault)
{
    EXPECT_FALSE(TryGetCurrentCancelableContext());
}

TEST(TCurrentCancelableContextTest, GuardInstallsAndRestores)
{
    auto context = New<TCancelableContext>();

    EXPECT_FALSE(TryGetCurrentCancelableContext());
    {
        TCurrentCancelableContextGuard guard(context);
        EXPECT_EQ(TryGetCurrentCancelableContext(), context.Get());
    }
    EXPECT_FALSE(TryGetCurrentCancelableContext());
}

TEST(TCurrentCancelableContextTest, GuardsNest)
{
    auto outer = New<TCancelableContext>();
    auto inner = New<TCancelableContext>();

    TCurrentCancelableContextGuard outerGuard(outer);
    EXPECT_EQ(TryGetCurrentCancelableContext(), outer.Get());
    {
        TCurrentCancelableContextGuard innerGuard(inner);
        EXPECT_EQ(TryGetCurrentCancelableContext(), inner.Get());
    }
    EXPECT_EQ(TryGetCurrentCancelableContext(), outer.Get());
}

TEST(TCurrentCancelableContextTest, Switch)
{
    auto context = New<TCancelableContext>();

    EXPECT_FALSE(SwitchCurrentCancelableContext(context));
    EXPECT_EQ(TryGetCurrentCancelableContext(), context.Get());
    EXPECT_EQ(SwitchCurrentCancelableContext(nullptr), context);
    EXPECT_FALSE(TryGetCurrentCancelableContext());
}

TEST(TCurrentCancelableContextTest, MovedFromGuardRestoresNothing)
{
    auto context = New<TCancelableContext>();

    {
        std::optional<TCurrentCancelableContextGuard> moved;
        {
            TCurrentCancelableContextGuard guard(context);
            EXPECT_EQ(TryGetCurrentCancelableContext(), context.Get());
            moved.emplace(std::move(guard));
            // Moved-from guard is inactive: its destruction here must not restore anything.
        }
        EXPECT_EQ(TryGetCurrentCancelableContext(), context.Get());
    }
    // The moved-to guard performs the single restore.
    EXPECT_FALSE(TryGetCurrentCancelableContext());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
