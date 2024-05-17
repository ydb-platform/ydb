#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/logging/log.h>

namespace NYT {
namespace {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

class TOffloadedDtorObject
    : public TRefCounted
{
public:
    explicit TOffloadedDtorObject(TThreadId dtorThreadId)
        : DtorThreadId_(dtorThreadId)
    {
        YT_LOG_INFO("TOffloadedDtorObject::TOffloadedDtorObject()");
    }

    ~TOffloadedDtorObject()
    {
        YT_LOG_INFO("TOffloadedDtorObject::~TOffloadedDtorObject()");
        EXPECT_EQ(GetCurrentThreadId(), DtorThreadId_);
    }

private:
    const TThreadId DtorThreadId_;
};

TEST(TNewWithOffloadedDtorTest, OffloadDtor)
{
    auto dtorQueue = New<TActionQueue>("Offload");
    auto dtorInvoker = dtorQueue->GetInvoker();
    auto dtorThreadId = dtorInvoker->GetThreadId();
    EXPECT_NE(dtorThreadId, InvalidThreadId);

    auto typeKey = GetRefCountedTypeKey<TOffloadedDtorObject>();
    EXPECT_EQ(TRefCountedTracker::Get()->GetObjectsAllocated(typeKey), 0u);
    EXPECT_EQ(TRefCountedTracker::Get()->GetObjectsAlive(typeKey), 0u);

    auto obj = NewWithOffloadedDtor<TOffloadedDtorObject>(dtorInvoker, dtorThreadId);
    EXPECT_EQ(TRefCountedTracker::Get()->GetObjectsAllocated(typeKey), 1u);
    EXPECT_EQ(TRefCountedTracker::Get()->GetObjectsAlive(typeKey), 1u);

    obj.Reset();
    WaitForPredicate([=] {
        return TRefCountedTracker::Get()->GetObjectsAlive(typeKey) == 0u;
    });

    dtorQueue->Shutdown(/*graceful*/ true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
