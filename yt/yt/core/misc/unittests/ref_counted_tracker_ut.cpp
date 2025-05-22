#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/unittests/proto/ref_counted_tracker_ut.pb.h>

#define YT_ENABLE_REF_COUNTED_TRACKING

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref_tracked.h>
#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT {
namespace {

using namespace NYT::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template <class T>
size_t GetAliveCount()
{
    return TRefCountedTracker::Get()->GetObjectsAlive(GetRefCountedTypeKey<T>());
}

template <class T>
size_t GetAliveBytes()
{
    return TRefCountedTracker::Get()->GetBytesAlive(GetRefCountedTypeKey<T>());
}

template <class T>
size_t GetAllocatedCount()
{
    return TRefCountedTracker::Get()->GetObjectsAllocated(GetRefCountedTypeKey<T>());
}

template <class T>
size_t GetAllocatedBytes()
{
    return TRefCountedTracker::Get()->GetBytesAllocated(GetRefCountedTypeKey<T>());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TRefCountedTraits;

////////////////////////////////////////////////////////////////////////////////

class TSimpleRefCountedObject
    : public TRefCounted
{ };

template <>
class TRefCountedTraits<TSimpleRefCountedObject>
{
public:
    static TIntrusivePtr<TSimpleRefCountedObject> Create()
    {
        return New<TSimpleRefCountedObject>();
    }

    static size_t GetInstanceSize()
    {
        return sizeof(TSimpleRefCountedObject);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TProtoRefCountedObject = TRefCountedProto<TRefCountedMessage>;

template <>
class TRefCountedTraits<TProtoRefCountedObject>
{
public:
    static TIntrusivePtr<TProtoRefCountedObject> Create()
    {
        static auto message = CreateMessage();
        return New<TProtoRefCountedObject>(message);
    }

    static size_t GetInstanceSize()
    {
        static auto message = CreateMessage();
        return message.SpaceUsed() - sizeof(TRefCountedMessage) + sizeof(TProtoRefCountedObject);
    }

private:
    static TRefCountedMessage CreateMessage()
    {
        TRefCountedMessage message;
        message.set_a("string");
        message.mutable_c()->set_a(10);
        message.add_d()->set_a(1);
        message.add_d()->set_a(2);
        message.add_d()->set_a(3);

        return message;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TRefCountedTrackerTest
    : public ::testing::Test
{ };

using TypeList = ::testing::Types<TSimpleRefCountedObject, TProtoRefCountedObject>;
TYPED_TEST_SUITE(TRefCountedTrackerTest, TypeList);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TRefCountedTrackerTest, SinglethreadedRefCounted)
{
    const auto instanceSize = TRefCountedTraits<TypeParam>::GetInstanceSize();
    auto create = [] {
        return TRefCountedTraits<TypeParam>::Create();
    };

    auto countBase = GetAllocatedCount<TypeParam>();
    auto bytesBase = GetAllocatedBytes<TypeParam>();

    std::vector<TIntrusivePtr<TypeParam>> container;
    container.reserve(2000);

    EXPECT_EQ(0u, GetAliveCount<TypeParam>());
    EXPECT_EQ(0u, GetAliveBytes<TypeParam>());
    EXPECT_EQ(countBase, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(bytesBase, GetAllocatedBytes<TypeParam>());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(create());
    }

    EXPECT_EQ(1000u, GetAliveCount<TypeParam>());
    EXPECT_EQ(1000u * instanceSize, GetAliveBytes<TypeParam>());
    EXPECT_EQ(countBase + 1000u, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(bytesBase + 1000u * instanceSize, GetAllocatedBytes<TypeParam>());

    for (size_t i = 0; i < 1000; ++i) {
        container.push_back(create());
    }

    EXPECT_EQ(2000u, GetAliveCount<TypeParam>());
    EXPECT_EQ(countBase + 2000u, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(2000u * instanceSize, GetAliveBytes<TypeParam>());
    EXPECT_EQ(bytesBase + 2000u * instanceSize, GetAllocatedBytes<TypeParam>());

    container.resize(1000);

    EXPECT_EQ(1000u, GetAliveCount<TypeParam>());
    EXPECT_EQ(countBase + 2000u, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(1000u * instanceSize, GetAliveBytes<TypeParam>());
    EXPECT_EQ(bytesBase + 2000u * instanceSize, GetAllocatedBytes<TypeParam>());

    container.resize(0);

    EXPECT_EQ(0u, GetAliveCount<TypeParam>());
    EXPECT_EQ(countBase + 2000u, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(0u, GetAliveBytes<TypeParam>());
    EXPECT_EQ(bytesBase + 2000u * instanceSize, GetAllocatedBytes<TypeParam>());
}

TYPED_TEST(TRefCountedTrackerTest, MultithreadedRefCounted)
{
    const auto instanceSize = TRefCountedTraits<TypeParam>::GetInstanceSize();
    auto create = [] {
        return TRefCountedTraits<TypeParam>::Create();
    };

    auto countBase = GetAllocatedCount<TypeParam>();
    auto bytesBase = GetAllocatedBytes<TypeParam>();

    auto obj1 = create();

    auto queue = New<TActionQueue>();
    BIND([&] {
        auto obj2 = create();
        EXPECT_EQ(countBase + 2u, GetAllocatedCount<TypeParam>());
        EXPECT_EQ(2u, GetAliveCount<TypeParam>());
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .Get();
    queue->Shutdown();

    EXPECT_EQ(countBase + 2u, GetAllocatedCount<TypeParam>());
    EXPECT_EQ(1u, GetAliveCount<TypeParam>());
    EXPECT_EQ(bytesBase + 2u * instanceSize, GetAllocatedBytes<TypeParam>());
    EXPECT_EQ(instanceSize, GetAliveBytes<TypeParam>());
}

////////////////////////////////////////////////////////////////////////////////

struct TBlobTag
{ };

TEST(TRefCountedTrackerTest, TBlobAllocatedMemoryTracker)
{
    auto allocatedBytesBase = GetAllocatedBytes<TBlobTag>();
    auto allocatedObjectsBase = GetAllocatedCount<TBlobTag>();

    EXPECT_EQ(0u, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(0u, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase, GetAllocatedCount<TBlobTag>());

    auto blob = TBlob(GetRefCountedTypeCookie<TBlobTag>(), 1);
    auto blobCapacity1 = blob.Capacity();

    EXPECT_EQ(blobCapacity1, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(1u, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase + 1, GetAllocatedCount<TBlobTag>());

    blob.Resize(3000);
    auto blobCapacity2 = blob.Capacity();

    EXPECT_EQ(blobCapacity2, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(1u, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1 + blobCapacity2, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase + 1, GetAllocatedCount<TBlobTag>());

    blob = TBlob(GetRefCountedTypeCookie<TBlobTag>());

    EXPECT_EQ(0u, GetAliveBytes<TBlobTag>());
    EXPECT_EQ(0u, GetAliveCount<TBlobTag>());
    EXPECT_EQ(allocatedBytesBase + blobCapacity1 + blobCapacity2, GetAllocatedBytes<TBlobTag>());
    EXPECT_EQ(allocatedObjectsBase + 1, GetAllocatedCount<TBlobTag>());
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TThrowingConstructorObject)
TThrowingConstructorObjectPtr GlobalObject;

class TThrowingConstructorObject
    : public TRefCounted
{
public:
    explicit TThrowingConstructorObject(bool passThisToSomebodyElse)
    {
        if (passThisToSomebodyElse) {
            GlobalObject = this;
        }
        THROW_ERROR_EXCEPTION("Some error");
    }
};

DEFINE_REFCOUNTED_TYPE(TThrowingConstructorObject)

////////////////////////////////////////////////////////////////////////////////

TEST(TRefCountedTrackerTest, ThrowingExceptionsInConstructor)
{
    TThrowingConstructorObjectPtr object;
    EXPECT_THROW(object = New<TThrowingConstructorObject>(false), std::exception);
    // TODO(max42): enable this when death tests are allowed in unittests.
    // ASSERT_DEATH(object = New<TThrowingConstructorObject>(true), "YT_VERIFY\\(GetRefCount\\(\\) == 1\\).*");
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleRefTrackedObject
    : public TRefTracked<TSimpleRefTrackedObject>
{ };

TEST(TRefCountedTrackerTest, RefTracked)
{
    auto countBase = GetAllocatedCount<TSimpleRefTrackedObject>();
    auto bytesBase = GetAllocatedBytes<TSimpleRefTrackedObject>();

    {
        TSimpleRefTrackedObject obj;
        EXPECT_EQ(countBase + 1, GetAllocatedCount<TSimpleRefTrackedObject>());
        EXPECT_EQ(bytesBase + sizeof(TSimpleRefTrackedObject), GetAllocatedBytes<TSimpleRefTrackedObject>());
        EXPECT_EQ(1u, GetAliveCount<TSimpleRefTrackedObject>());
        EXPECT_EQ(sizeof(TSimpleRefTrackedObject), GetAliveBytes<TSimpleRefTrackedObject>());
    }

    EXPECT_EQ(countBase + 1, GetAllocatedCount<TSimpleRefTrackedObject>());
    EXPECT_EQ(bytesBase + sizeof(TSimpleRefTrackedObject), GetAllocatedBytes<TSimpleRefTrackedObject>());
    EXPECT_EQ(0u, GetAliveCount<TSimpleRefTrackedObject>());
    EXPECT_EQ(0u, GetAliveBytes<TSimpleRefTrackedObject>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
