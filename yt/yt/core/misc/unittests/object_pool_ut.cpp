#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/object_pool.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPooledTestObject
{
    static inline int AliveCount = 0;

    int Value = 0;

    TPooledTestObject()
    {
        ++AliveCount;
    }

    ~TPooledTestObject()
    {
        --AliveCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

template <>
struct TPooledObjectTraits<TPooledTestObject>
    : public TPooledObjectTraitsBase<TPooledTestObject>
{
    static void Clean(TPooledTestObject* obj)
    {
        obj->Value = 0;
    }
};

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTestPool = TObjectPool<TPooledTestObject>;

TEST(TObjectPoolTest, UniqueHandleReturnsCleanedInstance)
{
    auto& pool = ObjectPool<TPooledTestObject>();
    pool.Release(TPooledObjectTraits<TPooledTestObject>::GetMaxPoolSize());

    auto object = pool.AllocateUnique();
    auto* rawObject = object.get();
    object->Value = 42;
    object.reset();

    EXPECT_EQ(pool.GetSize(), 1);

    object = pool.AllocateUnique();
    EXPECT_EQ(object.get(), rawObject);
    EXPECT_EQ(object->Value, 0);
}

TEST(TObjectPoolTest, UnpooledHandleDestroysInstance)
{
    auto aliveCount = TPooledTestObject::AliveCount;

    {
        auto object = TTestPool::AllocateUniqueUnpooled();
        EXPECT_EQ(TPooledTestObject::AliveCount, aliveCount + 1);
    }

    EXPECT_EQ(TPooledTestObject::AliveCount, aliveCount);
}

TEST(TObjectPoolTest, DirectlyConstructedUniqueHandleDestroysInstance)
{
    auto aliveCount = TPooledTestObject::AliveCount;

    {
        TTestPool::TObjectUniquePtr object(new TPooledTestObject());
        EXPECT_EQ(TPooledTestObject::AliveCount, aliveCount + 1);
    }

    EXPECT_EQ(TPooledTestObject::AliveCount, aliveCount);
}

TEST(TObjectPoolTest, SharedHandleReturnsInstance)
{
    auto& pool = ObjectPool<TPooledTestObject>();
    pool.Release(TPooledObjectTraits<TPooledTestObject>::GetMaxPoolSize());
    auto object = pool.AllocateShared();
    auto* rawObject = object.get();

    object.reset();
    EXPECT_EQ(pool.GetSize(), 1);
    EXPECT_EQ(pool.AllocateShared().get(), rawObject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
