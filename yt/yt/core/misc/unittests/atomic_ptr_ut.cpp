#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TAtomicPtrTest
    : public ::testing::Test
{ };

struct TFinalObject final
{
    static constexpr bool EnableHazard = true;
};

struct TRefCountedObject
    : public TRefCounted
{
    static constexpr bool EnableHazard = true;
};

struct TVirtualRefCountedObject
    : public virtual TRefCounted
{
    static constexpr bool EnableHazard = true;
};

using TObjectTypes = ::testing::Types<
    TFinalObject,
    TRefCountedObject,
    TVirtualRefCountedObject
>;

TYPED_TEST_SUITE(TAtomicPtrTest, TObjectTypes);

TYPED_TEST(TAtomicPtrTest, Simple)
{
    using TObject = TypeParam;

    TAtomicPtr<TObject> atomicPtr;
    EXPECT_FALSE(atomicPtr.Acquire());

    auto obj1 = New<TObject>();
    atomicPtr.Store(obj1);

    auto obj2 = atomicPtr.Acquire();
    EXPECT_EQ(obj1, obj2);

    atomicPtr.Reset();
    EXPECT_FALSE(atomicPtr.Acquire());
}

TEST(TAtomicPtrTest, AcquireHazard)
{
    TAtomicPtr<TRefCountedObject, /*EnableAcquireHazard*/ true> atomicPtr;

    EXPECT_FALSE(atomicPtr.Acquire());

    auto obj = New<TRefCountedObject>();
    atomicPtr.Store(obj);

    auto hazardObj = atomicPtr.AcquireHazard();
    EXPECT_EQ(hazardObj.Get(), obj.Get());
    EXPECT_EQ(obj->GetRefCount(), 2);

    atomicPtr.Reset();
    ReclaimHazardPointers();

    EXPECT_EQ(obj->GetRefCount(), 2);

    hazardObj.Reset();
    ReclaimHazardPointers();

    EXPECT_EQ(obj->GetRefCount(), 1);
}
////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
