#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TSaveContext = NPhoenix::TSaveContext;
using TLoadContext = NPhoenix::TLoadContext;
using IPersistent = NPhoenix::IPersistent;
using TPersistenceContext = NPhoenix::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

template <class T>
TSharedRef Serialize(const T& value)
{
    TBlobOutput output;
    TSaveContext context(&output);
    Save(context, value);
    context.Finish();
    return output.Flush();
}

template <class T>
void Deserialize(T& value, TRef ref)
{
    TMemoryInput input(ref.Begin(), ref.Size());
    TLoadContext context(&input);
    Load(context, value);
}

template <class T>
void InplaceDeserialize(TIntrusivePtr<T> value, TRef ref)
{
    TMemoryInput input(ref.Begin(), ref.Size());
    TLoadContext context(&input);
    NPhoenix::TSerializer::InplaceLoad(context, value);
}

////////////////////////////////////////////////////////////////////////////////

namespace NSimple {

TEST(TPhoenixTest, Scalar)
{
    struct C
    {
        C()
            : A(-1)
            , B(-1)
        { }

        int A;
        double B;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, A);
            Persist(context, B);
        }
    };

    C c1;
    c1.A = 10;
    c1.B = 3.14;

    C c2;
    Deserialize(c2, Serialize(c1));

    EXPECT_EQ(c1.A, c2.A);
    EXPECT_EQ(c1.B, c2.B);
}

TEST(TPhoenixTest, Guid)
{
    TGuid g1(1, 2, 3, 4);

    TGuid g2(5, 6, 7, 8);
    Deserialize(g2, Serialize(g1));

    EXPECT_EQ(g1, g2);
}

TEST(TPhoenixTest, Vector)
{
    std::vector<int> v1;
    v1.push_back(1);
    v1.push_back(2);
    v1.push_back(3);

    std::vector<int> v2;
    v2.push_back(33);
    Deserialize(v2, Serialize(v1));

    EXPECT_EQ(v1, v2);
}

} // namespace NSimple

////////////////////////////////////////////////////////////////////////////////

namespace NRef1 {

struct A;
struct B;

struct A
    : public TRefCounted
{
    TIntrusivePtr<B> X;
    TIntrusivePtr<B> Y;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
        Persist(context, Y);
    }

};

struct B
    : public TRefCounted
{
    B()
        : V(-1)
    { }

    int V;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, V);
    }

};

TEST(TPhoenixTest, Ref1)
{
    auto a1 = New<A>();
    a1->X = New<B>();
    a1->X->V = 1;
    a1->Y = New<B>();
    a1->Y->V = 2;

    TIntrusivePtr<A> a2;
    Deserialize(a2, Serialize(a1));

    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_EQ(a2->X->GetRefCount(), 1);
    EXPECT_EQ(a2->X->V, 1);
    EXPECT_EQ(a2->Y->GetRefCount(), 1);
    EXPECT_EQ(a2->Y->V, 2);
}

} // namespace NRef1

////////////////////////////////////////////////////////////////////////////////

namespace NRef2 {

struct A
    : public TRefCounted
{
    TIntrusivePtr<A> X;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
    }

};

TEST(TPhoenixTest, Ref2)
{
    auto a1 = New<A>();

    TIntrusivePtr<A> a2;
    Deserialize(a2, Serialize(a1));

    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_FALSE(a2->X.operator bool());
}

} // namespace NRef2

////////////////////////////////////////////////////////////////////////////////

namespace NRef3 {

struct A
    : public TRefCounted
{
    TIntrusivePtr<A> X;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
    }

};

TEST(TPhoenixTest, Ref3)
{
    auto a1 = New<A>();
    a1->X = a1;

    TIntrusivePtr<A> a2;
    Deserialize(a2, Serialize(a1));

    EXPECT_EQ(a2->GetRefCount(), 2);
    EXPECT_EQ(a2->X, a2);

    a1->X.Reset();
    a2->X.Reset();
}

} // namespace NRef3

////////////////////////////////////////////////////////////////////////////////

namespace NRef4 {

struct A
    : public TRefCounted
{
    A()
        : X(nullptr)
    { }

    A* X;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
    }

};

TEST(TPhoenixTest, Ref4)
{
    auto a1 = New<A>();
    a1->X = a1.Get();

    TIntrusivePtr<A> a2;
    Deserialize(a2, Serialize(a1));

    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_EQ(a2->X, a2);
}

} // namespace NRef4

////////////////////////////////////////////////////////////////////////////////

namespace NRef5 {

struct A;
struct B;

struct A
    : public TRefCounted
{
    A()
        : X(nullptr)
        , Y(nullptr)
    { }

    B* X;
    TIntrusivePtr<B> Y;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
        Persist(context, Y);
    }
};

struct B
    : public TRefCounted
{
    B()
        : V(-1)
    { }

    int V;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, V);
    }

};

TEST(TPhoenixTest, Ref5)
{
    auto a1 = New<A>();
    a1->Y = New<B>();
    a1->Y->V = 7;
    a1->X = a1->Y.Get();

    TIntrusivePtr<A> a2;
    Deserialize(a2, Serialize(a1));

    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_EQ(a2->Y->GetRefCount(), 1);
    EXPECT_EQ(a2->Y->V, 7);
    EXPECT_EQ(a2->X, a2->Y);
}

} // namespace NRef5

////////////////////////////////////////////////////////////////////////////////

namespace NRef6 {

struct TBase
    : public TRefCounted
    , public IPersistent
{ };

struct TDerived1
    : public TBase
{
    TDerived1()
        : V(-1)
    { }

    int V;

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, V);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TDerived1, 0x71297841);

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TDerived1);

struct TDerived2
    : public TBase
{
    TDerived2()
        : V(-1)
    { }

    double V;

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, V);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TDerived2, 0x62745629);

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TDerived2);

TEST(TPhoenixTest, Ref6)
{
    auto derived1 = New<TDerived1>();
    derived1->V = 5;
    TIntrusivePtr<TBase> base1(derived1);

    TIntrusivePtr<TBase> base2;
    Deserialize(base2, Serialize(base1));

    EXPECT_EQ(base2->GetRefCount(), 1);
    auto* derived2 = dynamic_cast<TDerived1*>(base2.Get());
    EXPECT_NE(derived2, nullptr);
    EXPECT_EQ(derived2->V, 5);
}

} // namespace NRef6

////////////////////////////////////////////////////////////////////////////////

namespace NRef7 {

struct TNonConstructable
    : public TRefCounted
    , public NPhoenix::TFactoryTag<NPhoenix::TNullFactory>
{
    explicit TNonConstructable(int x)
        : X(x)
    { }

    int X;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TNonConstructable, 0x14712618);

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TNonConstructable);

TEST(TPhoenixTest, Ref7)
{
    auto obj1 = New<TNonConstructable>(123);
    EXPECT_EQ(obj1->X, 123);

    auto obj2 = New<TNonConstructable>(456);

    InplaceDeserialize(obj2, Serialize(obj1));

    EXPECT_EQ(obj2->X, 123);
}

} // namespace NRef7

////////////////////////////////////////////////////////////////////////////////

namespace NRef8 {

struct A;
struct B;

struct A
    : public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
    int X;
    std::unique_ptr<B> T;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
        Persist(context, T);
    }

};

struct B
    : public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
    int Y;
    A* Z;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, Y);
        Persist(context, Z);
    }

};

TEST(TPhoenixTest, Ref8)
{
    std::unique_ptr<A> a1(new A());
    a1->X = 123;
    a1->T.reset(new B());
    a1->T->Y = 456;
    a1->T->Z = a1.get();

    std::unique_ptr<A> a2;
    Deserialize(a2, Serialize(a1));

    EXPECT_EQ(a2->X, 123);
    EXPECT_EQ(a2->T->Y, 456);
    EXPECT_EQ(a2->T->Z, a2.get());
}

} // namespace NRef8

////////////////////////////////////////////////////////////////////////////////

namespace NRef9 {

struct A;
struct B;

struct A
    : public IPersistent
{
    explicit A(int x)
        : X(x)
    { }

    int X;

    virtual void Foo() = 0;

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, X);
    }

};

struct B
    : public A
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
    B()
        : A(0)
        , Y(0)
        , Z(nullptr)
    { }

    int Y;
    A* Z;

    void Foo() override
    { }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        A::Persist(context);
        Persist(context, Y);
        Persist(context, Z);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(B, 0x54717818);

};

DEFINE_DYNAMIC_PHOENIX_TYPE(B);

TEST(TPhoenixTest, Ref9)
{
    std::unique_ptr<B> b1(new B());
    b1->X = 123;
    b1->Y = 456;
    b1->Z = b1.get();

    std::unique_ptr<A> a1(b1.release());

    std::unique_ptr<A> a2;
    Deserialize(a2, Serialize(a1));

    B* b2 = dynamic_cast<B*>(a2.get());
    EXPECT_NE(b2, nullptr);
    EXPECT_EQ(b2->X, 123);
    EXPECT_EQ(b2->Y, 456);
    EXPECT_EQ(b2->Z, b2);
}

} // namespace NRef9

////////////////////////////////////////////////////////////////////////////////

namespace NRef10 {

struct TBase
    : public TRefCounted
    , public NPhoenix::TDynamicTag
{
    TBase()
        : X (0)
    { }

    int X;

    virtual void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
    }
};

struct TDerived
    : public TBase
{
    TDerived()
    { }

    int Y;

    void Persist(const TPersistenceContext& context) override
    {
        TBase::Persist(context);

        using NYT::Persist;
        Persist(context, Y);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TDerived, 0x57818795);

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TDerived);

TEST(TPhoenixTest, Ref10)
{
    auto obj1 = New<TDerived>();
    obj1->X = 123;
    obj1->Y = 456;

    auto obj2 = New<TDerived>();

    InplaceDeserialize(obj2, Serialize(TIntrusivePtr<TBase>(obj1)));

    EXPECT_EQ(obj2->X, 123);
    EXPECT_EQ(obj2->Y, 456);
}

} // namespace NRef10

////////////////////////////////////////////////////////////////////////////////
} // namespace
} // namespace NYT
