#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>
#include <yt/yt/core/phoenix/polymorphic.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TSaveContext = NPhoenix::TSaveContext;
using TLoadContext = NPhoenix::TLoadContext;
using TPersistenceContext = NPhoenix::TPersistenceContext;
using IPersistent = NPhoenix::IPersistent;
using IPersistent2 = NPhoenix2::ICustomPersistent<TSaveContext, TLoadContext, TPersistenceContext>;

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

namespace NPhoenixCompatibilityMixedComposition {

struct TComponentV1
{
    int A = 0;
    double B = 0;

    bool operator==(const TComponentV1& other) const = default;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, A);
        Persist(context, B);
    }
};

struct TComponentV2
{
    int C = 0;
    double D = 0;

    bool operator==(const TComponentV2& other) const = default;

    PHOENIX_DECLARE_TYPE(TComponentV2, 0x89e65bad);
};

void TComponentV2::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::C>("c")();
    registrar.template Field<2, &TThis::D>("d")();
}

PHOENIX_DEFINE_TYPE(TComponentV2);

struct TCompositeV1
{
    TComponentV1 X;
    TComponentV2 Y;

    bool operator==(const TCompositeV1& other) const = default;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, X);
        Persist(context, Y);
    }
};

struct TCompositeV2
{
    TComponentV1 U;
    TComponentV2 V;

    bool operator==(const TCompositeV2& other) const = default;

    PHOENIX_DECLARE_TYPE(TCompositeV2, 0xd1c75b13);
};

void TCompositeV2::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::U>("u")();
    registrar.template Field<2, &TThis::V>("v")();
}

PHOENIX_DEFINE_TYPE(TCompositeV2);

} // namespace NPhoenixCompatibilityMixedComposition

TEST(TPhoenixCompatibilityTest, MixedComposition)
{
    using namespace NPhoenixCompatibilityMixedComposition;

    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TComponentV1>);
    EXPECT_TRUE(NPhoenix2::SupportsPhoenix2<TComponentV2>);
    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TCompositeV1>);
    EXPECT_TRUE(NPhoenix2::SupportsPhoenix2<TCompositeV2>);

    struct TDerived : public TComponentV2 {
    };

    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TDerived>);

    TComponentV1 component_v1;
    component_v1.A = 7;
    component_v1.B = 3.1416;

    TComponentV2 component_v2;
    component_v2.C = 13;
    component_v2.D = 2.7183;

    {
        TCompositeV1 composite_v1;
        composite_v1.X = component_v1;
        composite_v1.Y = component_v2;

        TCompositeV1 composite_v1_2;
        Deserialize(composite_v1_2, Serialize(composite_v1));

        EXPECT_EQ(composite_v1_2, composite_v1);
    }

    {
        TCompositeV2 composite_v2;
        composite_v2.U = component_v1;
        composite_v2.V = component_v2;

        TCompositeV2 composite_v2_2;
        Deserialize(composite_v2_2, Serialize(composite_v2));

        EXPECT_EQ(composite_v2_2, composite_v2);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NPhoenixCompatibilityMixedInheritance {

struct TBaseV1
{
    int A = 0;
    double B = 0;

    bool operator==(const TBaseV1& other) const = default;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, A);
        Persist(context, B);
    }

    PHOENIX_DECLARE_OPAQUE_TYPE(TBaseV1, 0xc5b6cc03);
};

PHOENIX_DEFINE_OPAQUE_TYPE(TBaseV1);

struct TBaseV2
{
    int C = 0;
    double D = 0;

    bool operator==(const TBaseV2& other) const = default;

    PHOENIX_DECLARE_TYPE(TBaseV2, 0x5d59f4a3);
};

void TBaseV2::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::C>("c")();
    registrar.template Field<2, &TThis::D>("d")();
}

PHOENIX_DEFINE_TYPE(TBaseV2);

struct TDerivedV1
    : public TBaseV1
    , public TBaseV2
{
    int E = 0;
    double F = 0;

    bool operator==(const TDerivedV1& other) const = default;

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        TBaseV1::Persist(context);
        TBaseV2::Persist(context);

        Persist(context, E);
        Persist(context, F);
    }
};

struct TDerivedV2
    : public TBaseV1
    , public TBaseV2
{
    int E = 0;
    double F = 0;

    bool operator==(const TDerivedV2& other) const = default;

    PHOENIX_DECLARE_TYPE(TDerivedV2, 0x7d64560b);
};

void TDerivedV2::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBaseV1>();
    registrar.template BaseType<TBaseV2>();

    registrar.template Field<1, &TThis::E>("e")();
    registrar.template Field<2, &TThis::F>("f")();
}

PHOENIX_DEFINE_TYPE(TDerivedV2);

} // namespace NPhoenixCompatibilityMixedInheritance

TEST(TPhoenixCompatibilityTest, MixedInheritance)
{
    using namespace NPhoenixCompatibilityMixedInheritance;

    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TBaseV1>);
    EXPECT_TRUE(NPhoenix2::SupportsPhoenix2<TBaseV2>);
    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TDerivedV1>);
    EXPECT_TRUE(NPhoenix2::SupportsPhoenix2<TDerivedV2>);

    TDerivedV1 derived_v1;
    derived_v1.A = 77;
    derived_v1.B = -0.77;
    derived_v1.C = 33;
    derived_v1.D = -0.33;
    derived_v1.E = 5;
    derived_v1.F = -0.5;

    TDerivedV1 derived_v1_2;
    Deserialize(derived_v1_2, Serialize(derived_v1));

    EXPECT_EQ(derived_v1_2, derived_v1);

    TDerivedV2 derived_v2;
    derived_v2.A = 77;
    derived_v2.B = -0.77;
    derived_v2.C = 33;
    derived_v2.D = -0.33;
    derived_v2.E = 5;
    derived_v2.F = -0.5;

    TDerivedV2 derived_v2_2;
    Deserialize(derived_v2_2, Serialize(derived_v2));

    EXPECT_EQ(derived_v2_2, derived_v2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NPhoenixCompatibilityMixedPolymorphic {

struct TBaseV1
    : public TRefCounted
    , public IPersistent
{
    int A = 0;
    double B = 0;

    bool operator==(const TBaseV1& other) const = default;

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        Persist(context, A);
        Persist(context, B);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TBaseV1, 0xbb6b6874);
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TBaseV1);

struct TBaseV2
    : public TRefCounted
    , public IPersistent2
{
    int C = 0;
    double D = 0;

    bool operator==(const TBaseV2& other) const = default;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TBaseV2, 0x20673022);
};

void TBaseV2::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::C>("c")();
    registrar.template Field<2, &TThis::D>("d")();
}

PHOENIX_DEFINE_TYPE(TBaseV2);

struct TDerivedV1
    : public TBaseV2
{
    int E = 0;
    double F = 0;

    bool operator==(const TDerivedV1& other) const = default;

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        TBaseV2::Persist(context);

        Persist(context, E);
        Persist(context, F);
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TDerivedV1, 0x5b176bab);
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TDerivedV1);

struct TDerivedV2
    : public TBaseV1
    , public IPersistent2
{
    int E = 0;
    double F = 0;

    bool operator==(const TDerivedV2& other) const = default;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TDerivedV2, 0xc461765b);
};

void TDerivedV2::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBaseV1>();

    registrar.template Field<1, &TThis::E>("e")();
    registrar.template Field<2, &TThis::F>("f")();
}

PHOENIX_DEFINE_TYPE(TDerivedV2);

} // namespace NPhoenixCompatibilityMixedPolymorphic

TEST(TPhoenixCompatibilityTest, MixedPolymorphic)
{
    using namespace NPhoenixCompatibilityMixedPolymorphic;

    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TBaseV1>);
    EXPECT_TRUE(NPhoenix2::SupportsPhoenix2<TBaseV2>);
    EXPECT_FALSE(NPhoenix2::SupportsPhoenix2<TDerivedV1>);
    EXPECT_TRUE(NPhoenix2::SupportsPhoenix2<TDerivedV2>);

    EXPECT_TRUE(NPhoenix2::NDetail::TPolymorphicTraits<TBaseV1>::Polymorphic);
    EXPECT_TRUE(NPhoenix2::NDetail::TPolymorphicTraits<TBaseV2>::Polymorphic);
    EXPECT_TRUE(NPhoenix2::NDetail::TPolymorphicTraits<TDerivedV1>::Polymorphic);
    EXPECT_TRUE(NPhoenix2::NDetail::TPolymorphicTraits<TDerivedV2>::Polymorphic);

    auto derived_v1 = New<TDerivedV1>();
    derived_v1->C = 33;
    derived_v1->D = -0.33;
    derived_v1->E = 5;
    derived_v1->F = -0.5;

    TIntrusivePtr<TBaseV2> base_v2_2;
    Deserialize(base_v2_2, Serialize(derived_v1));

    auto* derived_v1_2 = dynamic_cast<TDerivedV1*>(base_v2_2.Get());
    EXPECT_NE(derived_v1_2, nullptr);
    EXPECT_EQ(derived_v1_2->C, 33);
    EXPECT_EQ(derived_v1_2->D, -0.33);
    EXPECT_EQ(derived_v1_2->E, 5);
    EXPECT_EQ(derived_v1_2->F, -0.5);

    auto derived_v2 = New<TDerivedV2>();
    derived_v2->A = 33;
    derived_v2->B = -0.33;
    derived_v2->E = 5;
    derived_v2->F = -0.5;

    TIntrusivePtr<TBaseV1> base_v1_2;
    Deserialize(base_v1_2, Serialize(derived_v2));

    auto* derived_v2_2 = dynamic_cast<TDerivedV2*>(base_v1_2.Get());
    EXPECT_NE(derived_v2_2, nullptr);
    EXPECT_EQ(derived_v2_2->A, 33);
    EXPECT_EQ(derived_v2_2->B, -0.33);
    EXPECT_EQ(derived_v2_2->E, 5);
    EXPECT_EQ(derived_v2_2->F, -0.5);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
