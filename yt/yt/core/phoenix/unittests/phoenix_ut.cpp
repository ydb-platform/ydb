#include <gtest/gtest.h>

#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/phoenix/load.h>
#include <yt/yt/core/phoenix/schemas.h>
#include <yt/yt/core/phoenix/type_registry.h>
#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>
#include <yt/yt/core/phoenix/yson_decl.h>
#include <yt/yt/core/phoenix/yson_def.h>

#include <library/cpp/testing/gtest_extensions/assertions.h>

namespace NYT::NPhoenix2 {
namespace {

using namespace NYson;
using namespace NYTree;

using NYT::Save;
using NYT::Load;

////////////////////////////////////////////////////////////////////////////////

template <class T>
TString Serialize(const T& value)
{
    TString buffer;
    TStringOutput output(buffer);
    TSaveContext context(&output);
    Save(context, value);
    context.Finish();
    return buffer;
}

template <class F>
TString MakeBuffer(F&& func)
{
    TString buffer;
    TStringOutput output(buffer);
    TSaveContext context(&output);
    func(context);
    context.Finish();
    return buffer;
}

template <class T>
T Deserialize(const TString& buffer, int version = 0)
{
    T value;
    TStringInput input(buffer);
    TLoadContext context(&input);
    context.SetVersion(version);
    context.Dumper().SetEnabled(true);
    Load(context, value);
    return value;
}

template <class T>
void InplaceDeserialize(const TIntrusivePtr<T>& value, const TString& buffer, int version = 0)
{
    TStringInput input(buffer);
    TLoadContext context(&input);
    context.SetVersion(version);
    context.Dumper().SetEnabled(true);
    NPhoenix2::NDetail::TSerializer::InplaceLoad(context, value);
}

////////////////////////////////////////////////////////////////////////////////

class TPoint
{
public:
    TPoint() = default;
    TPoint(int x, int y)
        : X_(x)
        , Y_(y)
    { }

    bool operator==(const TPoint&) const = default;

    DEFINE_BYVAL_RO_PROPERTY(int, X);
    DEFINE_BYVAL_RO_PROPERTY(int, Y);

private:
    PHOENIX_DECLARE_TYPE(TPoint, 0xba6234ad);
    PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN(TPoint);
};

void TPoint::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X_>("x")();
    registrar.template Field<2, &TThis::Y_>("y")();
}

PHOENIX_DEFINE_TYPE(TPoint);
PHOENIX_DEFINE_YSON_DUMPABLE_TYPE_MIXIN(TPoint);

////////////////////////////////////////////////////////////////////////////////

struct TBaseStruct
{
    int A;

    bool operator==(const TBaseStruct&) const = default;

    PHOENIX_DECLARE_TYPE(TBaseStruct, 0x791ba62a);
    PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN(TBaseStruct);
};

void TBaseStruct::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::A>("a")();
}

PHOENIX_DEFINE_TYPE(TBaseStruct);
PHOENIX_DEFINE_YSON_DUMPABLE_TYPE_MIXIN(TBaseStruct);

////////////////////////////////////////////////////////////////////////////////

struct TDerivedStruct
    : public TBaseStruct
{
    int B;

    bool operator==(const TDerivedStruct&) const = default;

    PHOENIX_DECLARE_TYPE(TDerivedStruct, 0x216ba9fa);
    PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN(TDerivedStruct);
};

void TDerivedStruct::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBaseStruct>();
    registrar.template Field<1, &TThis::B>("b")();
}

PHOENIX_DEFINE_TYPE(TDerivedStruct);
PHOENIX_DEFINE_YSON_DUMPABLE_TYPE_MIXIN(TDerivedStruct);

////////////////////////////////////////////////////////////////////////////////

template <class T1, class T2>
struct TPair
{
    T1 First;
    T2 Second;

    bool operator==(const TPair&) const = default;

    PHOENIX_DECLARE_TEMPLATE_TYPE(TPair, 0xa36b7192);
    PHOENIX_DECLARE_YSON_DUMPABLE_TEMPLATE_MIXIN(TPair);
};

template <class T1, class T2>
void TPair<T1, T2>::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::First>("first")();
    registrar.template Field<2, &TThis::Second>("second")();
}

PHOENIX_DEFINE_TEMPLATE_TYPE(TPair, (<int, int>));

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedStruct
    : public TRefCounted
{
    PHOENIX_DECLARE_TYPE(TRefCountedStruct, 0x0baf628e1);
};

void TRefCountedStruct::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(TRefCountedStruct);

////////////////////////////////////////////////////////////////////////////////

struct TAbstractStruct
{
    virtual void Foo() = 0;

    PHOENIX_DECLARE_TYPE(TAbstractStruct, 0xb692acf9);
};

void TAbstractStruct::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(TAbstractStruct);

////////////////////////////////////////////////////////////////////////////////

struct TConcreteStruct
    : public TAbstractStruct
{
    void Foo() override
    { }

    PHOENIX_DECLARE_TYPE(TConcreteStruct, 0x6629b1f9);
};

void TConcreteStruct::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(TConcreteStruct);

////////////////////////////////////////////////////////////////////////////////

TEST(TPhoenixTest, Point)
{
    TPoint p1(123, 456);

    auto buffer = Serialize(p1);

    auto p2 = Deserialize<TPoint>(buffer);
    EXPECT_EQ(p1, p2);
}

TEST(TPhoenixTest, Derived)
{
    TDerivedStruct s1;
    s1.A = 123;
    s1.B = 456;

    auto buffer = Serialize(s1);

    auto s2 = Deserialize<TDerivedStruct>(buffer);
    EXPECT_EQ(s1, s2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NSinceVersion {

struct S
{
    int A;
    int B;
    int C;

    bool operator==(const S&) const = default;

    PHOENIX_DECLARE_TYPE(S, 0x67bc71fa);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::A>("a")();
    registrar.template Field<2, &TThis::B>("b")
        .SinceVersion(100)();
    registrar.template Field<3, &TThis::C>("c")
        .SinceVersion(200)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->C = 777;
        })();
}

PHOENIX_DEFINE_TYPE(S);

} // namespace NSinceVersion

TEST(TPhoenixTest, SinceVersionOld)
{
    using namespace NSinceVersion;

    S s1;
    s1.A = 123;
    s1.B = 0;
    s1.C = 777;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
    });

    auto s2 = Deserialize<S>(buffer);
    EXPECT_EQ(s1, s2);
}

TEST(TPhoenixTest, SinceVersionNew)
{
    using namespace NSinceVersion;

    S s1;
    s1.A = 123;
    s1.B = 456;
    s1.C = 321;

    auto buffer = Serialize(s1);

    auto s2 = Deserialize<S>(buffer, /*version*/ 200);
    EXPECT_EQ(s1, s2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NInVersions {

struct S
{
    int A;
    int B;
    int C;

    bool operator==(const S&) const = default;

    PHOENIX_DECLARE_TYPE(S, 0x81be71aa);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::A>("a")();
    registrar.template Field<2, &TThis::B>("b")
        .InVersions([] (int version) {
            return version >= 150 && version <= 250;
        })();
    registrar.template Field<3, &TThis::C>("c")
        .InVersions([] (int version) {
            return version >= 100 && version <= 200;
        })
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->C = 777;
        })();
}

PHOENIX_DEFINE_TYPE(S);

} // namespace NVersions

TEST(TPhoenixTest, InVersion1)
{
    using namespace NInVersions;

    S s1;
    s1.A = 123;
    s1.B = 0;
    s1.C = 777;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
    });

    auto s2 = Deserialize<S>(buffer, /*version*/ 10);
    EXPECT_EQ(s1, s2);
}

TEST(TPhoenixTest, InVersion2)
{
    using namespace NInVersions;

    S s1;
    s1.A = 123;
    s1.B = 0;
    s1.C = 456;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
        Save<int>(context, 456);
    });

    auto s2 = Deserialize<S>(buffer, /*version*/ 100);
    EXPECT_EQ(s1, s2);
}

TEST(TPhoenixTest, InVersion3)
{
    using namespace NInVersions;

    S s1;
    s1.A = 123;
    s1.B = 456;
    s1.C = 789;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
        Save<int>(context, 456);
        Save<int>(context, 789);
    });

    auto s2 = Deserialize<S>(buffer, /*version*/ 150);
    EXPECT_EQ(s1, s2);
}

TEST(TPhoenixTest, InVersion4)
{
    using namespace NInVersions;

    S s1;
    s1.A = 123;
    s1.B = 456;
    s1.C = 777;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
        Save<int>(context, 456);
    });

    auto s2 = Deserialize<S>(buffer, /*version*/ 210);
    EXPECT_EQ(s1, s2);
}

TEST(TPhoenixTest, InVersion5)
{
    using namespace NInVersions;

    S s1;
    s1.A = 123;
    s1.B = 0;
    s1.C = 777;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
    });

    auto s2 = Deserialize<S>(buffer, /*version*/ 300);
    EXPECT_EQ(s1, s2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NAfterLoad {

struct S
{
    bool AfterLoadInvoked = false;

    PHOENIX_DECLARE_TYPE(S, 0x76ba8fd1);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        EXPECT_FALSE(std::exchange(this_->AfterLoadInvoked, true));
    });
}

PHOENIX_DEFINE_TYPE(S);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAfterLoad

TEST(TPhoenixTest, AfterLoad)
{
    using namespace NAfterLoad;

    auto s = Deserialize<S>(TString());
    EXPECT_TRUE(s.AfterLoadInvoked);
}

////////////////////////////////////////////////////////////////////////////////

namespace NFieldSerializer {

struct TSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        NYT::Save<bool>(context, true);
        NYT::Save(context, value);
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        EXPECT_TRUE(NYT::Load<bool>(context));
        NYT::Load(context, value);
    }
};

struct S
{
    int A;
    int B;

    bool operator==(const S&) const = default;

    PHOENIX_DECLARE_TYPE(S, 0xb6fe9151);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::A>("a")
        .template Serializer<TSerializer>()();
    registrar.template Field<2, &TThis::B>("b")();
}

PHOENIX_DEFINE_TYPE(S);

} // namespace NFieldSerializer

TEST(TPhoenixTest, NativeFieldSerializer)
{
    using namespace NFieldSerializer;

    S s1;
    s1.A = 123;
    s1.B = 456;

    auto buffer = Serialize(s1);
    EXPECT_EQ(buffer.length(), sizeof(bool) + 2 * sizeof(int));

    auto s2 = Deserialize<S>(buffer);
    EXPECT_EQ(s1, s2);
}

TEST(TPhoenixTest, CompatFieldSerializer)
{
    using namespace NFieldSerializer;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<bool>(context, true);
        Save<int>(context, 123);
    });

    auto loadSchema = ConvertTo<TUniverseSchemaPtr>(TYsonString(TString(R"""(
        {
            types = [
                {
                    name = S;
                    tag = 3070136657u;
                    fields = [
                        {
                            name = a;
                            tag = 1u;
                        }
                    ]
                }
            ];
        }
    )""")));
    TLoadSessionGuard guard(loadSchema);
    EXPECT_TRUE(NDetail::UniverseLoadState->Schedule);

    auto s = Deserialize<S>(buffer);
    EXPECT_EQ(s.A, 123);
    EXPECT_EQ(s.B, 0);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPhoenixTest, YsonDumpablePair)
{
    TPair<TString, double> p{.First = "hello", .Second = 3.14};
    auto ysonStr = ConvertToYsonString(p);
    auto canonicalYsonStr = TYsonString(TString("{first=hello;second=3.14}"));
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(ysonStr), ConvertToNode(canonicalYsonStr)));
}

TEST(TPhoenixTest, YsonDumpablePoint)
{
    TPoint p(123, 456);
    auto ysonStr = ConvertToYsonString(p);
    auto canonicalYsonStr = TYsonString(TString("{x=123;y=456}"));
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(ysonStr), ConvertToNode(canonicalYsonStr)));
}

TEST(TPhoenixTest, YsonDumpableDerived)
{
    TDerivedStruct s;
    s.A = 123;
    s.B = 456;
    auto ysonStr = ConvertToYsonString(s);
    auto canonicalYsonStr = TYsonString(TString("{a=123;b=456}"));
    EXPECT_TRUE(AreNodesEqual(ConvertToNode(ysonStr), ConvertToNode(canonicalYsonStr)));
}

////////////////////////////////////////////////////////////////////////////////

namespace NCompatPointLoadSchema {

const auto Schema = ConvertTo<TUniverseSchemaPtr>(TYsonString(TString(R"""(
    {
        types = [
            {
                name = TPoint;
                tag = 3126998189u;
                fields = [
                    {name = y; tag = 2u};
                ];
            }
        ];
    }
)""")));

} // namespace NCompatPointLoadSchema

TEST(TPhoenixTest, CompatLoadPointFieldAdded)
{
    using namespace NCompatPointLoadSchema;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
    });

    TLoadSessionGuard guard(Schema);
    EXPECT_TRUE(NDetail::UniverseLoadState->Schedule);

    auto p = Deserialize<TPoint>(buffer);
    EXPECT_EQ(p.GetX(), 0);
    EXPECT_EQ(p.GetY(), 123);
}

TEST(TPhoenixTest, CompatLoadPointsFieldAdded)
{
    using namespace NCompatPointLoadSchema;

    constexpr int N = 10;

    auto buffer = MakeBuffer([] (auto& context) {
        TSizeSerializer::Save(context, N);
        for (int i = 0; i < N; i++) {
            Save<int>(context, i + 123);
        }
    });

    TLoadSessionGuard guard(ConvertTo<TUniverseSchemaPtr>(Schema));
    EXPECT_TRUE(NDetail::UniverseLoadState->Schedule);

    auto points = Deserialize<std::vector<TPoint>>(buffer);
    EXPECT_EQ(std::ssize(points), N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(points[i].GetX(), 0);
        EXPECT_EQ(points[i].GetY(), i + 123);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPhoenixTest, NativeLoadWithIdenticalSchema)
{
    auto schema = ITypeRegistry::Get()->GetUniverseDescriptor().GetSchema();
    TLoadSessionGuard guard(schema);
    EXPECT_FALSE(NDetail::UniverseLoadState->Schedule);
}

TEST(TPhoenixTest, NativeLoadWithEquivalentSchema)
{
    auto schemaNode = ConvertTo<IMapNodePtr>(ITypeRegistry::Get()->GetUniverseDescriptor().GetSchemaYson());
    auto typesNode = schemaNode->GetChildOrThrow("types")->AsList();
    for (const auto& typeNode : typesNode->GetChildren()) {
        auto nameNode = typeNode->AsMap()->GetChildOrThrow("name")->AsString();
        nameNode->SetValue("~" + nameNode->GetValue());
    }

    auto loadSchema = ConvertTo<TUniverseSchemaPtr>(schemaNode);
    TLoadSessionGuard guard(loadSchema);
    EXPECT_FALSE(NDetail::UniverseLoadState->Schedule);
}

TEST(TPhoenixTest, NativeLoadDerivedStructNoSchema)
{
    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
        Save<int>(context, 456);
    });

    auto loadSchema = ConvertTo<TUniverseSchemaPtr>(TYsonString(TString(R"""(
        {
            types = [];
        }
    )""")));
    TLoadSessionGuard guard(loadSchema);
    EXPECT_FALSE(NDetail::UniverseLoadState->Schedule);

    auto s = Deserialize<TDerivedStruct>(buffer);
    EXPECT_EQ(s.A, 123);
    EXPECT_EQ(s.B, 456);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDeprecatedField {

struct S
{
    int A = 0;

    PHOENIX_DECLARE_TYPE(S, 0x6282bc99);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.template DeprecatedField<1>("a", [] (TThis* this_, auto& context) {
        this_->A = Load<int>(context);
    })();
}

PHOENIX_DEFINE_TYPE(S);

} // namespace NDeprecatedField

TEST(TPhoenixTest, DeprecatedField)
{
    using namespace NDeprecatedField;

    auto buffer = MakeBuffer([] (auto& context) {
        Save<int>(context, 123);
    });

    auto loadSchema = ConvertTo<TUniverseSchemaPtr>(TYsonString(TString(R"""(
        {
            types = [
                {
                    name = S;
                    tag = 1652735129u;
                    fields = [
                        {name = a; tag = 1u};
                    ];
                }
            ];
        }
    )""")));
    TLoadSessionGuard guard(loadSchema);
    EXPECT_TRUE(NDetail::UniverseLoadState->Schedule);

    auto s = Deserialize<S>(buffer);
    EXPECT_EQ(s.A, 123);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPhoenixTest, Pair)
{
    TPair<TString, double> p1{.First = "hello", .Second = 3.14};

    auto buffer = Serialize(p1);

    auto p2 = Deserialize<TPair<TString, double>>(buffer);
    EXPECT_EQ(p1, p2);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPhoenixTest, UniverseSchema)
{
    const auto& descriptor = ITypeRegistry::Get()->GetUniverseDescriptor();
    Cerr << ConvertToYsonString(descriptor.GetSchema(), EYsonFormat::Pretty).AsStringBuf() << Endl;
}

TEST(TPhoenixTest, UnknownTypeTag)
{
    const auto& universeDescriptor = ITypeRegistry::Get()->GetUniverseDescriptor();
    constexpr auto tag = TTypeTag(0xbebebebe);
    EXPECT_EQ(universeDescriptor.FindTypeDescriptorByTag(tag), nullptr);
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        universeDescriptor.GetTypeDescriptorByTagOrThrow(tag),
        std::exception,
        "is not registered");
}

TEST(TPhoenixTest, UnknownTypeInfo)
{
    const auto& universeDescriptor = ITypeRegistry::Get()->GetUniverseDescriptor();
    EXPECT_EQ(universeDescriptor.FindTypeDescriptorByTypeIndex(typeid (void)), nullptr);
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        universeDescriptor.GetTypeDescriptorByTypeIndexOrThrow(typeid (void)),
        std::exception,
        "is not registered");
}

TEST(TPhoenixTest, TypeDescriptorByTypeInfo)
{
    const auto& universeDescriptor = ITypeRegistry::Get()->GetUniverseDescriptor();
    const auto* typeDescriptor = &universeDescriptor.GetTypeDescriptorByTagOrThrow(TPoint::TypeTag);
    EXPECT_EQ(universeDescriptor.FindTypeDescriptorByTypeIndex(typeid(TPoint)), typeDescriptor);
    EXPECT_EQ(&universeDescriptor.GetTypeDescriptorByTypeIndexOrThrow(typeid(TPoint)), typeDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPhoenixTest, InstantiateSimple)
{
    const auto& descriptor = ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTagOrThrow(TPoint::TypeTag);
    auto* p = descriptor.ConstructOrThrow<TPoint>();
    delete p;
}

TEST(TPhoenixTest, InstantiateRefCounted)
{
    const auto& descriptor = ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTagOrThrow(TRefCountedStruct::TypeTag);
    auto* s = descriptor.ConstructOrThrow<TRefCountedStruct>();
    EXPECT_EQ(s->GetRefCount(), 1);
    s->Unref();
}

TEST(TPhoenixTest, InstantiateNonconstructable)
{
    const auto& descriptor = ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTagOrThrow(TAbstractStruct::TypeTag);
    EXPECT_EQ(descriptor.TryConstruct<TAbstractStruct>(), nullptr);
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        descriptor.ConstructOrThrow<TAbstractStruct>(),
        std::exception,
        "Cannot instantiate");
}

////////////////////////////////////////////////////////////////////////////////

namespace NInstantiatePolymorphic {

struct TBase
    : public TPolymorphicBase
{
    int A;

    PHOENIX_DECLARE_TYPE(TBase, 0xbfad62ab);
};

void TBase::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::A>("a")();
}

PHOENIX_DEFINE_TYPE(TBase);

struct TDerived
    : public virtual TBase
{
    PHOENIX_DECLARE_TYPE(TDerived, 0x623bdf71);
};

void TDerived::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBase>();
}

PHOENIX_DEFINE_TYPE(TDerived);

} // namespace NInstantiatePolymorphic

TEST(TPhoenixTest, InstantiatePolymorphic)
{
    using namespace NInstantiatePolymorphic;

    const auto& descriptor = ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTagOrThrow(TDerived::TypeTag);
    auto* b = descriptor.ConstructOrThrow<TBase>();
    auto* d = dynamic_cast<TDerived*>(b);
    d->A = 123;
    EXPECT_EQ(d->A, 123);
    delete b;
}

////////////////////////////////////////////////////////////////////////////////

namespace NIntrusivePtr {

struct A;
struct B;

struct A
    : public TRefCounted
{
    TIntrusivePtr<B> X;
    TIntrusivePtr<B> Y;

    PHOENIX_DECLARE_TYPE(A, 0xb894f591);
};

void A::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("a")();
    registrar.template Field<2, &TThis::Y>("b")();
}

PHOENIX_DEFINE_TYPE(A);

struct B
    : public TRefCounted
{
    int V = -1;

    PHOENIX_DECLARE_TYPE(B, 0x7e871324);
};

void B::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::V>("v")();
}

PHOENIX_DEFINE_TYPE(B);

} // namespace NIntrusivePtr

TEST(TPhoenixTest, IntrusivePtr)
{
    using namespace NIntrusivePtr;

    auto a1 = New<A>();
    a1->X = New<B>();
    a1->X->V = 1;
    a1->Y = New<B>();
    a1->Y->V = 2;

    auto a2 = Deserialize<TIntrusivePtr<A>>(Serialize(a1));
    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_EQ(a2->X->GetRefCount(), 1);
    EXPECT_EQ(a2->X->V, 1);
    EXPECT_EQ(a2->Y->GetRefCount(), 1);
    EXPECT_EQ(a2->Y->V, 2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NIntrusivePtrCycle {

struct A
    : public TRefCounted
{
    TIntrusivePtr<A> X;

    PHOENIX_DECLARE_TYPE(A, 0xa85e9743);
};

void A::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("x")();
}

PHOENIX_DEFINE_TYPE(A);

} // namespace NIntrusivePtrCycle

TEST(TPhoenixTest, IntrusivePtrCycle1)
{
    using namespace NIntrusivePtrCycle;

    auto a1 = New<A>();

    auto a2 = Deserialize<TIntrusivePtr<A>>(Serialize(a1));
    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_FALSE(a2->X.operator bool());
}

TEST(TPhoenixTest, IntrusivePtrCycle2)
{
    using namespace NIntrusivePtrCycle;

    auto a1 = New<A>();
    a1->X = a1;

    auto a2 = Deserialize<TIntrusivePtr<A>>(Serialize(a1));
    EXPECT_EQ(a2->GetRefCount(), 2);
    EXPECT_EQ(a2->X, a2);

    // Kill cycles to avoid leaking memory.
    a1->X.Reset();
    a2->X.Reset();
}

////////////////////////////////////////////////////////////////////////////////

namespace NRawPtrCycle {

struct A
    : public TRefCounted
{
    A* X = nullptr;

    PHOENIX_DECLARE_TYPE(A, 0x5e4825a2);
};

void A::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("x")();
}

PHOENIX_DEFINE_TYPE(A);

} // namespace NRawPtrCycle

TEST(TPhoenixTest, RawPtrCycle1)
{
    using namespace NRawPtrCycle;

    auto a1 = New<A>();

    auto a2 = Deserialize<TIntrusivePtr<A>>(Serialize(a1));
    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_FALSE(a2->X);
}

TEST(TPhoenixTest, RawPtrCycle2)
{
    using namespace NRawPtrCycle;

    auto a1 = New<A>();
    a1->X = a1.Get();

    auto a2 = Deserialize<TIntrusivePtr<A>>(Serialize(a1));
    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_TRUE(a2->X);
    EXPECT_EQ(a2->X, a2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NIntrusiveAndRawPtr {

struct A;
struct B;

struct A
    : public TRefCounted
{
    B* X = nullptr;
    TIntrusivePtr<B> Y;

    PHOENIX_DECLARE_TYPE(A, 0xba21394a);
};

void A::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("x")();
    registrar.template Field<2, &TThis::Y>("y")();
}

PHOENIX_DEFINE_TYPE(A);

struct B
    : public TRefCounted
{
    int V = -1;

    PHOENIX_DECLARE_TYPE(B, 0xea191431);
};

void B::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &B::V>("v")();
}

PHOENIX_DEFINE_TYPE(B);

} // namespace NIntrusiveAndRawPtr

TEST(TPhoenixTest, IntrusiveAndRawPtr)
{
    using namespace NIntrusiveAndRawPtr;

    auto a1 = New<A>();
    a1->Y = New<B>();
    a1->Y->V = 7;
    a1->X = a1->Y.Get();

    auto a2 = Deserialize<TIntrusivePtr<A>>(Serialize(a1));
    EXPECT_EQ(a2->GetRefCount(), 1);
    EXPECT_EQ(a2->Y->GetRefCount(), 1);
    EXPECT_EQ(a2->Y->V, 7);
    EXPECT_EQ(a2->X, a2->Y);
}

////////////////////////////////////////////////////////////////////////////////

namespace NPersistentPolymorphic {

struct TBase
    : public TRefCounted
    , public IPersistent
{
    PHOENIX_DECLARE_TYPE(TBase, 0x612bb411);
};

void TBase::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(TBase);

struct TDerived1
    : public TBase
{
    int V = -1;

    PHOENIX_DECLARE_TYPE(TDerived1, 0x71297841);
};

void TDerived1::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBase>();
    registrar.template Field<1, &TThis::V>("v")();
}

PHOENIX_DEFINE_TYPE(TDerived1);

struct TDerived2
    : public TBase
{
    double V = -1;

    PHOENIX_DECLARE_TYPE(TDerived2, 0x62745629);
};

void TDerived2::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBase>();
    registrar.template Field<1, &TThis::V>("v")();
}

PHOENIX_DEFINE_TYPE(TDerived2);

} // namespace NPersistentPolymorphic

TEST(TPhoenixTest, PersistentPolymorphic)
{
    using namespace NPersistentPolymorphic;

    auto derived1 = New<TDerived1>();
    derived1->V = 5;
    TIntrusivePtr<TBase> base1(derived1);

    auto base2 = Deserialize<TIntrusivePtr<TBase>>(Serialize(base1));
    EXPECT_EQ(base2->GetRefCount(), 1);
    auto* derived2 = dynamic_cast<TDerived1*>(base2.Get());
    EXPECT_NE(derived2, nullptr);
    EXPECT_EQ(derived2->V, 5);
}

////////////////////////////////////////////////////////////////////////////////

namespace NInplaceNonconstructable {

struct S
    : public TRefCounted
{
    explicit S(int x)
        : X(x)
    { }

    int X;

    PHOENIX_DECLARE_TYPE(S, 0x14712618);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("x")();
}

PHOENIX_DEFINE_TYPE(S);

} // namespace NInplaceNonconstructable

TEST(TPhoenixTest, InplaceNonConstructable)
{
    using namespace NInplaceNonconstructable;

    auto obj1 = New<S>(123);
    EXPECT_EQ(obj1->X, 123);

    auto obj2 = New<S>(456);
    InplaceDeserialize(obj2, Serialize(obj1));
    EXPECT_EQ(obj2->X, 123);
}

////////////////////////////////////////////////////////////////////////////////

namespace NUniquePtr {

struct A;
struct B;

struct A
{
    int X = 0;
    std::unique_ptr<B> T;

    PHOENIX_DECLARE_TYPE(A, 0x61839413);
};

void A::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("x")();
    registrar.template Field<2, &TThis::T>("t")();
}

PHOENIX_DEFINE_TYPE(A);

struct B
{
    int Y = 0;
    A* Z = nullptr;

    PHOENIX_DECLARE_TYPE(B, 0x98734632);
};

void B::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::Y>("y")();
    registrar.template Field<2, &TThis::Z>("z")();
}

PHOENIX_DEFINE_TYPE(B);

} // namespace NUniquePtr

TEST(TPhoenixTest, UniquePtr)
{
    using namespace NUniquePtr;

    std::unique_ptr<A> a1(new A());
    a1->X = 123;
    a1->T.reset(new B());
    a1->T->Y = 456;
    a1->T->Z = a1.get();

    auto a2 = Deserialize<std::unique_ptr<A>>(Serialize(a1));
    EXPECT_EQ(a2->X, 123);
    EXPECT_EQ(a2->T->Y, 456);
    EXPECT_EQ(a2->T->Z, a2.get());
}

////////////////////////////////////////////////////////////////////////////////

namespace NPolymorphicRawPtr {

struct TBase;
struct TDervied;

struct TBase
    : public IPersistent
{
    explicit TBase(int x)
        : X(x)
    { }

    int X;

    virtual void Foo() = 0;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TBase, 0x12b64e8c);
};

void TBase::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X>("x")();
}

PHOENIX_DEFINE_TYPE(TBase);

struct TDervied
    : public TBase
{
    TDervied()
        : TBase(0)
    { }

    int Y = 0;
    TBase* Z = nullptr;

    void Foo() override
    { }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TDervied, 0x54717818);
};

void TDervied::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBase>();
    registrar.template Field<1, &TThis::Y>("y")();
    registrar.template Field<2, &TThis::Z>("z")();
}

PHOENIX_DEFINE_TYPE(TDervied);

} // namespace NPolymorphicRawPtr

TEST(TPhoenixTest, PolymorphicRawPtr)
{
    using namespace NPolymorphicRawPtr;

    std::unique_ptr<TDervied> b1(new TDervied());
    b1->X = 123;
    b1->Y = 456;
    b1->Z = b1.get();
    std::unique_ptr<TBase> a1(b1.release());

    auto a2 = Deserialize<std::unique_ptr<TBase>>(Serialize(a1));
    TDervied* b2 = dynamic_cast<TDervied*>(a2.get());
    EXPECT_NE(b2, nullptr);
    EXPECT_EQ(b2->X, 123);
    EXPECT_EQ(b2->Y, 456);
    EXPECT_EQ(b2->Z, b2);
}

////////////////////////////////////////////////////////////////////////////////

namespace NPolymorphicIntrusivePtr {

struct TBase1
    : public virtual TRefCounted
    , public virtual IPersistent
{
    int X1 = 0;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TBase1, 0x149f8345);
};

void TBase1::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X1>("x1")();
}

PHOENIX_DEFINE_TYPE(TBase1);

struct TBase2
    : public virtual TRefCounted
    , public virtual IPersistent
{
    int X2 = 0;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TBase2, 0x185ec0d);
};

void TBase2::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::X2>("x2")();
}

PHOENIX_DEFINE_TYPE(TBase2);

struct TDerived
    : public TBase1
    , public TBase2
{
    int Y;
    TIntrusivePtr<TBase2> Z;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TDerived, 0x57818795);
};

void TDerived::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TBase1>();
    registrar.template BaseType<TBase2>();
    registrar.template Field<1, &TThis::Y>("y")();
    registrar.template Field<2, &TThis::Z>("z")();
}

PHOENIX_DEFINE_TYPE(TDerived);

} // namespace NPolymorphicIntrusivePtr

TEST(TPhoenixTest, PolymorphicIntrusivePtr)
{
    using namespace NPolymorphicIntrusivePtr;

    auto obj1 = New<TDerived>();
    obj1->X1= 123;
    obj1->Y = 456;
    obj1->Z = obj1.Get();

    auto obj2 = New<TDerived>();
    InplaceDeserialize(obj2, Serialize(TIntrusivePtr<TBase1>(obj1)));
    EXPECT_EQ(obj2->X1, 123);
    EXPECT_EQ(obj2->Y, 456);
    EXPECT_EQ(obj2->Z, obj2);

    // Kill cycles to avoid leaking memory.
    obj1->Z.Reset();
    obj2->Z.Reset();
}

TEST(TPhoenixTest, PolymorphicMultipleInheritance)
{
    using namespace NPolymorphicIntrusivePtr;

    auto obj1 = New<TDerived>();
    obj1->X1 = 11;
    obj1->X2 = 12;
    obj1->Y = 13;

    auto obj2 = New<TDerived>();
    obj2->X1 = 21;
    obj2->X2 = 22;
    obj2->Y = 23;
    obj2->Z = obj1;

    auto obj3 = New<TDerived>();
    auto x = Serialize(TIntrusivePtr<TBase1>(obj2));
    InplaceDeserialize(obj3, x);
    EXPECT_EQ(obj3->X1, 21);
    EXPECT_EQ(obj3->X2, 22);
    EXPECT_EQ(obj3->Y, 23);
    EXPECT_EQ(obj3->Z->X2, 12);
}

////////////////////////////////////////////////////////////////////////////////

namespace NOpaque {

using namespace NYTree;

struct TYsonBase
    : public TYsonStruct
    , public TPolymorphicBase
{
    int X;

    REGISTER_YSON_STRUCT(TYsonBase);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("x", &TThis::X);
    }
};

struct TYsonDerived
    : public TYsonBase
{
    int Y;

    PHOENIX_DECLARE_OPAQUE_TYPE(TDerived, 0xbab60123);
    REGISTER_YSON_STRUCT(TYsonDerived);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("y", &TThis::Y);
    }
};

PHOENIX_DEFINE_OPAQUE_TYPE(TYsonDerived);

struct S
{
    TIntrusivePtr<TYsonBase> A;

    PHOENIX_DECLARE_TYPE(S, 0x6134b91a);
};

void S::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::A>("a")();
}

PHOENIX_DEFINE_TYPE(S);

} // namespace NOpaque

TEST(TPhoenixTest, Opaque)
{
    using namespace NOpaque;

    S s1;
    auto obj1 = New<TYsonDerived>();
    obj1->X = 123;
    obj1->Y = 456;
    s1.A = obj1;

    auto s2 = Deserialize<S>(Serialize(s1));
    EXPECT_NE(s2.A, nullptr);
    auto obj2 = DynamicPointerCast<TYsonDerived>(s2.A);
    EXPECT_NE(obj2, nullptr);
    EXPECT_EQ(obj2->X, 123);
    EXPECT_EQ(obj2->Y, 456);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPhoenix2
