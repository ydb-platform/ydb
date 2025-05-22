#include "type.h"

#include "type_factory.h"
#include "type_equivalence.h"

#include <util/generic/overloaded.h>

#include <util/digest/murmur.h>
#include <util/generic/hash_set.h>
#include <util/string/escape.h>

namespace {
    inline ui64 Hash(NTi::ETypeName type) {
        return IntHash(static_cast<ui64>(type));
    }

    inline ui64 Hash(ui64 value, ui64 seed) {
        return MurmurHash(&value, sizeof(value), seed);
    }

    inline ui64 Hash(TStringBuf string, ui64 seed) {
        seed = ::Hash(string.size(), seed);
        return MurmurHash(string.data(), string.size(), seed);
    }

    inline ui64 Hash(TMaybe<TStringBuf> string, ui64 seed) {
        if (string.Defined()) {
            return MurmurHash(string->data(), string->size(), seed);
        } else {
            return seed;
        }
    }

    TString Quote(TStringBuf s) {
        TString result;
        result.push_back('\'');
        result += EscapeC(s);
        result.push_back('\'');
        return result;
    }
}

namespace NTi {
    TType::TType(TMaybe<ui64> hash, ETypeName typeName) noexcept
        : TypeName_(typeName)
        , HasHash_(hash.Defined())
        , Hash_(hash.GetOrElse(0))
    {
    }

    ui64 TType::CalculateHash() const noexcept {
        return ::Hash(TypeName_);
    }

    TMaybe<ui64> TType::GetHashRaw() const noexcept {
        if (HasHash_.load(std::memory_order_seq_cst)) {
            return Hash_.load(std::memory_order_seq_cst);
        } else {
            return Nothing();
        }
    }

    ui64 TType::GetHash() const {
        if (HasHash_.load(std::memory_order_seq_cst)) {
            return Hash_.load(std::memory_order_seq_cst);
        } else {
            ui64 hash = VisitRaw([](const auto* type) {
                return type->CalculateHash();
            });
            Hash_.store(hash, std::memory_order_seq_cst);
            HasHash_.store(true, std::memory_order_seq_cst);
            return hash;
        }
    }

    TTypePtr TType::StripTags() const noexcept {
        return StripTagsRaw()->AsPtr();
    }

    const TType* TType::StripTagsRaw() const noexcept {
        auto type = this;
        while (type->IsTagged()) {
            type = type->AsTaggedRaw()->GetItemTypeRaw();
        }
        return type;
    }

    TTypePtr TType::StripOptionals() const noexcept {
        return StripOptionalsRaw()->AsPtr();
    }

    const TType* TType::StripOptionalsRaw() const noexcept {
        auto type = this;
        while (type->IsOptional()) {
            type = type->AsOptionalRaw()->GetItemTypeRaw();
        }
        return type;
    }

    TTypePtr TType::StripTagsAndOptionals() const noexcept {
        return StripTagsAndOptionalsRaw()->AsPtr();
    }

    const TType* TType::StripTagsAndOptionalsRaw() const noexcept {
        auto type = this;
        while (type->IsTagged() || type->IsOptional()) {
            if (type->IsTagged()) {
                type = type->AsTaggedRaw()->GetItemTypeRaw();
            } else {
                type = type->AsOptionalRaw()->GetItemTypeRaw();
            }
        }
        return type;
    }

    const TType* TType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return VisitRaw([&factory](const auto* type) -> const NTi::TType* {
            return type->Clone(factory);
        });
    }

    void TType::Drop(ITypeFactoryInternal& factory) noexcept {
        VisitRaw([&factory](const auto* type) {
            using T = std::remove_const_t<std::remove_pointer_t<decltype(type)>>;
            return const_cast<T*>(type)->Drop(factory);
        });
    }

    ITypeFactoryInternal* TType::GetFactory() const noexcept {
        size_t manager_or_rc = FactoryOrRc_.load(std::memory_order_relaxed);
        if (IsRc(manager_or_rc)) {
            return NPrivate::GetDefaultHeapFactory();
        } else if (IsFactory(manager_or_rc)) {
            return CastToFactory(manager_or_rc);
        } else {
            return nullptr;
        }
    }

    void TType::SetFactory(ITypeFactoryInternal* factory) noexcept {
        if (factory == NPrivate::GetDefaultHeapFactory()) {
            FactoryOrRc_.store(0b1u, std::memory_order_release);
        } else {
            FactoryOrRc_.store(CastFromFactory(factory), std::memory_order_release);
        }
    }

    ITypeFactoryInternal& TType::FactoryInternal(ITypeFactory& factory) noexcept {
        return static_cast<ITypeFactoryInternal&>(factory);
    }

    template <bool RefFactory>
    void TType::RefImpl() noexcept {
        size_t factoryOrRc = FactoryOrRc_.load(std::memory_order_relaxed);
        if (Y_LIKELY(IsRc(factoryOrRc))) {
            FactoryOrRc_.fetch_add(0b10u, std::memory_order_acq_rel);
        } else if (Y_LIKELY(IsFactory(factoryOrRc))) {
            auto factory = CastToFactory(factoryOrRc);
            if (RefFactory) {
                factory->Ref();
            }
            factory->RefType(this);
        }
    }

    template void TType::RefImpl<true>() noexcept;
    template void TType::RefImpl<false>() noexcept;

    template <bool UnRefFactory>
    void TType::UnRefImpl() noexcept {
        size_t factoryOrRc = FactoryOrRc_.load(std::memory_order_relaxed);
        if (Y_LIKELY(IsRc(factoryOrRc))) {
            size_t rc = FactoryOrRc_.fetch_sub(0b10u, std::memory_order_acq_rel);
            if (rc == 0b11u) {
                auto factory = NPrivate::GetDefaultHeapFactory();
                Drop(*factory);
                factory->Delete(this);
            }
        } else if (Y_LIKELY(IsFactory(factoryOrRc))) {
            auto factory = CastToFactory(factoryOrRc);
            factory->UnRefType(this);
            if (UnRefFactory) {
                factory->UnRef();
            }
        }
    }

    template void TType::UnRefImpl<true>() noexcept;
    template void TType::UnRefImpl<false>() noexcept;

    template <bool DecRefFactory>
    void TType::DecRefImpl() noexcept {
        size_t factoryOrRc = FactoryOrRc_.load(std::memory_order_relaxed);
        if (Y_LIKELY(IsRc(factoryOrRc))) {
            size_t rc = FactoryOrRc_.fetch_sub(2, std::memory_order_acq_rel);
            if (rc == 2) {
                Y_ABORT("DecRef isn't supposed to drop");
            }
        } else if (Y_LIKELY(IsFactory(factoryOrRc))) {
            auto factory = CastToFactory(factoryOrRc);
            factory->DecRefType(this);
            if (DecRefFactory) {
                factory->DecRef();
            }
        }
    }

    template void TType::DecRefImpl<true>() noexcept;
    template void TType::DecRefImpl<false>() noexcept;

    long TType::RefCountImpl() const noexcept {
        size_t factoryOrRc = FactoryOrRc_.load(std::memory_order_relaxed);
        if (Y_LIKELY(IsRc(factoryOrRc))) {
            return factoryOrRc >> 1u;
        } else if (Y_LIKELY(IsFactory(factoryOrRc))) {
            return CastToFactory(factoryOrRc)->RefCountType(this);
        } else {
            return 0;
        }
    }

    template <typename T, typename TCtor>
    const T* TType::Cached(const T* type, ITypeFactoryInternal& factory, TCtor&& ctor) {
        const TType* result = factory.LookupCache(type);

        if (result == nullptr) {
            result = std::forward<TCtor>(ctor)();
            factory.SaveCache(result);
        }

        Y_ABORT_UNLESS(result->GetTypeName() == type->GetTypeName());
        Y_DEBUG_ABORT_UNLESS(result->GetHash() == type->GetHash());
        return static_cast<const T*>(result);
    }

    bool operator==(const TType& lhs, const TType& rhs) {
        Y_ABORT_UNLESS(&lhs);
        Y_ABORT_UNLESS(&rhs);
        return NEq::TStrictlyEqual().IgnoreHash(&lhs, &rhs);
    }

    bool operator!=(const TType& lhs, const TType& rhs)
    {
        return !(lhs == rhs);
    }

    TVoidType::TVoidType()
        : TType({}, ETypeName::Void)
    {
    }

    TVoidTypePtr TVoidType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TVoidType* TVoidType::InstanceRaw() {
        static auto singleton = TVoidType();
        return &singleton;
    }

    const TVoidType* TVoidType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TVoidType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TNullType::TNullType()
        : TType({}, ETypeName::Null)
    {
    }

    TNullTypePtr TNullType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TNullType* TNullType::InstanceRaw() {
        static auto singleton = TNullType();
        return &singleton;
    }

    const TNullType* TNullType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TNullType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TPrimitiveType::TPrimitiveType(TMaybe<ui64> hash, EPrimitiveTypeName primitiveTypeName) noexcept
        : TType(hash, ToTypeName(primitiveTypeName))
    {
    }

    TBoolType::TBoolType()
        : TPrimitiveType({}, EPrimitiveTypeName::Bool)
    {
    }

    TBoolTypePtr TBoolType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TBoolType* TBoolType::InstanceRaw() {
        static auto singleton = TBoolType();
        return &singleton;
    }

    const TBoolType* TBoolType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TBoolType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TInt8Type::TInt8Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Int8)
    {
    }

    TInt8TypePtr TInt8Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TInt8Type* TInt8Type::InstanceRaw() {
        static auto singleton = TInt8Type();
        return &singleton;
    }

    const TInt8Type* TInt8Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TInt8Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TInt16Type::TInt16Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Int16)
    {
    }

    TInt16TypePtr TInt16Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TInt16Type* TInt16Type::InstanceRaw() {
        static auto singleton = TInt16Type();
        return &singleton;
    }

    const TInt16Type* TInt16Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TInt16Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TInt32Type::TInt32Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Int32)
    {
    }

    TInt32TypePtr TInt32Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TInt32Type* TInt32Type::InstanceRaw() {
        static auto singleton = TInt32Type();
        return &singleton;
    }

    const TInt32Type* TInt32Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TInt32Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TInt64Type::TInt64Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Int64)
    {
    }

    TInt64TypePtr TInt64Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TInt64Type* TInt64Type::InstanceRaw() {
        static auto singleton = TInt64Type();
        return &singleton;
    }

    const TInt64Type* TInt64Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TInt64Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TUint8Type::TUint8Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Uint8)
    {
    }

    TUint8TypePtr TUint8Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TUint8Type* TUint8Type::InstanceRaw() {
        static auto singleton = TUint8Type();
        return &singleton;
    }

    const TUint8Type* TUint8Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TUint8Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TUint16Type::TUint16Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Uint16)
    {
    }

    TUint16TypePtr TUint16Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TUint16Type* TUint16Type::InstanceRaw() {
        static auto singleton = TUint16Type();
        return &singleton;
    }

    const TUint16Type* TUint16Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TUint16Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TUint32Type::TUint32Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Uint32)
    {
    }

    TUint32TypePtr TUint32Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TUint32Type* TUint32Type::InstanceRaw() {
        static auto singleton = TUint32Type();
        return &singleton;
    }

    const TUint32Type* TUint32Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TUint32Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TUint64Type::TUint64Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Uint64)
    {
    }

    TUint64TypePtr TUint64Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TUint64Type* TUint64Type::InstanceRaw() {
        static auto singleton = TUint64Type();
        return &singleton;
    }

    const TUint64Type* TUint64Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TUint64Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TFloatType::TFloatType()
        : TPrimitiveType({}, EPrimitiveTypeName::Float)
    {
    }

    TFloatTypePtr TFloatType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TFloatType* TFloatType::InstanceRaw() {
        static auto singleton = TFloatType();
        return &singleton;
    }

    const TFloatType* TFloatType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TFloatType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TDoubleType::TDoubleType()
        : TPrimitiveType({}, EPrimitiveTypeName::Double)
    {
    }

    TDoubleTypePtr TDoubleType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TDoubleType* TDoubleType::InstanceRaw() {
        static auto singleton = TDoubleType();
        return &singleton;
    }

    const TDoubleType* TDoubleType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TDoubleType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TStringType::TStringType()
        : TPrimitiveType({}, EPrimitiveTypeName::String)
    {
    }

    TStringTypePtr TStringType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TStringType* TStringType::InstanceRaw() {
        static auto singleton = TStringType();
        return &singleton;
    }

    const TStringType* TStringType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TStringType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TUtf8Type::TUtf8Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Utf8)
    {
    }

    TUtf8TypePtr TUtf8Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TUtf8Type* TUtf8Type::InstanceRaw() {
        static auto singleton = TUtf8Type();
        return &singleton;
    }

    const TUtf8Type* TUtf8Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TUtf8Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TDateType::TDateType()
        : TPrimitiveType({}, EPrimitiveTypeName::Date)
    {
    }

    TDateTypePtr TDateType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TDateType* TDateType::InstanceRaw() {
        static auto singleton = TDateType();
        return &singleton;
    }

    const TDateType* TDateType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TDateType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TDatetimeType::TDatetimeType()
        : TPrimitiveType({}, EPrimitiveTypeName::Datetime)
    {
    }

    TDatetimeTypePtr TDatetimeType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TDatetimeType* TDatetimeType::InstanceRaw() {
        static auto singleton = TDatetimeType();
        return &singleton;
    }

    const TDatetimeType* TDatetimeType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TDatetimeType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TTimestampType::TTimestampType()
        : TPrimitiveType({}, EPrimitiveTypeName::Timestamp)
    {
    }

    TTimestampTypePtr TTimestampType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TTimestampType* TTimestampType::InstanceRaw() {
        static auto singleton = TTimestampType();
        return &singleton;
    }

    const TTimestampType* TTimestampType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TTimestampType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TTzDateType::TTzDateType()
        : TPrimitiveType({}, EPrimitiveTypeName::TzDate)
    {
    }

    TTzDateTypePtr TTzDateType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TTzDateType* TTzDateType::InstanceRaw() {
        static auto singleton = TTzDateType();
        return &singleton;
    }

    const TTzDateType* TTzDateType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TTzDateType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TTzDatetimeType::TTzDatetimeType()
        : TPrimitiveType({}, EPrimitiveTypeName::TzDatetime)
    {
    }

    TTzDatetimeTypePtr TTzDatetimeType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TTzDatetimeType* TTzDatetimeType::InstanceRaw() {
        static auto singleton = TTzDatetimeType();
        return &singleton;
    }

    const TTzDatetimeType* TTzDatetimeType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TTzDatetimeType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TTzTimestampType::TTzTimestampType()
        : TPrimitiveType({}, EPrimitiveTypeName::TzTimestamp)
    {
    }

    TTzTimestampTypePtr TTzTimestampType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TTzTimestampType* TTzTimestampType::InstanceRaw() {
        static auto singleton = TTzTimestampType();
        return &singleton;
    }

    const TTzTimestampType* TTzTimestampType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TTzTimestampType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TIntervalType::TIntervalType()
        : TPrimitiveType({}, EPrimitiveTypeName::Interval)
    {
    }

    TIntervalTypePtr TIntervalType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TIntervalType* TIntervalType::InstanceRaw() {
        static auto singleton = TIntervalType();
        return &singleton;
    }

    const TIntervalType* TIntervalType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TIntervalType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TDecimalType::TDecimalType(TMaybe<ui64> hash, ui8 precision, ui8 scale) noexcept
        : TPrimitiveType(hash, EPrimitiveTypeName::Decimal)
        , Precision_(precision)
        , Scale_(scale)
    {
    }

    TDecimalTypePtr TDecimalType::Create(ITypeFactory& factory, ui8 precision, ui8 scale) {
        return CreateRaw(factory, precision, scale)->AsPtr();
    }

    const TDecimalType* TDecimalType::CreateRaw(ITypeFactory& factory, ui8 precision, ui8 scale) {
        Y_ENSURE_EX(
            scale <= precision,
            TIllegalTypeException() << "decimal scale " << (ui32)scale
                                    << " should be no greater than decimal precision " << (ui32)precision);

        return TDecimalType({}, precision, scale).Clone(FactoryInternal(factory));
    }

    ui64 TDecimalType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Precision_, hash);
        hash = ::Hash(Scale_, hash);
        return hash;
    }

    const TDecimalType* TDecimalType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TDecimalType* {
            auto hash = GetHashRaw();
            auto precision = Precision_;
            auto scale = Scale_;
            return factory.New<TDecimalType>(hash, precision, scale);
        });
    }

    void TDecimalType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TJsonType::TJsonType()
        : TPrimitiveType({}, EPrimitiveTypeName::Json)
    {
    }

    TJsonTypePtr TJsonType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TJsonType* TJsonType::InstanceRaw() {
        static auto singleton = TJsonType();
        return &singleton;
    }

    const TJsonType* TJsonType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TJsonType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TYsonType::TYsonType()
        : TPrimitiveType({}, EPrimitiveTypeName::Yson)
    {
    }

    TYsonTypePtr TYsonType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TYsonType* TYsonType::InstanceRaw() {
        static auto singleton = TYsonType();
        return &singleton;
    }

    const TYsonType* TYsonType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TYsonType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TUuidType::TUuidType()
        : TPrimitiveType({}, EPrimitiveTypeName::Uuid)
    {
    }

    TUuidTypePtr TUuidType::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TUuidType* TUuidType::InstanceRaw() {
        static auto singleton = TUuidType();
        return &singleton;
    }

    const TUuidType* TUuidType::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TUuidType::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TDate32Type::TDate32Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Date32)
    {
    }

    TDate32TypePtr TDate32Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TDate32Type* TDate32Type::InstanceRaw() {
        static auto singleton = TDate32Type();
        return &singleton;
    }

    const TDate32Type* TDate32Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TDate32Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TDatetime64Type::TDatetime64Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Datetime64)
    {
    }

    TDatetime64TypePtr TDatetime64Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TDatetime64Type* TDatetime64Type::InstanceRaw() {
        static auto singleton = TDatetime64Type();
        return &singleton;
    }

    const TDatetime64Type* TDatetime64Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TDatetime64Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TTimestamp64Type::TTimestamp64Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Timestamp64)
    {
    }

    TTimestamp64TypePtr TTimestamp64Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TTimestamp64Type* TTimestamp64Type::InstanceRaw() {
        static auto singleton = TTimestamp64Type();
        return &singleton;
    }

    const TTimestamp64Type* TTimestamp64Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TTimestamp64Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TInterval64Type::TInterval64Type()
        : TPrimitiveType({}, EPrimitiveTypeName::Interval64)
    {
    }

    TInterval64TypePtr TInterval64Type::Instance() {
        return InstanceRaw()->AsPtr();
    }

    const TInterval64Type* TInterval64Type::InstanceRaw() {
        static auto singleton = TInterval64Type();
        return &singleton;
    }

    const TInterval64Type* TInterval64Type::Clone(ITypeFactoryInternal& factory) const noexcept {
        Y_UNUSED(factory);
        return InstanceRaw();
    }

    void TInterval64Type::Drop(ITypeFactoryInternal& factory) noexcept {
        Y_UNUSED(factory);
    }

    TOptionalType::TOptionalType(TMaybe<ui64> hash, const TType* item) noexcept
        : TType(hash, ETypeName::Optional)
        , Item_(item)
    {
    }

    TOptionalTypePtr TOptionalType::Create(ITypeFactory& factory, TTypePtr item) {
        return CreateRaw(factory, item.Get())->AsPtr();
    }

    const TOptionalType* TOptionalType::CreateRaw(ITypeFactory& factory, const TType* item) {
        return TOptionalType({}, item).Clone(FactoryInternal(factory));
    }

    const TOptionalType* TOptionalType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TOptionalType* {
            auto hash = GetHashRaw();
            auto item = factory.Own(Item_);
            return factory.New<TOptionalType>(hash, item);
        });
    }

    void TOptionalType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.Disown(Item_);
    }

    ui64 TOptionalType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Item_->GetHash(), hash);
        return hash;
    }

    TListType::TListType(TMaybe<ui64> hash, const TType* item) noexcept
        : TType(hash, ETypeName::List)
        , Item_(item)
    {
    }

    TListTypePtr TListType::Create(ITypeFactory& factory, TTypePtr item) {
        return CreateRaw(factory, item.Get())->AsPtr();
    }

    const TListType* TListType::CreateRaw(ITypeFactory& factory, const TType* item) {
        return TListType({}, item).Clone(FactoryInternal(factory));
    }

    const TListType* TListType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TListType* {
            auto hash = GetHashRaw();
            auto item = factory.Own(Item_);
            return factory.New<TListType>(hash, item);
        });
    }

    void TListType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.Disown(Item_);
    }

    ui64 TListType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Item_->GetHash(), hash);
        return hash;
    }

    TDictType::TDictType(TMaybe<ui64> hash, const TType* key, const TType* value) noexcept
        : TType(hash, ETypeName::Dict)
        , Key_(key)
        , Value_(value)
    {
    }

    TDictTypePtr TDictType::Create(ITypeFactory& factory, TTypePtr key, TTypePtr value) {
        return CreateRaw(factory, key.Get(), value.Get())->AsPtr();
    }

    const TDictType* TDictType::CreateRaw(ITypeFactory& factory, const TType* key, const TType* value) {
        return TDictType({}, key, value).Clone(FactoryInternal(factory));
    }

    const TDictType* TDictType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TDictType* {
            auto hash = GetHashRaw();
            auto key = factory.Own(Key_);
            auto value = factory.Own(Value_);
            return factory.New<TDictType>(hash, key, value);
        });
    }

    void TDictType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.Disown(Key_);
        factory.Disown(Value_);
    }

    ui64 TDictType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Key_->GetHash(), hash);
        hash = ::Hash(Value_->GetHash(), hash);
        return hash;
    }

    TStructType::TMember::TMember(TStringBuf name, const TType* type)
        : Name_(name)
        , Type_(type)
    {
    }

    ui64 TStructType::TMember::Hash() const {
        auto hash = 0x10000;
        hash = ::Hash(Name_, hash);
        hash = ::Hash(Type_->GetHash(), hash);
        return hash;
    }

    TStructType::TOwnedMember::TOwnedMember(TString name, TTypePtr type)
        : Name_(std::move(name))
        , Type_(std::move(type))
    {
    }

    TStructType::TOwnedMember::operator TStructType::TMember() const& {
        return TStructType::TMember(Name_, Type_.Get());
    }

    TStructType::TStructType(TMaybe<ui64> hash, TMaybe<TStringBuf> name, TMembers members, TConstArrayRef<size_t> sortedItems) noexcept
        : TType(hash, ETypeName::Struct)
        , Name_(name)
        , Members_(members)
        , SortedMembers_(sortedItems)
    {
    }

    TStructTypePtr TStructType::Create(ITypeFactory& factory, TStructType::TOwnedMembers members) {
        return Create(factory, Nothing(), members);
    }

    TStructTypePtr TStructType::Create(ITypeFactory& factory, TMaybe<TStringBuf> name, TStructType::TOwnedMembers members) {
        auto rawItems = TTempArray<TMember>(members.size());
        for (size_t i = 0; i < members.size(); ++i) {
            new (rawItems.Data() + i) TMember(members[i]);
        }
        return CreateRaw(factory, name, TArrayRef(rawItems.Data(), members.size()))->AsPtr();
    }

    const TStructType* TStructType::CreateRaw(ITypeFactory& factory, TStructType::TMembers members) {
        return CreateRaw(factory, Nothing(), members);
    }

    const TStructType* TStructType::CreateRaw(ITypeFactory& factory, TMaybe<TStringBuf> name, TStructType::TMembers members) {
        auto sortedMembersArray = TTempArray<size_t>(members.size());
        auto sortedMembers = TArrayRef(sortedMembersArray.Data(), members.size());
        MakeSortedMembers(members, sortedMembers);
        return TStructType({}, name, members, sortedMembers).Clone(FactoryInternal(factory));
    }

    void TStructType::MakeSortedMembers(TStructType::TMembers members, TArrayRef<size_t> sortedItems) {
        Y_ABORT_UNLESS(members.size() == sortedItems.size());

        for (size_t i = 0; i < members.size(); ++i) {
            sortedItems[i] = i;
        }

        Sort(sortedItems.begin(), sortedItems.end(), [members](size_t lhs, size_t rhs) {
            return members[lhs].GetName() < members[rhs].GetName();
        });

        for (size_t i = 1; i < members.size(); ++i) {
            if (members[sortedItems[i - 1]].GetName() == members[sortedItems[i]].GetName()) {
                ythrow TIllegalTypeException() << "duplicate struct item " << Quote(members[sortedItems[i]].GetName());
            }
        }
    }

    const TStructType* TStructType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TStructType* {
            auto hash = GetHashRaw();
            auto name = factory.AllocateStringMaybe(Name_);
            auto members = factory.NewArray<TMember>(Members_.size(), [this, &factory](TMember* item, size_t i) {
                auto name = factory.AllocateString(Members_[i].GetName());
                auto type = factory.Own(Members_[i].GetTypeRaw());
                new (item) TMember(name, type);
            });
            auto sortedItems = factory.AllocateArrayFor<size_t>(SortedMembers_.size());
            Copy(SortedMembers_.begin(), SortedMembers_.end(), sortedItems);
            return factory.New<TStructType>(hash, name, members, TArrayRef{sortedItems, SortedMembers_.size()});
        });
    }

    void TStructType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.FreeStringMaybe(Name_);
        factory.DeleteArray(Members_, [&factory](const TMember* item, size_t) {
            factory.FreeString(item->GetName());
            factory.Disown(item->GetTypeRaw());
        });
        factory.Free(const_cast<void*>(static_cast<const void*>(SortedMembers_.data())));
    }

    ui64 TStructType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Name_, hash);
        hash = ::Hash(Members_.size(), hash);
        for (auto& item : Members_) {
            hash = ::Hash(item.Hash(), hash);
        }
        return hash;
    }

    bool TStructType::HasMember(TStringBuf name) const noexcept {
        return GetMemberIndex(name) != -1;
    }

    const TStructType::TMember& TStructType::GetMember(TStringBuf name) const {
        auto idx = GetMemberIndex(name);
        if (idx == -1) {
            ythrow TItemNotFound() << "no item named " << Quote(name);
        } else {
            return Members_[idx];
        }
    }

    ssize_t TStructType::GetMemberIndex(TStringBuf name) const noexcept {
        auto it = LowerBound(SortedMembers_.begin(), SortedMembers_.end(), name, [this](size_t i, TStringBuf name) {
            return Members_[i].GetName() < name;
        });

        if (it == SortedMembers_.end() || Members_[*it].GetName() != name) {
            return -1;
        } else {
            return *it;
        }
    }

    TTupleType::TElement::TElement(const TType* type)
        : Type_(type)
    {
    }

    ui64 TTupleType::TElement::Hash() const {
        auto hash = 0x10001;
        hash = ::Hash(Type_->GetHash(), hash);
        return hash;
    }

    TTupleType::TOwnedElement::TOwnedElement(TTypePtr type)
        : Type_(std::move(type))
    {
    }

    TTupleType::TOwnedElement::operator TTupleType::TElement() const& {
        return TTupleType::TElement(Type_.Get());
    }

    TTupleType::TTupleType(TMaybe<ui64> hash, TMaybe<TStringBuf> name, TElements elements) noexcept
        : TType(hash, ETypeName::Tuple)
        , Name_(name)
        , Elements_(elements)
    {
    }

    TTupleTypePtr TTupleType::Create(ITypeFactory& factory, TTupleType::TOwnedElements elements) {
        return Create(factory, Nothing(), elements);
    }

    TTupleTypePtr TTupleType::Create(ITypeFactory& factory, TMaybe<TStringBuf> name, TTupleType::TOwnedElements elements) {
        auto rawItems = TTempArray<TElement>(elements.size());
        for (size_t i = 0; i < elements.size(); ++i) {
            new (rawItems.Data() + i) TElement(elements[i]);
        }
        return CreateRaw(factory, name, TArrayRef(rawItems.Data(), elements.size()))->AsPtr();
    }

    const TTupleType* TTupleType::CreateRaw(ITypeFactory& factory, TTupleType::TElements elements) {
        return CreateRaw(factory, Nothing(), elements);
    }

    const TTupleType* TTupleType::CreateRaw(ITypeFactory& factory, TMaybe<TStringBuf> name, TTupleType::TElements elements) {
        return TTupleType({}, name, elements).Clone(FactoryInternal(factory));
    }

    const TTupleType* TTupleType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TTupleType* {
            auto hash = GetHashRaw();
            auto name = factory.AllocateStringMaybe(Name_);
            auto elements = factory.NewArray<TElement>(Elements_.size(), [this, &factory](TElement* item, size_t i) {
                auto type = factory.Own(Elements_[i].GetTypeRaw());
                new (item) TElement(type);
            });
            return factory.New<TTupleType>(hash, name, elements);
        });
    }

    void TTupleType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.FreeStringMaybe(Name_);
        factory.DeleteArray(Elements_, [&factory](const TElement* item, size_t) {
            factory.Disown(item->GetTypeRaw());
        });
    }

    ui64 TTupleType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Name_, hash);
        hash = ::Hash(Elements_.size(), hash);
        for (auto& item : Elements_) {
            hash = ::Hash(item.Hash(), hash);
        }
        return hash;
    }

    TVariantType::TVariantType(TMaybe<ui64> hash, TMaybe<TStringBuf> name, const TType* inner) noexcept
        : TType(hash, ETypeName::Variant)
        , Name_(name)
        , Underlying_(inner)
    {
    }

    TVariantTypePtr TVariantType::Create(ITypeFactory& factory, TTypePtr inner) {
        return Create(factory, Nothing(), std::move(inner));
    }

    TVariantTypePtr TVariantType::Create(ITypeFactory& factory, TMaybe<TStringBuf> name, TTypePtr inner) {
        return CreateRaw(factory, name, inner.Get())->AsPtr();
    }

    const TVariantType* TVariantType::CreateRaw(ITypeFactory& factory, const TType* inner) {
        return CreateRaw(factory, Nothing(), inner);
    }

    const TVariantType* TVariantType::CreateRaw(ITypeFactory& factory, TMaybe<TStringBuf> name, const TType* inner) {
        inner->VisitRaw(TOverloaded{
            [&](const TStructType* s) {
                Y_ENSURE_EX(
                    !s->GetMembers().empty(),
                    TIllegalTypeException() << "variant should contain at least one alternative");
            },
            [&](const TTupleType* t) {
                Y_ENSURE_EX(
                    !t->GetElements().empty(),
                    TIllegalTypeException() << "variant should contain at least one alternative");
            },
            [](const TType* t) {
                ythrow TIllegalTypeException() << "variants can only contain structs and tuples, got "
                                               << t->GetTypeName() << " instead";
            }});

        return TVariantType({}, name, inner).Clone(FactoryInternal(factory));
    }

    const TVariantType* TVariantType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TVariantType* {
            auto hash = GetHashRaw();
            auto name = factory.AllocateStringMaybe(Name_);
            auto inner = factory.Own(Underlying_);
            return factory.New<TVariantType>(hash, name, inner);
        });
    }

    void TVariantType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.FreeStringMaybe(Name_);
        factory.Disown(Underlying_);
    }

    ui64 TVariantType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Name_, hash);
        hash = ::Hash(Underlying_->GetHash(), hash);
        return hash;
    }

    TTaggedType::TTaggedType(TMaybe<ui64> hash, const TType* item, TStringBuf tag) noexcept
        : TType(hash, ETypeName::Tagged)
        , Item_(item)
        , Tag_(tag)
    {
    }

    TTaggedTypePtr TTaggedType::Create(ITypeFactory& factory, TTypePtr type, TStringBuf tag) {
        return CreateRaw(factory, type.Get(), tag)->AsPtr();
    }

    const TTaggedType* TTaggedType::CreateRaw(ITypeFactory& factory, const TType* type, TStringBuf tag) {
        return TTaggedType({}, type, tag).Clone(FactoryInternal(factory));
    }

    const TTaggedType* TTaggedType::Clone(ITypeFactoryInternal& factory) const noexcept {
        return Cached(this, factory, [this, &factory]() -> const TTaggedType* {
            auto hash = GetHashRaw();
            auto item = factory.Own(Item_);
            auto tag = factory.AllocateString(Tag_);
            return factory.New<TTaggedType>(hash, item, tag);
        });
    }

    void TTaggedType::Drop(ITypeFactoryInternal& factory) noexcept {
        factory.FreeString(Tag_);
        factory.Disown(Item_);
    }

    ui64 TTaggedType::CalculateHash() const noexcept {
        auto hash = TType::CalculateHash();
        hash = ::Hash(Tag_, hash);
        hash = ::Hash(Item_->GetHash(), hash);
        return hash;
    }

    TVoidTypePtr Void() {
        return NPrivate::GetDefaultHeapFactory()->Void();
    }

    TNullTypePtr Null() {
        return NPrivate::GetDefaultHeapFactory()->Null();
    }

    TBoolTypePtr Bool() {
        return NPrivate::GetDefaultHeapFactory()->Bool();
    }

    TInt8TypePtr Int8() {
        return NPrivate::GetDefaultHeapFactory()->Int8();
    }

    TInt16TypePtr Int16() {
        return NPrivate::GetDefaultHeapFactory()->Int16();
    }

    TInt32TypePtr Int32() {
        return NPrivate::GetDefaultHeapFactory()->Int32();
    }

    TInt64TypePtr Int64() {
        return NPrivate::GetDefaultHeapFactory()->Int64();
    }

    TUint8TypePtr Uint8() {
        return NPrivate::GetDefaultHeapFactory()->Uint8();
    }

    TUint16TypePtr Uint16() {
        return NPrivate::GetDefaultHeapFactory()->Uint16();
    }

    TUint32TypePtr Uint32() {
        return NPrivate::GetDefaultHeapFactory()->Uint32();
    }

    TUint64TypePtr Uint64() {
        return NPrivate::GetDefaultHeapFactory()->Uint64();
    }

    TFloatTypePtr Float() {
        return NPrivate::GetDefaultHeapFactory()->Float();
    }

    TDoubleTypePtr Double() {
        return NPrivate::GetDefaultHeapFactory()->Double();
    }

    TStringTypePtr String() {
        return NPrivate::GetDefaultHeapFactory()->String();
    }

    TUtf8TypePtr Utf8() {
        return NPrivate::GetDefaultHeapFactory()->Utf8();
    }

    TDateTypePtr Date() {
        return NPrivate::GetDefaultHeapFactory()->Date();
    }

    TDatetimeTypePtr Datetime() {
        return NPrivate::GetDefaultHeapFactory()->Datetime();
    }

    TTimestampTypePtr Timestamp() {
        return NPrivate::GetDefaultHeapFactory()->Timestamp();
    }

    TTzDateTypePtr TzDate() {
        return NPrivate::GetDefaultHeapFactory()->TzDate();
    }

    TTzDatetimeTypePtr TzDatetime() {
        return NPrivate::GetDefaultHeapFactory()->TzDatetime();
    }

    TTzTimestampTypePtr TzTimestamp() {
        return NPrivate::GetDefaultHeapFactory()->TzTimestamp();
    }

    TIntervalTypePtr Interval() {
        return NPrivate::GetDefaultHeapFactory()->Interval();
    }

    TDecimalTypePtr Decimal(ui8 precision, ui8 scale) {
        return NPrivate::GetDefaultHeapFactory()->Decimal(precision, scale);
    }

    TJsonTypePtr Json() {
        return NPrivate::GetDefaultHeapFactory()->Json();
    }

    TYsonTypePtr Yson() {
        return NPrivate::GetDefaultHeapFactory()->Yson();
    }

    TUuidTypePtr Uuid() {
        return NPrivate::GetDefaultHeapFactory()->Uuid();
    }

    TDate32TypePtr Date32() {
        return NPrivate::GetDefaultHeapFactory()->Date32();
    }

    TDatetime64TypePtr Datetime64() {
        return NPrivate::GetDefaultHeapFactory()->Datetime64();
    }

    TTimestamp64TypePtr Timestamp64() {
        return NPrivate::GetDefaultHeapFactory()->Timestamp64();
    }

    TInterval64TypePtr Interval64() {
        return NPrivate::GetDefaultHeapFactory()->Interval64();
    }

    TOptionalTypePtr Optional(TTypePtr item) {
        return NPrivate::GetDefaultHeapFactory()->Optional(std::move(item));
    }

    TListTypePtr List(TTypePtr item) {
        return NPrivate::GetDefaultHeapFactory()->List(std::move(item));
    }

    TDictTypePtr Dict(TTypePtr key, TTypePtr value) {
        return NPrivate::GetDefaultHeapFactory()->Dict(std::move(key), std::move(value));
    }

    TStructTypePtr Struct(TStructType::TOwnedMembers members) {
        return NPrivate::GetDefaultHeapFactory()->Struct(members);
    }

    TStructTypePtr Struct(TMaybe<TStringBuf> name, TStructType::TOwnedMembers members) {
        return NPrivate::GetDefaultHeapFactory()->Struct(name, members);
    }

    TTupleTypePtr Tuple(TTupleType::TOwnedElements elements) {
        return NPrivate::GetDefaultHeapFactory()->Tuple(elements);
    }

    TTupleTypePtr Tuple(TMaybe<TStringBuf> name, TTupleType::TOwnedElements elements) {
        return NPrivate::GetDefaultHeapFactory()->Tuple(name, elements);
    }

    TVariantTypePtr Variant(TTypePtr underlying) {
        return NPrivate::GetDefaultHeapFactory()->Variant(std::move(underlying));
    }

    TVariantTypePtr Variant(TMaybe<TStringBuf> name, TTypePtr underlying) {
        return NPrivate::GetDefaultHeapFactory()->Variant(name, std::move(underlying));
    }

    TTaggedTypePtr Tagged(TTypePtr type, TStringBuf tag) {
        return NPrivate::GetDefaultHeapFactory()->Tagged(std::move(type), tag);
    }
}

Y_DECLARE_OUT_SPEC(, NTi::TType, o, v) {
    v.VisitRaw([&o](const auto* v) { o << *v; });
}

Y_DECLARE_OUT_SPEC(, NTi::TPrimitiveType, o, v) {
    v.VisitPrimitiveRaw([&o](const auto* v) { o << *v; });
}

Y_DECLARE_OUT_SPEC(, NTi::TVoidType, o, v) {
    Y_UNUSED(v);
    o << "Void";
}

Y_DECLARE_OUT_SPEC(, NTi::TNullType, o, v) {
    Y_UNUSED(v);
    o << "Null";
}

Y_DECLARE_OUT_SPEC(, NTi::TBoolType, o, v) {
    Y_UNUSED(v);
    o << "Bool";
}

Y_DECLARE_OUT_SPEC(, NTi::TInt8Type, o, v) {
    Y_UNUSED(v);
    o << "Int8";
}

Y_DECLARE_OUT_SPEC(, NTi::TInt16Type, o, v) {
    Y_UNUSED(v);
    o << "Int16";
}

Y_DECLARE_OUT_SPEC(, NTi::TInt32Type, o, v) {
    Y_UNUSED(v);
    o << "Int32";
}

Y_DECLARE_OUT_SPEC(, NTi::TInt64Type, o, v) {
    Y_UNUSED(v);
    o << "Int64";
}

Y_DECLARE_OUT_SPEC(, NTi::TUint8Type, o, v) {
    Y_UNUSED(v);
    o << "Uint8";
}

Y_DECLARE_OUT_SPEC(, NTi::TUint16Type, o, v) {
    Y_UNUSED(v);
    o << "Uint16";
}

Y_DECLARE_OUT_SPEC(, NTi::TUint32Type, o, v) {
    Y_UNUSED(v);
    o << "Uint32";
}

Y_DECLARE_OUT_SPEC(, NTi::TUint64Type, o, v) {
    Y_UNUSED(v);
    o << "Uint64";
}

Y_DECLARE_OUT_SPEC(, NTi::TFloatType, o, v) {
    Y_UNUSED(v);
    o << "Float";
}

Y_DECLARE_OUT_SPEC(, NTi::TDoubleType, o, v) {
    Y_UNUSED(v);
    o << "Double";
}

Y_DECLARE_OUT_SPEC(, NTi::TStringType, o, v) {
    Y_UNUSED(v);
    o << "String";
}

Y_DECLARE_OUT_SPEC(, NTi::TUtf8Type, o, v) {
    Y_UNUSED(v);
    o << "Utf8";
}

Y_DECLARE_OUT_SPEC(, NTi::TDateType, o, v) {
    Y_UNUSED(v);
    o << "Date";
}

Y_DECLARE_OUT_SPEC(, NTi::TDatetimeType, o, v) {
    Y_UNUSED(v);
    o << "Datetime";
}

Y_DECLARE_OUT_SPEC(, NTi::TTimestampType, o, v) {
    Y_UNUSED(v);
    o << "Timestamp";
}

Y_DECLARE_OUT_SPEC(, NTi::TTzDateType, o, v) {
    Y_UNUSED(v);
    o << "TzDate";
}

Y_DECLARE_OUT_SPEC(, NTi::TTzDatetimeType, o, v) {
    Y_UNUSED(v);
    o << "TzDatetime";
}

Y_DECLARE_OUT_SPEC(, NTi::TTzTimestampType, o, v) {
    Y_UNUSED(v);
    o << "TzTimestamp";
}

Y_DECLARE_OUT_SPEC(, NTi::TIntervalType, o, v) {
    Y_UNUSED(v);
    o << "Interval";
}

Y_DECLARE_OUT_SPEC(, NTi::TDecimalType, o, v) {
    o << "Decimal(" << (i32)v.GetPrecision() << ", " << (i32)v.GetScale() << ')';
}

Y_DECLARE_OUT_SPEC(, NTi::TJsonType, o, v) {
    Y_UNUSED(v);
    o << "Json";
}

Y_DECLARE_OUT_SPEC(, NTi::TYsonType, o, v) {
    Y_UNUSED(v);
    o << "Yson";
}

Y_DECLARE_OUT_SPEC(, NTi::TUuidType, o, v) {
    Y_UNUSED(v);
    o << "Uuid";
}

Y_DECLARE_OUT_SPEC(, NTi::TDate32Type, o, v) {
    Y_UNUSED(v);
    o << "Date32";
}

Y_DECLARE_OUT_SPEC(, NTi::TDatetime64Type, o, v) {
    Y_UNUSED(v);
    o << "Datetime64";
}

Y_DECLARE_OUT_SPEC(, NTi::TTimestamp64Type, o, v) {
    Y_UNUSED(v);
    o << "Timestamp64";
}

Y_DECLARE_OUT_SPEC(, NTi::TInterval64Type, o, v) {
    Y_UNUSED(v);
    o << "Interval64";
}

Y_DECLARE_OUT_SPEC(, NTi::TOptionalType, o, v) {
    o << "Optional<" << *v.GetItemTypeRaw() << ">";
}

Y_DECLARE_OUT_SPEC(, NTi::TListType, o, v) {
    o << "List<" << *v.GetItemTypeRaw() << ">";
}

Y_DECLARE_OUT_SPEC(, NTi::TDictType, o, v) {
    o << "Dict<" << *v.GetKeyTypeRaw() << ", " << *v.GetValueTypeRaw() << ">";
}

Y_DECLARE_OUT_SPEC(, NTi::TStructType, o, v) {
    o << "Struct";

    if (v.GetName().Defined()) {
        o << "[" << Quote(*v.GetName()) << "]";
    }

    o << "<";
    const char* sep = "";
    for (auto& item : v.GetMembers()) {
        o << sep << Quote(item.GetName()) << ": " << *item.GetTypeRaw();
        sep = ", ";
    }
    o << ">";
}

Y_DECLARE_OUT_SPEC(, NTi::TTupleType, o, v) {
    o << "Tuple";

    if (v.GetName().Defined()) {
        o << "[" << Quote(*v.GetName()) << "]";
    }

    o << "<";
    const char* sep = "";
    for (auto& item : v.GetElements()) {
        o << sep << *item.GetTypeRaw();
        sep = ", ";
    }
    o << ">";
}

Y_DECLARE_OUT_SPEC(, NTi::TVariantType, o, v) {
    o << "Variant";

    if (v.GetName().Defined()) {
        o << "[" << Quote(*v.GetName()) << "]";
    }

    o << "<";
    v.VisitUnderlyingRaw(
        TOverloaded{
            [&o](const NTi::TStructType* s) {
                const char* sep = "";
                for (auto& item : s->GetMembers()) {
                    o << sep << Quote(item.GetName()) << ": " << *item.GetTypeRaw();
                    sep = ", ";
                }
            },
            [&o](const NTi::TTupleType* t) {
                const char* sep = "";
                for (auto& item : t->GetElements()) {
                    o << sep << *item.GetTypeRaw();
                    sep = ", ";
                }
            }});
    o << ">";
}

Y_DECLARE_OUT_SPEC(, NTi::TTaggedType, o, v) {
    o << "Tagged<" << *v.GetItemTypeRaw() << ", " << Quote(v.GetTag()) << ">";
}

static_assert(std::is_trivially_destructible_v<NTi::TVoidType>);
static_assert(std::is_trivially_destructible_v<NTi::TNullType>);
static_assert(std::is_trivially_destructible_v<NTi::TPrimitiveType>);
static_assert(std::is_trivially_destructible_v<NTi::TBoolType>);
static_assert(std::is_trivially_destructible_v<NTi::TInt8Type>);
static_assert(std::is_trivially_destructible_v<NTi::TInt16Type>);
static_assert(std::is_trivially_destructible_v<NTi::TInt32Type>);
static_assert(std::is_trivially_destructible_v<NTi::TInt64Type>);
static_assert(std::is_trivially_destructible_v<NTi::TUint8Type>);
static_assert(std::is_trivially_destructible_v<NTi::TUint16Type>);
static_assert(std::is_trivially_destructible_v<NTi::TUint32Type>);
static_assert(std::is_trivially_destructible_v<NTi::TUint64Type>);
static_assert(std::is_trivially_destructible_v<NTi::TFloatType>);
static_assert(std::is_trivially_destructible_v<NTi::TDoubleType>);
static_assert(std::is_trivially_destructible_v<NTi::TStringType>);
static_assert(std::is_trivially_destructible_v<NTi::TUtf8Type>);
static_assert(std::is_trivially_destructible_v<NTi::TDateType>);
static_assert(std::is_trivially_destructible_v<NTi::TDatetimeType>);
static_assert(std::is_trivially_destructible_v<NTi::TTimestampType>);
static_assert(std::is_trivially_destructible_v<NTi::TTzDateType>);
static_assert(std::is_trivially_destructible_v<NTi::TTzDatetimeType>);
static_assert(std::is_trivially_destructible_v<NTi::TTzTimestampType>);
static_assert(std::is_trivially_destructible_v<NTi::TIntervalType>);
static_assert(std::is_trivially_destructible_v<NTi::TDecimalType>);
static_assert(std::is_trivially_destructible_v<NTi::TJsonType>);
static_assert(std::is_trivially_destructible_v<NTi::TYsonType>);
static_assert(std::is_trivially_destructible_v<NTi::TUuidType>);
static_assert(std::is_trivially_destructible_v<NTi::TDate32Type>);
static_assert(std::is_trivially_destructible_v<NTi::TDatetime64Type>);
static_assert(std::is_trivially_destructible_v<NTi::TTimestamp64Type>);
static_assert(std::is_trivially_destructible_v<NTi::TInterval64Type>);
static_assert(std::is_trivially_destructible_v<NTi::TOptionalType>);
static_assert(std::is_trivially_destructible_v<NTi::TListType>);
static_assert(std::is_trivially_destructible_v<NTi::TDictType>);
static_assert(std::is_trivially_destructible_v<NTi::TStructType>);
static_assert(std::is_trivially_destructible_v<NTi::TStructType::TMember>);
static_assert(std::is_trivially_destructible_v<NTi::TTupleType>);
static_assert(std::is_trivially_destructible_v<NTi::TTupleType::TElement>);
static_assert(std::is_trivially_destructible_v<NTi::TVariantType>);
static_assert(std::is_trivially_destructible_v<NTi::TTaggedType>);
