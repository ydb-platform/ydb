#include "type_factory.h"

#include "type.h"
#include "type_equivalence.h"

#include <util/memory/pool.h>
#include <util/generic/hash_set.h>

#include <cstdlib>

namespace NTi {
    TVoidTypePtr ITypeFactory::Void() {
        return TVoidType::Instance();
    }

    const TVoidType* IPoolTypeFactory::VoidRaw() {
        return TVoidType::InstanceRaw();
    }

    TNullTypePtr ITypeFactory::Null() {
        return TNullType::Instance();
    }

    const TNullType* IPoolTypeFactory::NullRaw() {
        return TNullType::InstanceRaw();
    }

    TBoolTypePtr ITypeFactory::Bool() {
        return TBoolType::Instance();
    }

    const TBoolType* IPoolTypeFactory::BoolRaw() {
        return TBoolType::InstanceRaw();
    }

    TInt8TypePtr ITypeFactory::Int8() {
        return TInt8Type::Instance();
    }

    const TInt8Type* IPoolTypeFactory::Int8Raw() {
        return TInt8Type::InstanceRaw();
    }

    TInt16TypePtr ITypeFactory::Int16() {
        return TInt16Type::Instance();
    }

    const TInt16Type* IPoolTypeFactory::Int16Raw() {
        return TInt16Type::InstanceRaw();
    }

    TInt32TypePtr ITypeFactory::Int32() {
        return TInt32Type::Instance();
    }

    const TInt32Type* IPoolTypeFactory::Int32Raw() {
        return TInt32Type::InstanceRaw();
    }

    TInt64TypePtr ITypeFactory::Int64() {
        return TInt64Type::Instance();
    }

    const TInt64Type* IPoolTypeFactory::Int64Raw() {
        return TInt64Type::InstanceRaw();
    }

    TUint8TypePtr ITypeFactory::Uint8() {
        return TUint8Type::Instance();
    }

    const TUint8Type* IPoolTypeFactory::Uint8Raw() {
        return TUint8Type::InstanceRaw();
    }

    TUint16TypePtr ITypeFactory::Uint16() {
        return TUint16Type::Instance();
    }

    const TUint16Type* IPoolTypeFactory::Uint16Raw() {
        return TUint16Type::InstanceRaw();
    }

    TUint32TypePtr ITypeFactory::Uint32() {
        return TUint32Type::Instance();
    }

    const TUint32Type* IPoolTypeFactory::Uint32Raw() {
        return TUint32Type::InstanceRaw();
    }

    TUint64TypePtr ITypeFactory::Uint64() {
        return TUint64Type::Instance();
    }

    const TUint64Type* IPoolTypeFactory::Uint64Raw() {
        return TUint64Type::InstanceRaw();
    }

    TFloatTypePtr ITypeFactory::Float() {
        return TFloatType::Instance();
    }

    const TFloatType* IPoolTypeFactory::FloatRaw() {
        return TFloatType::InstanceRaw();
    }

    TDoubleTypePtr ITypeFactory::Double() {
        return TDoubleType::Instance();
    }

    const TDoubleType* IPoolTypeFactory::DoubleRaw() {
        return TDoubleType::InstanceRaw();
    }

    TStringTypePtr ITypeFactory::String() {
        return TStringType::Instance();
    }

    const TStringType* IPoolTypeFactory::StringRaw() {
        return TStringType::InstanceRaw();
    }

    TUtf8TypePtr ITypeFactory::Utf8() {
        return TUtf8Type::Instance();
    }

    const TUtf8Type* IPoolTypeFactory::Utf8Raw() {
        return TUtf8Type::InstanceRaw();
    }

    TDateTypePtr ITypeFactory::Date() {
        return TDateType::Instance();
    }

    const TDateType* IPoolTypeFactory::DateRaw() {
        return TDateType::InstanceRaw();
    }

    TDatetimeTypePtr ITypeFactory::Datetime() {
        return TDatetimeType::Instance();
    }

    const TDatetimeType* IPoolTypeFactory::DatetimeRaw() {
        return TDatetimeType::InstanceRaw();
    }

    TTimestampTypePtr ITypeFactory::Timestamp() {
        return TTimestampType::Instance();
    }

    const TTimestampType* IPoolTypeFactory::TimestampRaw() {
        return TTimestampType::InstanceRaw();
    }

    TTzDateTypePtr ITypeFactory::TzDate() {
        return TTzDateType::Instance();
    }

    const TTzDateType* IPoolTypeFactory::TzDateRaw() {
        return TTzDateType::InstanceRaw();
    }

    TTzDatetimeTypePtr ITypeFactory::TzDatetime() {
        return TTzDatetimeType::Instance();
    }

    const TTzDatetimeType* IPoolTypeFactory::TzDatetimeRaw() {
        return TTzDatetimeType::InstanceRaw();
    }

    TTzTimestampTypePtr ITypeFactory::TzTimestamp() {
        return TTzTimestampType::Instance();
    }

    const TTzTimestampType* IPoolTypeFactory::TzTimestampRaw() {
        return TTzTimestampType::InstanceRaw();
    }

    TIntervalTypePtr ITypeFactory::Interval() {
        return TIntervalType::Instance();
    }

    const TIntervalType* IPoolTypeFactory::IntervalRaw() {
        return TIntervalType::InstanceRaw();
    }

    TDecimalTypePtr ITypeFactory::Decimal(ui8 precision, ui8 scale) {
        return TDecimalType::Create(*this, precision, scale);
    }

    const TDecimalType* IPoolTypeFactory::DecimalRaw(ui8 precision, ui8 scale) {
        return TDecimalType::CreateRaw(*this, precision, scale);
    }

    TJsonTypePtr ITypeFactory::Json() {
        return TJsonType::Instance();
    }

    const TJsonType* IPoolTypeFactory::JsonRaw() {
        return TJsonType::InstanceRaw();
    }

    TYsonTypePtr ITypeFactory::Yson() {
        return TYsonType::Instance();
    }

    const TYsonType* IPoolTypeFactory::YsonRaw() {
        return TYsonType::InstanceRaw();
    }

    TUuidTypePtr ITypeFactory::Uuid() {
        return TUuidType::Instance();
    }

    const TUuidType* IPoolTypeFactory::UuidRaw() {
        return TUuidType::InstanceRaw();
    }

    TDate32TypePtr ITypeFactory::Date32() {
        return TDate32Type::Instance();
    }

    const TDate32Type* IPoolTypeFactory::Date32Raw() {
        return TDate32Type::InstanceRaw();
    }

    TDatetime64TypePtr ITypeFactory::Datetime64() {
        return TDatetime64Type::Instance();
    }

    const TDatetime64Type* IPoolTypeFactory::Datetime64Raw() {
        return TDatetime64Type::InstanceRaw();
    }

    TTimestamp64TypePtr ITypeFactory::Timestamp64() {
        return TTimestamp64Type::Instance();
    }

    const TTimestamp64Type* IPoolTypeFactory::Timestamp64Raw() {
        return TTimestamp64Type::InstanceRaw();
    }

    TInterval64TypePtr ITypeFactory::Interval64() {
        return TInterval64Type::Instance();
    }

    const TInterval64Type* IPoolTypeFactory::Interval64Raw() {
        return TInterval64Type::InstanceRaw();
    }

    TOptionalTypePtr ITypeFactory::Optional(TTypePtr item) {
        return TOptionalType::Create(*this, std::move(item));
    }

    const TOptionalType* IPoolTypeFactory::OptionalRaw(const TType* item) {
        return TOptionalType::CreateRaw(*this, item);
    }

    TListTypePtr ITypeFactory::List(TTypePtr item) {
        return TListType::Create(*this, std::move(item));
    }

    const TListType* IPoolTypeFactory::ListRaw(const TType* item) {
        return TListType::CreateRaw(*this, item);
    }

    TDictTypePtr ITypeFactory::Dict(TTypePtr key, TTypePtr value) {
        return TDictType::Create(*this, std::move(key), std::move(value));
    }

    const TDictType* IPoolTypeFactory::DictRaw(const TType* key, const TType* value) {
        return TDictType::CreateRaw(*this, key, value);
    }

    TStructTypePtr ITypeFactory::Struct(TStructType::TOwnedMembers items) {
        return TStructType::Create(*this, items);
    }

    TStructTypePtr ITypeFactory::Struct(TMaybe<TStringBuf> name, TStructType::TOwnedMembers items) {
        return TStructType::Create(*this, name, items);
    }

    const TStructType* IPoolTypeFactory::StructRaw(TStructType::TMembers items) {
        return TStructType::CreateRaw(*this, items);
    }

    const TStructType* IPoolTypeFactory::StructRaw(TMaybe<TStringBuf> name, TStructType::TMembers items) {
        return TStructType::CreateRaw(*this, name, items);
    }

    TTupleTypePtr ITypeFactory::Tuple(TTupleType::TOwnedElements items) {
        return TTupleType::Create(*this, items);
    }

    TTupleTypePtr ITypeFactory::Tuple(TMaybe<TStringBuf> name, TTupleType::TOwnedElements items) {
        return TTupleType::Create(*this, name, items);
    }

    const TTupleType* IPoolTypeFactory::TupleRaw(TTupleType::TElements items) {
        return TTupleType::CreateRaw(*this, items);
    }

    const TTupleType* IPoolTypeFactory::TupleRaw(TMaybe<TStringBuf> name, TTupleType::TElements items) {
        return TTupleType::CreateRaw(*this, name, items);
    }

    TVariantTypePtr ITypeFactory::Variant(TTypePtr inner) {
        return TVariantType::Create(*this, std::move(inner));
    }

    TVariantTypePtr ITypeFactory::Variant(TMaybe<TStringBuf> name, TTypePtr inner) {
        return TVariantType::Create(*this, name, std::move(inner));
    }

    const TVariantType* IPoolTypeFactory::VariantRaw(const TType* inner) {
        return TVariantType::CreateRaw(*this, inner);
    }

    const TVariantType* IPoolTypeFactory::VariantRaw(TMaybe<TStringBuf> name, const TType* inner) {
        return TVariantType::CreateRaw(*this, name, inner);
    }

    TTaggedTypePtr ITypeFactory::Tagged(TTypePtr type, TStringBuf tag)  {
        return TTaggedType::Create(*this, std::move(type), tag);
    }

    const TTaggedType* IPoolTypeFactory::TaggedRaw(const TType* type, TStringBuf tag) {
        return TTaggedType::CreateRaw(*this, type, tag);
    }

    namespace {
        class TPoolFactory: public NTi::IPoolTypeFactory {
        public:
            TPoolFactory(size_t initial, TMemoryPool::IGrowPolicy* grow, IAllocator* alloc, TMemoryPool::TOptions options)
                : Pool_(initial, grow, alloc, options)
            {
            }

        public:
            void* Allocate(size_t size, size_t align) noexcept override {
                return Pool_.Allocate(size, align);
            }

            void Free(void* data) noexcept override {
                Y_UNUSED(data);
            }

        protected:
            const NTi::TType* LookupCache(const NTi::TType* type) noexcept override {
                Y_UNUSED(type);
                return nullptr;
            }

            void SaveCache(const NTi::TType* type) noexcept override {
                Y_UNUSED(type);
            }

            void Ref() noexcept override {
                Counter_.Inc();
            }

            void UnRef() noexcept override {
                if (Counter_.Dec() == 0) {
                    delete this;
                }
            }

            void DecRef() noexcept override {
                if (Counter_.Dec() == 0) {
                    Y_ABORT("DecRef is not supposed to drop");
                }
            }

            long RefCount() const noexcept override {
                return Counter_.Val();
            }

            void RefType(NTi::TType* type) noexcept override {
                Y_UNUSED(type);
            }

            void UnRefType(NTi::TType* type) noexcept override {
                Y_UNUSED(type);
            }

            void DecRefType(NTi::TType* type) noexcept override {
                Y_UNUSED(type);
            }

            long RefCountType(const NTi::TType* type) const noexcept override {
                Y_UNUSED(type);
                return RefCount();
            }

        public:
            size_t Available() const noexcept override {
                return Pool_.Available();
            }

            size_t MemoryAllocated() const noexcept override {
                return Pool_.MemoryAllocated();
            }

            size_t MemoryWaste() const noexcept override {
                return Pool_.MemoryWaste();
            }

        private:
            TAtomicCounter Counter_;
            TMemoryPool Pool_;
        };

        class TPoolFactoryDedup: public TPoolFactory {
        public:
            using TPoolFactory::TPoolFactory;

        public:
            const NTi::TType* LookupCache(const NTi::TType* type) noexcept override {
                if (auto it = Cache_.find(type); it != Cache_.end()) {
                    return *it;
                } else {
                    return nullptr;
                }
            }

            void SaveCache(const NTi::TType* type) noexcept override {
                Cache_.insert(type);
            }

        protected:
            TStringBuf AllocateString(TStringBuf str) noexcept override {
                if (str.empty()) {
                    return TStringBuf(); // `str` could still point somewhere whereas empty strbuf points to NULL
                }

                if (auto it = StringCache_.find(str); it != StringCache_.end()) {
                    return *it;
                } else {
                    return *StringCache_.insert(it, ITypeFactoryInternal::AllocateString(str));
                }
            }

        private:
            THashSet<const NTi::TType*, NTi::NEq::TStrictlyEqualHash, NTi::NEq::TStrictlyEqual> Cache_;
            THashSet<TStringBuf> StringCache_;
        };

        class THeapFactory: public NTi::ITypeFactory {
        public:
            void* Allocate(size_t size, size_t align) noexcept override {
                Y_UNUSED(align);
                return malloc(size);
            }

            void Free(void* data) noexcept override {
                free(data);
            }

        protected:
            const NTi::TType* LookupCache(const NTi::TType* type) noexcept override {
                Y_UNUSED(type);
                return nullptr;
            }

            void SaveCache(const NTi::TType* type) noexcept override {
                Y_UNUSED(type);
            }

            void Ref() noexcept override {
                // nothing
            }

            void UnRef() noexcept override {
                // nothing
            }

            void DecRef() noexcept override {
                // nothing
            }

            long RefCount() const noexcept override {
                return 0;
            }

            void RefType(NTi::TType* type) noexcept override {
                Y_UNUSED(type);
                Y_ABORT("not supposed to be called");
            }

            void UnRefType(NTi::TType* type) noexcept override {
                Y_UNUSED(type);
                Y_ABORT("not supposed to be called");
            }

            void DecRefType(NTi::TType* type) noexcept override {
                Y_UNUSED(type);
                Y_ABORT("not supposed to be called");
            }

            long RefCountType(const NTi::TType* type) const noexcept override {
                Y_UNUSED(type);
                Y_ABORT("not supposed to be called");
            }
        };

        THeapFactory HEAP_FACTORY;
    }

    IPoolTypeFactoryPtr PoolFactory(bool deduplicate, size_t initial, TMemoryPool::IGrowPolicy* grow, IAllocator* alloc, TMemoryPool::TOptions options) {
        if (deduplicate) {
            return new TPoolFactoryDedup(initial, grow, alloc, options);
        } else {
            return new TPoolFactory(initial, grow, alloc, options);
        }
    }

    namespace NPrivate {
        ITypeFactory* GetDefaultHeapFactory() {
            return &HEAP_FACTORY;
        }
    }

    ITypeFactoryPtr HeapFactory() {
        return NPrivate::GetDefaultHeapFactory();
    }
}
