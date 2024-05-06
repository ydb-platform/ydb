#pragma once

//! @file type.h
//!
//! Hierarchy of classes that represent types.
#include "fwd.h"

#include "error.h"
#include "type_list.h"

#include <atomic>
#include <util/generic/array_ref.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NTi {
    /// Represents a single type.
    ///
    /// Create instances of types using type factory (see `NTi::ITypeFactory`).
    ///
    /// Introspect them using associated methods and functions that work with `ETypeName`.
    ///
    /// Pattern-match them using the `Visit` method.
    ///
    /// Serialize and deserialize them using functions from `NTi::NIo`.
    class TType {
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;
        template <typename T>
        friend class ::TDefaultIntrusivePtrOps;

    public:
        TTypePtr AsPtr() const noexcept {
            return const_cast<TType*>(this);
        }

    protected:
        explicit TType(TMaybe<ui64> hash, ETypeName typeName) noexcept;

    protected:
        /// Calculate hash for this type. This function is lazily called by `GetHash`.
        ///
        /// Note: this function is not marked as `virtual` because we use our own dispatch via `Visit`.
        ui64 CalculateHash() const noexcept;

    public:
        /// Get hash of this type. Hashes follow the 'strict equivalence' relation (see `type_equivalence.h`).
        ui64 GetHash() const;

        /// Get hash of this type. If hash is not calculated, returns nothing.
        TMaybe<ui64> GetHashRaw() const noexcept;

        /// Get name of this type as a `NTi::ETypeName` enumerator.
        ETypeName GetTypeName() const noexcept {
            return TypeName_;
        }

        /// @name Simple type downcast functions
        ///
        /// Check if type is of the given subclass and convert between subclasses.
        /// Conversions panic if downcasting into an incompatible type.
        ///
        /// @{
        inline bool IsVoid() const noexcept;
        inline TVoidTypePtr AsVoid() const noexcept;
        inline const TVoidType* AsVoidRaw() const noexcept;

        inline bool IsNull() const noexcept;
        inline TNullTypePtr AsNull() const noexcept;
        inline const TNullType* AsNullRaw() const noexcept;

        inline bool IsPrimitive() const noexcept;
        inline TPrimitiveTypePtr AsPrimitive() const noexcept;
        inline const TPrimitiveType* AsPrimitiveRaw() const noexcept;

        inline bool IsBool() const noexcept;
        inline TBoolTypePtr AsBool() const noexcept;
        inline const TBoolType* AsBoolRaw() const noexcept;

        inline bool IsInt8() const noexcept;
        inline TInt8TypePtr AsInt8() const noexcept;
        inline const TInt8Type* AsInt8Raw() const noexcept;

        inline bool IsInt16() const noexcept;
        inline TInt16TypePtr AsInt16() const noexcept;
        inline const TInt16Type* AsInt16Raw() const noexcept;

        inline bool IsInt32() const noexcept;
        inline TInt32TypePtr AsInt32() const noexcept;
        inline const TInt32Type* AsInt32Raw() const noexcept;

        inline bool IsInt64() const noexcept;
        inline TInt64TypePtr AsInt64() const noexcept;
        inline const TInt64Type* AsInt64Raw() const noexcept;

        inline bool IsUint8() const noexcept;
        inline TUint8TypePtr AsUint8() const noexcept;
        inline const TUint8Type* AsUint8Raw() const noexcept;

        inline bool IsUint16() const noexcept;
        inline TUint16TypePtr AsUint16() const noexcept;
        inline const TUint16Type* AsUint16Raw() const noexcept;

        inline bool IsUint32() const noexcept;
        inline TUint32TypePtr AsUint32() const noexcept;
        inline const TUint32Type* AsUint32Raw() const noexcept;

        inline bool IsUint64() const noexcept;
        inline TUint64TypePtr AsUint64() const noexcept;
        inline const TUint64Type* AsUint64Raw() const noexcept;

        inline bool IsFloat() const noexcept;
        inline TFloatTypePtr AsFloat() const noexcept;
        inline const TFloatType* AsFloatRaw() const noexcept;

        inline bool IsDouble() const noexcept;
        inline TDoubleTypePtr AsDouble() const noexcept;
        inline const TDoubleType* AsDoubleRaw() const noexcept;

        inline bool IsString() const noexcept;
        inline TStringTypePtr AsString() const noexcept;
        inline const TStringType* AsStringRaw() const noexcept;

        inline bool IsUtf8() const noexcept;
        inline TUtf8TypePtr AsUtf8() const noexcept;
        inline const TUtf8Type* AsUtf8Raw() const noexcept;

        inline bool IsDate() const noexcept;
        inline TDateTypePtr AsDate() const noexcept;
        inline const TDateType* AsDateRaw() const noexcept;

        inline bool IsDatetime() const noexcept;
        inline TDatetimeTypePtr AsDatetime() const noexcept;
        inline const TDatetimeType* AsDatetimeRaw() const noexcept;

        inline bool IsTimestamp() const noexcept;
        inline TTimestampTypePtr AsTimestamp() const noexcept;
        inline const TTimestampType* AsTimestampRaw() const noexcept;

        inline bool IsTzDate() const noexcept;
        inline TTzDateTypePtr AsTzDate() const noexcept;
        inline const TTzDateType* AsTzDateRaw() const noexcept;

        inline bool IsTzDatetime() const noexcept;
        inline TTzDatetimeTypePtr AsTzDatetime() const noexcept;
        inline const TTzDatetimeType* AsTzDatetimeRaw() const noexcept;

        inline bool IsTzTimestamp() const noexcept;
        inline TTzTimestampTypePtr AsTzTimestamp() const noexcept;
        inline const TTzTimestampType* AsTzTimestampRaw() const noexcept;

        inline bool IsInterval() const noexcept;
        inline TIntervalTypePtr AsInterval() const noexcept;
        inline const TIntervalType* AsIntervalRaw() const noexcept;

        inline bool IsDecimal() const noexcept;
        inline TDecimalTypePtr AsDecimal() const noexcept;
        inline const TDecimalType* AsDecimalRaw() const noexcept;

        inline bool IsJson() const noexcept;
        inline TJsonTypePtr AsJson() const noexcept;
        inline const TJsonType* AsJsonRaw() const noexcept;

        inline bool IsYson() const noexcept;
        inline TYsonTypePtr AsYson() const noexcept;
        inline const TYsonType* AsYsonRaw() const noexcept;

        inline bool IsUuid() const noexcept;
        inline TUuidTypePtr AsUuid() const noexcept;
        inline const TUuidType* AsUuidRaw() const noexcept;

        inline bool IsDate32() const noexcept;
        inline TDate32TypePtr AsDate32() const noexcept;
        inline const TDate32Type* AsDate32Raw() const noexcept;

        inline bool IsDatetime64() const noexcept;
        inline TDatetime64TypePtr AsDatetime64() const noexcept;
        inline const TDatetime64Type* AsDatetime64Raw() const noexcept;

        inline bool IsTimestamp64() const noexcept;
        inline TTimestamp64TypePtr AsTimestamp64() const noexcept;
        inline const TTimestamp64Type* AsTimestamp64Raw() const noexcept;

        inline bool IsInterval64() const noexcept;
        inline TInterval64TypePtr AsInterval64() const noexcept;
        inline const TInterval64Type* AsInterval64Raw() const noexcept;

        inline bool IsOptional() const noexcept;
        inline TOptionalTypePtr AsOptional() const noexcept;
        inline const TOptionalType* AsOptionalRaw() const noexcept;

        inline bool IsList() const noexcept;
        inline TListTypePtr AsList() const noexcept;
        inline const TListType* AsListRaw() const noexcept;

        inline bool IsDict() const noexcept;
        inline TDictTypePtr AsDict() const noexcept;
        inline const TDictType* AsDictRaw() const noexcept;

        inline bool IsStruct() const noexcept;
        inline TStructTypePtr AsStruct() const noexcept;
        inline const TStructType* AsStructRaw() const noexcept;

        inline bool IsTuple() const noexcept;
        inline TTupleTypePtr AsTuple() const noexcept;
        inline const TTupleType* AsTupleRaw() const noexcept;

        inline bool IsVariant() const noexcept;
        inline TVariantTypePtr AsVariant() const noexcept;
        inline const TVariantType* AsVariantRaw() const noexcept;

        inline bool IsTagged() const noexcept;
        inline TTaggedTypePtr AsTagged() const noexcept;
        inline const TTaggedType* AsTaggedRaw() const noexcept;

        /// @}

        /// Recursively descends to tagged types and returns first non-tagged type.
        TTypePtr StripTags() const noexcept;

        /// Like `StripTags`, but returns a raw pointer.
        const TType* StripTagsRaw() const noexcept;

        /// Recursively descends to optional types and returns first non-optional type.
        TTypePtr StripOptionals() const noexcept;

        /// Like `StripOptionals`, but returns a raw pointer.
        const TType* StripOptionalsRaw() const noexcept;

        /// Recursively descends to tagged and optional types and returns first non-tagged non-optional type.
        TTypePtr StripTagsAndOptionals() const noexcept;

        /// Like `StripTagsAndOptionals`, but returns a raw pointer.
        const TType* StripTagsAndOptionalsRaw() const noexcept;

        /// Cast this base class down to the most-derived class and pass it to the `visitor`.
        ///
        /// This function is used as a safer alternative to manually downcasting types via `IsType`/`AsType` calls.
        /// It works like `std::visit` for types, except that it doesn't produce as much code bloat as `std::visit`
        /// does, and can be optimized better. It casts an instance of `NTi::TType` down to the most-derived class,
        /// and passes an intrusive pointer to that concrete type to the `visitor` functor. That is, `visitor` should
        /// be a callable which can handle `NTi::TVoidTypePtr`, `NTi::TOptionalTypePtr`, etc.
        ///
        /// This function returns whatever the `visitor` returns.
        ///
        ///
        /// # Example: visitor
        ///
        /// A simple visitor that returns name for a type would look like this:
        ///
        /// ```
        /// struct TGetNameVisitor {
        ///     TString operator()(TVoidTypePtr) {
        ///         return "Void";
        ///     }
        ///
        ///     TString operator()(TStringTypePtr) {
        ///         return "String";
        ///     }
        ///
        ///     // ...
        ///
        ///     TString operator()(TStructTypePtr type) {
        ///         return TString(type->GetName().GetOrElse("Struct"));;
        ///     }
        ///
        ///     // ...
        /// }
        /// ```
        ///
        /// Now, we can use this visitor as following:
        ///
        /// ```
        /// TString typeName = type->Visit(TGetNameVisitor());
        /// ```
        ///
        ///
        /// # Example: overloaded struct
        ///
        /// Writing a separate struct each time one needs a visitor is tedious. Thanks to C++17 magic, we may avoid it.
        /// Using lambdas and `TOverloaded` from `library/cpp/overloaded` allows replacing separate struct with
        /// a bunch of lambdas:
        ///
        /// ```
        /// TString typeName = type->Visit(TOverloaded{
        ///     [](TVoidTypePtr) -> TString {
        ///         return "Void";
        ///     },
        ///     [](TStringTypePtr) -> TString {
        ///         return "String";
        ///     },
        ///
        ///     // ...
        ///
        /// });
        /// ```
        ///
        ///
        /// # Example: handling all primitives at once
        ///
        /// Since all primitives derive from `TPrimitiveType`, they can be handled all at once,
        /// by accepting `TPrimitiveTypePtr`:
        ///
        /// ```
        /// TString typeName = type->Visit(TOverloaded{
        ///     // All primitive types are handled by this lambda.
        ///     [](TPrimitiveTypePtr) -> TString {
        ///         return "Primitive";
        ///     },
        ///
        ///     // Special handler for string type. Strings are handled by this lambda
        ///     // because of how C++ prioritizes overloads.
        ///     [](TStringTypePtr) -> TString {
        ///         return "String";
        ///     },
        ///
        ///     // ...
        ///
        /// });
        /// ```
        template <typename V>
        inline decltype(auto) Visit(V&& visitor) const;

        /// Like `Visit`, but passes const raw pointers to the visitor.
        template <typename V>
        inline decltype(auto) VisitRaw(V&& visitor) const;

        /// @}

    protected:
        /// @name Internal interface for adoption semantics support
        ///
        /// Do not call these functions manually!
        ///
        /// See `type_factory.h`'s section on implementation details for more info.
        ///
        /// @{
        //-
        /// Create a new instance of the class using this instance as a prototype.
        ///
        /// This is a [virtual copy constructor]. Typical implementation does the following:
        ///
        /// 1. for nested types, if any, it calls the factory's `Own` function. The `Own` acquires internal
        ///    ownership over the nested types, thus guaranteeing that they'll outlive the object that've
        ///    owned them. Depending on the particular factory implementation, `Own` may either recursively deepcopy
        ///    the whole nested type, increment some reference counter, or do nothing;
        /// 2. for other resources owned by this type (i.e. arrays, strings, etc.), it copies them into the given
        ///    factory by calling factory's `New` and `Allocate` functions;
        /// 3. finally, it creates a new instance of the type by invoking its constructor via the factory's
        ///    `New` function.
        ///
        /// Note: there are no guarantees on the value stored in `FactoryOrRc_` during invocation of this function.
        /// Specifically, creating types on the stack (`FactoryOrRc_` is `0` in this case) and moving them
        /// into a factory is a valid technique used extensively throughout this library.
        ///
        /// See `type_factory.h`'s section on implementation details for more info.
        ///
        /// Note: this function is not marked as `virtual` because we use our own dispatch via `Visit`.
        ///
        /// [virtual move constructor]: https://isocpp.org/wiki/faq/virtual-functions#virtual-ctors
        const TType* Clone(ITypeFactoryInternal& factory) const noexcept;

        /// Release internal resources that were allocated in `Clone`.
        ///
        /// This function is the opposite of `Clone`. It releases all memory that was allocated within `Clone`,
        /// and disowns nested types.
        ///
        /// This function is called by factories that perform active memory management, such as the default
        /// heap factory. Typical implementation does the following:
        ///
        /// 1. for each `Clone`'s call to `Own` it calls `Disown`. The `Disown` releases internal ownership
        ///    over the nested types, thus allowing factory to free their memory. Depending
        ///    on the particular factory implementation, `Disown` may either decrement some reference counter,
        ///    call `Drop` and free the underlying memory, or do nothing;
        /// 2. for each `Clone`'s call to `New` and `Allocate`, it calls `Delete` and `Free`;
        /// 3. it should *not* call `Delete(this)` to mirror `Clone`'s final call to `New` (see the third bullet
        ///    in the `Clone`'s documentation). It is the factory's job to release memory under the type that's
        ///    being dropped.
        ///
        /// Note: there are no guarantees on whether this method will be called or not. For example, the default
        /// heap factory will call it when some type's reference counter reaches zero. The default memory pool
        /// factory will not call it.
        ///
        /// Note: this function is not marked as `virtual` because we use our own dispatch via `Visit`.
        ///
        /// See `type_factory.h`'s section on implementation details for more info.
        void Drop(ITypeFactoryInternal& factory) noexcept;

        /// Get factory that manages this instance.
        ///
        /// If this instance is refcounted, returns the default heap factory. If it is unmanaged, returns `nullptr`.
        /// Otherwise, returns pointer to the instance's factory.
        ///
        /// Remember that factories are not thread safe, thus using factory from this method may not be safe.
        ITypeFactoryInternal* GetFactory() const noexcept;

        /// Mark this instance as managed by the given factory.
        void SetFactory(ITypeFactoryInternal* factory) noexcept;

        /// Get factory's internal interface.
        static ITypeFactoryInternal& FactoryInternal(ITypeFactory& factory) noexcept;

        /// @}

    protected:
        /// @name Internal interface for reference counting
        ///
        /// See `type_factory.h`'s section on implementation details for more info.
        ///
        /// @{
        //-
        /// Increase reference count of this type.
        void RefSelf() noexcept {
            RefImpl</* RefFactory = */ false>();
        }

        /// Increase reference count of this type and its factory.
        void Ref() noexcept {
            RefImpl</* RefFactory = */ true>();
        }

        /// Decrease reference count of this type.
        void UnRefSelf() noexcept {
            UnRefImpl</* UnRefFactory = */ false>();
        }

        /// Decrease reference count of this type and its factory.
        void UnRef() noexcept {
            UnRefImpl</* UnRefFactory = */ true>();
        }

        /// Decrease reference count of this type. Panic if any of it reaches zero.
        void DecRefSelf() noexcept {
            DecRefImpl</* DecRefFactory = */ false>();
        }

        /// Decrease reference count of type and its factory. Panic if any of it reaches zero.
        void DecRef() noexcept {
            DecRefImpl</* DecRefFactory = */ true>();
        }

        /// Get current reference count for this type.
        long RefCount() const noexcept {
            return RefCountImpl();
        }

        /// @}

    private:
        template <bool RefFactory>
        void RefImpl() noexcept;

        template <bool UnRefFactory>
        void UnRefImpl() noexcept;

        template <bool DecRefFactory>
        void DecRefImpl() noexcept;

        long RefCountImpl() const noexcept;

    protected:
        /// Helper for implementing `Clone` with caching.
        template <typename T, typename TCtor>
        static const T* Cached(const T* type, ITypeFactoryInternal& factory, TCtor&& ctor);

    private:
        /// Pointer to the type factory that've created this instance.
        /// If this instance is static, this variable contains zero.
        /// If this instance was created by the default heap factory, this variable is used as a reference counter.
        std::atomic<size_t> FactoryOrRc_ = 0;

        /// Name of this type. Can be used to check before downcast.
        ETypeName TypeName_;

        /// Hash is calculated lazily.
        mutable std::atomic<bool> HasHash_;
        mutable std::atomic<ui64> Hash_;

    private:
        static bool IsRc(size_t factoryOrRc) noexcept {
            return factoryOrRc & 1u;
        }
        static bool IsFactory(size_t factoryOrRc) noexcept {
            return factoryOrRc != 0 && !IsRc(factoryOrRc);
        }
        static size_t CastFromFactory(ITypeFactoryInternal* factory) noexcept {
            return reinterpret_cast<size_t>(factory);
        }
        static ITypeFactoryInternal* CastToFactory(size_t factoryOrRc) noexcept {
            return reinterpret_cast<ITypeFactoryInternal*>(factoryOrRc);
        }
    };

    static_assert(sizeof(TType) == 24);

    bool operator==(const TTypePtr& lhs, const TTypePtr& rhs) = delete;
    bool operator!=(const TTypePtr& lhs, const TTypePtr& rhs) = delete;

    /// @brief Check for strict equivalence of the types.
    ///
    /// @see NTi::NEq::TStrictlyEqual
    ///
    /// @{
    bool operator==(const TType& lhs, const TType& rhs);
    bool operator!=(const TType& lhs, const TType& rhs);
    /// @}

    /// A singular type. This type has only one value. When serialized, it takes no space because it carries no data.
    ///
    /// Historically, YQL's `Void` is what's known as unit type (see https://en.wikipedia.org/wiki/Unit_type),
    /// i.e. a type with only one possible value. This is similar to Python's `NoneType` or Rust's `()`.
    class TVoidType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TVoidTypePtr AsPtr() const noexcept {
            return const_cast<TVoidType*>(this);
        }

    private:
        explicit TVoidType();

    public:
        static TVoidTypePtr Instance();
        static const TVoidType* InstanceRaw();

    protected:
        const TVoidType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// Create new `Void` type using the default heap factory.
    TVoidTypePtr Void();

    /// A singular type, value of an empty optional.
    ///
    /// This type is used by YQL for `NULL` literal. Use `TVoidType` unless you have reasons to use this one.
    class TNullType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TNullTypePtr AsPtr() const noexcept {
            return const_cast<TNullType*>(this);
        }

    private:
        explicit TNullType();

    public:
        static TNullTypePtr Instance();
        static const TNullType* InstanceRaw();

    protected:
        const TNullType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// Base class for all primitive types.
    class TPrimitiveType: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TPrimitiveTypePtr AsPtr() const noexcept {
            return const_cast<TPrimitiveType*>(this);
        }

    protected:
        explicit TPrimitiveType(TMaybe<ui64> hash, EPrimitiveTypeName primitiveTypeName) noexcept;

    public:
        /// Get name of this primitive type as a `NTi::EPrimitiveTypeName` enumerator.
        EPrimitiveTypeName GetPrimitiveTypeName() const noexcept {
            return ToPrimitiveTypeName(GetTypeName());
        }

        /// Cast this scalar class down to the most-derived type and pass it to the `visitor`.
        ///
        /// This function works like `NTi::TType::Visit`, but only handles primitive types.
        ///
        ///
        /// # Example:
        ///
        /// ```
        /// auto name = scalar->VisitPrimitive(TOverloaded{
        ///     [](TBoolTypePtr t) { return "Bool" },
        ///     [](TStringTypePtr t) { return "String" },
        ///     // ...
        /// });
        /// ```
        template <typename V>
        inline decltype(auto) VisitPrimitive(V&& visitor) const;

        /// Like `VisitPrimitive`, but passes raw pointers to the visitor.
        template <typename V>
        inline decltype(auto) VisitPrimitiveRaw(V&& visitor) const;
    };

    /// A logical type capable of holding one of the two values: true or false.
    class TBoolType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TBoolTypePtr AsPtr() const noexcept {
            return const_cast<TBoolType*>(this);
        }

    private:
        explicit TBoolType();

    public:
        static TBoolTypePtr Instance();
        static const TBoolType* InstanceRaw();

    protected:
        const TBoolType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A signed integer, one byte.
    class TInt8Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TInt8TypePtr AsPtr() const noexcept {
            return const_cast<TInt8Type*>(this);
        }

    private:
        explicit TInt8Type();

    public:
        static TInt8TypePtr Instance();
        static const TInt8Type* InstanceRaw();

    protected:
        const TInt8Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A signed integer, two bytes.
    class TInt16Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TInt16TypePtr AsPtr() const noexcept {
            return const_cast<TInt16Type*>(this);
        }

    private:
        explicit TInt16Type();

    public:
        static TInt16TypePtr Instance();
        static const TInt16Type* InstanceRaw();

    protected:
        const TInt16Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A signed integer, four bytes.
    class TInt32Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TInt32TypePtr AsPtr() const noexcept {
            return const_cast<TInt32Type*>(this);
        }

    private:
        explicit TInt32Type();

    public:
        static TInt32TypePtr Instance();
        static const TInt32Type* InstanceRaw();

    protected:
        const TInt32Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A signed integer, eight bytes.
    class TInt64Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TInt64TypePtr AsPtr() const noexcept {
            return const_cast<TInt64Type*>(this);
        }

    private:
        explicit TInt64Type();

    public:
        static TInt64TypePtr Instance();
        static const TInt64Type* InstanceRaw();

    protected:
        const TInt64Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An unsigned integer, one byte.
    class TUint8Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TUint8TypePtr AsPtr() const noexcept {
            return const_cast<TUint8Type*>(this);
        }

    private:
        explicit TUint8Type();

    public:
        static TUint8TypePtr Instance();
        static const TUint8Type* InstanceRaw();

    protected:
        const TUint8Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An unsigned integer, two bytes.
    class TUint16Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TUint16TypePtr AsPtr() const noexcept {
            return const_cast<TUint16Type*>(this);
        }

    private:
        explicit TUint16Type();

    public:
        static TUint16TypePtr Instance();
        static const TUint16Type* InstanceRaw();

    protected:
        const TUint16Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An unsigned integer, four bytes.
    class TUint32Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TUint32TypePtr AsPtr() const noexcept {
            return const_cast<TUint32Type*>(this);
        }

    private:
        explicit TUint32Type();

    public:
        static TUint32TypePtr Instance();
        static const TUint32Type* InstanceRaw();

    protected:
        const TUint32Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An unsigned integer, eight bytes.
    class TUint64Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TUint64TypePtr AsPtr() const noexcept {
            return const_cast<TUint64Type*>(this);
        }

    private:
        explicit TUint64Type();

    public:
        static TUint64TypePtr Instance();
        static const TUint64Type* InstanceRaw();

    protected:
        const TUint64Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A floating point number, four bytes.
    class TFloatType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TFloatTypePtr AsPtr() const noexcept {
            return const_cast<TFloatType*>(this);
        }

    private:
        explicit TFloatType();

    public:
        static TFloatTypePtr Instance();
        static const TFloatType* InstanceRaw();

    protected:
        const TFloatType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A floating point number, eight bytes.
    class TDoubleType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDoubleTypePtr AsPtr() const noexcept {
            return const_cast<TDoubleType*>(this);
        }

    private:
        explicit TDoubleType();

    public:
        static TDoubleTypePtr Instance();
        static const TDoubleType* InstanceRaw();

    protected:
        const TDoubleType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A binary blob.
    ///
    /// This type can be used for any binary data. For text, consider using type `Utf8`.
    class TStringType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TStringTypePtr AsPtr() const noexcept {
            return const_cast<TStringType*>(this);
        }

    private:
        explicit TStringType();

    public:
        static TStringTypePtr Instance();
        static const TStringType* InstanceRaw();

    protected:
        const TStringType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A utf-8 encoded text.
    class TUtf8Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TUtf8TypePtr AsPtr() const noexcept {
            return const_cast<TUtf8Type*>(this);
        }

    private:
        explicit TUtf8Type();

    public:
        static TUtf8TypePtr Instance();
        static const TUtf8Type* InstanceRaw();

    protected:
        const TUtf8Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An absolute point in time in range `[1970-01-01, 2106-01-01)`, precision up to days.
    class TDateType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDateTypePtr AsPtr() const noexcept {
            return const_cast<TDateType*>(this);
        }

    private:
        explicit TDateType();

    public:
        static TDateTypePtr Instance();
        static const TDateType* InstanceRaw();

    protected:
        const TDateType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An absolute point in time in range `[1970-01-01, 2106-01-01)`, precision up to seconds.
    class TDatetimeType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDatetimeTypePtr AsPtr() const noexcept {
            return const_cast<TDatetimeType*>(this);
        }

    private:
        explicit TDatetimeType();

    public:
        static TDatetimeTypePtr Instance();
        static const TDatetimeType* InstanceRaw();

    protected:
        const TDatetimeType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An absolute point in time in range `[1970-01-01, 2106-01-01)`, precision up to milliseconds.
    class TTimestampType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TTimestampTypePtr AsPtr() const noexcept {
            return const_cast<TTimestampType*>(this);
        }

    private:
        explicit TTimestampType();

    public:
        static TTimestampTypePtr Instance();
        static const TTimestampType* InstanceRaw();

    protected:
        const TTimestampType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// `TDateType` with additional timezone mark.
    class TTzDateType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TTzDateTypePtr AsPtr() const noexcept {
            return const_cast<TTzDateType*>(this);
        }

    private:
        explicit TTzDateType();

    public:
        static TTzDateTypePtr Instance();
        static const TTzDateType* InstanceRaw();

    protected:
        const TTzDateType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// `TDatetimeType` with additional timezone mark.
    class TTzDatetimeType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TTzDatetimeTypePtr AsPtr() const noexcept {
            return const_cast<TTzDatetimeType*>(this);
        }

    private:
        explicit TTzDatetimeType();

    public:
        static TTzDatetimeTypePtr Instance();
        static const TTzDatetimeType* InstanceRaw();

    protected:
        const TTzDatetimeType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// `TTimestampType` with additional timezone mark.
    class TTzTimestampType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TTzTimestampTypePtr AsPtr() const noexcept {
            return const_cast<TTzTimestampType*>(this);
        }

    private:
        explicit TTzTimestampType();

    public:
        static TTzTimestampTypePtr Instance();
        static const TTzTimestampType* InstanceRaw();

    protected:
        const TTzTimestampType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// Signed delta between two timestamps.
    class TIntervalType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TIntervalTypePtr AsPtr() const noexcept {
            return const_cast<TIntervalType*>(this);
        }

    private:
        explicit TIntervalType();

    public:
        static TIntervalTypePtr Instance();
        static const TIntervalType* InstanceRaw();

    protected:
        const TIntervalType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A 128-bit number with controlled exponent/significand size.
    ///
    /// Decimal is a type for extra precise calculations. Internally, it is represented by a float-like 128-bit number.
    ///
    /// Two parameters control number of decimal digits in the decimal value. `Precision` is the total number
    /// of decimal digits. `Scale` is the number of digits after the decimal point.
    class TDecimalType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDecimalTypePtr AsPtr() const noexcept {
            return const_cast<TDecimalType*>(this);
        }

    private:
        explicit TDecimalType(TMaybe<ui64> hash, ui8 precision, ui8 scale) noexcept;
        static TDecimalTypePtr Create(ITypeFactory& factory, ui8 precision, ui8 scale);
        static const TDecimalType* CreateRaw(ITypeFactory& factory, ui8 precision, ui8 scale);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get total number of decimal digits.
        ui8 GetPrecision() const noexcept {
            return Precision_;
        }

        /// Get number of decimal digits after the decimal point.
        ui8 GetScale() const noexcept {
            return Scale_;
        }

    protected:
        const TDecimalType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        ui8 Precision_;
        ui8 Scale_;
    };

    /// A string with valid JSON.
    class TJsonType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TJsonTypePtr AsPtr() const noexcept {
            return const_cast<TJsonType*>(this);
        }

    private:
        explicit TJsonType();

    public:
        static TJsonTypePtr Instance();
        static const TJsonType* InstanceRaw();

    protected:
        const TJsonType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A string with valid YSON.
    class TYsonType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TYsonTypePtr AsPtr() const noexcept {
            return const_cast<TYsonType*>(this);
        }

    private:
        explicit TYsonType();

    public:
        static TYsonTypePtr Instance();
        static const TYsonType* InstanceRaw();

    protected:
        const TYsonType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// A string with valid UUID.
    class TUuidType final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TUuidTypePtr AsPtr() const noexcept {
            return const_cast<TUuidType*>(this);
        }

    private:
        explicit TUuidType();

    public:
        static TUuidTypePtr Instance();
        static const TUuidType* InstanceRaw();

    protected:
        const TUuidType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An absolute point in time in range `[-144169-01-01, 148108-01-01)`, precision up to days (Unix epoch 1970-01-01 - 0 days).
    class TDate32Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDate32TypePtr AsPtr() const noexcept {
            return const_cast<TDate32Type*>(this);
        }

    private:
        explicit TDate32Type();

    public:
        static TDate32TypePtr Instance();
        static const TDate32Type* InstanceRaw();

    protected:
        const TDate32Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An absolute point in time in range `[-144169-01-01, 148108-01-01)`, precision up to seconds (Unix epoch 1970-01-01 - 0 seconds).
    class TDatetime64Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDatetime64TypePtr AsPtr() const noexcept {
            return const_cast<TDatetime64Type*>(this);
        }

    private:
        explicit TDatetime64Type();

    public:
        static TDatetime64TypePtr Instance();
        static const TDatetime64Type* InstanceRaw();

    protected:
        const TDatetime64Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// An absolute point in time in range `[-144169-01-01, 148108-01-01)`, precision up to milliseconds (Unix epoch 1970-01-01 - 0 milliseconds).
    class TTimestamp64Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TTimestamp64TypePtr AsPtr() const noexcept {
            return const_cast<TTimestamp64Type*>(this);
        }

    private:
        explicit TTimestamp64Type();

    public:
        static TTimestamp64TypePtr Instance();
        static const TTimestamp64Type* InstanceRaw();

    protected:
        const TTimestamp64Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// Signed delta between two timestamps64.
    class TInterval64Type final: public TPrimitiveType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TInterval64TypePtr AsPtr() const noexcept {
            return const_cast<TInterval64Type*>(this);
        }

    private:
        explicit TInterval64Type();

    public:
        static TInterval64TypePtr Instance();
        static const TInterval64Type* InstanceRaw();

    protected:
        const TInterval64Type* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;
    };

    /// Object which can store a value or a singular `NULL` value.
    ///
    /// This type is used to encode a value or its absence.
    class TOptionalType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TOptionalTypePtr AsPtr() const noexcept {
            return const_cast<TOptionalType*>(this);
        }

    private:
        explicit TOptionalType(TMaybe<ui64> hash, const TType* item) noexcept;
        static TOptionalTypePtr Create(ITypeFactory& factory, TTypePtr item);
        static const TOptionalType* CreateRaw(ITypeFactory& factory, const TType* item);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get underlying type.
        TTypePtr GetItemType() const noexcept {
            return GetItemTypeRaw()->AsPtr();
        }

        /// Like `GetMemberType`, but returns a raw pointer.
        const TType* GetItemTypeRaw() const noexcept {
            return Item_;
        }

    protected:
        const TOptionalType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        const TType* Item_;
    };

    /// A variable-size collection of homogeneous values.
    class TListType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TListTypePtr AsPtr() const noexcept {
            return const_cast<TListType*>(this);
        }

    private:
        explicit TListType(TMaybe<ui64> hash, const TType* item) noexcept;
        static TListTypePtr Create(ITypeFactory& factory, TTypePtr item);
        static const TListType* CreateRaw(ITypeFactory& factory, const TType* item);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get underlying type.
        TTypePtr GetItemType() const noexcept {
            return GetItemTypeRaw()->AsPtr();
        }

        /// Like `GetMemberType`, but returns a raw pointer.
        const TType* GetItemTypeRaw() const noexcept {
            return Item_;
        }

    protected:
        const TListType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        const TType* Item_;
    };

    /// An associative key-value container.
    ///
    /// Values of this type are usually represented as hashmaps.
    class TDictType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TDictTypePtr AsPtr() const noexcept {
            return const_cast<TDictType*>(this);
        }

    private:
        explicit TDictType(TMaybe<ui64> hash, const TType* key, const TType* value) noexcept;
        static TDictTypePtr Create(ITypeFactory& factory, TTypePtr key, TTypePtr value);
        static const TDictType* CreateRaw(ITypeFactory& factory, const TType* key, const TType* value);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get the key type.
        TTypePtr GetKeyType() const noexcept {
            return GetKeyTypeRaw()->AsPtr();
        }

        /// Like `GetKeyType`, but returns a raw pointer.
        const TType* GetKeyTypeRaw() const noexcept {
            return Key_;
        }

        /// Get the value type.
        TTypePtr GetValueType() const noexcept {
            return GetValueTypeRaw()->AsPtr();
        }

        /// Like `GetValueType`, but returns a raw pointer.
        const TType* GetValueTypeRaw() const noexcept {
            return Value_;
        }

    protected:
        const TDictType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        const TType* Key_;
        const TType* Value_;
    };

    /// A fixed-size collection of named heterogeneous values.
    ///
    /// Structs represent multiple values grouped into a single entity. Values stored in a struct are called items.
    /// Each item have an associated type and a name.
    ///
    /// Struct type is represented by an array of item types and their names.
    ///
    /// Even though struct elements are accessed by their names, we use vector to represent struct type because order
    /// of struct items matters. If affects memory layout of a struct, how struct is serialized and deserialized.
    /// Items order is vital for struct versioning. New fields should always be added to the end of the struct.
    /// This way older parsers can read values serialized by newer writers: they'll simply read known head of a struct
    /// and skip tail that contains unknown fields.
    ///
    ///
    /// # Struct names
    ///
    /// Each struct defined by YDL must have an associated name. This name is used to refer struct in code, to generate
    /// code representing this struct in other programming languages, and to report errors. The struct's name is saved
    /// in this field.
    ///
    /// Note that, even though YDL requires struct names, name field is optional because other systems might
    /// use anonymous structs (or maybe they dont't use struct names at all).
    ///
    /// Note also that type aliases (especially `newtype`) use tags to name types, so primitives don't have name field.
    class TStructType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;
        friend class TStructBuilderRaw;

    public:
        TStructTypePtr AsPtr() const noexcept {
            return const_cast<TStructType*>(this);
        }

    public:
        /// A single struct element.
        class TMember {
        public:
            TMember(TStringBuf name, const TType* type);

        public:
            /// Get name of this item.
            TStringBuf GetName() const {
                return Name_;
            }

            /// Get type of this item.
            TTypePtr GetType() const {
                return GetTypeRaw()->AsPtr();
            }

            /// Like `GetType`, but returns a raw pointer.
            const TType* GetTypeRaw() const {
                return Type_;
            }

            /// Calculate this item's hash. Hashes follow the 'strict equivalence' (see type_equivalence.h).
            ui64 Hash() const;

        private:
            TStringBuf Name_;
            const TType* Type_;
        };

        using TMembers = TConstArrayRef<TMember>;

        /// Like `TMember`, but owns its contents. Used in non-raw constructors to guarantee data validity.
        class TOwnedMember {
        public:
            TOwnedMember(TString name, TTypePtr type);

        public:
            operator TMember() const&;

        private:
            TString Name_;
            TTypePtr Type_;
        };

        using TOwnedMembers = TConstArrayRef<TOwnedMember>;

    private:
        explicit TStructType(TMaybe<ui64> hash, TMaybe<TStringBuf> name, TMembers members, TConstArrayRef<size_t> sortedMembers) noexcept;
        static TStructTypePtr Create(ITypeFactory& factory, TOwnedMembers members);
        static TStructTypePtr Create(ITypeFactory& factory, TMaybe<TStringBuf> name, TOwnedMembers members);
        static const TStructType* CreateRaw(ITypeFactory& factory, TMembers members);
        static const TStructType* CreateRaw(ITypeFactory& factory, TMaybe<TStringBuf> name, TMembers members);
        static void MakeSortedMembers(TMembers members, TArrayRef<size_t> sortedItems);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get name of this struct.
        TMaybe<TStringBuf> GetName() const noexcept {
            return Name_;
        }

        /// Get description of structure members.
        TMembers GetMembers() const noexcept {
            return Members_;
        }

        /// Check if there is an item with the given name in this struct.
        bool HasMember(TStringBuf name) const noexcept;

        /// Lookup struct item by name. Throw an error if there is no such item.
        const TMember& GetMember(TStringBuf name) const;

        /// Lookup struct item by name, return its index or `-1`, if item was not found.
        /// This function works in `O(log(n))` time, where `n` is number of struct members.
        ssize_t GetMemberIndex(TStringBuf name) const noexcept;

    protected:
        const TStructType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        TMaybe<TStringBuf> Name_;
        TMembers Members_;
        TConstArrayRef<size_t> SortedMembers_;
    };

    /// A fixed-size collection of named heterogeneous values.
    ///
    /// Tuples, like structs, represent multiple values, also called items, grouped into a single entity. Unlike
    /// structs, though, tuple items are unnamed. Instead of names, they are accessed by their indexes.
    ///
    /// For a particular tuple type, number of items, their order and types are fixed, they should be known before
    /// creating instances of tuples and can't change over time.
    ///
    ///
    /// # Tuple names
    ///
    /// YDL requires each tuple definition to have a name (see `TStructType`). The name might not be mandatory
    /// in other systems, so name field is optional.
    class TTupleType: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;
        friend class TTupleBuilderRaw;

    public:
        TTupleTypePtr AsPtr() const noexcept {
            return const_cast<TTupleType*>(this);
        }

    public:
        /// A single tuple element.
        class TElement {
        public:
            TElement(const TType* type);

        public:
            /// Get type of this item.
            TTypePtr GetType() const {
                return GetTypeRaw()->AsPtr();
            }

            /// Like `GetType`, but returns a raw pointer.
            const TType* GetTypeRaw() const {
                return Type_;
            }

            /// Calculate this item's hash. Hashes follow the 'strict equivalence' (see type_equivalence.h).
            ui64 Hash() const;

        private:
            const TType* Type_;
        };

        using TElements = TConstArrayRef<TElement>;

        /// Like `TElement`, but owns its contents. Used in non-raw constructors to guarantee data validity.
        class TOwnedElement {
        public:
            TOwnedElement(TTypePtr type);

        public:
            operator TElement() const&;

        private:
            TTypePtr Type_;
        };

        using TOwnedElements = TConstArrayRef<TOwnedElement>;

    private:
        explicit TTupleType(TMaybe<ui64> hash, TMaybe<TStringBuf> name, TElements items) noexcept;
        static TTupleTypePtr Create(ITypeFactory& factory, TOwnedElements items);
        static TTupleTypePtr Create(ITypeFactory& factory, TMaybe<TStringBuf> name, TOwnedElements items);
        static const TTupleType* CreateRaw(ITypeFactory& factory, TElements items);
        static const TTupleType* CreateRaw(ITypeFactory& factory, TMaybe<TStringBuf> name, TElements items);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get name of this type.
        TMaybe<TStringBuf> GetName() const noexcept {
            return Name_;
        }

        /// Get description of tuple items.
        TElements GetElements() const noexcept {
            return Elements_;
        }

    protected:
        const TTupleType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        TMaybe<TStringBuf> Name_;
        TElements Elements_;
    };

    /// A tagged union with named or unnamed alternatives (a.k.a. variant over struct or variant over tuple).
    ///
    /// Variants are used to store values of different types.
    ///
    /// For example, a variant over struct which holds an ip address could look like
    /// `Ip = Variant<v4: IpV4, v6: IpV6>`, where `IpV4` and `IpV6` are some other types. Now, a value of type `Ip`
    /// could store either a value of type `IpV4` or a value of type `IpV6`.
    ///
    /// Even though item types can be the same, each item represents a distinct state of a variant. For example,
    /// a variant for a user identifier can look like `Uid = Variant<yuid: String, ip: String>`. This variant can
    /// contain either user's yandexuid of user's ip. Despite both items are of the same type `String`, `Uid` which
    /// contains a `yuid` and `Uid` which contains an `ip` are never equal.
    /// That is, `Uid.yuid("000000") != Uid.ip("000000")`.
    ///
    /// Exactly like with structs or tuples, order of variant items matter. Indexes of variant items are used
    /// instead of names when variant is serialized.
    ///
    ///
    /// # Variant names
    ///
    /// YDL requires each variant definition to have a name (see `TStructType`). The name might not be mandatory
    /// in other systems, so name field is optional.
    class TVariantType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;
        friend class TStructBuilderRaw;
        friend class TTupleBuilderRaw;

    public:
        TVariantTypePtr AsPtr() const noexcept {
            return const_cast<TVariantType*>(this);
        }

    private:
        explicit TVariantType(TMaybe<ui64> hash, TMaybe<TStringBuf> name, const TType* inner) noexcept;
        static TVariantTypePtr Create(ITypeFactory& factory, TTypePtr inner);
        static TVariantTypePtr Create(ITypeFactory& factory, TMaybe<TStringBuf> name, TTypePtr inner);
        static const TVariantType* CreateRaw(ITypeFactory& factory, const TType* inner);
        static const TVariantType* CreateRaw(ITypeFactory& factory, TMaybe<TStringBuf> name, const TType* inner);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get name of this variant.
        TMaybe<TStringBuf> GetName() const noexcept {
            return Name_;
        }

        /// Get vector of variant items.
        TTypePtr GetUnderlyingType() const noexcept {
            return Underlying_->AsPtr();
        }

        /// Like `GetUnderlyingType`, but returns a raw pointer.
        const TType* GetUnderlyingTypeRaw() const noexcept {
            return Underlying_;
        }

        /// Check if this variant's inner type is a struct.
        bool IsVariantOverStruct() const noexcept {
            return GetUnderlyingTypeRaw()->GetTypeName() == ETypeName::Struct;
        }

        /// Check if this variant's inner type is a tuple.
        bool IsVariantOverTuple() const noexcept {
            return GetUnderlyingTypeRaw()->GetTypeName() == ETypeName::Tuple;
        }

        /// Visit inner type of this variant. This function works like `Visit`, but only casts inner type
        /// to struct or tuple, so you don't need to handle other types in a visitor.
        template <typename V>
        inline decltype(auto) VisitUnderlying(V&& visitor) const;

        /// Like `VisitUnderlying`, but passes const raw pointers to the visitor.
        template <typename V>
        inline decltype(auto) VisitUnderlyingRaw(V&& visitor) const;

    protected:
        const TVariantType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        TMaybe<TStringBuf> Name_;
        const TType* Underlying_;
    };

    /// Named or tagged or user-defined type.
    ///
    /// Tags are used to create new types from existing ones by assigning them a tag, i.e. a name. They wrap other types
    /// adding them additional semantics.
    ///
    /// On physical level, tags do not change types. Both `Tagged<Int32, 'GeoId'>` and `Int32` have exactly the same
    /// representation when serialized.
    ///
    /// On logical level, tags change semantics of a type. This can affect how types are displayed, how they
    /// are checked and converted, etc.
    class TTaggedType final: public TType {
        friend class TType;
        friend class ITypeFactoryInternal;
        friend class ITypeFactory;
        friend class IPoolTypeFactory;

    public:
        TTaggedTypePtr AsPtr() const noexcept {
            return const_cast<TTaggedType*>(this);
        }

    private:
        explicit TTaggedType(TMaybe<ui64> hash, const TType* type, TStringBuf tag) noexcept;
        static TTaggedTypePtr Create(ITypeFactory& factory, TTypePtr type, TStringBuf tag);
        static const TTaggedType* CreateRaw(ITypeFactory& factory, const TType* type, TStringBuf tag);

    protected:
        ui64 CalculateHash() const noexcept;

    public:
        /// Get tag content, i.e. name of the new type.
        TStringBuf GetTag() const noexcept {
            return Tag_;
        }

        /// Get the wrapped type.
        TTypePtr GetItemType() const noexcept {
            return GetItemTypeRaw()->AsPtr();
        }

        /// Like `GetMemberType`, but returns a raw pointer.
        const TType* GetItemTypeRaw() const noexcept {
            return Item_;
        }

    protected:
        const TTaggedType* Clone(ITypeFactoryInternal& factory) const noexcept;
        void Drop(ITypeFactoryInternal& factory) noexcept;

    private:
        const TType* Item_;
        TStringBuf Tag_;
    };

#ifdef __JETBRAINS_IDE__
#pragma clang diagnostic push
#pragma ide diagnostic ignored "cppcoreguidelines-pro-type-static-cast-downcast"
#endif

    bool TType::IsPrimitive() const noexcept {
        return ::NTi::IsPrimitive(TypeName_);
    }

    TPrimitiveTypePtr TType::AsPrimitive() const noexcept {
        return AsPrimitiveRaw()->AsPtr();
    }

    const TPrimitiveType* TType::AsPrimitiveRaw() const noexcept {
        Y_ABORT_UNLESS(IsPrimitive());
        return static_cast<const TPrimitiveType*>(this);
    }

    bool TType::IsVoid() const noexcept {
        return TypeName_ == ETypeName::Void;
    }

    TVoidTypePtr TType::AsVoid() const noexcept {
        return AsVoidRaw()->AsPtr();
    }

    const TVoidType* TType::AsVoidRaw() const noexcept {
        Y_ABORT_UNLESS(IsVoid());
        return static_cast<const TVoidType*>(this);
    }

    bool TType::IsNull() const noexcept {
        return TypeName_ == ETypeName::Null;
    }

    TNullTypePtr TType::AsNull() const noexcept {
        return AsNullRaw()->AsPtr();
    }

    const TNullType* TType::AsNullRaw() const noexcept {
        Y_ABORT_UNLESS(IsNull());
        return static_cast<const TNullType*>(this);
    }

    bool TType::IsBool() const noexcept {
        return TypeName_ == ETypeName::Bool;
    }

    TBoolTypePtr TType::AsBool() const noexcept {
        return AsBoolRaw()->AsPtr();
    }

    const TBoolType* TType::AsBoolRaw() const noexcept {
        Y_ABORT_UNLESS(IsBool());
        return static_cast<const TBoolType*>(this);
    }

    bool TType::IsInt8() const noexcept {
        return TypeName_ == ETypeName::Int8;
    }

    TInt8TypePtr TType::AsInt8() const noexcept {
        return AsInt8Raw()->AsPtr();
    }

    const TInt8Type* TType::AsInt8Raw() const noexcept {
        Y_ABORT_UNLESS(IsInt8());
        return static_cast<const TInt8Type*>(this);
    }

    bool TType::IsInt16() const noexcept {
        return TypeName_ == ETypeName::Int16;
    }

    TInt16TypePtr TType::AsInt16() const noexcept {
        return AsInt16Raw()->AsPtr();
    }

    const TInt16Type* TType::AsInt16Raw() const noexcept {
        Y_ABORT_UNLESS(IsInt16());
        return static_cast<const TInt16Type*>(this);
    }

    bool TType::IsInt32() const noexcept {
        return TypeName_ == ETypeName::Int32;
    }

    TInt32TypePtr TType::AsInt32() const noexcept {
        return AsInt32Raw()->AsPtr();
    }

    const TInt32Type* TType::AsInt32Raw() const noexcept {
        Y_ABORT_UNLESS(IsInt32());
        return static_cast<const TInt32Type*>(this);
    }

    bool TType::IsInt64() const noexcept {
        return TypeName_ == ETypeName::Int64;
    }

    TInt64TypePtr TType::AsInt64() const noexcept {
        return AsInt64Raw()->AsPtr();
    }

    const TInt64Type* TType::AsInt64Raw() const noexcept {
        Y_ABORT_UNLESS(IsInt64());
        return static_cast<const TInt64Type*>(this);
    }

    bool TType::IsUint8() const noexcept {
        return TypeName_ == ETypeName::Uint8;
    }

    TUint8TypePtr TType::AsUint8() const noexcept {
        return AsUint8Raw()->AsPtr();
    }

    const TUint8Type* TType::AsUint8Raw() const noexcept {
        Y_ABORT_UNLESS(IsUint8());
        return static_cast<const TUint8Type*>(this);
    }

    bool TType::IsUint16() const noexcept {
        return TypeName_ == ETypeName::Uint16;
    }

    TUint16TypePtr TType::AsUint16() const noexcept {
        return AsUint16Raw()->AsPtr();
    }

    const TUint16Type* TType::AsUint16Raw() const noexcept {
        Y_ABORT_UNLESS(IsUint16());
        return static_cast<const TUint16Type*>(this);
    }

    bool TType::IsUint32() const noexcept {
        return TypeName_ == ETypeName::Uint32;
    }

    TUint32TypePtr TType::AsUint32() const noexcept {
        return AsUint32Raw()->AsPtr();
    }

    const TUint32Type* TType::AsUint32Raw() const noexcept {
        Y_ABORT_UNLESS(IsUint32());
        return static_cast<const TUint32Type*>(this);
    }

    bool TType::IsUint64() const noexcept {
        return TypeName_ == ETypeName::Uint64;
    }

    TUint64TypePtr TType::AsUint64() const noexcept {
        return AsUint64Raw()->AsPtr();
    }

    const TUint64Type* TType::AsUint64Raw() const noexcept {
        Y_ABORT_UNLESS(IsUint64());
        return static_cast<const TUint64Type*>(this);
    }

    bool TType::IsFloat() const noexcept {
        return TypeName_ == ETypeName::Float;
    }

    TFloatTypePtr TType::AsFloat() const noexcept {
        return AsFloatRaw()->AsPtr();
    }

    const TFloatType* TType::AsFloatRaw() const noexcept {
        Y_ABORT_UNLESS(IsFloat());
        return static_cast<const TFloatType*>(this);
    }

    bool TType::IsDouble() const noexcept {
        return TypeName_ == ETypeName::Double;
    }

    TDoubleTypePtr TType::AsDouble() const noexcept {
        return AsDoubleRaw()->AsPtr();
    }

    const TDoubleType* TType::AsDoubleRaw() const noexcept {
        Y_ABORT_UNLESS(IsDouble());
        return static_cast<const TDoubleType*>(this);
    }

    bool TType::IsString() const noexcept {
        return TypeName_ == ETypeName::String;
    }

    TStringTypePtr TType::AsString() const noexcept {
        return AsStringRaw()->AsPtr();
    }

    const TStringType* TType::AsStringRaw() const noexcept {
        Y_ABORT_UNLESS(IsString());
        return static_cast<const TStringType*>(this);
    }

    bool TType::IsUtf8() const noexcept {
        return TypeName_ == ETypeName::Utf8;
    }

    TUtf8TypePtr TType::AsUtf8() const noexcept {
        return AsUtf8Raw()->AsPtr();
    }

    const TUtf8Type* TType::AsUtf8Raw() const noexcept {
        Y_ABORT_UNLESS(IsUtf8());
        return static_cast<const TUtf8Type*>(this);
    }

    bool TType::IsDate() const noexcept {
        return TypeName_ == ETypeName::Date;
    }

    TDateTypePtr TType::AsDate() const noexcept {
        return AsDateRaw()->AsPtr();
    }

    const TDateType* TType::AsDateRaw() const noexcept {
        Y_ABORT_UNLESS(IsDate());
        return static_cast<const TDateType*>(this);
    }

    bool TType::IsDatetime() const noexcept {
        return TypeName_ == ETypeName::Datetime;
    }

    TDatetimeTypePtr TType::AsDatetime() const noexcept {
        return AsDatetimeRaw()->AsPtr();
    }

    const TDatetimeType* TType::AsDatetimeRaw() const noexcept {
        Y_ABORT_UNLESS(IsDatetime());
        return static_cast<const TDatetimeType*>(this);
    }

    bool TType::IsTimestamp() const noexcept {
        return TypeName_ == ETypeName::Timestamp;
    }

    TTimestampTypePtr TType::AsTimestamp() const noexcept {
        return AsTimestampRaw()->AsPtr();
    }

    const TTimestampType* TType::AsTimestampRaw() const noexcept {
        Y_ABORT_UNLESS(IsTimestamp());
        return static_cast<const TTimestampType*>(this);
    }

    bool TType::IsTzDate() const noexcept {
        return TypeName_ == ETypeName::TzDate;
    }

    TTzDateTypePtr TType::AsTzDate() const noexcept {
        return AsTzDateRaw()->AsPtr();
    }

    const TTzDateType* TType::AsTzDateRaw() const noexcept {
        Y_ABORT_UNLESS(IsTzDate());
        return static_cast<const TTzDateType*>(this);
    }

    bool TType::IsTzDatetime() const noexcept {
        return TypeName_ == ETypeName::TzDatetime;
    }

    TTzDatetimeTypePtr TType::AsTzDatetime() const noexcept {
        return AsTzDatetimeRaw()->AsPtr();
    }

    const TTzDatetimeType* TType::AsTzDatetimeRaw() const noexcept {
        Y_ABORT_UNLESS(IsTzDatetime());
        return static_cast<const TTzDatetimeType*>(this);
    }

    bool TType::IsTzTimestamp() const noexcept {
        return TypeName_ == ETypeName::TzTimestamp;
    }

    TTzTimestampTypePtr TType::AsTzTimestamp() const noexcept {
        return AsTzTimestampRaw()->AsPtr();
    }

    const TTzTimestampType* TType::AsTzTimestampRaw() const noexcept {
        Y_ABORT_UNLESS(IsTzTimestamp());
        return static_cast<const TTzTimestampType*>(this);
    }

    bool TType::IsInterval() const noexcept {
        return TypeName_ == ETypeName::Interval;
    }

    TIntervalTypePtr TType::AsInterval() const noexcept {
        return AsIntervalRaw()->AsPtr();
    }

    const TIntervalType* TType::AsIntervalRaw() const noexcept {
        Y_ABORT_UNLESS(IsInterval());
        return static_cast<const TIntervalType*>(this);
    }

    bool TType::IsDecimal() const noexcept {
        return TypeName_ == ETypeName::Decimal;
    }

    TDecimalTypePtr TType::AsDecimal() const noexcept {
        return AsDecimalRaw()->AsPtr();
    }

    const TDecimalType* TType::AsDecimalRaw() const noexcept {
        Y_ABORT_UNLESS(IsDecimal());
        return static_cast<const TDecimalType*>(this);
    }

    bool TType::IsJson() const noexcept {
        return TypeName_ == ETypeName::Json;
    }

    TJsonTypePtr TType::AsJson() const noexcept {
        return AsJsonRaw()->AsPtr();
    }

    const TJsonType* TType::AsJsonRaw() const noexcept {
        Y_ABORT_UNLESS(IsJson());
        return static_cast<const TJsonType*>(this);
    }

    bool TType::IsYson() const noexcept {
        return TypeName_ == ETypeName::Yson;
    }

    TYsonTypePtr TType::AsYson() const noexcept {
        return AsYsonRaw()->AsPtr();
    }

    const TYsonType* TType::AsYsonRaw() const noexcept {
        Y_ABORT_UNLESS(IsYson());
        return static_cast<const TYsonType*>(this);
    }

    bool TType::IsUuid() const noexcept {
        return TypeName_ == ETypeName::Uuid;
    }

    TUuidTypePtr TType::AsUuid() const noexcept {
        return AsUuidRaw()->AsPtr();
    }

    const TUuidType* TType::AsUuidRaw() const noexcept {
        Y_ABORT_UNLESS(IsUuid());
        return static_cast<const TUuidType*>(this);
    }

    bool TType::IsDate32() const noexcept {
        return TypeName_ == ETypeName::Date32;
    }

    TDate32TypePtr TType::AsDate32() const noexcept {
        return AsDate32Raw()->AsPtr();
    }

    const TDate32Type* TType::AsDate32Raw() const noexcept {
        Y_ABORT_UNLESS(IsDate32());
        return static_cast<const TDate32Type*>(this);
    }

    bool TType::IsDatetime64() const noexcept {
        return TypeName_ == ETypeName::Datetime64;
    }

    TDatetime64TypePtr TType::AsDatetime64() const noexcept {
        return AsDatetime64Raw()->AsPtr();
    }

    const TDatetime64Type* TType::AsDatetime64Raw() const noexcept {
        Y_ABORT_UNLESS(IsDatetime64());
        return static_cast<const TDatetime64Type*>(this);
    }

    bool TType::IsTimestamp64() const noexcept {
        return TypeName_ == ETypeName::Timestamp64;
    }

    TTimestamp64TypePtr TType::AsTimestamp64() const noexcept {
        return AsTimestamp64Raw()->AsPtr();
    }

    const TTimestamp64Type* TType::AsTimestamp64Raw() const noexcept {
        Y_ABORT_UNLESS(IsTimestamp64());
        return static_cast<const TTimestamp64Type*>(this);
    }

    bool TType::IsInterval64() const noexcept {
        return TypeName_ == ETypeName::Interval64;
    }

    TInterval64TypePtr TType::AsInterval64() const noexcept {
        return AsInterval64Raw()->AsPtr();
    }

    const TInterval64Type* TType::AsInterval64Raw() const noexcept {
        Y_ABORT_UNLESS(IsInterval64());
        return static_cast<const TInterval64Type*>(this);
    }

    bool TType::IsOptional() const noexcept {
        return TypeName_ == ETypeName::Optional;
    }

    TOptionalTypePtr TType::AsOptional() const noexcept {
        return AsOptionalRaw()->AsPtr();
    }

    const TOptionalType* TType::AsOptionalRaw() const noexcept {
        Y_ABORT_UNLESS(IsOptional());
        return static_cast<const TOptionalType*>(this);
    }

    bool TType::IsList() const noexcept {
        return TypeName_ == ETypeName::List;
    }

    TListTypePtr TType::AsList() const noexcept {
        return AsListRaw()->AsPtr();
    }

    const TListType* TType::AsListRaw() const noexcept {
        Y_ABORT_UNLESS(IsList());
        return static_cast<const TListType*>(this);
    }

    bool TType::IsDict() const noexcept {
        return TypeName_ == ETypeName::Dict;
    }

    TDictTypePtr TType::AsDict() const noexcept {
        return AsDictRaw()->AsPtr();
    }

    const TDictType* TType::AsDictRaw() const noexcept {
        Y_ABORT_UNLESS(IsDict());
        return static_cast<const TDictType*>(this);
    }

    bool TType::IsStruct() const noexcept {
        return TypeName_ == ETypeName::Struct;
    }

    TStructTypePtr TType::AsStruct() const noexcept {
        return AsStructRaw()->AsPtr();
    }

    const TStructType* TType::AsStructRaw() const noexcept {
        Y_ABORT_UNLESS(IsStruct());
        return static_cast<const TStructType*>(this);
    }

    bool TType::IsTuple() const noexcept {
        return TypeName_ == ETypeName::Tuple;
    }

    TTupleTypePtr TType::AsTuple() const noexcept {
        return AsTupleRaw()->AsPtr();
    }

    const TTupleType* TType::AsTupleRaw() const noexcept {
        Y_ABORT_UNLESS(IsTuple());
        return static_cast<const TTupleType*>(this);
    }

    bool TType::IsVariant() const noexcept {
        return TypeName_ == ETypeName::Variant;
    }

    TVariantTypePtr TType::AsVariant() const noexcept {
        return AsVariantRaw()->AsPtr();
    }

    const TVariantType* TType::AsVariantRaw() const noexcept {
        Y_ABORT_UNLESS(IsVariant());
        return static_cast<const TVariantType*>(this);
    }

    bool TType::IsTagged() const noexcept {
        return TypeName_ == ETypeName::Tagged;
    }

    TTaggedTypePtr TType::AsTagged() const noexcept {
        return AsTaggedRaw()->AsPtr();
    }

    const TTaggedType* TType::AsTaggedRaw() const noexcept {
        Y_ABORT_UNLESS(IsTagged());
        return static_cast<const TTaggedType*>(this);
    }

#ifdef __JETBRAINS_IDE__
#pragma clang diagnostic pop
#endif

    template <typename V>
    decltype(auto) TType::Visit(V&& visitor) const {
        switch (TypeName_) {
            case ETypeName::Bool:
                return std::forward<V>(visitor)(this->AsBool());
            case ETypeName::Int8:
                return std::forward<V>(visitor)(this->AsInt8());
            case ETypeName::Int16:
                return std::forward<V>(visitor)(this->AsInt16());
            case ETypeName::Int32:
                return std::forward<V>(visitor)(this->AsInt32());
            case ETypeName::Int64:
                return std::forward<V>(visitor)(this->AsInt64());
            case ETypeName::Uint8:
                return std::forward<V>(visitor)(this->AsUint8());
            case ETypeName::Uint16:
                return std::forward<V>(visitor)(this->AsUint16());
            case ETypeName::Uint32:
                return std::forward<V>(visitor)(this->AsUint32());
            case ETypeName::Uint64:
                return std::forward<V>(visitor)(this->AsUint64());
            case ETypeName::Float:
                return std::forward<V>(visitor)(this->AsFloat());
            case ETypeName::Double:
                return std::forward<V>(visitor)(this->AsDouble());
            case ETypeName::String:
                return std::forward<V>(visitor)(this->AsString());
            case ETypeName::Utf8:
                return std::forward<V>(visitor)(this->AsUtf8());
            case ETypeName::Date:
                return std::forward<V>(visitor)(this->AsDate());
            case ETypeName::Datetime:
                return std::forward<V>(visitor)(this->AsDatetime());
            case ETypeName::Timestamp:
                return std::forward<V>(visitor)(this->AsTimestamp());
            case ETypeName::TzDate:
                return std::forward<V>(visitor)(this->AsTzDate());
            case ETypeName::TzDatetime:
                return std::forward<V>(visitor)(this->AsTzDatetime());
            case ETypeName::TzTimestamp:
                return std::forward<V>(visitor)(this->AsTzTimestamp());
            case ETypeName::Interval:
                return std::forward<V>(visitor)(this->AsInterval());
            case ETypeName::Decimal:
                return std::forward<V>(visitor)(this->AsDecimal());
            case ETypeName::Json:
                return std::forward<V>(visitor)(this->AsJson());
            case ETypeName::Yson:
                return std::forward<V>(visitor)(this->AsYson());
            case ETypeName::Uuid:
                return std::forward<V>(visitor)(this->AsUuid());
            case ETypeName::Date32:
                return std::forward<V>(visitor)(this->AsDate32());
            case ETypeName::Datetime64:
                return std::forward<V>(visitor)(this->AsDatetime64());
            case ETypeName::Timestamp64:
                return std::forward<V>(visitor)(this->AsTimestamp64());
            case ETypeName::Interval64:
                return std::forward<V>(visitor)(this->AsInterval64());
            case ETypeName::Void:
                return std::forward<V>(visitor)(this->AsVoid());
            case ETypeName::Null:
                return std::forward<V>(visitor)(this->AsNull());
            case ETypeName::Optional:
                return std::forward<V>(visitor)(this->AsOptional());
            case ETypeName::List:
                return std::forward<V>(visitor)(this->AsList());
            case ETypeName::Dict:
                return std::forward<V>(visitor)(this->AsDict());
            case ETypeName::Struct:
                return std::forward<V>(visitor)(this->AsStruct());
            case ETypeName::Tuple:
                return std::forward<V>(visitor)(this->AsTuple());
            case ETypeName::Variant:
                return std::forward<V>(visitor)(this->AsVariant());
            case ETypeName::Tagged:
                return std::forward<V>(visitor)(this->AsTagged());
        }

        Y_UNREACHABLE();
    }

    template <typename V>
    decltype(auto) TType::VisitRaw(V&& visitor) const {
        switch (TypeName_) {
            case ETypeName::Bool:
                return std::forward<V>(visitor)(this->AsBoolRaw());
            case ETypeName::Int8:
                return std::forward<V>(visitor)(this->AsInt8Raw());
            case ETypeName::Int16:
                return std::forward<V>(visitor)(this->AsInt16Raw());
            case ETypeName::Int32:
                return std::forward<V>(visitor)(this->AsInt32Raw());
            case ETypeName::Int64:
                return std::forward<V>(visitor)(this->AsInt64Raw());
            case ETypeName::Uint8:
                return std::forward<V>(visitor)(this->AsUint8Raw());
            case ETypeName::Uint16:
                return std::forward<V>(visitor)(this->AsUint16Raw());
            case ETypeName::Uint32:
                return std::forward<V>(visitor)(this->AsUint32Raw());
            case ETypeName::Uint64:
                return std::forward<V>(visitor)(this->AsUint64Raw());
            case ETypeName::Float:
                return std::forward<V>(visitor)(this->AsFloatRaw());
            case ETypeName::Double:
                return std::forward<V>(visitor)(this->AsDoubleRaw());
            case ETypeName::String:
                return std::forward<V>(visitor)(this->AsStringRaw());
            case ETypeName::Utf8:
                return std::forward<V>(visitor)(this->AsUtf8Raw());
            case ETypeName::Date:
                return std::forward<V>(visitor)(this->AsDateRaw());
            case ETypeName::Datetime:
                return std::forward<V>(visitor)(this->AsDatetimeRaw());
            case ETypeName::Timestamp:
                return std::forward<V>(visitor)(this->AsTimestampRaw());
            case ETypeName::TzDate:
                return std::forward<V>(visitor)(this->AsTzDateRaw());
            case ETypeName::TzDatetime:
                return std::forward<V>(visitor)(this->AsTzDatetimeRaw());
            case ETypeName::TzTimestamp:
                return std::forward<V>(visitor)(this->AsTzTimestampRaw());
            case ETypeName::Interval:
                return std::forward<V>(visitor)(this->AsIntervalRaw());
            case ETypeName::Decimal:
                return std::forward<V>(visitor)(this->AsDecimalRaw());
            case ETypeName::Json:
                return std::forward<V>(visitor)(this->AsJsonRaw());
            case ETypeName::Yson:
                return std::forward<V>(visitor)(this->AsYsonRaw());
            case ETypeName::Uuid:
                return std::forward<V>(visitor)(this->AsUuidRaw());
            case ETypeName::Date32:
                return std::forward<V>(visitor)(this->AsDate32Raw());
            case ETypeName::Datetime64:
                return std::forward<V>(visitor)(this->AsDatetime64Raw());
            case ETypeName::Timestamp64:
                return std::forward<V>(visitor)(this->AsTimestamp64Raw());
            case ETypeName::Interval64:
                return std::forward<V>(visitor)(this->AsInterval64Raw());
            case ETypeName::Void:
                return std::forward<V>(visitor)(this->AsVoidRaw());
            case ETypeName::Null:
                return std::forward<V>(visitor)(this->AsNullRaw());
            case ETypeName::Optional:
                return std::forward<V>(visitor)(this->AsOptionalRaw());
            case ETypeName::List:
                return std::forward<V>(visitor)(this->AsListRaw());
            case ETypeName::Dict:
                return std::forward<V>(visitor)(this->AsDictRaw());
            case ETypeName::Struct:
                return std::forward<V>(visitor)(this->AsStructRaw());
            case ETypeName::Tuple:
                return std::forward<V>(visitor)(this->AsTupleRaw());
            case ETypeName::Variant:
                return std::forward<V>(visitor)(this->AsVariantRaw());
            case ETypeName::Tagged:
                return std::forward<V>(visitor)(this->AsTaggedRaw());
        }

        Y_UNREACHABLE();
    }

    template <typename V>
    decltype(auto) TPrimitiveType::VisitPrimitive(V&& visitor) const {
        switch (GetPrimitiveTypeName()) {
            case EPrimitiveTypeName::Bool:
                return std::forward<V>(visitor)(this->AsBool());
            case EPrimitiveTypeName::Int8:
                return std::forward<V>(visitor)(this->AsInt8());
            case EPrimitiveTypeName::Int16:
                return std::forward<V>(visitor)(this->AsInt16());
            case EPrimitiveTypeName::Int32:
                return std::forward<V>(visitor)(this->AsInt32());
            case EPrimitiveTypeName::Int64:
                return std::forward<V>(visitor)(this->AsInt64());
            case EPrimitiveTypeName::Uint8:
                return std::forward<V>(visitor)(this->AsUint8());
            case EPrimitiveTypeName::Uint16:
                return std::forward<V>(visitor)(this->AsUint16());
            case EPrimitiveTypeName::Uint32:
                return std::forward<V>(visitor)(this->AsUint32());
            case EPrimitiveTypeName::Uint64:
                return std::forward<V>(visitor)(this->AsUint64());
            case EPrimitiveTypeName::Float:
                return std::forward<V>(visitor)(this->AsFloat());
            case EPrimitiveTypeName::Double:
                return std::forward<V>(visitor)(this->AsDouble());
            case EPrimitiveTypeName::String:
                return std::forward<V>(visitor)(this->AsString());
            case EPrimitiveTypeName::Utf8:
                return std::forward<V>(visitor)(this->AsUtf8());
            case EPrimitiveTypeName::Date:
                return std::forward<V>(visitor)(this->AsDate());
            case EPrimitiveTypeName::Datetime:
                return std::forward<V>(visitor)(this->AsDatetime());
            case EPrimitiveTypeName::Timestamp:
                return std::forward<V>(visitor)(this->AsTimestamp());
            case EPrimitiveTypeName::TzDate:
                return std::forward<V>(visitor)(this->AsTzDate());
            case EPrimitiveTypeName::TzDatetime:
                return std::forward<V>(visitor)(this->AsTzDatetime());
            case EPrimitiveTypeName::TzTimestamp:
                return std::forward<V>(visitor)(this->AsTzTimestamp());
            case EPrimitiveTypeName::Interval:
                return std::forward<V>(visitor)(this->AsInterval());
            case EPrimitiveTypeName::Decimal:
                return std::forward<V>(visitor)(this->AsDecimal());
            case EPrimitiveTypeName::Json:
                return std::forward<V>(visitor)(this->AsJson());
            case EPrimitiveTypeName::Yson:
                return std::forward<V>(visitor)(this->AsYson());
            case EPrimitiveTypeName::Uuid:
                return std::forward<V>(visitor)(this->AsUuid());
            case EPrimitiveTypeName::Date32:
                return std::forward<V>(visitor)(this->AsDate32());
            case EPrimitiveTypeName::Datetime64:
                return std::forward<V>(visitor)(this->AsDatetime64());
            case EPrimitiveTypeName::Timestamp64:
                return std::forward<V>(visitor)(this->AsTimestamp64());
            case EPrimitiveTypeName::Interval64:
                return std::forward<V>(visitor)(this->AsInterval64());
        }

        Y_UNREACHABLE();
    }

    template <typename V>
    decltype(auto) TPrimitiveType::VisitPrimitiveRaw(V&& visitor) const {
        switch (GetPrimitiveTypeName()) {
            case EPrimitiveTypeName::Bool:
                return std::forward<V>(visitor)(this->AsBoolRaw());
            case EPrimitiveTypeName::Int8:
                return std::forward<V>(visitor)(this->AsInt8Raw());
            case EPrimitiveTypeName::Int16:
                return std::forward<V>(visitor)(this->AsInt16Raw());
            case EPrimitiveTypeName::Int32:
                return std::forward<V>(visitor)(this->AsInt32Raw());
            case EPrimitiveTypeName::Int64:
                return std::forward<V>(visitor)(this->AsInt64Raw());
            case EPrimitiveTypeName::Uint8:
                return std::forward<V>(visitor)(this->AsUint8Raw());
            case EPrimitiveTypeName::Uint16:
                return std::forward<V>(visitor)(this->AsUint16Raw());
            case EPrimitiveTypeName::Uint32:
                return std::forward<V>(visitor)(this->AsUint32Raw());
            case EPrimitiveTypeName::Uint64:
                return std::forward<V>(visitor)(this->AsUint64Raw());
            case EPrimitiveTypeName::Float:
                return std::forward<V>(visitor)(this->AsFloatRaw());
            case EPrimitiveTypeName::Double:
                return std::forward<V>(visitor)(this->AsDoubleRaw());
            case EPrimitiveTypeName::String:
                return std::forward<V>(visitor)(this->AsStringRaw());
            case EPrimitiveTypeName::Utf8:
                return std::forward<V>(visitor)(this->AsUtf8Raw());
            case EPrimitiveTypeName::Date:
                return std::forward<V>(visitor)(this->AsDateRaw());
            case EPrimitiveTypeName::Datetime:
                return std::forward<V>(visitor)(this->AsDatetimeRaw());
            case EPrimitiveTypeName::Timestamp:
                return std::forward<V>(visitor)(this->AsTimestampRaw());
            case EPrimitiveTypeName::TzDate:
                return std::forward<V>(visitor)(this->AsTzDateRaw());
            case EPrimitiveTypeName::TzDatetime:
                return std::forward<V>(visitor)(this->AsTzDatetimeRaw());
            case EPrimitiveTypeName::TzTimestamp:
                return std::forward<V>(visitor)(this->AsTzTimestampRaw());
            case EPrimitiveTypeName::Interval:
                return std::forward<V>(visitor)(this->AsIntervalRaw());
            case EPrimitiveTypeName::Decimal:
                return std::forward<V>(visitor)(this->AsDecimalRaw());
            case EPrimitiveTypeName::Json:
                return std::forward<V>(visitor)(this->AsJsonRaw());
            case EPrimitiveTypeName::Yson:
                return std::forward<V>(visitor)(this->AsYsonRaw());
            case EPrimitiveTypeName::Uuid:
                return std::forward<V>(visitor)(this->AsUuidRaw());
            case NTi::EPrimitiveTypeName::Date32:
                return std::forward<V>(visitor)(this->AsDate32Raw());
            case NTi::EPrimitiveTypeName::Datetime64:
                return std::forward<V>(visitor)(this->AsDatetime64Raw());
            case NTi::EPrimitiveTypeName::Timestamp64:
                return std::forward<V>(visitor)(this->AsTimestamp64Raw());
            case NTi::EPrimitiveTypeName::Interval64:
                return std::forward<V>(visitor)(this->AsInterval64Raw());
        }

        Y_UNREACHABLE();
    }

    template <typename V>
    decltype(auto) TVariantType::VisitUnderlying(V&& visitor) const {
        switch (GetUnderlyingTypeRaw()->GetTypeName()) {
            case ETypeName::Struct:
                return std::forward<V>(visitor)(this->GetUnderlyingTypeRaw()->AsStruct());
            case ETypeName::Tuple:
                return std::forward<V>(visitor)(this->GetUnderlyingTypeRaw()->AsTuple());
            default:
                Y_UNREACHABLE();
        }
    }

    template <typename V>
    decltype(auto) TVariantType::VisitUnderlyingRaw(V&& visitor) const {
        switch (GetUnderlyingTypeRaw()->GetTypeName()) {
            case ETypeName::Struct:
                return std::forward<V>(visitor)(this->GetUnderlyingTypeRaw()->AsStructRaw());
            case ETypeName::Tuple:
                return std::forward<V>(visitor)(this->GetUnderlyingTypeRaw()->AsTupleRaw());
            default:
                Y_UNREACHABLE();
        }
    }
} // namespace NTi
