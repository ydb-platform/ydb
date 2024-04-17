#pragma once

//! @file type_factory.h
//!
//! Type factory creates type instances and manages their lifetimes and destruction.
//!
//! Type info supports multiple ways of allocating type instances and managing their lifetimes:
//!
//! - [heap-based allocation] is the standard memory allocation method for C++. It offers great flexibility
//!   as it can allocate memory regions of almost arbitrary size and alignment, deallocate them, re-use them
//!   for new allocations or return the to the operating system. This flexibility comes with a price, though.
//!   Heap allocators usually require synchronisation (i.e. a mutex or a spinlock), and some memory overhead to track
//!   allocated regions. Also, they provide no guarantees on data locality and CPU cache friendliness whatsoever.
//!
//!   When using a heap-based factory, each type instance have a separate reference counter. `TTypePtr`s will
//!   increment and decrement this counter. Whenever it reaches zero, they will destroy the type instance and free
//!   its memory. This is the standard reference counting technique, just like in `TRefCounted`.
//!
//! - [memory pool] is a data structure for faster memory allocation. It pre-allocates large chunks of memory and uses
//!   them to allocate smaller objects. This way, we don't have to `malloc` memory for each new object.
//!   Also, we can drop all objects at once by discarding all memory chunks.
//!
//!   When using memory-pool-based factory, types don't have individual reference counters. Instead, they store
//!   a pointer to the factory that've created them. Whenever such type is referenced or unreferenced, the factory's
//!   own reference counter is incremented or decremented. Factory dies and releases all pool's memory when the last
//!   reference to a type in its pool dies.
//!
//! [heap-based allocation]: https://en.wikipedia.org/wiki/Memory_management#Dynamic_memory_allocation
//! [Reference counting]: https://en.wikipedia.org/wiki/Reference_counting
//! [memory pool]: https://en.wikipedia.org/wiki/Region-based_memory_management
//!
//! The rule of thumb is: if you have an intrusive pointer to a type, you can be sure it points to an alive object;
//! if you have a raw pointer to a type, it's your responsibility to ensure its validity.
//!
//! Whenever you have a valid raw pointer to a type, you can promote it to an intrusive pointer by calling `AsPtr`.
//! This is always safe because there's no way to create a type instance on the stack or in the static memory
//! of a program.
//!
//!
//! # Implementation details
//!
//! We're building a hierarchy of classes that work equally well with both heap-based allocation model
//! and memory-pool-based allocation model. In order to understand how we do it, we should first understand how
//! object ownership works in both models.
//!
//!
//! ## Ownership schema
//!
//! Depending on the chosen factory implementation, ownership schema may vary.
//!
//! When using the heap-based factory, each type has its own reference counter. User own types, and types own
//! their nested types. For `Optional<String>`, user owns the `Optional` type, and `Optional` type
//! owns the `String` type:
//!
//! ```text
//!  Legend:      A ───> B -- A owns B   |   A ─ ─> B -- A points to B
//!
//!  User code:             Objects:
//! ┌─────────────┐        ┌────────────────┐
//! │ IFactoryPtr │───────>│ Factory        │
//! └─────────────┘        └────────────────┘
//! ┌─────────────┐        ┌────────────────┐
//! │ TTypePtr    │───────>│ Type: Optional │──┐
//! └─────────────┘        └────────────────┘  │
//!                     ┌──────────────────────┘
//!                     │  ┌────────────────┐
//!                     └─>│ Type: String   │
//!                        └────────────────┘
//! ```
//!
//! When using a pool-based factory, all allocated types are owned by pool. So, in the `Optional<String>` example,
//! user owns factory, factory owns the `Optional` type and the `String` type. All references between types
//! are just borrows:
//!
//! ```text
//!  Legend:      A ───> B -- A owns B   |   A ─ ─> B -- A points to B
//!
//!  User code:             Objects:
//! ┌─────────────┐        ┌────────────────┐
//! │ IFactoryPtr │─┌─────>│ Factory        │──┐
//! └─────────────┘ │      └────────────────┘  │
//! ┌─────────────┐ │      ┌────────────────┐  │
//! │ TTypePtr    │─┘─ ─ ─>│ Type: Optional │<─┤
//! └─────────────┘        └───────┬────────┘  │
//!                   ┌ ─ ─ ─ ─ ─ ─┘           │
//!                   ╎    ┌────────────────┐  │
//!                   └─ ─>│ Type: String   │<─┘
//!                        └────────────────┘
//! ```
//!
//! Notice how `TTypePtr` points to `Optional`, but doesn't own it. Instead, it owns the factory which,
//! transitively, owns `Optional`. This way we can be sure that the factory will not be destroyed before
//! all `IFactoryPtr`s and `TTypePtr`s die, thus guaranteeing that all `TTypePtr` always stay valid.
//!
//! With that in mind, we can derive two types of ownership here.
//!
//! Whenever `TTypePtr` owns some type, we call it 'external ownership' (because it's code that is external
//! to this library owns something). In this situation, ref and unref procedures must increment type's own reference
//! counter, if there is one, and also increment factory's reference counter. `NTi::TType::Ref` and `NTi::TType::Unref`
//! do this by calling `NTi::ITypeFactoryInternal::Ref`, `NTi::ITypeFactoryInternal::RefType`,
//! `NTi::ITypeFactoryInternal::UnRef`, `NTi::ITypeFactoryInternal::UnRefType`.
//!
//! Whenever one type owns another type, we call it 'internal ownership'. In this situation, ref and unref procedures
//! must only increment and decrement type's own reference counter. They should *not* increment or decrement
//! factory's reference counter. `NTi::TType::RefSelf` and `NTi::TType::UnrefSelf` do this by calling
//! `NTi::ITypeFactoryInternal::RefType` and `NTi::ITypeFactoryInternal::UnRefType`, and not calling
//! `NTi::ITypeFactoryInternal::Ref` and `NTi::ITypeFactoryInternal::UnRef`.
//!
//!
//! ## Adoption semantics
//!
//! Let's see now what should happen when we create some complex type using more that one factory.
//!
//! Suppose we're creating a container type, such as `Optional`, using factory `a`. If its nested type is
//! managed by the same factory, we have no issues at all:
//!
//! ```
//! auto a = NTi::PoolFactory();
//! auto string = a->String();
//! auto optional = a->Optional(string);
//! ```
//!
//! But what if the nested type is managed by some other factory `b`? Consider:
//!
//! ```
//! auto b = NTi::PoolFactory();
//! auto a = NTi::PoolFactory();
//! auto string = b->String();
//! auto optional = a->Optional(string);
//! ```
//!
//! Well, we're in trouble. It's not enough for `optional` to just acquire internal ownership over `string`.
//! Actually, `optional` has to own both `string` and its factory, `b`. Otherwise, if `b` dies, `string` will
//! die with it, leaving `optional` with a dangling pointer.
//!
//! However, we can't have a type owning its factory. It will create a loop. The only safe solution is to copy
//! the nested type from one factory to another. If we have some type managed by factory `a`, all nested types
//! must also be managed by the same factory `a`.
//!
//! So, whenever we create a type, we must ensure that all its nested types are managed by the same factory.
//! If they're not, we must deep-copy them to the current factory. This is called the 'adoption semantics'. That is,
//! we adopt nested types into the current factory.
//!
//! In fact, adoption semantics is a bit more complicated that this. Some types are not managed by any factory.
//! They have static storage duration and therefore need no management. So, if we see that there's no factory
//! associated with some type, we don't perform deep-copy. Instead, we just return it as is.
//!
//!
//! ## Overview
//!
//! So, now we're ready to put this puzzle together.
//!
//! When we create a new type instance, we do the following:
//!
//! 1: we adopt all nested types and acquire internal ownership over them. This is done by the `Own` function;
//! 2: we allocate some memory for the type. This is done by `New`, `Allocate` and other functions;
//! 3: we initialize all this memory;
//! 4: finally, we acquire external ownership over the newly created type and return it to the user.
//!
//! Steps 1 through 3 are performed by the `NTi::TType::Clone` function. Step 4 is performed by the factory.
//!
//! When we adopt some type, we check if it's managed by any factory other that the current one. If needed,
//! we deep-copy it by the `NTi::TType::Clone` function. That is, `Clone` copies type to a new factory and adopts
//! nested types, adoption calls nested type's `Clone`, causing recursive deep-copying.
//!
//! When we release external or internal ownership over some type, factory might decide to destroy it (if, for
//! example, this type's reference count reached zero). In this case, we need to release internal ownership
//! over the nested types, and release all memory that was allocated in step 3.
//! This is done by `NTi::TType::Drop` function.

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/memory/pool.h>

#include "type.h"

#include "fwd.h"

namespace NTi {
    namespace NPrivate {
        /// If `T` is derived from `NTi::TType` (ignoring cv-qualification), provides the member constant `value`
        /// equal to `true`. Otherwise `value` is `false`.
        template <typename T>
        struct TIsType: public std::is_base_of<TType, T> {
        };

        /// Helper template variable for `TIsType`.
        template <typename T>
        inline constexpr const bool IsTypeV = TIsType<T>::value;

        /// Statically assert that `T` is a managed object.
        template <typename T>
        void AssertIsType() {
            static_assert(IsTypeV<T>, "object should be derived from 'NTi::TType'");
        }
    }

    /// Internal interface for type factory.
    ///
    /// This interface is accessible from `TType` implementation, but not from outside.
    class ITypeFactoryInternal {
        friend class TType;
        template <typename T>
        friend class ::TDefaultIntrusivePtrOps;

    public:
        virtual ~ITypeFactoryInternal() = default;

    public:
        /// @name Object creation and lifetime management interface
        ///
        /// @{
        //-
        /// Create a new object of type `T` by allocating memory for it and constructing it with arguments `args`.
        ///
        /// If `T` is derived from `TType`, it will be linked to this factory via the `NTi::TType::SetFactory` method.
        ///
        /// This method does not acquire any ownership over the created object, not internal nor external. It just
        /// allocates some memory and calls a constructor and that's it.
        template <typename T, typename... Args>
        inline T* New(Args&&... args) {
            // Note: it's important to understand difference between `New` and `Create` - read the docs!

            static_assert(std::is_trivially_destructible_v<T>, "can only create trivially destructible types");

            auto value = new (AllocateFor<T>()) T(std::forward<Args>(args)...);

            if constexpr (NPrivate::IsTypeV<T>) {
                value->SetFactory(this);
            }

            return value;
        }

        /// Delete an object that was created via the `New` function.
        ///
        /// This function essentially just calls `Free`. Note that `New` can only create trivially destructible types,
        /// thus `Delete` does not neet to call object's destructor.
        template <typename T>
        inline void Delete(T* obj) noexcept {
            Free(obj);
        }

        /// Create a new array of `count` objects of type `T`.
        ///
        /// This function will allocate an array of `count` objects of type `T`. Then, for each object in the array,
        /// it will call the `ctor(T* memory, size_t index)`, passing a pointer to the uninitialized object
        /// and its index in the array. `ctor` should call placement new on the passed pointer.
        ///
        /// If `T` is derived from `TType`, it will be linked to this factory via the `NTi::TType::SetFactory` method.
        ///
        /// This method does not acquire any ownership over the created array elements, not internal nor external.
        /// It just allocates some memory and calls a constructor and that's it.
        ///
        ///
        /// # Examples
        ///
        /// Copy vector to factory:
        ///
        /// ```
        /// TVector<T> source = ...;
        /// TArrayRef<T> copied = NMem::NewArray<T>(factory, source.size(), [&source](T* ptr, size_t i) {
        ///     new (ptr) T(source[i]);
        /// });
        /// ```
        template <typename T, typename TCtor>
        inline TArrayRef<T> NewArray(size_t count, TCtor&& ctor) {
            // Note: it's important to understand difference between `New` and `Create` - read the docs!

            static_assert(std::is_trivially_destructible_v<T>, "can only create trivially destructible types");

            auto items = AllocateArrayFor<T>(count);

            for (size_t i = 0; i < count; ++i) {
                auto value = items + i;
                ctor(value, i);
                if constexpr (NPrivate::IsTypeV<T>) {
                    value->SetFactory(this);
                }
            }

            return TArrayRef(items, count);
        }

        /// Delete an object that was created via the `NewArray` function.
        ///
        /// This function runs `dtor` on each element of the array and then just calls `Free`. Note that `NewArray`
        /// can only create arrays for trivially destructible types, thus `DeleteArray`
        /// does not call object destructors.
        template <typename T, typename TDtor>
        inline void DeleteArray(TArrayRef<T> obj, TDtor&& dtor) noexcept {
            for (size_t i = 0; i < obj.size(); ++i) {
                auto value = obj.data() + i;
                dtor(value, i);
            }

            Free(const_cast<void*>(static_cast<const void*>(obj.data())));
        }

        /// Adopt `type` into this factory.
        ///
        /// This function works like `AdoptRaw`, but it wraps its result to an intrusive pointer, thus acquiring
        /// external ownership over it.
        ///
        /// It is used by external code to copy types between factories.
        template <typename T>
        inline TIntrusiveConstPtr<T> Adopt(TIntrusiveConstPtr<T> type) noexcept {
            NPrivate::AssertIsType<T>();

            return TIntrusiveConstPtr<T>(const_cast<T*>(AdoptRaw<T>(type.Get())));
        }

        /// Adopt `type` into this factory.
        ///
        /// This function is used to transfer types between factories. It has the following semantics:
        ///
        /// - if `type` is managed by this factory, this function returns its argument unchanged;
        /// - if `type` is not managed by any factory, this function also returns its argument unchanged;
        /// - if `type` is managed by another factory, this function deep-copies `type` into this factory and returns
        ///   a pointer to the new deep-copied value.
        ///
        /// This function uses `NTi::TType::Clone` to deep-copy types. It does not acquire any ownership over
        /// the returned object, not internal nor external. If you need to adopt and acquire external ownership
        /// (i.e. you're returning type to a user), use `Adopt`. If you need to adopt and acquire internal ownership
        /// (i.e. you're writing `NTi::TType::Clone` implementation), use `Own`.
        template <typename T>
        inline const T* AdoptRaw(const T* type) noexcept {
            NPrivate::AssertIsType<T>();

            ITypeFactoryInternal* typeFactory = type->GetFactory();
            if (typeFactory == nullptr || typeFactory == this) {
                return type;
            } else {
                return type->Clone(*this);
            }
        }

        /// Adopt some type and acquire internal ownership over it.
        ///
        /// This function works like `AdoptRaw`, but acquires internal ownership over the returned type.
        ///
        /// It is used by `NTi::TType::Clone` implementations to acquire ownership over the nested types.
        template <typename T>
        inline const T* Own(const T* type) noexcept {
            NPrivate::AssertIsType<T>();

            type = AdoptRaw(type);
            const_cast<T*>(type)->RefSelf();
            return type;
        }

        /// Release internal ownership over some type.
        ///
        /// This function is used by `NTi::TType::Drop` to release internal ownership over nested types.
        template <typename T>
        inline void Disown(const T* type) noexcept {
            NPrivate::AssertIsType<T>();

            const_cast<T*>(type)->UnRefSelf();
        }

        /// @}

    public:
        /// @name Memory management interface
        ///
        /// Functions for dealing with raw unmanaged memory.
        ///
        /// @{
        //-
        /// Allocate a new chunk of memory of size `size` aligned on `align`.
        ///
        /// The behaviour is undefined if `size` is zero, `align` is zero, is not a power of two,
        /// or greater than `PLATFORM_DATA_ALIGN` (i.e., over-aligned).
        ///
        /// This function panics if memory can't be allocated.
        ///
        /// Returned memory may or may not be initialized.
        virtual void* Allocate(size_t size, size_t align) noexcept = 0;

        /// Allocate a chunk of memory suitable for storing an object of type `T`.
        ///
        /// This function panics if memory can't be allocated.
        ///
        /// Returned memory may or may not be initialized.
        template <typename T>
        T* AllocateFor() noexcept {
            static_assert(alignof(T) <= PLATFORM_DATA_ALIGN, "over-aligned types are not supported");
            return static_cast<T*>(Allocate(sizeof(T), alignof(T)));
        }

        /// Allocate a new chunk of memory suitable for storing `count` of objects of size `size` aligned on `align`.
        ///
        /// The behaviour is undefined if `count` is zero, `size` is zero, `align` is zero, is not a power of two,
        /// or greater than `PLATFORM_DATA_ALIGN` (i.e., over-aligned).
        ///
        /// This function panics if memory can't be allocated.
        ///
        /// Returned memory may or may not be initialized.
        void* AllocateArray(size_t count, size_t size, size_t align) noexcept {
            return Allocate(size * count, align);
        }

        /// Allocate a chunk of memory suitable for storing an array of `count` objects of type `T`.
        ///
        /// This function panics if memory can't be allocated.
        ///
        /// Returned memory may or may not be initialized.
        template <typename T>
        T* AllocateArrayFor(size_t count) noexcept {
            static_assert(alignof(T) <= PLATFORM_DATA_ALIGN, "over-aligned types are not supported");
            return static_cast<T*>(AllocateArray(count, sizeof(T), alignof(T)));
        }

        /// Reclaim a chunk of memory memory that was allocated via one of the above `Allocate*` functions.
        ///
        /// This function is not suitable for freeing memory allocated via `AllocateString` or `AllocateStringMaybe`.
        ///
        /// Behaviour of this function varies between implementations. Specifically, in the memory-pool-based factory,
        /// this function does nothing, while in heap-based factory, it calls `free`.
        ///
        /// Passing a null pointer here is ok and does nothing.
        ///
        /// The behaviour is undefined if the given pointer is not null and it wasn't obtained from a call
        /// to the `Allocate` function or it was obtained from a call to the `Allocate` function
        /// of a different factory.
        virtual void Free(void* data) noexcept = 0;

        /// Allocate memory for string `str` and copy `str` to the allocated memory.
        ///
        /// Note: the returned string is not guaranteed to be null-terminated.
        ///
        /// Note: some factories may cache results of this function to avoid repeated allocations. Read documentation
        /// for specific factory for more info.
        virtual TStringBuf AllocateString(TStringBuf str) noexcept {
            return TStringBuf(MemCopy(AllocateArrayFor<char>(str.size()), str.data(), str.size()), str.size());
        }

        /// Reclaim a chunk of memory memory that was allocated via the `AllocateString` function.
        virtual void FreeString(TStringBuf str) noexcept {
            Free(const_cast<char*>(str.Data()));
        }

        /// Like `AllocateString`, but works with `TMaybe<TStringBuf>`.
        ///
        /// If the given `str` contains a value, pass it to `AllocateString`.
        /// If it contains no value, return an empty `TMaybe`.
        TMaybe<TStringBuf> AllocateStringMaybe(TMaybe<TStringBuf> str) noexcept {
            if (str.Defined()) {
                return AllocateString(str.GetRef());
            } else {
                return Nothing();
            }
        }

        /// Reclaim a chunk of memory memory that was allocated via the `AllocateStringMaybe` function.
        void FreeStringMaybe(TMaybe<TStringBuf> str) noexcept {
            if (str.Defined()) {
                FreeString(str.GetRef());
            }
        }

        /// @}

    public:
        /// @name Type caching and deduplication interface
        ///
        /// @{
        //-
        /// Lookup the given type in the factory cache. Return cached version of the type or `nullptr`.
        virtual const TType* LookupCache(const TType* type) noexcept = 0;

        /// Save the given type to the factory cache. Type must be allocated via this factory.
        virtual void SaveCache(const TType* type) noexcept = 0;

        /// @}

    public:
        /// @name Factory reference counting interface
        ///
        /// @{
        //-
        /// Increase reference count of this factory.
        virtual void Ref() noexcept = 0;

        /// Decrease reference count of this factory.
        /// If reference count reaches zero, drop this factory.
        virtual void UnRef() noexcept = 0;

        /// Decrease reference count of this factory.
        /// If reference count reaches zero, panic.
        virtual void DecRef() noexcept = 0;

        /// Get current reference count of this factory.
        virtual long RefCount() const noexcept = 0;

        /// @}

    public:
        /// @name Type instance reference counting interface
        ///
        /// @{
        //-
        /// Increase reference count of some object managed by this factory.
        virtual void RefType(TType*) noexcept = 0;

        /// Decrease reference count of some object managed by this factory.
        /// If reference count reaches zero, call `TType::Drop` and free object's memory, if needed.
        virtual void UnRefType(TType*) noexcept = 0;

        /// Decrease reference count of some object managed by this factory.
        /// If reference count reaches zero, panic.
        virtual void DecRefType(TType*) noexcept = 0;

        /// Get current reference count of some object managed by this factory.
        virtual long RefCountType(const TType*) const noexcept = 0;

        /// @}
    };

    /// Base interface for type factory. There are functions to create type instances, and also a function to
    /// adopt a type from one factory to another. See the file-level documentation for more info.
    class ITypeFactory: protected ITypeFactoryInternal {
        friend class TType;
        template <typename T>
        friend class ::TDefaultIntrusivePtrOps;

    public:
        /// Adopt type into this factory.
        ///
        /// This function is used to transfer types between factories. It has the following semantics:
        ///
        /// - if `type` is managed by this factory, this function returns its argument unchanged;
        /// - if `type` is not managed by any factory, this function also returns its argument unchanged;
        /// - if `type` is managed by another factory, this function deep-copies `type` into this factory and returns
        ///   a pointer to the new deep-copied value.
        ///
        /// This function should be used on API borders, whenever you receive ownership over some type that wasn't
        /// created by you. For example, you may ensure that the type you got is allocated in heap, or you may want to
        /// copy some type into a memory pool that you have control over.
        template <typename T>
        inline TIntrusiveConstPtr<T> Adopt(TIntrusiveConstPtr<T> value) noexcept {
            return ITypeFactoryInternal::Adopt<T>(std::move(value));
        }

    public:
        /// Create a new instance of a type using this factory.
        /// @{
        //-
        /// Create a new `Void` type. See `NTi::TVoidType` for more info.
        TVoidTypePtr Void();

        /// Create a new `Null` type. See `NTi::TNullType` for more info.
        TNullTypePtr Null();

        /// Create a new `Bool` type. See `NTi::TBoolType` for more info.
        TBoolTypePtr Bool();

        /// Create a new `Int8` type. See `NTi::TInt8Type` for more info.
        TInt8TypePtr Int8();

        /// Create a new `Int16` type. See `NTi::TInt16Type` for more info.
        TInt16TypePtr Int16();

        /// Create a new `Int32` type. See `NTi::TInt32Type` for more info.
        TInt32TypePtr Int32();

        /// Create a new `Int64` type. See `NTi::TInt64Type` for more info.
        TInt64TypePtr Int64();

        /// Create a new `Uint8` type. See `NTi::TUint8Type` for more info.
        TUint8TypePtr Uint8();

        /// Create a new `Uint16` type. See `NTi::TUint16Type` for more info.
        TUint16TypePtr Uint16();

        /// Create a new `Uint32` type. See `NTi::TUint32Type` for more info.
        TUint32TypePtr Uint32();

        /// Create a new `Uint64` type. See `NTi::TUint64Type` for more info.
        TUint64TypePtr Uint64();

        /// Create a new `Float` type. See `NTi::TFloatType` for more info.
        TFloatTypePtr Float();

        /// Create a new `Double` type. See `NTi::TDoubleType` for more info.
        TDoubleTypePtr Double();

        /// Create a new `String` type. See `NTi::TStringType` for more info.
        TStringTypePtr String();

        /// Create a new `Utf8` type. See `NTi::TUtf8Type` for more info.
        TUtf8TypePtr Utf8();

        /// Create a new `Date` type. See `NTi::TDateType` for more info.
        TDateTypePtr Date();

        /// Create a new `Datetime` type. See `NTi::TDatetimeType` for more info.
        TDatetimeTypePtr Datetime();

        /// Create a new `Timestamp` type. See `NTi::TTimestampType` for more info.
        TTimestampTypePtr Timestamp();

        /// Create a new `TzDate` type. See `NTi::TTzDateType` for more info.
        TTzDateTypePtr TzDate();

        /// Create a new `TzDatetime` type. See `NTi::TTzDatetimeType` for more info.
        TTzDatetimeTypePtr TzDatetime();

        /// Create a new `TzTimestamp` type. See `NTi::TTzTimestampType` for more info.
        TTzTimestampTypePtr TzTimestamp();

        /// Create a new `Interval` type. See `NTi::TIntervalType` for more info.
        TIntervalTypePtr Interval();

        /// Create a new `Decimal` type. See `NTi::TDecimalType` for more info.
        TDecimalTypePtr Decimal(ui8 precision, ui8 scale);

        /// Create a new `Json` type. See `NTi::TJsonType` for more info.
        TJsonTypePtr Json();

        /// Create a new `Yson` type. See `NTi::TYsonType` for more info.
        TYsonTypePtr Yson();

        /// Create a new `Uuid` type. See `NTi::TUuidType` for more info.
        TUuidTypePtr Uuid();

        /// Create a new `Date32` type. See `NTi::TDate32Type` for more info.
        TDate32TypePtr Date32();

        /// Create a new `Datetime64` type. See `NTi::TDatetime64Type` for more info.
        TDatetime64TypePtr Datetime64();

        /// Create a new `Timestamp64` type. See `NTi::TTimestamp64Type` for more info.
        TTimestamp64TypePtr Timestamp64();

        /// Create a new `Interval64` type. See `NTi::TInterval64Type` for more info.
        TInterval64TypePtr Interval64();

        /// Create a new `Optional` type. See `NTi::TOptionalType` for more info.
        /// If `item` is managed by some other factory, it will be deep-copied into this factory.
        TOptionalTypePtr Optional(TTypePtr item);

        /// Create a new `List` type. See `NTi::TListType` for more info.
        /// If `item` is managed by some other factory, it will be deep-copied into this factory.
        TListTypePtr List(TTypePtr item);

        /// Create a new `Dict` type. See `NTi::TDictType` for more info.
        /// If `key` or `value` are managed by some other factory, they will be deep-copied into this factory.
        TDictTypePtr Dict(TTypePtr key, TTypePtr value);

        /// Create a new `Struct` type. See `NTi::TStructType` for more info.
        /// If item types are managed by some other factory, they will be deep-copied into this factory.
        TStructTypePtr Struct(TStructType::TOwnedMembers items);
        TStructTypePtr Struct(TMaybe<TStringBuf> name, TStructType::TOwnedMembers items);

        /// Create a new `Tuple` type. See `NTi::TTupleType` for more info.
        /// If item types are managed by some other factory, they will be deep-copied into this factory.
        TTupleTypePtr Tuple(TTupleType::TOwnedElements items);
        TTupleTypePtr Tuple(TMaybe<TStringBuf> name, TTupleType::TOwnedElements items);

        /// Create a new `Variant` type. See `NTi::TVariantType` for more info.
        /// If `inner` is managed by some other factory, it will be deep-copied into this factory.
        TVariantTypePtr Variant(TTypePtr inner);
        TVariantTypePtr Variant(TMaybe<TStringBuf> name, TTypePtr inner);

        /// Create a new `Tagged` type. See `NTi::TTaggedType` for more info.
        /// If `type` is managed by some other factory, it will be deep-copied into this factory.
        TTaggedTypePtr Tagged(TTypePtr type, TStringBuf tag);

        /// @}
    };

    /// Type factory over a memory pool.
    ///
    /// This interface provides some additional info related to the underlying memory pool state.
    ///
    /// Also, since all types that're created via this factory will live exactly as long as this factory lives,
    /// one doesn't have to actually refcount them. Thus, it is safe to work with raw pointers instead of intrusive
    /// pointers when using this kind of factory. So we provide the 'raw' interface that creates type instances
    /// as usual, but doesn't increment any reference counters, thus saving some atomic operations. It still
    /// adopts nested types into this factory, though.
    class IPoolTypeFactory: public ITypeFactory {
        friend class TNamedTypeBuilderRaw;
        friend class TStructBuilderRaw;
        friend class TTupleBuilderRaw;
        friend class TTaggedBuilderRaw;

    public:
        /// Memory available in the current chunk.
        ///
        /// Allocating object of size greater than this number will cause allocation of a new chunk. When this happens,
        /// available memory in the current chunk is lost, i.e. it'll never be used again.
        virtual size_t Available() const noexcept = 0;

        /// Number of bytes that're currently used by some object (i.e. they were allocated).
        ///
        /// Total memory consumed by arena is `MemoryAllocated() + MemoryWaste()`.
        virtual size_t MemoryAllocated() const noexcept = 0;

        /// Number of bytes that're not used by anyone.
        ///
        /// Total memory consumed by arena is `MemoryAllocated() + MemoryWaste()`.
        ///
        /// Memory that's lost and will not be reused is `MemoryWaste() - Available()`.
        virtual size_t MemoryWaste() const noexcept = 0;

    public:
        /// Adopt type into this factory.
        ///
        /// This works like `ITypeFactory::Adopt`, but returns a raw pointer.
        template <typename T>
        inline const T* AdoptRaw(const T* value) noexcept {
            return ITypeFactoryInternal::AdoptRaw<T>(value);
        }

    public:
        /// Raw versions of type constructors. See the class-level documentation for more info.
        ///
        /// @{
        //-
        /// Create a new `Void` type. See `NTi::TVoidType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TVoidType* VoidRaw();

        /// Create a new `Null` type. See `NTi::TNullType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TNullType* NullRaw();

        /// Create a new `Bool` type. See `NTi::TBoolType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TBoolType* BoolRaw();

        /// Create a new `Int8` type. See `NTi::TInt8Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TInt8Type* Int8Raw();

        /// Create a new `Int16` type. See `NTi::TInt16Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TInt16Type* Int16Raw();

        /// Create a new `Int32` type. See `NTi::TInt32Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TInt32Type* Int32Raw();

        /// Create a new `Int64` type. See `NTi::TInt64Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TInt64Type* Int64Raw();

        /// Create a new `Uint8` type. See `NTi::TUint8Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TUint8Type* Uint8Raw();

        /// Create a new `Uint16` type. See `NTi::TUint16Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TUint16Type* Uint16Raw();

        /// Create a new `Uint32` type. See `NTi::TUint32Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TUint32Type* Uint32Raw();

        /// Create a new `Uint64` type. See `NTi::TUint64Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TUint64Type* Uint64Raw();

        /// Create a new `Float` type. See `NTi::TFloatType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TFloatType* FloatRaw();

        /// Create a new `Double` type. See `NTi::TDoubleType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDoubleType* DoubleRaw();

        /// Create a new `String` type. See `NTi::TStringType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TStringType* StringRaw();

        /// Create a new `Utf8` type. See `NTi::TUtf8Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TUtf8Type* Utf8Raw();

        /// Create a new `Date` type. See `NTi::TDateType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDateType* DateRaw();

        /// Create a new `Datetime` type. See `NTi::TDatetimeType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDatetimeType* DatetimeRaw();

        /// Create a new `Timestamp` type. See `NTi::TTimestampType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTimestampType* TimestampRaw();

        /// Create a new `TzDate` type. See `NTi::TTzDateType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTzDateType* TzDateRaw();

        /// Create a new `TzDatetime` type. See `NTi::TTzDatetimeType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTzDatetimeType* TzDatetimeRaw();

        /// Create a new `TzTimestamp` type. See `NTi::TTzTimestampType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTzTimestampType* TzTimestampRaw();

        /// Create a new `Interval` type. See `NTi::TIntervalType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TIntervalType* IntervalRaw();

        /// Create a new `Decimal` type. See `NTi::TDecimalType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDecimalType* DecimalRaw(ui8 precision, ui8 scale);

        /// Create a new `Json` type. See `NTi::TJsonType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TJsonType* JsonRaw();

        /// Create a new `Yson` type. See `NTi::TYsonType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TYsonType* YsonRaw();

        /// Create a new `Uuid` type. See `NTi::TUuidType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TUuidType* UuidRaw();

        /// Create a new `Date32` type. See `NTi::TDate32Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDate32Type* Date32Raw();

        /// Create a new `Datetime64` type. See `NTi::TDatetime64Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDatetime64Type* Datetime64Raw();

        /// Create a new `Timestamp64` type. See `NTi::TTimestamp64Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTimestamp64Type* Timestamp64Raw();

        /// Create a new `Interval64` type. See `NTi::TInterval64Type` for more info.
        /// The returned object will live for as long as this factory lives.
        const TInterval64Type* Interval64Raw();

        /// Create a new `Optional` type. See `NTi::TOptionalType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TOptionalType* OptionalRaw(const TType* item);

        /// Create a new `List` type. See `NTi::TListType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TListType* ListRaw(const TType* item);

        /// Create a new `Dict` type. See `NTi::TDictType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TDictType* DictRaw(const TType* key, const TType* value);

        /// Create a new `Struct` type. See `NTi::TStructType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TStructType* StructRaw(TStructType::TMembers items);
        const TStructType* StructRaw(TMaybe<TStringBuf> name, TStructType::TMembers items);

        /// Create a new `Tuple` type. See `NTi::TTupleType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTupleType* TupleRaw(TTupleType::TElements items);
        const TTupleType* TupleRaw(TMaybe<TStringBuf> name, TTupleType::TElements items);

        /// Create a new `Variant` type. See `NTi::TVariantType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TVariantType* VariantRaw(const TType* inner);
        const TVariantType* VariantRaw(TMaybe<TStringBuf> name, const TType* inner);

        /// Create a new `Tagged` type. See `NTi::TTaggedType` for more info.
        /// The returned object will live for as long as this factory lives.
        const TTaggedType* TaggedRaw(const TType* type, TStringBuf tag);

        /// @}
    };

    namespace NPrivate {
        ITypeFactory* GetDefaultHeapFactory();
    }

    /// Create a heap-based factory.
    ///
    /// This factory uses heap to allocate type instances, and reference counting to track individual type lifetimes.
    ///
    /// Choose this factory if you need safety and flexibility.
    ITypeFactoryPtr HeapFactory();

    /// Create a memory-pool-based factory.
    ///
    /// This factory uses a memory pool to allocate type instances. All allocated memory will be released when
    /// the factory dies.
    ///
    /// Choose this factory if you need speed and memory efficiency. You have to understand how memory pool works.
    /// We advise testing your code with msan when using this factory.
    ///
    /// If in doubt, use a heap-based factory instead.
    ///
    /// @param deduplicate
    ///     if enabled, factory will cache all types and avoid allocations when creating a new type that is
    ///     strictly-equal (see `type_equivalence.h`) to some previously created type.
    ///
    ///     For example:
    ///
    ///     ```
    ///     auto a = factory.Optional(factory.String());
    ///     auto b = factory.Optional(factory.String());
    ///     Y_ASSERT(a == b);
    ///     ```
    ///
    ///     If `deduplicate` is disabled, this assert will fail. If, however, `deduplicate` is enabled,
    ///     the assert will not fail because `a` and `b` will point to the same type instance.
    ///
    ///     This behaviour slows down creation process, but can save some memory when working with complex types.
    ///
    /// @param initial
    ///     number of bytes in the first page of the memory pool.
    ///     By default, we pre-allocate `64` bytes.
    ///
    /// @param grow
    ///     policy for calculating size for a next pool page.
    ///     By default, next page is twice as big as the previous one.
    ///
    /// @param alloc
    ///     underlying allocator that'll be used to allocate pool pages.
    ///
    /// @param options
    ///     other memory pool options.
    IPoolTypeFactoryPtr PoolFactory(
        bool deduplicate = true,
        size_t initial = 64,
        TMemoryPool::IGrowPolicy* grow = TMemoryPool::TExpGrow::Instance(),
        IAllocator* alloc = TDefaultAllocator::Instance(),
        TMemoryPool::TOptions options = {});
}
