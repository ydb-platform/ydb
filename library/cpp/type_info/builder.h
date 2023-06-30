#pragma once

//! @file builder.h
//!
//! Builders help with creating complex types piece-by-piece.

#include <util/generic/vector.h>

#include "type.h"

#include "fwd.h"

namespace NTi {
    /// An interface for building structs using the raw interface (via memory-pool-based factory).
    ///
    /// This interface allows allocating structs piece-by-piece. You can feed it struct items, one-by-one, and they'll
    /// be copied into the memory pool before the struct is created. This way, you don't have to allocate heap memory
    /// to temporarily store item names or types.
    ///
    /// Note: this builder doesn't own the underlying factory.
    class TStructBuilderRaw {
    public:
        /// Create a new builder with the given factory.
        TStructBuilderRaw(IPoolTypeFactory& factory) noexcept;

        /// Create a new builder with the given factory and add fields from the given struct.
        /// Note that struct name is not copied to the builder.
        TStructBuilderRaw(IPoolTypeFactory& factory, TStructTypePtr prototype) noexcept;
        TStructBuilderRaw(IPoolTypeFactory& factory, const TStructType* prototype) noexcept;

        /// Set a new struct name.
        ///
        /// Note that the name is copied to the factory right away. If you call this function twice, second call will
        /// overwrite name from the first call, but will not remove it from the memory pool.
        TStructBuilderRaw& SetName(TMaybe<TStringBuf> name) & noexcept;
        TStructBuilderRaw SetName(TMaybe<TStringBuf> name) && noexcept;

        /// Check if there's a struct name set.
        bool HasName() const noexcept;

        /// Get a struct name.
        ///
        /// Name was copied to the factory's memory pool and will live as long as the factory lives.
        TMaybe<TStringBuf> GetName() const noexcept;

        /// Reserve some place in the underlying vector that collects struct items.
        TStructBuilderRaw& Reserve(size_t size) & noexcept;
        TStructBuilderRaw Reserve(size_t size) && noexcept;

        /// Append a new struct member.
        TStructBuilderRaw& AddMember(TStringBuf name, TTypePtr type) & noexcept;
        TStructBuilderRaw AddMember(TStringBuf name, TTypePtr type) && noexcept;
        TStructBuilderRaw& AddMember(TStringBuf name, const TType* type) & noexcept;
        TStructBuilderRaw AddMember(TStringBuf name, const TType* type) && noexcept;

        /// Partial member creation interface.
        ///
        /// This interface allows building individual struct items piece-by-piece. You can pass member's name
        /// and copy it to the pool without passing its type. For example:
        ///
        /// ```
        /// auto builder = TStructBuilderRaw(factory);
        /// builder.AddMemberName("name");  // add name for a new member.
        /// builder.AddMemberType(type);  // add type for a new member.
        /// builder.AddMember();  // use added name and type to construct and append an member.
        /// ```
        ///
        /// This interface is useful when you can't store member name for a long time. For example, if you building
        /// a parser, at some point have an member name, but you don't have an member type yet. Then you can add member name
        /// when you have it, and add member type later.
        ///
        /// @{
        //-
        /// Set name for pending member.
        ///
        /// Note that the name is copied to the factory right away. If you call this function twice, second call will
        /// overwrite name from the first call, but will not remove it from the memory pool.
        TStructBuilderRaw& AddMemberName(TStringBuf name) & noexcept;
        TStructBuilderRaw AddMemberName(TStringBuf name) && noexcept;

        /// Unset name for pending member.
        ///
        /// Note that member name is copied to the factory right away. If you discard it via this method,
        /// it will not be removed from the memory pool.
        TStructBuilderRaw& DiscardMemberName() & noexcept;
        TStructBuilderRaw DiscardMemberName() && noexcept;

        /// Check if there's a name set for pending member.
        bool HasMemberName() const noexcept;

        /// Get name for pending member.
        ///
        /// Name was copied to the factory's memory pool and will live as long as the factory lives.
        TMaybe<TStringBuf> GetMemberName() const noexcept;

        /// Set type for pending member.
        ///
        /// Note that the type is copied to the factory right away. If you call this function twice, second call will
        /// overwrite type from the first call, but will not remove it from the memory pool.
        TStructBuilderRaw& AddMemberType(TTypePtr type) & noexcept;
        TStructBuilderRaw AddMemberType(TTypePtr type) && noexcept;
        TStructBuilderRaw& AddMemberType(const TType* type) & noexcept;
        TStructBuilderRaw AddMemberType(const TType* type) && noexcept;

        /// Unset type for pending member.
        ///
        /// Note that member type is copied to the factory right away. If you discard it via this method,
        /// it will not be removed from the memory pool.
        TStructBuilderRaw& DiscardMemberType() & noexcept;
        TStructBuilderRaw DiscardMemberType() && noexcept;

        /// Check if there's a type set for pending member.
        bool HasMemberType() const noexcept;

        /// Get type for pending member.
        TMaybe<const TType*> GetMemberType() const noexcept;

        /// Check if both name and type are set for pending member.
        bool CanAddMember() const noexcept;

        /// Use data added via `AddMemberName` and `AddMemberType` to construct a new struct member
        /// and append it to this builder. This function panics if there's no name or no type set for pending member.
        TStructBuilderRaw& AddMember() & noexcept;
        TStructBuilderRaw AddMember() && noexcept;

        /// Discard all data added via `AddMemberName` and `AddMemberType` functions.
        ///
        /// Note that member name and type are copied to the factory right away. If you discard them via this method,
        /// they will not be removed from the memory pool.
        TStructBuilderRaw& DiscardMember() & noexcept;
        TStructBuilderRaw DiscardMember() && noexcept;

        /// @}

        /// Get immutable list of items that've been added to this builder.
        /// The returned object is invalidated when this builder dies or when `AddMember` is called.
        TStructType::TMembers GetMembers() const noexcept;

        /// Reset struct name and items.
        TStructBuilderRaw& Reset() & noexcept;
        TStructBuilderRaw Reset() && noexcept;

        /// Create a new struct type using name and items from this builder.
        TStructTypePtr Build();

        /// Like `Build`, but returns a raw pointer.
        const TStructType* BuildRaw();

        /// Create a new variant over struct using items from this builder.
        TVariantTypePtr BuildVariant();

        /// Like `BuildVariant`, but returns a raw pointer.
        const TVariantType* BuildVariantRaw();

    private:
        const TStructType* DoBuildRaw(TMaybe<TStringBuf> name);

    private:
        IPoolTypeFactory* Factory_;
        TMaybe<TStringBuf> Name_;
        TVector<TStructType::TMember> Members_;
        TMaybe<TStringBuf> PendingMemberName_;
        TMaybe<const TType*> PendingMemberType_;
    };

    /// An interface for building tuples using the raw interface (via memory-pool-based factory).
    ///
    /// Note: this builder doesn't own the underlying factory.
    class TTupleBuilderRaw {
    public:
        /// Create a new builder with the given factory.
        TTupleBuilderRaw(IPoolTypeFactory& factory) noexcept;

        /// Create a new builder with the given factory and add fields from the given tuple.
        /// Note that tuple name is not copied to the builder.
        TTupleBuilderRaw(IPoolTypeFactory& factory, TTupleTypePtr prototype) noexcept;
        TTupleBuilderRaw(IPoolTypeFactory& factory, const TTupleType* prototype) noexcept;

        /// Set a new tuple name.
        /// Note that the name is copied to the factory right away. If you call this function twice, second call will
        /// overwrite name from the first call, but will not remove it from the memory pool.
        TTupleBuilderRaw& SetName(TMaybe<TStringBuf> name) & noexcept;
        TTupleBuilderRaw SetName(TMaybe<TStringBuf> name) && noexcept;

        /// Check if there's a tuple name set.
        bool HasName() const noexcept;

        /// Get a tuple name.
        ///
        /// Name was copied to the factory's memory pool and will live as long as the factory lives.
        TMaybe<TStringBuf> GetName() const noexcept;

        /// Reserve some place in the underlying vector that collects tuple items.
        TTupleBuilderRaw& Reserve(size_t size) & noexcept;
        TTupleBuilderRaw Reserve(size_t size) && noexcept;

        /// Append a new tuple item.
        TTupleBuilderRaw& AddElement(TTypePtr type) & noexcept;
        TTupleBuilderRaw AddElement(TTypePtr type) && noexcept;
        TTupleBuilderRaw& AddElement(const TType* type) & noexcept;
        TTupleBuilderRaw AddElement(const TType* type) && noexcept;

        /// Partial item creation interface.
        ///
        /// This interface allows building individual tuple items piece-by-piece. It mirrors the same type of interface
        /// in the struct builder.
        ///
        /// @{
        //-
        /// Set type for pending item.
        ///
        /// Note that the type is copied to the factory right away. If you call this function twice, second call will
        /// overwrite type from the first call, but will not remove it from the memory pool.
        TTupleBuilderRaw& AddElementType(TTypePtr type) & noexcept;
        TTupleBuilderRaw AddElementType(TTypePtr type) && noexcept;
        TTupleBuilderRaw& AddElementType(const TType* type) & noexcept;
        TTupleBuilderRaw AddElementType(const TType* type) && noexcept;

        /// Unset type for pending item.
        ///
        /// Note that item type is copied to the factory right away. If you discard it via this method,
        /// it will not be removed from the memory pool.
        TTupleBuilderRaw& DiscardElementType() & noexcept;
        TTupleBuilderRaw DiscardElementType() && noexcept;

        /// Check if there's a type set for pending item.
        bool HasElementType() const noexcept;

        /// Get type for pending item.
        TMaybe<const TType*> GetElementType() const noexcept;

        /// Check if type is set for pending item.
        bool CanAddElement() const noexcept;

        /// Use data added via `AddElementType` to construct a new tuple item and append it to this builder.
        /// This function panics if there's no type set for pending item.
        TTupleBuilderRaw& AddElement() & noexcept;
        TTupleBuilderRaw AddElement() && noexcept;

        /// Discard all data added via `AddElementType` function.
        ///
        /// Note that item type is copied to the factory right away. If you discard it via this method,
        /// it will not be removed from the memory pool.
        TTupleBuilderRaw& DiscardElement() & noexcept;
        TTupleBuilderRaw DiscardElement() && noexcept;

        /// @}

        /// Get immutable list of items that've been added to this builder.
        /// The returned object is invalidated when this builder dies or when `AddElement` is called.
        TTupleType::TElements GetElements() const noexcept;

        /// Reset tuple name and items.
        TTupleBuilderRaw& Reset() & noexcept;
        TTupleBuilderRaw Reset() && noexcept;

        /// Create a new tuple type using name and items from this builder.
        TTupleTypePtr Build();

        /// Like `Build`, but returns a raw pointer.
        const TTupleType* BuildRaw();

        /// Create a new variant over tuple using items from this builder.
        TVariantTypePtr BuildVariant();

        /// Like `BuildVariant`, but returns a raw pointer.
        const TVariantType* BuildVariantRaw();

    private:
        const TTupleType* DoBuildRaw(TMaybe<TStringBuf> name);

    private:
        IPoolTypeFactory* Factory_;
        TMaybe<TStringBuf> Name_;
        TVector<TTupleType::TElement> Elements_;
        TMaybe<const TType*> PendingElementType_;
    };

    /// An interface for building tagged types using the raw interface (via memory-pool-based factory).
    ///
    /// Note: this builder doesn't own the underlying factory.
    class TTaggedBuilderRaw {
    public:
        /// Create a new builder with the given factory.
        TTaggedBuilderRaw(IPoolTypeFactory& factory) noexcept;

        /// Set a new tag.
        ///
        /// Note that the tag is copied to the factory right away. If you call this function twice, second call will
        /// overwrite tag from the first call, but will not remove it from the memory pool.
        TTaggedBuilderRaw& SetTag(TStringBuf tag) & noexcept;
        TTaggedBuilderRaw SetTag(TStringBuf tag) && noexcept;

        /// Unset tag.
        ///
        /// Note that tag is copied to the factory right away. If you discard it via this method,
        /// it will not be removed from the memory pool.
        TTaggedBuilderRaw& DiscardTag() & noexcept;
        TTaggedBuilderRaw DiscardTag() && noexcept;

        /// Check if a tag is set.
        bool HasTag() const noexcept;

        /// Get a tag.
        ///
        /// The tag was copied to the factory's memory pool and will live as long as the factory lives.
        TMaybe<TStringBuf> GetTag() const noexcept;

        /// Set type that's being tagged.
        TTaggedBuilderRaw& SetItem(TTypePtr type) & noexcept;
        TTaggedBuilderRaw SetItem(TTypePtr type) && noexcept;
        TTaggedBuilderRaw& SetItem(const TType* type) & noexcept;
        TTaggedBuilderRaw SetItem(const TType* type) && noexcept;

        /// Unset item type.
        ///
        /// Note that item type is copied to the factory right away. If you discard it via this method,
        /// it will not be removed from the memory pool.
        TTaggedBuilderRaw& DiscardItem() & noexcept;
        TTaggedBuilderRaw DiscardItem() && noexcept;

        /// Check if there's an item type set.
        bool HasItem() const noexcept;

        /// Get item type.
        TMaybe<const TType*> GetItem() const noexcept;

        /// Check if there's both name and item type set.
        bool CanBuild() const noexcept;

        /// Discard both tag and item.
        TTaggedBuilderRaw& Reset() & noexcept;
        TTaggedBuilderRaw Reset() && noexcept;

        /// Create a new tagged type using name and item from this builder.
        TTaggedTypePtr Build();

        /// Like `Build`, but returns a raw pointer.
        const TTaggedType* BuildRaw();

    private:
        IPoolTypeFactory* Factory_;
        TMaybe<TStringBuf> Tag_;
        TMaybe<const TType*> Item_;
    };
}
