#pragma once

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NClickHouse {
    using TTypeRef = TIntrusivePtr<class TType>;

    struct TEnumItem {
        TString Name;
        i16 Value;
    };

    class TType: public TAtomicRefCount<TType> {
    public:
        enum ECode {
            Void = 0,
            Int8,
            Int16,
            Int32,
            Int64,
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            Enum8,
            Enum16,
            Float32,
            Float64,
            String,
            FixedString,
            DateTime,
            Date,
            Array,
            Nullable,
            Tuple
        };

        /// Destructor
        ~TType();

        /// Type's code.
        ECode GetCode() const;

        /// Type of array's elements.
        TTypeRef GetItemType() const;

        /// Methods to work with enum types.
        const TVector<TEnumItem>& GetEnumItems() const;
        const TString& GetEnumName(i16 enumValue) const;
        i16 GetEnumValue(const TString& enumName) const;
        bool HasEnumName(const TString& enumName) const;
        bool HasEnumValue(i16 enumValue) const;

        /// String representation of the type.
        TString GetName() const;

        /// Is given type same as current one.
        bool IsEqual(const TTypeRef& other) const;

    public:
        static TTypeRef CreateArray(TTypeRef item_type);

        static TTypeRef CreateDate();

        static TTypeRef CreateDateTime();

        static TTypeRef CreateNullable(TTypeRef nested_type);

        template <typename T>
        static TTypeRef CreateSimple();

        static TTypeRef CreateString();

        static TTypeRef CreateString(size_t n);

        static TTypeRef CreateTuple(const TVector<TTypeRef>& item_types);

        static TTypeRef CreateEnum8(const TVector<TEnumItem>& enum_items);

        static TTypeRef CreateEnum16(const TVector<TEnumItem>& enum_items);

    private:
        TType(const ECode code);

        struct TArray {
            TTypeRef ItemType;
        };

        struct TNullable {
            TTypeRef NestedType;
        };

        struct TTuple {
            TVector<TTypeRef> ItemTypes;
        };

        TVector<TEnumItem> EnumItems_;
        THashMap<i16, TString> EnumValueToName_;
        THashMap<TString, i16> EnumNameToValue_;

        const ECode Code_;
        union {
            TArray* Array_;
            TNullable* Nullable_;
            TTuple* Tuple_;
            int StringSize_;
        };
    };

    template <>
    inline TTypeRef TType::CreateSimple<i8>() {
        return TTypeRef(new TType(Int8));
    }

    template <>
    inline TTypeRef TType::CreateSimple<i16>() {
        return TTypeRef(new TType(Int16));
    }

    template <>
    inline TTypeRef TType::CreateSimple<i32>() {
        return TTypeRef(new TType(Int32));
    }

    template <>
    inline TTypeRef TType::CreateSimple<i64>() {
        return TTypeRef(new TType(Int64));
    }

    template <>
    inline TTypeRef TType::CreateSimple<ui8>() {
        return TTypeRef(new TType(UInt8));
    }

    template <>
    inline TTypeRef TType::CreateSimple<ui16>() {
        return TTypeRef(new TType(UInt16));
    }

    template <>
    inline TTypeRef TType::CreateSimple<ui32>() {
        return TTypeRef(new TType(UInt32));
    }

    template <>
    inline TTypeRef TType::CreateSimple<ui64>() {
        return TTypeRef(new TType(UInt64));
    }

    template <>
    inline TTypeRef TType::CreateSimple<float>() {
        return TTypeRef(new TType(Float32));
    }

    template <>
    inline TTypeRef TType::CreateSimple<double>() {
        return TTypeRef(new TType(Float64));
    }

}
