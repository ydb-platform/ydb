#include "types.h"

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NClickHouse {
    TType::TType(const ECode code)
        : Code_(code)
    {
        if (Code_ == Array) {
            Array_ = new TArray;
        } else if (Code_ == Tuple) {
            Tuple_ = new TTuple;
        } else if (Code_ == Nullable) {
            Nullable_ = new TNullable;
        }
    }

    TType::~TType() {
        if (Code_ == Array) {
            delete Array_;
        } else if (Code_ == Tuple) {
            delete Tuple_;
        } else if (Code_ == Nullable) {
            delete Nullable_;
        }
    }

    TType::ECode TType::GetCode() const {
        return Code_;
    }

    TTypeRef TType::GetItemType() const {
        if (Code_ == Array) {
            return Array_->ItemType;
        }
        return TTypeRef();
    }

    const TVector<TEnumItem>& TType::GetEnumItems() const {
        return EnumItems_;
    }

    const TString& TType::GetEnumName(i16 enumValue) const {
        return EnumValueToName_.at(enumValue);
    }

    i16 TType::GetEnumValue(const TString& enumName) const {
        return EnumNameToValue_.at(enumName);
    }

    bool TType::HasEnumName(const TString& enumName) const {
        return EnumNameToValue_.contains(enumName);
    }

    bool TType::HasEnumValue(i16 enumValue) const {
        return EnumValueToName_.contains(enumValue);
    }

    TString TType::GetName() const {
        switch (Code_) {
            case Void:
                return "Void";
            case Int8:
                return "Int8";
            case Int16:
                return "Int16";
            case Int32:
                return "Int32";
            case Int64:
                return "Int64";
            case UInt8:
                return "UInt8";
            case UInt16:
                return "UInt16";
            case UInt32:
                return "UInt32";
            case UInt64:
                return "UInt64";
            case Enum8:
            case Enum16: {
                TVector<TString> pairs;
                for (const auto& item : EnumItems_) {
                    pairs.push_back(TStringBuilder() << "'" << item.Name << "' = " << item.Value);
                }
                TStringBuilder ret;
                if (Code_ == Enum8) {
                    ret << "Enum8";
                } else {
                    ret << "Enum16";
                }
                ret << "(" << JoinRange(", ", pairs.begin(), pairs.end()) << ")";
                return ret;
            }
            case Float32:
                return "Float32";
            case Float64:
                return "Float64";
            case String:
                return "String";
            case FixedString:
                return "FixedString(" + ToString(StringSize_) + ")";
            case DateTime:
                return "DateTime";
            case Date:
                return "Date";
            case Array:
                return TString("Array(") + Array_->ItemType->GetName() + ")";
            case Nullable:
                return TString("Nullable(") + Nullable_->NestedType->GetName() + ")";
            case Tuple: {
                TString result("Tuple(");
                for (size_t i = 0; i < Tuple_->ItemTypes.size(); ++i) {
                    result += Tuple_->ItemTypes[i]->GetName();

                    if (i + 1 != Tuple_->ItemTypes.size()) {
                        result += ", ";
                    }
                }
                result += ")";
                return result;
            }
        }

        return TString();
    }

    bool TType::IsEqual(const TTypeRef& other) const {
        return this->GetName() == other->GetName();
    }

    TTypeRef TType::CreateArray(TTypeRef item_type) {
        TTypeRef type(new TType(TType::Array));
        type->Array_->ItemType = item_type;
        return type;
    }

    TTypeRef TType::CreateDate() {
        return TTypeRef(new TType(TType::Date));
    }

    TTypeRef TType::CreateDateTime() {
        return TTypeRef(new TType(TType::DateTime));
    }

    TTypeRef TType::CreateNullable(TTypeRef nested_type) {
        TTypeRef type(new TType(TType::Nullable));
        type->Nullable_->NestedType = nested_type;
        return type;
    }

    TTypeRef TType::CreateString() {
        return TTypeRef(new TType(TType::String));
    }

    TTypeRef TType::CreateString(size_t n) {
        TTypeRef type(new TType(TType::FixedString));
        type->StringSize_ = n;
        return type;
    }

    TTypeRef TType::CreateTuple(const TVector<TTypeRef>& item_types) {
        TTypeRef type(new TType(TType::Tuple));
        type->Tuple_->ItemTypes.assign(item_types.begin(), item_types.end());
        return type;
    }

    TTypeRef TType::CreateEnum8(const TVector<TEnumItem>& enum_items) {
        for (const auto& item : enum_items) {
            Y_ENSURE(item.Value >= Min<i8>() && item.Value <= Max<i8>(),
                     Sprintf("Enum value %d for %s doesn't fit into Int8", item.Value, item.Name.data()));
        }

        TTypeRef type(new TType(TType::Enum8));
        type->EnumItems_.assign(enum_items.begin(), enum_items.end());
        for (const auto& item : enum_items) {
            type->EnumNameToValue_.insert({item.Name, item.Value});
            type->EnumValueToName_.insert({item.Value, item.Name});
        }

        return type;
    }

    TTypeRef TType::CreateEnum16(const TVector<TEnumItem>& enum_items) {
        TTypeRef type(new TType(TType::Enum16));
        type->EnumItems_.assign(enum_items.begin(), enum_items.end());
        for (const auto& item : enum_items) {
            type->EnumNameToValue_.insert({item.Name, item.Value});
            type->EnumValueToName_.insert({item.Value, item.Name});
        }

        return type;
    }

}
