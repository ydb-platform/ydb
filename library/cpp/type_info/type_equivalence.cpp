#include "type_equivalence.h"

#include <util/generic/overloaded.h>

#include "type.h"

namespace NTi::NEq {
    namespace {
        template <bool IgnoreHash>
        bool StrictlyEqual(const TType* lhs, const TType* rhs);

        // template <bool IgnoreHash>
        // bool StrictlyEqual(const TDocumentation& lhs, const TDocumentation& rhs) {
        //     ...
        // }

        // template <bool IgnoreHash>
        // bool StrictlyEqual(const TAnnotations& lhs, const TAnnotations& rhs) {
        //     ...
        // }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TVoidType&, const TVoidType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TNullType&, const TNullType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TBoolType&, const TBoolType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TInt8Type&, const TInt8Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TInt16Type&, const TInt16Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TInt32Type&, const TInt32Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TInt64Type&, const TInt64Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TUint8Type&, const TUint8Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TUint16Type&, const TUint16Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TUint32Type&, const TUint32Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TUint64Type&, const TUint64Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TFloatType&, const TFloatType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDoubleType&, const TDoubleType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TStringType&, const TStringType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TUtf8Type&, const TUtf8Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDateType&, const TDateType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDatetimeType&, const TDatetimeType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTimestampType&, const TTimestampType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTzDateType&, const TTzDateType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTzDatetimeType&, const TTzDatetimeType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTzTimestampType&, const TTzTimestampType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TIntervalType&, const TIntervalType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDecimalType& lhs, const TDecimalType& rhs) {
            return lhs.GetPrecision() == rhs.GetPrecision() && lhs.GetScale() == rhs.GetScale();
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TJsonType&, const TJsonType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TYsonType&, const TYsonType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TUuidType&, const TUuidType&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDate32Type&, const TDate32Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDatetime64Type&, const TDatetime64Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTimestamp64Type&, const TTimestamp64Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TInterval64Type&, const TInterval64Type&) {
            return true;
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TOptionalType& lhs, const TOptionalType& rhs) {
            return StrictlyEqual<IgnoreHash>(lhs.GetItemTypeRaw(), rhs.GetItemTypeRaw());
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TListType& lhs, const TListType& rhs) {
            return StrictlyEqual<IgnoreHash>(lhs.GetItemTypeRaw(), rhs.GetItemTypeRaw());
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TDictType& lhs, const TDictType& rhs) {
            return StrictlyEqual<IgnoreHash>(lhs.GetKeyTypeRaw(), rhs.GetKeyTypeRaw()) &&
                   StrictlyEqual<IgnoreHash>(lhs.GetValueTypeRaw(), rhs.GetValueTypeRaw());
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TStructType& lhs, const TStructType& rhs) {
            if (lhs.GetName() != rhs.GetName()) {
                return false;
            }

            return std::equal(
                lhs.GetMembers().begin(), lhs.GetMembers().end(),
                rhs.GetMembers().begin(), rhs.GetMembers().end(),
                [](const TStructType::TMember& lhs, const TStructType::TMember& rhs) -> bool {
                    return lhs.GetName() == rhs.GetName() &&
                           StrictlyEqual<IgnoreHash>(lhs.GetTypeRaw(), rhs.GetTypeRaw());
                    // && StrictlyEqual(lhs.GetAnnotations(), rhs.GetAnnotations())
                    // && StrictlyEqual(lhs.GetDocumentation(), rhs.GetDocumentation());
                });
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTupleType& lhs, const TTupleType& rhs) {
            if (lhs.GetName() != rhs.GetName()) {
                return false;
            }

            return std::equal(
                lhs.GetElements().begin(), lhs.GetElements().end(),
                rhs.GetElements().begin(), rhs.GetElements().end(),
                [](const TTupleType::TElement& lhs, const TTupleType::TElement& rhs) -> bool {
                    return StrictlyEqual<IgnoreHash>(lhs.GetTypeRaw(), rhs.GetTypeRaw());
                    // && StrictlyEqual(lhs.GetAnnotations(), rhs.GetAnnotations())
                    // && StrictlyEqual(lhs.GetDocumentation(), rhs.GetDocumentation());
                });
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TVariantType& lhs, const TVariantType& rhs) {
            if (lhs.GetName() != rhs.GetName()) {
                return false;
            }

            return StrictlyEqual<IgnoreHash>(lhs.GetUnderlyingTypeRaw(), rhs.GetUnderlyingTypeRaw());
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TTaggedType& lhs, const TTaggedType& rhs) {
            return lhs.GetTag() == rhs.GetTag() &&
                   StrictlyEqual<IgnoreHash>(lhs.GetItemTypeRaw(), rhs.GetItemTypeRaw());
        }

        template <bool IgnoreHash>
        bool StrictlyEqual(const TType* lhs, const TType* rhs) {
            if (lhs == rhs) {
                return true;
            }

            if (lhs == nullptr || rhs == nullptr) {
                return false;
            }

            if (!IgnoreHash && lhs->GetHash() != rhs->GetHash()) {
                return false;
            }

            if (lhs->GetTypeName() != rhs->GetTypeName()) {
                return false;
            }

            // FIXME: update `TStrictlyEqual`'s docs to explicitly state that documentation and annotations
            //        must be the same for types to compare strictly equal.

            // if (!StrictlyEqual(lhs->GetAnnotations(), rhs->GetAnnotations())) {
            //     return false;
            // }

            // if (!StrictlyEqual(lhs->GetDocumentation(), rhs->GetDocumentation())) {
            //     return false;
            // }

            return lhs->VisitRaw([&rhs](const auto* lhs) {
                using TSameType = decltype(lhs);
                return rhs->VisitRaw(TOverloaded{
                    [lhs](TSameType rhs) {
                        return StrictlyEqual<IgnoreHash>(*lhs, *rhs);
                    },
                    [](const auto* rhs) {
                        static_assert(!std::is_same_v<decltype(lhs), decltype(rhs)>);
                        return false;
                    }});
            });
        }
    }

    bool TStrictlyEqual::operator()(const TType* lhs, const TType* rhs) const {
        return StrictlyEqual<false>(lhs, rhs);
    }

    bool TStrictlyEqual::operator()(TTypePtr lhs, TTypePtr rhs) const {
        return operator()(lhs.Get(), rhs.Get());
    }

    bool TStrictlyEqual::IgnoreHash(const TType* lhs, const TType* rhs) const {
        return StrictlyEqual<true>(lhs, rhs);
    }

    bool TStrictlyEqual::IgnoreHash(TTypePtr lhs, TTypePtr rhs) const {
        return IgnoreHash(lhs.Get(), rhs.Get());
    }

    ui64 TStrictlyEqualHash::operator()(const TType* type) const {
        if (type == nullptr) {
            return 0;
        }

        return type->GetHash();
    }

    ui64 TStrictlyEqualHash::operator()(TTypePtr type) const {
        return operator()(type.Get());
    }
}
