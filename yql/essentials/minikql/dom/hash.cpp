#include "node.h"
#include "hash.h"

#include <yql/essentials/public/udf/udf_type_ops.h>

namespace NYql::NDom {

using namespace NUdf;

namespace {

THashType HashList(const NUdf::TUnboxedValuePod x) {
    THashType hash = 0ULL;
    if (x.IsBoxed()) {
        if (const auto elements = x.GetElements()) {
            const auto size = x.GetListLength();
            for (ui32 i = 0U; i < size; ++i) {
                hash = CombineHashes(hash, HashDom(elements[i]));
            }
        } else {
            const auto it = x.GetListIterator();
            for (TUnboxedValue v; it.Next(v); hash = CombineHashes(hash, HashDom(v))) {
                continue;
            }
        }
    }
    return hash;
}

THashType HashDict(const NUdf::TUnboxedValuePod x) {
    THashType hash = 0ULL;
    if (x.IsBoxed()) {
        const auto it = x.GetDictIterator();
        for (TUnboxedValue k, v; it.NextPair(k, v);) {
            hash = CombineHashes(hash, CombineHashes(GetStringHash(k), HashDom(v)));
        }
    }
    return hash;
}

bool EquateLists(const NUdf::TUnboxedValuePod x, const NUdf::TUnboxedValuePod y) {
    if (x.IsBoxed() && y.IsBoxed()) {
        const auto ex = x.GetElements();
        const auto ey = y.GetElements();
        if (ex && ey) {
            const auto size = x.GetListLength();
            if (size != y.GetListLength()) {
                return false;
            }
            for (ui32 i = 0U; i < size; ++i) {
                if (!EquateDoms(ex[i], ey[i])) {
                    return false;
                }
            }
        } else {
            const auto itx = x.GetListIterator();
            const auto ity = y.GetListIterator();
            for (TUnboxedValue vx, vy; itx.Next(vx);) {
                if (!ity.Next(vy)) {
                    return false;
                }
                if (!EquateDoms(vx, vy)) {
                    return false;
                }
            }
        }
        return true;
    }
    return x.IsBoxed() == y.IsBoxed();
}

bool EquateDicts(const NUdf::TUnboxedValuePod x, const NUdf::TUnboxedValuePod y) {
    if (x.IsBoxed() && y.IsBoxed()) {
        const auto size = x.GetDictLength();
        if (size != y.GetDictLength()) {
            return false;
        }

        const auto xr = static_cast<const TPair*>(x.GetResource());
        const auto yr = static_cast<const TPair*>(y.GetResource());
        // clone dict as attrnode
        if (xr && yr) {
            for (ui32 i = 0U; i < size; ++i) {
                if (!EquateStrings(ClearUtf8Mark(xr[i].first), ClearUtf8Mark(yr[i].first))) {
                    return false;
                }
                if (!EquateDoms(xr[i].second, yr[i].second)) {
                    return false;
                }
            }
        } else {
            const auto it = x.GetDictIterator();
            for (TUnboxedValue k, v; it.NextPair(k, v);) {
                if (auto l = y.Lookup(k)) {
                    if (EquateDoms(v, l.GetOptionalValue())) {
                        continue;
                    }
                }
                return false;
            }
        }
        return true;
    }
    return x.IsBoxed() == y.IsBoxed();
}

} // namespace

THashType HashDom(const NUdf::TUnboxedValuePod value) {
    switch (const auto type = GetNodeType(value); type) {
        case ENodeType::Double:
            return CombineHashes(THashType(type), GetFloatHash<double>(value));
        case ENodeType::Uint64:
            return CombineHashes(THashType(type), GetIntegerHash<ui64>(value));
        case ENodeType::Int64:
            return CombineHashes(THashType(type), GetIntegerHash<i64>(value));
        case ENodeType::Bool:
            return CombineHashes(THashType(type), std::hash<bool>()(value.Get<bool>()));
        case ENodeType::String:
            return CombineHashes(THashType(type), GetStringHash(ClearUtf8Mark(value)));
        case ENodeType::Entity:
            return CombineHashes(THashType(type), THashType(~0ULL));
        case ENodeType::List:
            return CombineHashes(THashType(type), HashList(value));
        case ENodeType::Dict:
            return CombineHashes(THashType(type), HashDict(value));
        case ENodeType::Attr:
            return CombineHashes(THashType(type), CombineHashes(HashDict(value), HashDom(value.GetVariantItem().Release())));
    }
}

bool EquateDoms(const NUdf::TUnboxedValuePod lhs, const NUdf::TUnboxedValuePod rhs) {
    if (const auto type = GetNodeType(lhs); type == GetNodeType(rhs)) {
        switch (type) {
            case ENodeType::Double:
                return EquateFloats<double>(lhs, rhs);
            case ENodeType::Uint64:
                return EquateIntegers<ui64>(lhs, rhs);
            case ENodeType::Int64:
                return EquateIntegers<i64>(lhs, rhs);
            case ENodeType::Bool:
                return lhs.Get<bool>() == rhs.Get<bool>();
            case ENodeType::String:
                return EquateStrings(ClearUtf8Mark(lhs), ClearUtf8Mark(rhs));
            case ENodeType::Entity:
                return true;
            case ENodeType::List:
                return EquateLists(lhs, rhs);
            case ENodeType::Dict:
                return EquateDicts(lhs, rhs);
            case ENodeType::Attr:
                return EquateDicts(lhs, rhs) && EquateDoms(lhs.GetVariantItem().Release(), rhs.GetVariantItem().Release());
        }
    }
    return false;
}

} // namespace NYql::NDom
