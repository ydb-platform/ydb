#include "node.h"
#include "hash.h"

#include <ydb/library/yql/public/udf/udf_type_ops.h>

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
            for (TUnboxedValue v; it.Next(v); hash = CombineHashes(hash, HashDom(v)))
                continue;
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
                if (!EquateDoms(ex[i], ey[i]))
                    return false;
            }
        } else {
            const auto itx = x.GetListIterator();
            const auto ity = y.GetListIterator();
            for (TUnboxedValue vx, vy; itx.Next(vx);) {
                if (!ity.Next(vy))
                    return false;
                if (!EquateDoms(vx, vy))
                    return false;
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

        const auto xr =  static_cast<const TPair*>(x.GetResource());
        const auto yr =  static_cast<const TPair*>(y.GetResource());
        // clone dict as attrnode
        if (xr && yr) {
            for (ui32 i = 0U; i < size; ++i) {
                if (!EquateStrings(xr[i].first, yr[i].first))
                    return false;
                if (!EquateDoms(xr[i].second, yr[i].second))
                    return false;
            }
        } else {
            const auto it = x.GetDictIterator();
            for (TUnboxedValue k, v; it.NextPair(k, v);) {
                if (auto l = y.Lookup(k))
                    if (EquateDoms(v, l.GetOptionalValue()))
                        continue;
                return false;
            }

        }
        return true;
    }
    return x.IsBoxed() == y.IsBoxed();
}

}

THashType HashDom(const NUdf::TUnboxedValuePod x) {
    switch (const auto type = GetNodeType(x); type) {
        case ENodeType::Double:
            return CombineHashes(THashType(type), GetFloatHash<double>(x));
        case ENodeType::Uint64:
            return CombineHashes(THashType(type), GetIntegerHash<ui64>(x));
        case ENodeType::Int64:
            return CombineHashes(THashType(type), GetIntegerHash<i64>(x));
        case ENodeType::Bool:
            return CombineHashes(THashType(type), std::hash<bool>()(x.Get<bool>()));
        case ENodeType::String:
            return CombineHashes(THashType(type), GetStringHash(x));
        case ENodeType::Entity:
            return CombineHashes(THashType(type), THashType(~0ULL));
        case ENodeType::List:
            return CombineHashes(THashType(type), HashList(x));
        case ENodeType::Dict:
            return CombineHashes(THashType(type), HashDict(x));
        case ENodeType::Attr:
            return CombineHashes(THashType(type), CombineHashes(HashDict(x), HashDom(x.GetVariantItem().Release())));
    }
}

bool EquateDoms(const NUdf::TUnboxedValuePod x, const NUdf::TUnboxedValuePod y) {
    if (const auto type = GetNodeType(x); type == GetNodeType(y)) {
        switch (type) {
            case ENodeType::Double:
                return EquateFloats<double>(x, y);
            case ENodeType::Uint64:
                return EquateIntegers<ui64>(x, y);
            case ENodeType::Int64:
                return EquateIntegers<i64>(x, y);
            case ENodeType::Bool:
                return x.Get<bool>() == y.Get<bool>();
            case ENodeType::String:
                return EquateStrings(x, y);
            case ENodeType::Entity:
                return true;
            case ENodeType::List:
                return EquateLists(x, y);
            case ENodeType::Dict:
                return EquateDicts(x, y);
            case ENodeType::Attr:
                return EquateDicts(x, y) && EquateDoms(x.GetVariantItem().Release(), y.GetVariantItem().Release());
        }
    }
    return false;
}

}
