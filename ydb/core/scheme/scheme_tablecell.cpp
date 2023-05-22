#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_registry.h>

#include <util/string/escape.h>

namespace NKikimr {

void TOwnedCellVec::TData::operator delete(void* mem) noexcept {
    ::free(mem);
}

TOwnedCellVec::TInit TOwnedCellVec::Allocate(TOwnedCellVec::TCellVec cells) {
    size_t cellCount = cells.size();

    if (cellCount == 0) {
        // Leave the data field empty
        return TInit{
            TCellVec(),
            nullptr,
            0,
        };
    }

    size_t size = sizeof(TData) + sizeof(TCell) * cellCount;
    for (auto& x : cells) {
        if (!x.IsNull() && !x.IsInline()) {
            const size_t xsz = x.Size();
            size += AlignUp(xsz);
        }
    }

    void* mem = ::malloc(size);
    if (Y_UNLIKELY(!mem)) {
        throw std::bad_alloc();
    }

    TCell* ptrCell = (TCell*)((TData*)mem + 1);
    char* ptrData = (char*)(ptrCell + cellCount);

    TConstArrayRef<TCell> cellvec(ptrCell, ptrCell + cellCount);

    for (auto& x : cells) {
        if (x.IsNull()) {
            new (ptrCell) TCell();
        } else if (x.IsInline()) {
            new (ptrCell) TCell(x);
        } else {
            const size_t cellSize = x.Size();
            if (Y_LIKELY(cellSize > 0)) {
                ::memcpy(ptrData, x.Data(), cellSize);
            }
            new (ptrCell) TCell(ptrData, cellSize);
            ptrData += AlignUp(cellSize);
        }

        ++ptrCell;
    }

    return TInit {
        cellvec,
        new (mem) TData(),
        size,
    };
}

TString DbgPrintCell(const TCell& r, NScheme::TTypeInfo typeInfo, const NScheme::TTypeRegistry &reg) {
    auto typeId = typeInfo.GetTypeId();
    TString res;

    if (typeId == NScheme::NTypeIds::Pg) {
        res = NPg::PgTypeNameFromTypeDesc(typeInfo.GetTypeDesc());
    } else {
        NScheme::ITypeSP t = reg.GetType(typeId);

        if (!t.IsKnownType())
            return Sprintf("Unknow typeId 0x%x", (ui32)typeId);

        res = t->GetName();
    }

    res += " : ";
    DbgPrintValue(res, r, typeInfo);

    return res;
}

void DbgPrintValue(TString &res, const TCell &r, NScheme::TTypeInfo typeInfo) {
    if (r.IsNull()) {
        res += "NULL";
    } else {
        switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            res += r.AsValue<bool>() ? "true" : "false";
            break;
        case NScheme::NTypeIds::Byte:
            res += ToString(r.AsValue<ui8>());
            break;
        case NScheme::NTypeIds::Int32:
            res += ToString(r.AsValue<i32>());
            break;
        case NScheme::NTypeIds::Uint32:
            res += ToString(r.AsValue<ui32>());
            break;
        case NScheme::NTypeIds::Int64:
            res += ToString(r.AsValue<i64>());
            break;
        case NScheme::NTypeIds::Uint64:
            res += ToString(r.AsValue<ui64>());
            break;
        case NScheme::NTypeIds::Float:
            res += ToString(r.AsValue<float>());
            break;
        case NScheme::NTypeIds::Double:
            res += ToString(r.AsValue<double>());
            break;
        case NScheme::NTypeIds::ActorId:
            res += ToString(r.AsValue<TActorId>());
            break;
        case NScheme::NTypeIds::Pg:
            // TODO: support pg types
            break;
        default:
            res += EscapeC(r.Data(), r.Size());
        }
    }
}

TString DbgPrintTuple(const TDbTupleRef& row, const NScheme::TTypeRegistry& typeRegistry) {
    TString res = "(";
    for (ui32 i = 0; i < row.ColumnCount; ++i) {
        res += DbgPrintCell(row.Columns[i], row.Types[i], typeRegistry);
        if (i < row.ColumnCount-1)
            res += ", ";
    }
    res += ")";
    return res;
}

} // namespace NKikimr

