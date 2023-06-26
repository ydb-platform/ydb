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

namespace {

#pragma pack(push,4)
    struct TValue {
        ui32 Size : 31;
        ui32 IsNull : 1;
    };
#pragma pack(pop)

    static constexpr ui8 CellVecCurrentSerializationVersion = 1;

    Y_FORCE_INLINE void SerializeCellVec(const TConstArrayRef<TCell>& cells, TString & resultBuffer, TVector<TCell> * resultCells) {
        if (cells.empty())
            return;

        size_t size = sizeof(ui16) + sizeof(ui8) + sizeof(ui16);
        for (auto& cell : cells) {
            size += sizeof(TValue) + cell.Size();
        }

        resultBuffer.resize(size);
        char * resultBufferData = const_cast<char *>(resultBuffer.data());

        ui16 header = std::numeric_limits<ui16>::max();
        WriteUnaligned<ui16>(resultBufferData, header);
        resultBufferData += sizeof(header);

        ui8 serialization_version = CellVecCurrentSerializationVersion;
        WriteUnaligned<ui8>(resultBufferData, serialization_version);
        resultBufferData += sizeof(serialization_version);

        ui16 cells_size = cells.size();
        WriteUnaligned<ui16>(resultBufferData, cells_size);
        resultBufferData += sizeof(cells_size);

        for (size_t i = 0; i < cells_size; ++i) {
            TValue header;
            header.Size = cells[i].Size();
            header.IsNull = cells[i].IsNull();
            memcpy(resultBufferData, &header, sizeof(header));
            resultBufferData += sizeof(header);
        }

        if (resultCells) {
            resultCells->resize(cells.size());
        }

        for (size_t i = 0; i < cells_size; ++i) {
            const auto & cell = cells[i];
            memcpy(resultBufferData, cell.Data(), cell.Size());
            resultBufferData += cell.Size();

            if (resultCells) {
                (*resultCells)[i] = TCell(resultBuffer.data() + resultBuffer.Size() - cell.Size(), cell.Size());
            }
        }
    }

    Y_FORCE_INLINE bool TryDeserializeCellVec(const TString & data, TString & resultBuffer, TVector<TCell> & resultCells) {
        resultCells.clear();
        if (data.empty())
            return true;

        const char* buf = data.data();
        const char* bufEnd = data.data() + data.size();
        if (bufEnd - buf < static_cast<i64>(sizeof(ui16)))
            return false;

        ui16 header = ReadUnaligned<ui16>(buf);
        buf += sizeof(header);

        if (header != std::numeric_limits<ui16>::max())
        {
            ui16 cells_size = header;
            resultCells.resize(cells_size);

            for (ui32 i = 0; i < cells_size; ++i) {
                if (bufEnd - buf < (long)sizeof(TValue))
                    return false;

                const TValue cellHeader = ReadUnaligned<TValue>((const TValue*)buf);
                if (bufEnd - buf < (long)sizeof(TValue) + cellHeader.Size)
                    return false;

                resultCells[i] = cellHeader.IsNull ? TCell() : TCell((const char*)((const TValue*)buf + 1), cellHeader.Size);
                buf += sizeof(TValue) + cellHeader.Size;
            }

            resultBuffer = data;
            return true;
        }

        if (bufEnd - buf < static_cast<i64>(sizeof(ui8) + sizeof(ui16)))
            return false;

        ui8 serialization_version = ReadUnaligned<ui8>(buf);
        buf += sizeof(serialization_version);
        Y_VERIFY(serialization_version == CellVecCurrentSerializationVersion);

        ui16 cells_size = ReadUnaligned<ui16>(buf);
        buf += sizeof(cells_size);

        if (bufEnd - buf < static_cast<i64>(cells_size * sizeof(TValue))) {
            return false;
        }

        const TValue * cellsHeaders = reinterpret_cast<const TValue *>(buf);
        buf += sizeof(cellsHeaders[0]) * cells_size;

        i64 size = 0;
        for (size_t i = 0; i < cells_size; ++i) {
            TValue cellHeader = cellsHeaders[i];
            size += cellHeader.Size;
        }

        i64 availableBufferSpace = bufEnd - buf;
        if (availableBufferSpace - size != 0) {
            return false;
        }

        resultBuffer = data;
        resultCells.resize(cells_size);

        for (size_t i = 0; i < cells_size; ++i) {
            TValue cellHeader = cellsHeaders[i];
            resultCells[i] = cellHeader.IsNull ? TCell() : TCell(buf, cellHeader.Size);
            buf += cellHeader.Size;
        }

        return true;
    }

}

TSerializedCellVec::TSerializedCellVec(const TConstArrayRef<TCell>& cells)
{
    SerializeCellVec(cells, Buf, &Cells);
}

void TSerializedCellVec::Serialize(TString& res, const TConstArrayRef<TCell>& cells) {
    SerializeCellVec(cells, res, nullptr /*resultCells*/);
}

TString TSerializedCellVec::Serialize(const TConstArrayRef<TCell>& cells) {
    TString result;
    SerializeCellVec(cells, result, nullptr /*resultCells*/);

    return result;
}

bool TSerializedCellVec::DoTryParse(const TString& data) {
    return TryDeserializeCellVec(data, Buf, Cells);
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

