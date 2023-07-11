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

    struct TCellHeader {
        TCellHeader() = default;

        TCellHeader(ui32 rawValue) : RawValue(rawValue) {}

        TCellHeader(ui32 cellSize, bool isNull)
            : RawValue(cellSize | (static_cast<ui32>(isNull) << 31))
        {}

        ui32 CellSize() const { return RawValue & ~(1ULL << 31); }

        bool IsNull() const { return RawValue & (1ULL << 31); };

        ui32 RawValue = 0;
    };

    static_assert(sizeof(TCellHeader) == sizeof(ui32));

    Y_FORCE_INLINE void SerializeCellVec(TConstArrayRef<TCell> cells, TString &resultBuffer, TVector<TCell> *resultCells) {
        resultBuffer.clear();
        if (resultCells)
            resultCells->clear();

        if (cells.empty()) {
            return;
        }

        size_t size = sizeof(ui16);
        for (auto& cell : cells) {
            size += sizeof(TCellHeader) + cell.Size();
        }

        resultBuffer.resize(size);
        char* resultBufferData = resultBuffer.Detach();

        ui16 cellsSize = cells.size();
        WriteUnaligned<ui16>(resultBufferData, cellsSize);
        resultBufferData += sizeof(cellsSize);

        if (resultCells) {
            resultCells->resize_uninitialized(cellsSize);
        }

        for (size_t i = 0; i < cellsSize; ++i) {
            TCellHeader header(cells[i].Size(), cells[i].IsNull());
            WriteUnaligned<ui32>(resultBufferData, header.RawValue);
            resultBufferData += sizeof(header);

            const auto & cell = cells[i];
            if (cell.Size() > 0) {
                memcpy(resultBufferData, cell.Data(), cell.Size());
            }

            if (resultCells) {
                if (cell.IsNull()) {
                    new (resultCells->data() + i) TCell();
                } else {
                    new (resultCells->data() + i) TCell(resultBufferData, cell.Size());
                }
            }

            resultBufferData += cell.Size();
        }
    }

    Y_FORCE_INLINE bool TryDeserializeCellVec(const TString & data, TString & resultBuffer, TVector<TCell> & resultCells) {
        resultBuffer.clear();
        resultCells.clear();

        if (data.empty())
            return true;

        const char* buf = data.data();
        const char* bufEnd = data.data() + data.size();
        if (Y_UNLIKELY(bufEnd - buf < static_cast<ptrdiff_t>(sizeof(ui16))))
            return false;

        ui16 cellsSize = ReadUnaligned<ui16>(buf);
        buf += sizeof(cellsSize);

        resultCells.resize_uninitialized(cellsSize);
        TCell* resultCellsData = resultCells.data();

        for (ui32 i = 0; i < cellsSize; ++i) {
            if (Y_UNLIKELY(bufEnd - buf < static_cast<ptrdiff_t>(sizeof(TCellHeader)))) {
                resultCells.clear();
                return false;
            }

            TCellHeader cellHeader = ReadUnaligned<TCellHeader>(buf);
            buf += sizeof(cellHeader);

            if (Y_UNLIKELY(bufEnd - buf < static_cast<ptrdiff_t>(cellHeader.CellSize()))) {
                resultCells.clear();
                return false;
            }

            if (cellHeader.IsNull()) {
                new (resultCellsData + i) TCell();
            } else {
                new (resultCellsData + i) TCell(buf, cellHeader.CellSize());
            }

            buf += cellHeader.CellSize();
        }

        resultBuffer = data;
        return true;
    }

}

TSerializedCellVec::TSerializedCellVec(TConstArrayRef<TCell> cells)
{
    SerializeCellVec(cells, Buf, &Cells);
}

void TSerializedCellVec::Serialize(TString& res, TConstArrayRef<TCell> cells) {
    SerializeCellVec(cells, res, nullptr /*resultCells*/);
}

TString TSerializedCellVec::Serialize(TConstArrayRef<TCell> cells) {
    TString result;
    SerializeCellVec(cells, result, nullptr /*resultCells*/);

    return result;
}

bool TSerializedCellVec::DoTryParse(const TString& data) {
    return TryDeserializeCellVec(data, Buf, Cells);
}

TOwnedCellVecBatch::TOwnedCellVecBatch()
    : Pool(std::make_unique<TMemoryPool>(InitialPoolSize)) {
}

size_t TOwnedCellVecBatch::Append(TConstArrayRef<TCell> cells) {
    size_t cellsSize = cells.size();
    if (cellsSize == 0) {
        CellVectors.emplace_back();
        return 0;
    }

    size_t size = sizeof(TCell) * cellsSize;
    for (auto& cell : cells) {
        if (!cell.IsNull() && !cell.IsInline()) {
            const size_t cellSize = cell.Size();
            size += AlignUp(cellSize);
        }
    }

    char * allocatedBuffer = reinterpret_cast<char *>(Pool->Allocate(size));

    TCell* ptrCell = reinterpret_cast<TCell*>(allocatedBuffer);
    char* ptrData = reinterpret_cast<char*>(ptrCell + cellsSize);

    TConstArrayRef<TCell> cellvec(ptrCell, ptrCell + cellsSize);

    for (auto& cell : cells) {
        if (cell.IsNull()) {
            new (ptrCell) TCell();
        } else if (cell.IsInline()) {
            new (ptrCell) TCell(cell);
        } else {
            const size_t cellSize = cell.Size();
            if (Y_LIKELY(cellSize > 0)) {
                ::memcpy(ptrData, cell.Data(), cellSize);
            }
            new (ptrCell) TCell(ptrData, cellSize);
            ptrData += AlignUp(cellSize);
        }

        ++ptrCell;
    }

    CellVectors.push_back(cellvec);
    return size;
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

