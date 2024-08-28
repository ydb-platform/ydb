#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_registry.h>

#include <ydb/library/actors/core/actorid.h>
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

    Y_FORCE_INLINE void SerializeCellVecBody(TConstArrayRef<TCell> cells, char* resultBufferData, TVector<TCell>* resultCells) {
        if (resultCells)
            resultCells->resize_uninitialized(cells.size());

        for (size_t i = 0; i < cells.size(); ++i) {
            TCellHeader header(cells[i].Size(), cells[i].IsNull());
            WriteUnaligned<ui32>(resultBufferData, header.RawValue);
            resultBufferData += sizeof(header);

            const auto& cell = cells[i];
            if (cell.Size() > 0) {
                cell.CopyDataInto(resultBufferData);
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

    Y_FORCE_INLINE bool SerializeCellVecInit(TConstArrayRef<TCell> cells, TString& resultBuffer, TVector<TCell>* resultCells) {
        resultBuffer.clear();
        if (resultCells)
            resultCells->clear();

        return !cells.empty();
    }

    Y_FORCE_INLINE void SerializeCellVec(TConstArrayRef<TCell> cells, TString& resultBuffer, TVector<TCell>* resultCells) {
        if (!SerializeCellVecInit(cells, resultBuffer, resultCells))
            return;

        size_t size = sizeof(ui16);
        for (auto& cell : cells)
            size += sizeof(TCellHeader) + cell.Size();

        resultBuffer.ReserveAndResize(size);
        char* resultBufferData = resultBuffer.Detach();

        ui16 cellCount = cells.size();
        WriteUnaligned<ui16>(resultBufferData, cellCount);
        resultBufferData += sizeof(cellCount);

        SerializeCellVecBody(cells, resultBufferData, resultCells);
    }

    constexpr size_t CellMatrixHeaderSize = sizeof(ui32) + sizeof(ui16);

    Y_FORCE_INLINE void SerializeCellMatrix(TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount, TString& resultBuffer, TVector<TCell>* resultCells) {
        Y_ABORT_UNLESS(cells.size() == (size_t)rowCount * (size_t)colCount);

        if (!SerializeCellVecInit(cells, resultBuffer, resultCells))
            return;

        size_t size = CellMatrixHeaderSize;
        for (auto& cell : cells)
            size += sizeof(TCellHeader) + cell.Size();

        resultBuffer.ReserveAndResize(size);
        char* resultBufferData = resultBuffer.Detach();

        WriteUnaligned<ui32>(resultBufferData, rowCount);
        resultBufferData += sizeof(rowCount);
        WriteUnaligned<ui16>(resultBufferData, colCount);
        resultBufferData += sizeof(colCount);

        SerializeCellVecBody(cells, resultBufferData, resultCells);
    }

    Y_FORCE_INLINE bool TryDeserializeCellVecBody(const char* buf, const char* bufEnd, ui64 cellCount, TVector<TCell>& resultCells) {
        resultCells.resize_uninitialized(cellCount);
        TCell* resultCellsData = resultCells.data();

        for (ui64 i = 0; i < cellCount; ++i) {
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

            if (cellHeader.IsNull())
                new (resultCellsData + i) TCell();
            else
                new (resultCellsData + i) TCell(buf, cellHeader.CellSize());

            buf += cellHeader.CellSize();
        }

        return true;
    }

    Y_FORCE_INLINE bool TryDeserializeCellVec(const TString& data, TString& resultBuffer, TVector<TCell> & resultCells) {
        resultBuffer.clear();
        resultCells.clear();

        if (data.empty())
            return true;

        const char* buf = data.data();
        const char* bufEnd = data.data() + data.size();
        if (Y_UNLIKELY(bufEnd - buf < static_cast<ptrdiff_t>(sizeof(ui16))))
            return false;

        ui16 cellCount = ReadUnaligned<ui16>(buf);
        buf += sizeof(cellCount);

        if (TryDeserializeCellVecBody(buf, bufEnd, cellCount, resultCells)) {
            resultBuffer = data;
            return true;
        }

        return false;
    }

    Y_FORCE_INLINE bool TryDeserializeCellMatrix(const TString& data, TString& resultBuffer, TVector<TCell>& resultCells, ui32& rowCount, ui16& colCount) {
        resultBuffer.clear();
        resultCells.clear();

        if (data.empty())
            return true;

        const char* buf = data.data();
        const char* bufEnd = data.data() + data.size();
        if (Y_UNLIKELY(bufEnd - buf < static_cast<ptrdiff_t>(sizeof(ui16))))
            return false;

        rowCount = ReadUnaligned<ui32>(buf);
        buf += sizeof(rowCount);
        colCount = ReadUnaligned<ui16>(buf);
        buf += sizeof(colCount);

        ui64 cellCount = (ui64)rowCount * (ui64)colCount;
        if (TryDeserializeCellVecBody(buf, bufEnd, cellCount, resultCells)) {
            resultBuffer = data;
            return true;
        }

        return false;
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

bool TSerializedCellVec::UnsafeAppendCells(TConstArrayRef<TCell> cells, TString& serializedCellVec) {
    if (Y_UNLIKELY(cells.size() == 0)) {
        return true;
    }

    if (!serializedCellVec) {
        TSerializedCellVec::Serialize(serializedCellVec, cells);
        return true;
    }

    const char* buf = serializedCellVec.data();
    const char* bufEnd = serializedCellVec.data() + serializedCellVec.size();
    const size_t bufSize = bufEnd - buf;

    if (Y_UNLIKELY(bufSize < static_cast<ptrdiff_t>(sizeof(ui16)))) {
        return false;
    }

    ui16 cellCount = ReadUnaligned<ui16>(buf);
    cellCount += cells.size();

    size_t newSize = serializedCellVec.size();

    for (auto& cell : cells) {
        newSize += sizeof(TCellHeader) + cell.Size();
    }

    serializedCellVec.ReserveAndResize(newSize);

    char* mutableBuf = serializedCellVec.Detach();
    char* oldBufEnd = mutableBuf + bufSize;

    WriteUnaligned<ui16>(mutableBuf, cellCount);

    SerializeCellVecBody(cells, oldBufEnd, nullptr);

    return true;
}

TSerializedCellMatrix::TSerializedCellMatrix(TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount)
    : RowCount(rowCount), ColCount(colCount)
{
    SerializeCellMatrix(cells, rowCount, colCount, Buf, &Cells);
}

const TCell& TSerializedCellMatrix::GetCell(ui32 row, ui16 column) const {
    Y_ABORT_UNLESS(row < RowCount && column < ColCount);
    return Cells.at(CalcIndex(row, column));
}


void TSerializedCellMatrix::GetSubmatrix(ui32 firstRow, ui32 lastRow, ui16 firstColumn, ui16 lastColumn, TVector<TCell>& resultCells) const {
    Y_ABORT_UNLESS(firstColumn < ColCount &&
                   lastColumn < ColCount &&
                   firstRow < RowCount &&
                   lastRow < RowCount &&
                   firstColumn <= lastColumn &&
                   firstRow <= lastRow);

    ui32 rowCount = (lastRow - firstRow + 1);
    ui16 colCount = (lastColumn - firstColumn + 1);
    size_t cellCount = colCount * rowCount;
    resultCells.clear();
    resultCells.resize_uninitialized(cellCount);

    for (ui32 row = firstRow; row <= lastRow; ++row) {
            for (ui16 col = firstColumn; col <= lastColumn; ++col) {
                    resultCells[CalcIndex(row - firstRow, col - firstColumn, colCount)] = GetCell(row, col);
                }
            }
}


void TSerializedCellMatrix::Serialize(TString& res, TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount) {
    SerializeCellMatrix(cells, rowCount, colCount, res, nullptr /*resultCells*/);
}

TString TSerializedCellMatrix::Serialize(TConstArrayRef<TCell> cells, ui32 rowCount, ui16 colCount) {
    TString result;
    SerializeCellMatrix(cells, rowCount, colCount, result, nullptr /*resultCells*/);

    return result;
}

bool TSerializedCellMatrix::DoTryParse(const TString& data) {
    return TryDeserializeCellMatrix(data, Buf, Cells, RowCount, ColCount);
}

void TCellsStorage::Reset(TArrayRef<const TCell> cells)
{
    size_t cellsSize = cells.size();
    size_t cellsDataSize = sizeof(TCell) * cellsSize;

    for (size_t i = 0; i < cellsSize; ++i) {
        const auto & cell = cells[i];
        if (!cell.IsNull() && !cell.IsInline() && cell.Size() != 0) {
            cellsDataSize += AlignUp(static_cast<size_t>(cell.Size()));
        }
    }

    CellsData.resize(cellsDataSize);

    char *cellsData = CellsData.data();
    Cells = TArrayRef<TCell>{reinterpret_cast<TCell *>(cellsData), cellsSize};
    cellsData += sizeof(TCell) * cellsSize;

    for (size_t i = 0; i < cellsSize; ++i) {
        const auto & cell = cells[i];

        if (!cell.IsNull() && !cell.IsInline() && cell.Size() != 0) {
            memcpy(cellsData, cell.Data(), cell.Size());
            Cells[i] = TCell(cellsData, cell.Size());
            cellsData += AlignUp(static_cast<size_t>(cell.Size()));
        } else {
            Cells[i] = cell;
        }
    }
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

    size_t size = EstimateSize(cells);

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
            res += ToString(r.AsValue<NActors::TActorId>());
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

size_t GetCellMatrixHeaderSize() {
    return CellMatrixHeaderSize;
}

size_t GetCellHeaderSize() {
    return sizeof(TCellHeader);
}

} // namespace NKikimr

