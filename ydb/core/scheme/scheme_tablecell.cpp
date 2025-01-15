#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_type_registry.h>

#include <ydb/library/actors/core/actorid.h>
#include <util/string/escape.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NKikimr {

namespace {

    struct TCellHeader {
        static constexpr ui32 NullFlag = ui32(1) << 31;

        ui32 RawValue = 0;

        TCellHeader() = default;

        TCellHeader(ui32 rawValue) : RawValue(rawValue) {}

        TCellHeader(ui32 cellSize, bool isNull)
            : RawValue(cellSize | (isNull ? NullFlag : 0))
        {}

        ui32 CellSize() const { return RawValue & ~NullFlag; }

        bool IsNull() const { return RawValue & NullFlag; };
    };

    static_assert(sizeof(TCellHeader) == sizeof(ui32));

    class TSerializedCellReader {
    public:
        TSerializedCellReader(std::string_view data) noexcept
            : Ptr(data.data())
            , Size(data.size())
        {}

        TSerializedCellReader(const char* p, size_t size) noexcept
            : Ptr(p)
            , Size(size)
        {}

        std::string_view Snapshot() const noexcept {
            return std::string_view(Ptr, Size);
        }

        void Reset(std::string_view data) noexcept {
            Ptr = data.data();
            Size = data.size();
        }

        bool Skip(size_t size) noexcept {
            if (Y_UNLIKELY(Size < size)) {
                return false;
            }

            Ptr += size;
            Size -= size;
            return true;
        }

        bool Skip(size_t size, const char** p) noexcept {
            if (Y_UNLIKELY(Size < size)) {
                return false;
            }

            *p = Ptr;
            Ptr += size;
            Size -= size;
            return true;
        }

        template<class T>
        bool Read(T* dst) noexcept {
            if (Y_UNLIKELY(Size < sizeof(T))) {
                return false;
            }

            ::memcpy(dst, Ptr, sizeof(T));
            Ptr += sizeof(T);
            Size -= sizeof(T);
            return true;
        }

        bool ReadNewCell(TCell* cell) noexcept {
            TCellHeader header;
            if (!Read(&header)) {
                return false;
            }

            if (Y_UNLIKELY(Size < header.CellSize())) {
                return false;
            }

            if (header.IsNull()) {
                new (cell) TCell();
            } else {
                new (cell) TCell(Ptr, header.CellSize());
            }

            Ptr += header.CellSize();
            Size -= header.CellSize();
            return true;
        }

    private:
        const char* Ptr;
        size_t Size;
    };

} // namespace

void TOwnedCellVec::TData::operator delete(TData* data, std::destroying_delete_t) noexcept {
    void* ptr = data;
    size_t size = data->DataSize;
    data->~TData();
    ::operator delete(ptr, size);
}

TOwnedCellVec::TInit TOwnedCellVec::Allocate(TOwnedCellVec::TCellVec cells) {
    size_t cellCount = cells.size();

    if (cellCount == 0) {
        // Leave the data field empty
        return TInit{
            TCellVec(),
            nullptr,
        };
    }

    size_t size = sizeof(TData) + sizeof(TCell) * cellCount;
    for (auto& x : cells) {
        if (!x.IsNull() && !x.IsInline()) {
            const size_t xsz = x.Size();
            size += AlignUp(xsz, size_t(4));
        }
    }

    void* mem = ::operator new(size);
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
            ptrData += AlignUp(cellSize, size_t(4));
        }

        ++ptrCell;
    }

    return TInit {
        cellvec,
        new (mem) TData(size),
    };
}

TOwnedCellVec::TInit TOwnedCellVec::AllocateFromSerialized(std::string_view data) {
    if (data.empty()) {
        // Leave the data field empty
        return TInit{
            TCellVec(),
            nullptr,
        };
    }

    TSerializedCellReader reader(data);

    ui16 cellCount;
    if (!reader.Read(&cellCount)) {
        throw std::invalid_argument("cannot deserialize cellvec header");
    }

    if (cellCount == 0) {
        // Leave the data field empty
        return TInit{
            TCellVec(),
            nullptr,
        };
    }

    size_t size = sizeof(TData) + sizeof(TCell) * cellCount;

    auto snapshot = reader.Snapshot();
    for (ui16 i = 0; i < cellCount; ++i) {
        TCellHeader cellHeader;
        if (!reader.Read(&cellHeader) || !reader.Skip(cellHeader.CellSize())) {
            throw std::invalid_argument("cannot deserialize cell data");
        }
        size_t cellSize = cellHeader.CellSize();
        if (!cellHeader.IsNull() && !TCell::CanInline(cellSize)) {
            size += AlignUp(cellSize, size_t(4));
        }
    }

    void* mem = ::operator new(size);
    if (Y_UNLIKELY(!mem)) {
        throw std::bad_alloc();
    }

    TCell* ptrCell = (TCell*)((TData*)mem + 1);
    char* ptrData = (char*)(ptrCell + cellCount);

    TConstArrayRef<TCell> cellvec(ptrCell, ptrCell + cellCount);

    reader.Reset(snapshot);
    for (ui16 i = 0; i < cellCount; ++i) {
        TCellHeader cellHeader;
        const char* src;
        if (!reader.Read(&cellHeader) || !reader.Skip(cellHeader.CellSize(), &src)) {
            Y_ABORT("Unexpected failure to deserialize cell data a second time");
        }
        size_t cellSize = cellHeader.CellSize();
        if (cellHeader.IsNull()) {
            new (ptrCell) TCell();
        } else if (TCell::CanInline(cellSize)) {
            new (ptrCell) TCell(src, cellSize);
        } else {
            if (Y_LIKELY(cellSize > 0)) {
                ::memcpy(ptrData, src, cellSize);
            }
            new (ptrCell) TCell(ptrData, cellSize);
            ptrData += AlignUp(cellSize, size_t(4));
        }

        ++ptrCell;
    }

    return TInit {
        cellvec,
        new (mem) TData(size),
    };
}

struct THashableKey {
    TConstArrayRef<TCell> Cells;

    template <typename H>
    friend H AbslHashValue(H h, const THashableKey& key) {
        h = H::combine(std::move(h), key.Cells.size());
        for (const TCell& cell : key.Cells) {
            h = H::combine(std::move(h), cell.IsNull());
            if (!cell.IsNull()) {
                h = H::combine(std::move(h), cell.Size());
                h = H::combine_contiguous(std::move(h), cell.Data(), cell.Size());
            }
        }
        return h;
    }
};

size_t TCellVectorsHash::operator()(TConstArrayRef<TCell> key) const {
    return absl::Hash<THashableKey>()(THashableKey{ key });
}

bool TCellVectorsEquals::operator() (TConstArrayRef<TCell> a, TConstArrayRef<TCell> b) const {
    if (a.size() != b.size()) {
        return false;
    }

    const TCell* pa = a.data();
    const TCell* pb = b.data();
    if (pa == pb) {
        return true;
    }

    size_t left = a.size();
    while (left > 0) {
        if (pa->IsNull()) {
            if (!pb->IsNull()) {
                return false;
            }
        } else {
            if (pb->IsNull()) {
                return false;
            }
            if (pa->Size() != pb->Size()) {
                return false;
            }
            if (pa->Size() > 0 && ::memcmp(pa->Data(), pb->Data(), pa->Size()) != 0) {
                return false;
            }
        }
        ++pa;
        ++pb;
        --left;
    }

    return true;
}

namespace {

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

        auto size = TSerializedCellVec::SerializedSize(cells);

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

    Y_FORCE_INLINE bool TryDeserializeCellVecBody(std::string_view buf, ui64 cellCount, TVector<TCell>& resultCells) {
        resultCells.resize_uninitialized(cellCount);
        TCell* dst = resultCells.data();

        TSerializedCellReader reader(buf);
        for (ui64 i = 0; i < cellCount; ++i) {
            if (!reader.ReadNewCell(dst)) {
                resultCells.clear();
                return false;
            }
            ++dst;
        }

        return true;
    }

    Y_FORCE_INLINE bool TryDeserializeCellVec(const TString& data, TString& resultBuffer, TVector<TCell> & resultCells) {
        resultBuffer.clear();
        resultCells.clear();

        if (data.empty()) {
            return true;
        }

        TSerializedCellReader reader(data);

        ui16 cellCount;
        if (!reader.Read(&cellCount)) {
            return false;
        }

        if (!TryDeserializeCellVecBody(reader.Snapshot(), cellCount, resultCells)) {
            return false;
        }

        resultBuffer = data;
        return true;
    }

    Y_FORCE_INLINE bool TryDeserializeCellMatrix(const TString& data, TString& resultBuffer, TVector<TCell>& resultCells, ui32& rowCount, ui16& colCount) {
        resultBuffer.clear();
        resultCells.clear();

        if (data.empty()) {
            return true;
        }

        TSerializedCellReader reader(data);
        if (!reader.Read(&rowCount) || !reader.Read(&colCount)) {
            return false;
        }

        ui64 cellCount = (ui64)rowCount * (ui64)colCount;
        if (!TryDeserializeCellVecBody(reader.Snapshot(), cellCount, resultCells)) {
            return false;
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

size_t TSerializedCellVec::SerializedSize(TConstArrayRef<TCell> cells) {
    size_t size = sizeof(ui16) + sizeof(TCellHeader) * cells.size();
    for (auto& cell : cells) {
        size += cell.Size();
    }
    return size;
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
            cellsDataSize += AlignUp(static_cast<size_t>(cell.Size()), size_t(4));
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
            cellsData += AlignUp(static_cast<size_t>(cell.Size()), size_t(4));
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
            ptrData += AlignUp(cellSize, size_t(4));
        }

        ++ptrCell;
    }

    CellVectors.push_back(cellvec);
    return size;
}


TString DbgPrintCell(const TCell& r, NScheme::TTypeInfo typeInfo, const NScheme::TTypeRegistry &reg) {
    Y_UNUSED(reg);
    TString typeName = NScheme::TypeName(typeInfo, "");
    typeName += " : ";
    DbgPrintValue(typeName, r, typeInfo);
    return typeName;
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
        case NScheme::NTypeIds::Decimal:
            res += typeInfo.GetDecimalType().CellValueToString(r.AsValue<std::pair<ui64, i64>>());
            break;
        case NScheme::NTypeIds::Pg: {
            auto convert = NPg::PgNativeTextFromNativeBinary(r.AsBuf(), typeInfo.GetPgTypeDesc());
            if (!convert.Error)
                res += convert.Str;
            else
                res += *convert.Error;
            break;
        }
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

