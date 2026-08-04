#include <ydb/core/scheme/scheme_borders.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstring>

namespace {

using namespace NKikimr;

constexpr size_t MaxCells = 12;
constexpr size_t MaxCellBytes = 40;
constexpr size_t MaxSteps = 160;

int Norm(int value) {
    return value == 0 ? 0 : (value < 0 ? -1 : 1);
}

bool SameCells(TConstArrayRef<TCell> left, TConstArrayRef<TCell> right) {
    return TCellVectorsEquals{}(left, right);
}

TString Bytes(FuzzedDataProvider& provider, size_t maxSize = MaxCellBytes) {
    return TString(provider.ConsumeRandomLengthString(provider.ConsumeIntegralInRange<size_t>(0, maxSize)));
}

template <class T>
TString PodBytes(FuzzedDataProvider& provider) {
    T value = provider.ConsumeIntegral<T>();
    TString bytes;
    bytes.ReserveAndResize(sizeof(T));
    std::memcpy(bytes.Detach(), &value, sizeof(value));
    return bytes;
}

struct TCellArena {
    TVector<TString> Bytes;
    TVector<TCell> Cells;
};

NScheme::TTypeInfo PickType(FuzzedDataProvider& provider, size_t index) {
    static constexpr std::array<NScheme::TTypeId, 10> Types = {
        NScheme::NTypeIds::Int32,
        NScheme::NTypeIds::Uint32,
        NScheme::NTypeIds::Int64,
        NScheme::NTypeIds::Uint64,
        NScheme::NTypeIds::Bool,
        NScheme::NTypeIds::String,
        NScheme::NTypeIds::Utf8,
        NScheme::NTypeIds::Date,
        NScheme::NTypeIds::Timestamp,
        NScheme::NTypeIds::Interval,
    };
    return NScheme::TTypeInfo(Types[(provider.ConsumeIntegral<unsigned>() + index) % Types.size()]);
}

TString MakeValueForType(FuzzedDataProvider& provider, NScheme::TTypeId typeId) {
    switch (typeId) {
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Timestamp:
            return PodBytes<ui32>(provider);
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Uint64:
        case NScheme::NTypeIds::Interval:
            return PodBytes<ui64>(provider);
        case NScheme::NTypeIds::Bool: {
            TString bytes;
            bytes.ReserveAndResize(1);
            bytes.Detach()[0] = provider.ConsumeBool() ? 1 : 0;
            return bytes;
        }
        case NScheme::NTypeIds::Date:
            return PodBytes<ui16>(provider);
        default:
            return Bytes(provider);
    }
}

TCellArena BuildCells(FuzzedDataProvider& provider, const TVector<NScheme::TTypeInfo>& types, size_t maxCells = MaxCells) {
    TCellArena arena;
    const size_t count = provider.ConsumeIntegralInRange<size_t>(0, std::min(maxCells, types.size()));
    arena.Bytes.reserve(count);
    arena.Cells.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        if (provider.ConsumeIntegralInRange<unsigned>(0, 7) == 0) {
            arena.Cells.emplace_back();
            continue;
        }
        arena.Bytes.push_back(MakeValueForType(provider, types[i].GetTypeId()));
        const TString& bytes = arena.Bytes.back();
        arena.Cells.emplace_back(bytes.data(), bytes.size());
    }
    return arena;
}

void CheckVecRoundtrip(TConstArrayRef<TCell> cells) {
    TString serialized = TSerializedCellVec::Serialize(cells);
    TSerializedCellVec parsed;
    Y_ABORT_UNLESS(TSerializedCellVec::TryParse(serialized, parsed));
    Y_ABORT_UNLESS(SameCells(cells, parsed.GetCells()));

    TOwnedCellVec owned(cells);
    TOwnedCellVec ownedFromSerialized = TOwnedCellVec::FromSerialized(serialized);
    Y_ABORT_UNLESS(SameCells(owned, ownedFromSerialized));

    for (size_t i = 0; i < cells.size(); ++i) {
        TCell extracted = TSerializedCellVec::ExtractCell(serialized, i);
        Y_ABORT_UNLESS(SameCells(TConstArrayRef<TCell>(&cells[i], 1), TConstArrayRef<TCell>(&extracted, 1)));
    }
    Y_ABORT_UNLESS(TSerializedCellVec::ExtractCell(serialized, cells.size()).IsNull());
}

void CheckMatrixRoundtrip(FuzzedDataProvider& provider, TConstArrayRef<TCell> cells) {
    if (cells.empty()) {
        return;
    }
    const ui16 colCount = provider.ConsumeIntegralInRange<ui16>(1, std::min<size_t>(cells.size(), 6));
    const ui32 rowCount = cells.size() / colCount;
    if (rowCount == 0 || rowCount * colCount != cells.size()) {
        return;
    }
    TString serialized = TSerializedCellMatrix::Serialize(cells, rowCount, colCount);
    TSerializedCellMatrix parsed;
    Y_ABORT_UNLESS(TSerializedCellMatrix::TryParse(serialized, parsed));
    Y_ABORT_UNLESS(parsed.GetRowCount() == rowCount);
    Y_ABORT_UNLESS(parsed.GetColCount() == colCount);
    Y_ABORT_UNLESS(SameCells(cells, parsed.GetCells()));
    for (ui32 row = 0; row < rowCount; ++row) {
        for (ui16 col = 0; col < colCount; ++col) {
            const TCell& cell = parsed.GetCell(row, col);
            const TCell& expected = cells[row * colCount + col];
            Y_ABORT_UNLESS(SameCells(TConstArrayRef<TCell>(&cell, 1), TConstArrayRef<TCell>(&expected, 1)));
        }
    }
}

void CheckMalformed(FuzzedDataProvider& provider) {
    TString data = Bytes(provider, 96);
    TSerializedCellVec vec;
    TSerializedCellMatrix matrix;
    const bool vecOk = TSerializedCellVec::TryParse(data, vec);
    const bool matrixOk = TSerializedCellMatrix::TryParse(data, matrix);
    if (!vecOk) {
        Y_ABORT_UNLESS(vec.GetCells().empty());
        Y_ABORT_UNLESS(vec.GetBuffer().empty());
    }
    if (!matrixOk) {
        Y_ABORT_UNLESS(matrix.GetCells().empty());
        Y_ABORT_UNLESS(matrix.GetBuffer().empty());
        Y_ABORT_UNLESS(matrix.GetRowCount() == 0);
        Y_ABORT_UNLESS(matrix.GetColCount() == 0);
    }
}

EPrefixMode PickMode(FuzzedDataProvider& provider) {
    static constexpr std::array<EPrefixMode, 4> Modes = {
        PrefixModeRightBorderNonInclusive,
        PrefixModeLeftBorderInclusive,
        PrefixModeRightBorderInclusive,
        PrefixModeLeftBorderNonInclusive,
    };
    return Modes[provider.ConsumeIntegralInRange<size_t>(0, Modes.size() - 1)];
}

void CheckCompare(FuzzedDataProvider& provider) {
    const size_t keySize = provider.ConsumeIntegralInRange<size_t>(1, 6);
    TVector<NScheme::TTypeInfo> types;
    types.reserve(keySize);
    for (size_t i = 0; i < keySize; ++i) {
        types.push_back(PickType(provider, i));
    }

    auto a = BuildCells(provider, types, keySize);
    auto b = BuildCells(provider, types, keySize);
    auto c = BuildCells(provider, types, keySize);
    const EPrefixMode am = PickMode(provider);
    const EPrefixMode bm = PickMode(provider);
    const EPrefixMode cm = PickMode(provider);

    const int ab = Norm(ComparePrefixBorders(types, a.Cells, am, b.Cells, bm));
    const int ba = Norm(ComparePrefixBorders(types, b.Cells, bm, a.Cells, am));
    Y_ABORT_UNLESS(ab == -ba);

    const int bc = Norm(ComparePrefixBorders(types, b.Cells, bm, c.Cells, cm));
    const int ac = Norm(ComparePrefixBorders(types, a.Cells, am, c.Cells, cm));
    if (ab <= 0 && bc <= 0) {
        Y_ABORT_UNLESS(ac <= 0);
    }
    if (ab >= 0 && bc >= 0) {
        Y_ABORT_UNLESS(ac >= 0);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, MaxSteps);
    for (size_t i = 0; i < steps && provider.remaining_bytes() > 0; ++i) {
        switch (provider.ConsumeIntegralInRange<unsigned>(0, 3)) {
            case 0: {
                TVector<NScheme::TTypeInfo> types;
                const size_t count = provider.ConsumeIntegralInRange<size_t>(0, MaxCells);
                for (size_t j = 0; j < count; ++j) {
                    types.push_back(PickType(provider, j));
                }
                auto cells = BuildCells(provider, types, MaxCells);
                CheckVecRoundtrip(cells.Cells);
                CheckMatrixRoundtrip(provider, cells.Cells);
                break;
            }
            case 1:
                CheckMalformed(provider);
                break;
            case 2:
            case 3:
                CheckCompare(provider);
                break;
        }
    }

    return 0;
}
