#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <util/generic/buffer.h>

#include <util/system/cpu_id.h>
#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

// Defines if data type of particular column variable or fixed
enum class EColumnSizeType { Fixed, Variable };

// Defines if particular column is key column or payload column
enum class EColumnRole { Key, Payload };

// Describes layout and size of particular column
struct TColumnDesc {
    ui32 ColumnIndex = 0;   // Index of the column in particular layout
    ui32 OriginalColumnIndex = 0; // Index of the column in input representation
    ui32 OriginalIndex = 0; // Index of the buffer in input representation
    EColumnRole Role = EColumnRole::Payload; // Role of the particular column in
                                             // tuple (Key or Payload)
    EColumnSizeType SizeType =
        EColumnSizeType::Fixed; // Fixed size or variable size column
    ui32 DataSize = 0; // Size of the column in bytes for fixed size part
                       // Must be same for matching key columns
    ui32 Offset =
        0; // Offset in bytes for column value from the beginning of tuple
};

// Defines in memory layout of tuple.
struct TTupleLayout {
    std::vector<TColumnDesc> OrigColumns; // Columns description and order as
                                          // passed during layout construction
    std::vector<TColumnDesc> Columns; // Vector describing all columns in order
                                      // corresponding to tuple layout
    std::vector<TColumnDesc> KeyColumns; // Vector describing key columns
    std::vector<TColumnDesc>
        PayloadColumns;      // Vector describing payload columns
    ui32 KeyColumnsNum;      // Total number of key columns
    ui32 KeyColumnsSize;     // Total size of all key columns in bytes
    ui32 KeyColumnsOffset;   // Start of row-packed keys data
    ui32 KeyColumnsFixedEnd; // Offset in row-packed keys data of first variable
                             // key (can be same as KeyColumnsEnd, if there are
                             // none)
    ui32 KeyColumnsFixedNum; // Number of fixed-size columns
    ui32 KeyColumnsEnd; // First byte after key columns. Start of bitmask for
                        // row-based columns
    ui32 BitmaskSize;   // Size of bitmask for null values flag in columns
    ui32 BitmaskOffset; // Offset of nulls bitmask. = KeyColumnsEnd
    ui32 BitmaskEnd;    // First byte after bitmask. = PayloadOffset
    ui32 PayloadSize;   // Total size in bytes of the payload columns
    ui32 PayloadOffset; // Offset of payload values. = BitmaskEnd.
    ui32 PayloadEnd;    // First byte after payload
    ui32 TotalRowSize;  // Total size of bytes for packed row

    // Creates new tuple layout based on provided columns description.
    static THolder<TTupleLayout>
    Create(const std::vector<TColumnDesc> &columns);

    TTupleLayout(const std::vector<TColumnDesc> &columns)
        : OrigColumns(columns) {}
    virtual ~TTupleLayout() {}

    // Takes array of pointer to columns, array of validity bitmaps,
    // outputs packed rows
    virtual void Pack(const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
                      std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
                      ui32 start, ui32 count) const = 0;

    // Takes packed rows,
    // outputs array of pointer to columns, array of validity bitmaps
    virtual void Unpack(ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
                        const std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
                        ui32 start, ui32 count) const = 0;
    
    // Takes packed rows,
    // outputs vector of column sizes in bytes
    virtual void CalculateColumnSizes(
        const ui8* res, ui32 count, std::vector<ui64, TMKQLAllocator<ui64>>& bytes) const = 0;

    bool KeysEqual(const ui8 *lhsRow, const ui8 *lhsOverflow, const ui8 *rhsRow, const ui8 *rhsOverflow) const;
};

template <typename TTrait> struct TTupleLayoutFallback : public TTupleLayout {

    TTupleLayoutFallback(const std::vector<TColumnDesc> &columns);

    void Pack(const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
              std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
              ui32 count) const override;

    void Unpack(ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
                const std::vector<ui8, TMKQLAllocator<ui8>> &overflow,
                ui32 start, ui32 count) const override;
    
    void CalculateColumnSizes(
        const ui8* res, ui32 count, std::vector<ui64, TMKQLAllocator<ui64>>& bytes) const override;

  private:
    std::array<std::vector<TColumnDesc>, 5>
        FixedPOTColumns_; // Fixed-size columns for power-of-two sizes from 1 to
                          // 16 bytes
    std::vector<TColumnDesc> FixedNPOTColumns_; // Remaining fixed-size columns
    std::vector<TColumnDesc> VariableColumns_;  // Variable-size columns only
    using TSimdI8 = typename TTrait::TSimdI8;
    template <class T> using TSimd = typename TTrait::template TSimd8<T>;

    static constexpr ui8 kSIMDMaxCols = 4;
    static constexpr ui8 kSIMDMaxInnerLoopSize = 4;

    size_t BlockRows_; // Estimated rows per cache block
    std::vector<size_t> BlockColsOffsets_;
    std::vector<size_t> BlockFixedColsSizes_;
    std::vector<size_t> BlockColumnsOrigInds_;

    struct SIMDSmallTupleDesc {
        ui8 Cols = 0;
        ui8 InnerLoopIters;
        size_t SmallTupleSize;
        size_t RowOffset;
    };
    SIMDSmallTupleDesc SIMDSmallTuple_;
    std::vector<TSimd<ui8>> SIMDPermMasks_; // small tuple precomputed masks

    struct SIMDTransposeDesc {
        ui8 Cols = 0;
        size_t RowOffset;
    };
    // [1, 2, 4, 8] bytes x [Key, Payload]
    std::array<SIMDTransposeDesc, 8> SIMDTranspositions_;
    static constexpr std::array<size_t, 4> SIMDTranspositionsColSizes_ = {1, 2,
                                                                          4, 8};
};

template <>
void TTupleLayoutFallback<NSimd::TSimdFallbackTraits>::Pack(
    const ui8 **columns, const ui8 **isValidBitmask, ui8 *res,
    std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;

template <>
void TTupleLayoutFallback<NSimd::TSimdFallbackTraits>::Unpack(
    ui8 **columns, ui8 **isValidBitmask, const ui8 *res,
    const std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start,
    ui32 count) const;

// It is expected that key columns layout is same for lhs and rhs
Y_FORCE_INLINE
bool CompareKeys(
    const TTupleLayout* lhsLayout, const ui8* lhsData, const std::vector<ui8, TMKQLAllocator<ui8>>& lhsOverflow,
    const TTupleLayout* rhsLayout, const ui8* rhsData, const std::vector<ui8, TMKQLAllocator<ui8>>& rhsOverflow)
{
    const ui8* lhsKey = lhsData + lhsLayout->KeyColumnsOffset;
    const ui8* rhsKey = rhsData + rhsLayout->KeyColumnsOffset;

    ui32 fixedEndOffset = lhsLayout->KeyColumnsFixedEnd - lhsLayout->KeyColumnsOffset;
    if (!std::equal(lhsKey, lhsKey + fixedEndOffset, rhsKey)) { // fixed size columns can be compared via std::equal
        return false;
    }

    lhsKey += fixedEndOffset;
    rhsKey += fixedEndOffset;

    const auto n = lhsLayout->KeyColumnsNum;
    for (ui32 i = lhsLayout->KeyColumnsFixedNum; i < n; ++i) {
        const auto& lhsCol = lhsLayout->KeyColumns[i];
        const auto& rhsCol = lhsLayout->KeyColumns[i];

        auto lhsSize = ReadUnaligned<ui8>(lhsKey);
        auto rhsSize = ReadUnaligned<ui8>(rhsKey);

        if (lhsSize != rhsSize) {
            return false;
        }

        if (lhsSize < 255) { // embedded str
            if (!std::equal(lhsKey + 1, lhsKey + 1 + lhsSize, rhsKey + 1)) {
                return false;
            }
        } else { // overflow buffer used
            const auto lhsPrefixSize = (lhsCol.DataSize - 1 - 2 * sizeof(ui32));
            const auto rhsPrefixSize = (rhsCol.DataSize - 1 - 2 * sizeof(ui32));

            if (lhsPrefixSize != rhsPrefixSize) {
                return false;
            }

            const auto lhsOverflowOffset = ReadUnaligned<ui32>(lhsKey + 1 + 0 * sizeof(ui32));
            const auto lhsOverflowSize = ReadUnaligned<ui32>(lhsKey + 1 + 1 * sizeof(ui32));

            const auto rhsOverflowOffset = ReadUnaligned<ui32>(rhsKey + 1 + 0 * sizeof(ui32));
            const auto rhsOverflowSize = ReadUnaligned<ui32>(rhsKey + 1 + 1 * sizeof(ui32));

            if (lhsOverflowSize != rhsOverflowSize) {
                return false;
            }

            if (!std::equal(lhsKey + 1 + 2 * sizeof(ui32), lhsKey + 1 + 2 * sizeof(ui32) + lhsPrefixSize, rhsKey + 1 + 2 * sizeof(ui32))) {
                return false;
            }
            auto lhsBit = lhsOverflow.begin() + lhsOverflowOffset;
            auto lhsEit = lhsOverflow.begin() + lhsOverflowOffset + lhsOverflowSize;
            auto rhsBit = rhsOverflow.begin() + rhsOverflowOffset;
            if (!std::equal(lhsBit, lhsEit, rhsBit)) {
                return false;
            }
        }

        lhsKey += lhsCol.DataSize;
        rhsKey += rhsCol.DataSize;
    }

    return true;
}

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
