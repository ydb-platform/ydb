#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <util/generic/buffer.h>

#include <util/system/cpu_id.h>
#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

// Defines if data type of particular column variable or fixed
enum class EColumnSizeType {Fixed, Variable};

// Defines if particular column is key column or payload column
enum class EColumnRole {Key, Payload};

// Describes layout and size of particular column
struct TColumnDesc {
    ui32    ColumnIndex = 0;    // Index of the column in particular layout
    ui32    OriginalIndex = 0;  // Index of the column in input representation
    EColumnRole Role = EColumnRole::Payload;    // Role of the particular column in tuple (Key or Payload)
    EColumnSizeType SizeType = EColumnSizeType::Fixed;  // Fixed size or variable size column
    ui32 DataSize = 0;  // Size of the column in bytes for fixed size part
                        // Must be same for matching key columns
    ui32 Offset = 0;    // Offset in bytes for column value from the beginning of tuple
};

// Defines in memory layout of tuple.
struct TTupleLayout {
    std::vector<TColumnDesc> OrigColumns;   // Columns description and order as passed during layout construction
    std::vector<TColumnDesc> Columns;       // Vector describing all columns in order corresponding to tuple layout
    std::vector<TColumnDesc> KeyColumns;    // Vector describing key columns
    std::vector<TColumnDesc> PayloadColumns;    // Vector describing payload columns
    ui32 KeyColumnsNum;                 // Total number of key columns
    ui32 KeyColumnsSize;                // Total size of all key columns in bytes
    ui32 KeyColumnsOffset;              // Start of row-packed keys data
    ui32 KeyColumnsFixedEnd;            // Offset in row-packed keys data of first variable key (can be same as KeyColumnsEnd, if there are none)
    ui32 KeyColumnsFixedNum;            // Number of fixed-size columns
    ui32 KeyColumnsEnd;                 // First byte after key columns. Start of bitmask for row-based columns
    ui32 BitmaskSize;                   // Size of bitmask for null values flag in columns
    ui32 BitmaskOffset;                 // Offset of nulls bitmask. = KeyColumnsEnd
    ui32 BitmaskEnd;                    // First byte after bitmask. = PayloadOffset
    ui32 PayloadSize;                   // Total size in bytes of the payload columns
    ui32 PayloadOffset;                 // Offset of payload values. = BitmaskEnd.
    ui32 PayloadEnd;                    // First byte after payload
    ui32 TotalRowSize;                  // Total size of bytes for packed row

    // Creates new tuple layout based on provided columns description.
    static THolder<TTupleLayout> Create(const std::vector<TColumnDesc>& columns);

    TTupleLayout(const std::vector<TColumnDesc> &columns):OrigColumns(columns) {}
    virtual ~TTupleLayout() {}

    // Takes array of pointer to columns, array of validity bitmaps,
    // outputs packed rows
    virtual void Pack( const ui8** columns, const ui8** isValidBitmask, ui8 * res, std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start, ui32 count) const = 0;
};

template <typename TTrait>
struct TTupleLayoutFallback: public TTupleLayout {

    TTupleLayoutFallback(const std::vector<TColumnDesc>& columns);

    void Pack( const ui8** columns, const ui8** isValidBitmask, ui8 * res, std::vector<ui8, TMKQLAllocator<ui8>> &overflow, ui32 start, ui32 count) const override;

private:
    std::array<std::vector<TColumnDesc>, 5> FixedPOTColumns_; // Fixed-size columns for power-of-two sizes from 1 to 16 bytes
    std::vector<TColumnDesc> FixedNPOTColumns_; // Remaining fixed-size columns
    std::vector<TColumnDesc> VariableColumns_; // Variable-size columns only
    using TSimdI8 = typename TTrait::TSimdI8;
};

}
}
}
