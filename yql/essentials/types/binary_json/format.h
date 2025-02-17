#pragma once

#include <util/generic/buffer.h>

namespace NKikimr::NBinaryJson {

constexpr ui32 TYPE_SHIFT = 5;
constexpr ui32 MAX_TYPE = (1 << TYPE_SHIFT) - 1;
constexpr ui32 OFFSET_SHIFT = 27;
constexpr ui32 MAX_OFFSET = (1 << OFFSET_SHIFT) - 1;

/**
 * @brief THeader stores BinaryJson version and offset to the String index
 *
 * Structure:
 *  +-----------------+------------------------------+
 *  | Version, 5 bits | String index offset, 27 bits |
 *  +-----------------+------------------------------+
 */
enum class EVersion {
    Draft = 0,
    V1 = 1,
    MaxVersion = MAX_TYPE,
};

constexpr EVersion CURRENT_VERSION = EVersion::V1;

struct THeader {
    THeader() = default;

    THeader(EVersion version, ui32 stringOffset)
        : Version(version)
        , StringOffset(stringOffset)
    {
        Y_DEBUG_ABORT_UNLESS(StringOffset <= MAX_OFFSET);
    }

    EVersion Version : 5;
    ui32 StringOffset : 27;
};
static_assert(sizeof(THeader) == sizeof(ui32));

/**
 * @brief TEntry stores type of BinaryJson node and optional offset to the TSEntry or container
 *
 * Structure:
 *  +--------------------+----------------+
 *  | Entry type, 5 bits | Value, 27 bits |
 *  +--------------------+----------------+
 */
enum class EEntryType {
    BoolFalse = 0,
    BoolTrue = 1,
    Null = 2,
    String = 3,
    Number = 4,
    Container = 5,
};

struct TEntry {
    TEntry() = default;

    TEntry(EEntryType type, ui32 value = 0)
        : Type(type)
        , Value(value)
    {
        Y_DEBUG_ABORT_UNLESS(value <= MAX_OFFSET);
    }

    EEntryType Type : 5;
    ui32 Value : 27;
};
static_assert(sizeof(TEntry) == sizeof(ui32));

/**
 * @brief TKeyEntry stores offset to the TSEntry containing object key
 */
using TKeyEntry = ui32;

/**
 * @brief TSEntry stores string type and offset to the string value in String index
 *
 * Structure:
 *  +---------------------+------------------------+
 *  | String type, 5 bits | String offset, 27 bits |
 *  +---------------------+------------------------+
 */
enum class EStringType {
    RawNullTerminated = 0,
};

struct TSEntry {
    TSEntry() = default;

    TSEntry(EStringType type, ui32 value)
        : Type(type)
        , Value(value)
    {
        Y_DEBUG_ABORT_UNLESS(value <= MAX_OFFSET);
    }

    EStringType Type : 5;
    ui32 Value : 27;
};
static_assert(sizeof(TSEntry) == sizeof(ui32));

/**
 * @brief TMeta stores container type and container size. For arrays container size is simply
 * array size and for objects it is 2 * (number of key-value pairs)
 *
 * Structure:
 *  +------------------------+---------------+
 *  | Container type, 5 bits | Size, 27 bits |
 *  +------------------------+---------------+
 */
enum class EContainerType {
    Array = 0,
    Object = 1,
    TopLevelScalar = 2,
};

struct TMeta {
    TMeta() = default;

    TMeta(EContainerType type, ui32 size)
        : Type(type)
        , Size(size)
    {
        Y_DEBUG_ABORT_UNLESS(size <= MAX_OFFSET);
    }

    EContainerType Type : 5;
    ui32 Size : 27;
};
static_assert(sizeof(TMeta) == sizeof(ui32));

/**
 * @brief Buffer to store serialized BinaryJson
 */
using TBinaryJson = TBuffer;

}