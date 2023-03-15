#pragma once

#include "format.h"

#include <ydb/library/yql/minikql/dom/node.h>

#include <util/system/unaligned_mem.h>
#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

namespace NKikimr::NBinaryJson {

class TContainerCursor;

/**
 * @brief Reads values inside BinaryJson. `Read...` methods of this class are not intended for direct use.
 * Consider using `GetRootCursor` method to get more convenient interface over BinaryJson data
 */
class TBinaryJsonReader : public TSimpleRefCount<TBinaryJsonReader> {
public:
    template <typename... Args>
    static TIntrusivePtr<TBinaryJsonReader> Make(Args&&... args) {
        return new TBinaryJsonReader{std::forward<Args>(args)...};
    }

    TContainerCursor GetRootCursor();

    TMeta ReadMeta(ui32 offset) const;

    TEntry ReadEntry(ui32 offset) const;

    TKeyEntry ReadKeyEntry(ui32 offset) const;

    /**
     * @brief Reads string from String index
     *
     * @param offset Offset to the beginning of TSEntry
     */
    const TStringBuf ReadString(ui32 offset) const;

    /**
     * @brief Reads number from Number index
     *
     * @param offset Offset to the beginning of number
     */
    double ReadNumber(ui32 offset) const;

private:
    explicit TBinaryJsonReader(const TBinaryJson& buffer);

    explicit TBinaryJsonReader(TStringBuf buffer);

    template <typename T>
    T ReadPOD(ui32 offset) const {
        static_assert(std::is_pod_v<T>, "Type must be POD");
        Y_ENSURE(offset + sizeof(T) <= Buffer.Size(),
            TStringBuilder() << "Not enough space in buffer to read value (" << offset << " + " << sizeof(T) << " > " << Buffer.Size() << ")");
        return ReadUnaligned<T>(Buffer.Data() + offset);
    }

    TStringBuf Buffer;
    THeader Header;
    ui32 TreeStart;
    ui32 StringSEntryStart;
    ui32 StringCount;
};

using TBinaryJsonReaderPtr = TIntrusivePtr<TBinaryJsonReader>;

/**
 * @brief Interface to single TEntry inside BinaryJson
 */
class TEntryCursor {
public:
    TEntryCursor(TBinaryJsonReaderPtr reader, TEntry entry);

    EEntryType GetType() const;

    TContainerCursor GetContainer() const;

    TStringBuf GetString() const;

    double GetNumber() const;

private:
    TBinaryJsonReaderPtr Reader;
    TEntry Entry;
};

/**
 * @brief Iterator to walk through array elements
 */
class TArrayIterator {
public:
    TArrayIterator(TBinaryJsonReaderPtr reader, ui32 startOffset, ui32 count);

    TEntryCursor Next();

    bool HasNext() const;

private:
    TBinaryJsonReaderPtr Reader;
    ui32 Offset;
    ui32 EndOffset;
};

/**
 * @brief Iterator to walk through object key-value pairs
 */
class TObjectIterator {
public:
    TObjectIterator(TBinaryJsonReaderPtr reader, ui32 startOffset, ui32 count);

    std::pair<TEntryCursor, TEntryCursor> Next();

    bool HasNext() const;

private:
    TBinaryJsonReaderPtr Reader;
    ui32 KeyOffset;
    ui32 ValueOffset;
    ui32 ValueEndOffset;
};

/**
 * @brief Interface to container inside BinaryJson
 */
class TContainerCursor {
public:
    TContainerCursor(TBinaryJsonReaderPtr reader, ui32 startOffset);

    EContainerType GetType() const;

    /**
     * @brief Get container size. Array length for arrays and count of unique keys for objects
     */
    ui32 GetSize() const;

    /**
     * @brief Get array element at specified index
     */
    TEntryCursor GetElement(ui32 index) const;

    /**
     * @brief Get iterator to array elements
     */
    TArrayIterator GetArrayIterator() const;

    /**
     * @brief Get value corresponding to given key in object
     */
    TMaybe<TEntryCursor> Lookup(const TStringBuf key) const;

    /**
     * @brief Get iterator to object key-value pairs
     */
    TObjectIterator GetObjectIterator() const;

private:
    TBinaryJsonReaderPtr Reader;
    ui32 StartOffset;
    TMeta Meta;
};

NUdf::TUnboxedValue ReadContainerToJsonDom(const TContainerCursor& cursor, const NUdf::IValueBuilder* valueBuilder);

NUdf::TUnboxedValue ReadElementToJsonDom(const TEntryCursor& cursor, const NUdf::IValueBuilder* valueBuilder);

/**
 * @brief Reads whole BinaryJson into TUnboxedValue using DOM layout from `yql/library/dom` library
 */
NUdf::TUnboxedValue ReadToJsonDom(const TBinaryJson& binaryJson, const NUdf::IValueBuilder* valueBuilder);

NUdf::TUnboxedValue ReadToJsonDom(TStringBuf binaryJson, const NUdf::IValueBuilder* valueBuilder);

/**
 * @brief Serializes whole BinaryJson into textual JSON
 */
TString SerializeToJson(const TBinaryJson& binaryJson);

TString SerializeToJson(TStringBuf binaryJson);

bool IsValidBinaryJson(TStringBuf buffer);

TMaybe<TStringBuf> IsValidBinaryJsonWithError(TStringBuf buffer);

}
