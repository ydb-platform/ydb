#pragma once
#include "others_storage.h"
#include "settings.h"
#include "stats.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <contrib/libs/xxhash/xxhash.h>
#include <util/string/join.h>

namespace NKikimr::NArrow::NAccessor {
class TSubColumnsArray;
}

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TColumnElements {
private:
    YDB_READONLY_DEF(TStringBuf, KeyName);
    YDB_READONLY_DEF(std::deque<TStringBuf>, Values);
    YDB_READONLY_DEF(std::vector<ui32>, RecordIndexes);
    YDB_READONLY(ui32, DataSize, 0);
    std::shared_ptr<IChunkedArray> Accessor;

public:
    const std::shared_ptr<IChunkedArray>& GetAccessorVerified() const {
        AFL_VERIFY(!!Accessor);
        return Accessor;
    }

    void BuildSparsedAccessor(const ui32 recordsCount);
    void BuildPlainAccessor(const ui32 recordsCount);

    TColumnElements(const TStringBuf key)
        : KeyName(key) {
    }

    void AddData(const TStringBuf sb, const ui32 index) {
        Values.emplace_back(sb);
        AFL_VERIFY(RecordIndexes.empty() || RecordIndexes.back() < index);
        RecordIndexes.emplace_back(index);
        DataSize += sb.size();
    }
};

class TDataBuilder {
public:
    class IBuffers {
    public:
        virtual ~IBuffers() = default;
    };

private:
    class TStorageAddress {
    private:
        const TStringBuf Prefix;
        const TStringBuf Key;
        const size_t Hash;

    public:
        TStorageAddress(const TStringBuf prefix, const TStringBuf key)
            : Prefix(prefix)
            , Key(key)
            , Hash(XXH3_64bits(Prefix.data(), Prefix.size()) ^ XXH3_64bits(Key.data(), Key.size())) {
        }

        operator size_t() const {
            return Hash;
        }

        bool operator==(const TStorageAddress& item) const {
            return Hash == item.Hash && Prefix == item.Prefix && Key == item.Key;
        }
    };

    ui32 CurrentRecordIndex = 0;
    THashMap<TStringBuf, TColumnElements> Elements;
    THashMap<TStorageAddress, std::string> StorageHash;
    std::deque<std::string> Storage;
    std::deque<TString> StorageStrings;
    const std::shared_ptr<arrow::DataType> Type;
    const TSettings Settings;
    std::vector<std::shared_ptr<IBuffers>> Buffers;

public:
    TDataBuilder(const std::shared_ptr<arrow::DataType>& type, const TSettings& settings);

    void StoreBuffer(const std::shared_ptr<IBuffers>& data) {
        Buffers.emplace_back(data);
    }

    void StartNextRecord() {
        ++CurrentRecordIndex;
    }

    TStringBuf AddKeyOwn(const TStringBuf currentPrefix, std::string&& key);
    TStringBuf AddKey(const TStringBuf currentPrefix, const TStringBuf key);

    void AddKVNull(const TStringBuf key) {
        auto itElements = Elements.find(key);
        if (itElements == Elements.end()) {
            itElements = Elements.emplace(key, key).first;
        }
        itElements->second.AddData(GetNullString(), CurrentRecordIndex);
    }

    static const TString& GetNullString() {
        const static TString nullString = "NULL";
        return nullString;
    }

    static std::string_view GetNullStringView() {
        return std::string_view(GetNullString().data(), GetNullString().size());
    }

    void AddKV(const TStringBuf key, const TStringBuf value) {
        auto itElements = Elements.find(key);
        if (itElements == Elements.end()) {
            itElements = Elements.emplace(key, key).first;
        }
        itElements->second.AddData(value, CurrentRecordIndex);
    }

    void AddKVOwn(const TStringBuf key, std::string&& value) {
        Storage.emplace_back(std::move(value));
        auto itElements = Elements.find(key);
        if (itElements == Elements.end()) {
            itElements = Elements.emplace(key, key).first;
        }
        itElements->second.AddData(Storage.back(), CurrentRecordIndex);
    }

    void AddKVOwn(const TStringBuf key, TString&& value) {
        StorageStrings.emplace_back(std::move(value));
        auto itElements = Elements.find(key);
        if (itElements == Elements.end()) {
            itElements = Elements.emplace(key, key).first;
        }
        itElements->second.AddData(StorageStrings.back(), CurrentRecordIndex);
    }

    class THeapElements {
    private:
        const TColumnElements* Elements;
        ui32 Index = 0;
        ui32 KeyIndex = 0;

    public:
        THeapElements(const TColumnElements* elements, const ui32 keyIndex)
            : Elements(elements)
            , KeyIndex(keyIndex) {
            AFL_VERIFY(Elements);
        }

        ui32 GetRecordIndex() const {
            return Elements->GetRecordIndexes()[Index];
        }

        TStringBuf GetKey() const {
            return Elements->GetKeyName();
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        const TStringBuf* GetValuePointer() const {
            return &Elements->GetValues()[Index];
        }

        bool operator<(const THeapElements& item) const {
            if (Elements->GetRecordIndexes()[Index] == item.Elements->GetRecordIndexes()[item.Index]) {
                return item.Elements->GetKeyName() < Elements->GetKeyName();
            } else {
                return item.Elements->GetRecordIndexes()[item.Index] < Elements->GetRecordIndexes()[Index];
            }
        }

        bool IsValid() const {
            return Index < Elements->GetRecordIndexes().size();
        }

        bool Next() {
            return ++Index < Elements->GetRecordIndexes().size();
        }
    };

    TDictStats BuildStats(const std::vector<TColumnElements*>& keys, const TSettings& settings, const ui32 recordsCount) const {
        auto builder = TDictStats::MakeBuilder();
        for (auto&& i : keys) {
            builder.Add(i->GetKeyName(), i->GetRecordIndexes().size(), i->GetDataSize(),
                settings.IsSparsed(i->GetRecordIndexes().size(), recordsCount) ? IChunkedArray::EType::SparsedArray
                                                                               : IChunkedArray::EType::Array);
        }
        return builder.Finish();
    }

    TOthersData MergeOthers(const std::vector<TColumnElements*>& otherKeys, const ui32 recordsCount) const;

    std::shared_ptr<TSubColumnsArray> Finish();
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
