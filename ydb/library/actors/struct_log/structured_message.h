#pragma once

#include "key_name.h"
#include "native_types_mapping.h"
#include "native_types_support.h"

#include <algorithm>
#include <initializer_list>
#include <iterator>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace NActors::NStructuredLog {

class TCreateMessageArg;

class TStructuredMessage {
    friend class TCreateMessageArg;
    static const unsigned PreallocatedValueCount = 16;
    static const unsigned PreallocatedDataSize = 256;

public:
    TStructuredMessage();

    TStructuredMessage(const TStructuredMessage&) = default;
    TStructuredMessage(TStructuredMessage&&) = default;

    TStructuredMessage& operator=(const TStructuredMessage&) = default;
    TStructuredMessage& operator=(TStructuredMessage&&) = default;

    template <typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type>
    inline TStructuredMessage& AppendValue(std::vector<TKeyName>&& name, const T& value) {
        auto typeCode = TTypesMapping::GetCode<T>();

        auto offset = Data.size();
        TTypesMapping::Serialize(value, Data);
        auto length = Data.size() - offset;

        AttachedValues.emplace_back(std::move(name), typeCode, offset, length, ++AddNumber);
        AttachedValuesSorted = false;

        return *this;
    }

    template <unsigned N>
    inline TStructuredMessage& AppendFixedValue(std::vector<TKeyName>&& name, const char (&value)[N]) {
        auto typeCode = TTypesMapping::GetCode<TString>();

        auto offset = Data.size();
        TTypesMapping::Serialize(TString(value), Data);
        auto length = Data.size() - offset;

        AttachedValues.emplace_back(std::move(name), typeCode, offset, length, ++AddNumber);
        AttachedValuesSorted = false;

        return *this;
    }

    inline TStructuredMessage& AppendMessage(const TStructuredMessage& message) {
        auto offset = Data.size();
        for (const auto& subItem : message.AttachedValues) {
            auto name = subItem.Name;
            AttachedValues.emplace_back(
                std::move(name), subItem.TypeCode, subItem.Offset + offset, subItem.Length, ++AddNumber
            );
        }

        auto oldSize = Data.size();
        auto addSize = message.Data.size();
        Data.resize(oldSize + addSize);
        memcpy(Data.data() + oldSize, message.Data.data(), addSize);
        AttachedValuesSorted = false;

        return *this;
    }

    inline TStructuredMessage& AppendSubMessage(TKeyName&& name, const TStructuredMessage& subMessage) {
        auto offset = Data.size();

        for (const auto& subItem : subMessage.AttachedValues) {
            std::vector<TKeyName> addKey{name};
            std::copy(begin(subItem.Name), end(subItem.Name), std::back_inserter(addKey));

            AttachedValues.emplace_back(
                std::move(addKey), subItem.TypeCode, subItem.Offset + offset, subItem.Length, ++AddNumber
            );
        }

        auto oldSize = Data.size();
        Data.resize(oldSize + subMessage.Data.size());
        std::memcpy(Data.data() + oldSize, subMessage.Data.data(), subMessage.Data.size());

        AttachedValuesSorted = false;

        return *this;
    }

    std::size_t GetValuesCount() const;

    const std::vector<TKeyName>& GetValueName(std::size_t index) const;

    std::optional<std::size_t> GetValueIndex(const TString& name) const;

    std::optional<std::size_t> GetValueIndex(const std::vector<TKeyName>& name) const;

    bool HasValue(const TString& name) const;

    template <typename T>
    bool CheckValueType(std::size_t index) const {
        EnsureSorted();
        return AttachedValues[index].TypeCode == TTypesMapping::GetCode<T>();
    }

    template <typename T>
    std::optional<T> GetValue(std::size_t index) const {
        EnsureSorted();

        T result;
        auto& attachedValue = AttachedValues[index];
        if (!TTypesMapping::Deserialize(
                result, attachedValue.TypeCode, Data.data() + attachedValue.Offset, attachedValue.Length
            )) {
            return {};
        }
        return result;
    }

    template <typename T>
    std::optional<T> GetValue(const TString& name) const {
        auto index = GetValueIndex(name);
        if (!index.has_value()) {
            return {};
        }
        return GetValue<T>(index.value());
    }

    void RemoveValue(std::size_t index);

    void RemoveValue(const TString& name);

    void RemoveValues(const std::initializer_list<TString>& names);

    void RenameValue(std::size_t index, std::vector<TKeyName>&& newName);

    void RenameValue(const TString& oldName, std::vector<TKeyName>&& newName);

    template <typename TCallable>
    bool ForEachSerialized(const TCallable& callable) const {
        EnsureSorted();

        for (auto& item : AttachedValues) {
            if (!callable(item.Name, item.TypeCode, Data.data() + item.Offset, item.Length)) {
                return false;
            }
        }
        return true;
    }

    template <typename TCallable>
    void ForEachTyped(const TCallable& callable) const {
        EnsureSorted();

        for (auto& item : AttachedValues) {
            TTypesMapping::Invoke(item.TypeCode, Data.data() + item.Offset, item.Length, [&](const auto& value) {
                callable(item.Name, value);
            });
        }
    }

    void Clear();

protected:
    struct TAttachedValue {
        std::vector<TKeyName> Name;
        TNativeTypeCode TypeCode;
        std::size_t Offset;
        std::size_t Length;
        unsigned AddNumber;

        TAttachedValue() = default;
        TAttachedValue(const TAttachedValue&) = default;
        TAttachedValue(TAttachedValue&&) = default;

        TAttachedValue(
            std::vector<TKeyName>&& name,
            TNativeTypeCode typeCode,
            std::size_t Offset,
            std::size_t Length,
            unsigned addNumber
        );

        TAttachedValue& operator=(const TAttachedValue&) = default;
        bool operator<(const TAttachedValue& value) const;
    };
    mutable std::vector<TAttachedValue> AttachedValues;
    mutable bool AttachedValuesSorted{true};

    unsigned AddNumber{0};
    TBinaryData Data;

    void EnsureSorted() const;

    void RemoveDups() const;
};

}  // namespace NActors::NStructuredLog
