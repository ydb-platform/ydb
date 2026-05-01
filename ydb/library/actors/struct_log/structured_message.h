#pragma once

#include "key_name.h"
#include "native_types_support.h"
#include "native_types_mapping.h"

#include <algorithm>
#include <initializer_list>
#include <iterator>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace NKikimr::NStructuredLog {

class TCreateMessageArg;
class TStructuredMessage {
    friend class TCreateMessageArg;
    static const unsigned PreallocatedValueCount = 16;
    static const unsigned PreallocatedDataSize = 64;

public:

    TStructuredMessage() {
        AttachedValues.reserve(16);
        Data.reserve(64);
    };

    TStructuredMessage(const TStructuredMessage&) = default;
    TStructuredMessage(TStructuredMessage&&) = default;

    TStructuredMessage& operator=(const TStructuredMessage&) = default;
    TStructuredMessage& operator=(TStructuredMessage&&) = default;

    template <typename T, typename V = typename std::enable_if<TNativeTypeSupport<T>::value>::type >
    inline TStructuredMessage& AppendValue(std::vector<TKeyName>&& name, const T& value) {
        auto typeCode = TTypesMapping::GetCode<T>();

        auto offset = Data.size();
        TTypesMapping::Serialize(value, Data);
        auto length = Data.size() - offset;

        AttachedValues.emplace_back(std::move(name), typeCode, offset, length, ++AddNumber);
        AttachedValuesSorted = false;

        return *this;
    }

    template<unsigned N>
    inline TStructuredMessage& AppendFixedValue(std::vector<TKeyName>&& name, const char(&value)[N]) {
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
        for(auto& subItem: message.AttachedValues) {
            auto name = subItem.Name;
            AttachedValues.emplace_back(std::move(name), subItem.TypeCode, subItem.Offset + offset, subItem.Length, ++AddNumber);
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

        for(auto subItem: subMessage.AttachedValues) {
            std::vector<TKeyName> addKey{name};
            std::copy(begin(subItem.Name), end(subItem.Name), std::back_inserter(addKey));

            AttachedValues.emplace_back(std::move(addKey), subItem.TypeCode, subItem.Offset + offset, subItem.Length, ++AddNumber);
        }

        auto oldSize = Data.size();
        Data.resize(oldSize + subMessage.Data.size());
        std::memcpy(Data.data() + oldSize, subMessage.Data.data(), subMessage.Data.size());

        AttachedValuesSorted = false;

        return *this;
    }

    std::size_t GetValuesCount() const {
        CheckSorted();
        return AttachedValues.size();
    }

    const std::vector<TKeyName>& GetValueName(std::size_t index) const {
        CheckSorted();
        return AttachedValues[index].Name;
    }

    std::optional<std::size_t> GetValueIndex(const TString& name) const {
        return GetValueIndex(std::vector<TKeyName>{{name}});
    }

    std::optional<std::size_t> GetValueIndex(const std::vector<TKeyName>& name) const {
        CheckSorted();

        auto it = std::upper_bound(begin(AttachedValues), end(AttachedValues), name,
            [](const auto& name, const auto& b) -> bool
            {
                return b.Name > name;
            } );
        if (it == begin(AttachedValues)) return {};

        it--;
        if (it->Name != name) return {};

        return it - begin(AttachedValues);
    }

    bool HasValue(const TString& name) const {
        return GetValueIndex(name).has_value();
    }

    template<typename T>
    bool CheckValueType(std::size_t index) const {
        CheckSorted();
        return AttachedValues[index].TypeCode == TTypesMapping::GetCode<T>();
    }

    template<typename T>
    std::optional<T> GetValue(std::size_t index) const {
        CheckSorted();

        T result;
        auto& attachedValue = AttachedValues[index];
        if (!TTypesMapping::Deserialize(result, attachedValue.TypeCode, Data.data() + attachedValue.Offset, attachedValue.Length)) {
            return {};
        }
        return result;
    }

    template<typename T>
    std::optional<T> GetValue(const TString& name) const {
        auto index = GetValueIndex(name);
        if (!index.has_value()) {
            return {};
        }
        return GetValue<T>(index.value());
    }

    void RemoveValue(std::size_t index) {
        CheckSorted();
        AttachedValues.erase(begin(AttachedValues) + index);
    }

    void RemoveValue(const TString& name) {
        auto index = GetValueIndex(name);
        if (index.has_value()) {
            AttachedValues.erase(begin(AttachedValues) + index.value());
        }
    }

    void RemoveValues(const std::initializer_list<TString>& names) {
        for(auto name: names) {
            RemoveValue(name);
        }
    }

    void RenameValue(std::size_t index, std::vector<TKeyName>&& newName) {
        CheckSorted();
        AttachedValues[index].Name = std::move(newName);

        auto value = AttachedValues[index];
        AttachedValues.erase(begin(AttachedValues) + index);

        auto pos = std::upper_bound( begin(AttachedValues), end(AttachedValues), value);
        value.AddNumber = AddNumber++;
        AttachedValues.insert(pos, std::move(value));
        RemoveDups();
    }

    void RenameValue(const TString& oldName, std::vector<TKeyName>&& newName) {
        CheckSorted();
        auto index = GetValueIndex(oldName);
        if (index.has_value()) {
            RenameValue(index.value(), std::move(newName));
        }
    }

    template<typename C>
    bool ForEachSerialized(const C& c) const {
        CheckSorted();

        for(auto& item:AttachedValues) {
            if (!c(item.Name, item.TypeCode, Data.data() + item.Offset, item.Length)) {
                return false;
            }
        }
        return true;
    }

    template<typename C>
    void ForEach(const C& c) const {
        CheckSorted();

        for(auto& item:AttachedValues) {
            TTypesMapping::Invoke(item.TypeCode, Data.data() + item.Offset, item.Length,
                [&](const auto& value) {c(item.Name, value);}
            );
        }
    }

    void Clear() {
        Data.clear();
        AttachedValues.clear();
    }

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

        TAttachedValue(std::vector<TKeyName>&& name, TNativeTypeCode typeCode, std::size_t Offset, std::size_t Length, unsigned addNumber):
            Name(std::move(name)),
            TypeCode(typeCode),
            Offset(Offset),
            Length(Length),
            AddNumber(addNumber)
        {};

        TAttachedValue& operator=(const TAttachedValue&) = default;
        bool operator<(const TAttachedValue& value) const
        {
            if (Name < value.Name) {
                return true;
            }
            if (Name > value.Name) {
                return false;
            }
            return AddNumber > value.AddNumber; // Last added value will be first and std::unique will has retained this value
        }
    };
    mutable std::vector<TAttachedValue> AttachedValues;
    mutable bool AttachedValuesSorted{true};

    unsigned AddNumber{0};
    TBinaryData Data;

    void CheckSorted() const {
        if (AttachedValuesSorted) {
            return ;
        }

        std::sort(begin(AttachedValues), end(AttachedValues));
        RemoveDups();
        AttachedValuesSorted = true;
    }

    void RemoveDups() const {
        auto it = std::unique( begin(AttachedValues), end(AttachedValues),
            [](const auto& a, const auto& b)->bool
            {
                return a.Name == b.Name;
            });
        AttachedValues.erase(it, end(AttachedValues));
    }
};

}
