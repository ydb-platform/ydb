#include "structured_message.h"

namespace NActors::NStructuredLog {

TStructuredMessage::TStructuredMessage() {
    AttachedValues.reserve(PreallocatedValueCount);
    Data.reserve(PreallocatedDataSize);
}

std::size_t TStructuredMessage::GetValuesCount() const {
    EnsureSorted();
    return AttachedValues.size();
}

const std::vector<TKeyName>& TStructuredMessage::GetValueName(std::size_t index) const {
    EnsureSorted();
    return AttachedValues[index].Name;
}

std::optional<std::size_t> TStructuredMessage::GetValueIndex(const TString& name) const {
    return GetValueIndex(std::vector<TKeyName>{{name}});
}

std::optional<std::size_t> TStructuredMessage::GetValueIndex(const std::vector<TKeyName>& name) const {
    EnsureSorted();

    auto it = std::upper_bound(begin(AttachedValues), end(AttachedValues), name,
        [](const auto& name, const auto& b) -> bool {
            return b.Name > name;
        }
    );
    if (it == begin(AttachedValues)) {
        return std::nullopt;
    }

    it--;
    if (it->Name != name) {
        return std::nullopt;
    }

    return it - begin(AttachedValues);
}

bool TStructuredMessage::HasValue(const TString& name) const {
    return GetValueIndex(name).has_value();
}

void TStructuredMessage::RemoveValue(std::size_t index) {
    EnsureSorted();
    AttachedValues.erase(begin(AttachedValues) + index);
}

void TStructuredMessage::RemoveValue(const TString& name) {
    auto index = GetValueIndex(name);
    if (index.has_value()) {
        AttachedValues.erase(begin(AttachedValues) + index.value());
    }
}

void TStructuredMessage::RemoveValues(const std::initializer_list<TString>& names) {
    for (const auto& name : names) {
        RemoveValue(name);
    }
}

void TStructuredMessage::RenameValue(std::size_t index, std::vector<TKeyName>&& newName) {
    EnsureSorted();
    AttachedValues[index].Name = std::move(newName);

    auto value = AttachedValues[index];
    AttachedValues.erase(begin(AttachedValues) + index);
    value.AddNumber = AddNumber++;

    auto pos = std::upper_bound(begin(AttachedValues), end(AttachedValues), value);
    AttachedValues.insert(pos, std::move(value));

    RemoveDups();
}

void TStructuredMessage::RenameValue(const TString& oldName, std::vector<TKeyName>&& newName) {
    EnsureSorted();
    auto index = TStructuredMessage::GetValueIndex(oldName);
    if (index.has_value()) {
        RenameValue(index.value(), std::move(newName));
    }
}

void TStructuredMessage::Clear() {
    Data.clear();
    AttachedValues.clear();
}


TStructuredMessage::TAttachedValue::TAttachedValue(
        std::vector<TKeyName>&& name,
        TNativeTypeCode typeCode,
        std::size_t Offset,
        std::size_t Length,
        unsigned addNumber)
    : Name(std::move(name))
    , TypeCode(typeCode)
    , Offset(Offset)
    , Length(Length)
    , AddNumber(addNumber)
{}

bool TStructuredMessage::TAttachedValue::operator<(const TAttachedValue& value) const {
    if (Name < value.Name) {
        return true;
    }
    if (Name > value.Name) {
        return false;
    }
    return AddNumber > value.AddNumber;  // Last added value will be first and std::unique will has retained this value
}

void TStructuredMessage::EnsureSorted() const {
    if (AttachedValuesSorted) {
        return;
    }

    std::sort(begin(AttachedValues), end(AttachedValues));
    RemoveDups();
    AttachedValuesSorted = true;
}

void TStructuredMessage::RemoveDups() const {
    auto it = std::unique(begin(AttachedValues), end(AttachedValues), [](const auto& a, const auto& b) -> bool {
        return a.Name == b.Name;
    });
    AttachedValues.erase(it, end(AttachedValues));
}

}  // namespace NActors::NStructuredLog
