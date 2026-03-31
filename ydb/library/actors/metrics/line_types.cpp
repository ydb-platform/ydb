#include "line_types.h"

#include <util/generic/algorithm.h>

namespace NActors {

    bool TLabel::operator==(const TLabel& rhs) const noexcept {
        return Name == rhs.Name && Value == rhs.Value;
    }

    bool TLineKey::operator==(const TLineKey& rhs) const noexcept {
        return Name == rhs.Name && Labels == rhs.Labels;
    }

    size_t TLineKeyHash::operator()(const TLineKey& key) const noexcept {
        size_t hash = THash<TString>()(key.Name);
        for (const auto& label : key.Labels) {
            hash = CombineHashes(hash, THash<TString>()(label.Name));
            hash = CombineHashes(hash, THash<TString>()(label.Value));
        }
        return hash;
    }

    TLineKey MakeLineKey(TStringBuf name, std::span<const TLabel> labels) {
        TLineKey key;
        key.Name = TString(name);
        key.Labels.reserve(labels.size());
        for (const auto& label : labels) {
            key.Labels.push_back(label);
        }
        Sort(key.Labels, [](const TLabel& lhs, const TLabel& rhs) {
            if (lhs.Name != rhs.Name) {
                return lhs.Name < rhs.Name;
            }
            return lhs.Value < rhs.Value;
        });
        return key;
    }

    TVector<TLabel> NormalizeCommonLabels(std::span<const TLabel> labels) {
        TVector<TLabel> normalized;
        normalized.reserve(labels.size());
        for (const auto& label : labels) {
            bool replaced = false;
            for (auto& current : normalized) {
                if (current.Name == label.Name) {
                    current.Value = label.Value;
                    replaced = true;
                    break;
                }
            }
            if (!replaced) {
                normalized.push_back(label);
            }
        }
        return normalized;
    }

} // namespace NActors
