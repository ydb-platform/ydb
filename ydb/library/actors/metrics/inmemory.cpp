#include "inmemory.h"

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

    TLineMeta::TLineMeta() noexcept
        : Frontend(&TRawLineFrontend<>::Descriptor())
    {
    }

    TLineMeta::TLineMeta(const TLineFrontendOps* frontend) noexcept
        : Frontend(frontend ? frontend : &TRawLineFrontend<>::Descriptor())
    {
    }

    TStringBuf TLineMeta::FrontendName() const noexcept {
        return Frontend ? Frontend->Name : TStringBuf();
    }

    TLineKey MakeLineKey(TStringBuf name, std::span<const TLabel> labels) {
        TLineKey key;
        key.Name = TString(name);
        key.Labels.reserve(labels.size());
        for (const auto& label : labels) {
            key.Labels.push_back(label);
        }
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
