#include "yaml_config_helpers.h"

namespace NKikimr::NYaml {

NJson::TJsonValue* Traverse(NJson::TJsonValue& json, const TVector<TString>& pathPieces) {
    NJson::TJsonValue* elem = &json;
    for (auto& piece : pathPieces) {
        if (elem == nullptr) { return elem; }

        ui32 id{};
        if (TryFromString(piece, id)) {
            if (!elem->GetValuePointer(id, const_cast<const NJson::TJsonValue**>(&elem))) {
                return nullptr;
            }
            continue;
        }

        if (!elem->GetValuePointer(piece, &elem)) {
            return nullptr;
        }
    }

    return elem;
}

NJson::TJsonValue* Traverse(NJson::TJsonValue& json, const TStringBuf& path, TString* lastName) {
    Y_ENSURE_BT(path.StartsWith('/'));
    Y_ENSURE_BT(!path.EndsWith('/'));
    TString pathCopy = TString(path);
    pathCopy.erase(0, 1);
    TVector<TString> pathPieces = StringSplitter(pathCopy).Split('/');
    Y_ENSURE_BT(pathPieces.size() > 0);
    if (lastName) {
        *lastName = pathPieces.back();
        pathPieces.resize(pathPieces.size() - 1);
    }
    return Traverse(json, pathPieces);
}

const NJson::TJsonValue* Traverse(const NJson::TJsonValue& json, const TVector<TString>& pathPieces) {
    const NJson::TJsonValue* elem = &json;
    for (auto& piece : pathPieces) {
        if (elem == nullptr) { return elem; }

        ui32 id{};
        if (TryFromString(piece, id)) {
            if (!elem->GetValuePointer(id, &elem)) {
                return nullptr;
            }
            continue;
        }

        if (!elem->GetValuePointer(piece, &elem)) {
            return nullptr;
        }
    }

    return elem;
}

const NJson::TJsonValue* Traverse(const NJson::TJsonValue& json, const TStringBuf& path, TString* lastName) {
    Y_ENSURE_BT(path.StartsWith('/'));
    Y_ENSURE_BT(!path.EndsWith('/'));
    TString pathCopy = TString(path);
    pathCopy.erase(0, 1);
    TVector<TString> pathPieces = StringSplitter(pathCopy).Split('/');
    Y_ENSURE_BT(pathPieces.size() > 0);
    if (lastName) {
        *lastName = pathPieces.back();
        pathPieces.resize(pathPieces.size() - 1);
    }
    return Traverse(json, pathPieces);
}

void Iterate(
    const NJson::TJsonValue& json,
    const std::span<TString>& pathPieces,
    std::function<void(const std::vector<ui32>&, const NJson::TJsonValue&)> onElem,
    std::vector<ui32>& offsets,
    size_t offsetId)
{
    const NJson::TJsonValue* elem = &json;
    for (ui32 i = 0; i < pathPieces.size(); ++i) {
        auto& piece = pathPieces[i];

        if (elem == nullptr) { return; }

        ui32 id{};
        if (TryFromString(piece, id)) {
            if (!elem->GetValuePointer(id, &elem)) {
                return;
            }
            continue;
        }

        if (piece == "*") {
            if (elem->IsArray()) {
                int j = 0;
                for (auto& item : elem->GetArraySafe()) {
                    if (offsets.size() < offsetId + 1) {
                        offsets.resize(offsetId + 1);
                    }
                    offsets[offsetId] = j;
                    if (i != pathPieces.size() - 1) {
                        Iterate(item, pathPieces.subspan(i + 1), onElem, offsets, offsetId + 1);
                    } else {
                        onElem(offsets, item);
                    }
                    ++j;
                }
            }
            break;
        } else if (i == pathPieces.size() - 1) {
            onElem(offsets, *elem);
        } else if (!elem->GetValuePointer(piece, &elem)) {
            return;
        }
    }
}

void Iterate(const NJson::TJsonValue& json, const TStringBuf& path, std::function<void(const std::vector<ui32>&, const NJson::TJsonValue&)> onElem) {
    Y_ENSURE_BT(path.StartsWith('/'));
    Y_ENSURE_BT(!path.EndsWith('/'));
    TString pathCopy = TString(path);
    pathCopy.erase(0, 1);
    TVector<TString> pathPieces = StringSplitter(pathCopy).Split('/');
    std::vector<ui32> offsets;
    Iterate(json, pathPieces, onElem, offsets);
    Y_ENSURE_BT(pathPieces.size() > 0);
}

void IterateMut(
    NJson::TJsonValue& json,
    const std::span<TString>& pathPieces,
    std::function<void(const std::vector<ui32>&, NJson::TJsonValue&)> onElem,
    std::vector<ui32>& offsets,
    size_t offsetId)
{
    NJson::TJsonValue* elem = &json;
    for (ui32 i = 0; i < pathPieces.size(); ++i) {
        auto& piece = pathPieces[i];

        if (elem == nullptr) { return; }

        ui32 id{};
        if (TryFromString(piece, id)) {
            if (!elem->GetValuePointer(id, const_cast<const NJson::TJsonValue**>(&elem))) {
                return;
            }
            continue;
        }

        if (piece == "*") {
            if (elem->IsArray()) {
                int j = 0;
                for (auto& item : elem->GetArraySafe()) {
                    if (offsets.size() < offsetId + 1) {
                        offsets.resize(offsetId + 1);
                    }
                    offsets[offsetId] = j;
                    if (i != pathPieces.size() - 1) {
                        IterateMut(item, pathPieces.subspan(i + 1), onElem, offsets, offsetId + 1);
                    } else {
                        onElem(offsets, item);
                    }
                }
            }
            break;
        } else if (i == pathPieces.size() - 1) {
            onElem(offsets, *elem);
        } else if (!elem->GetValuePointer(piece, &elem)) {
            return;
        }
    }
}

void IterateMut(NJson::TJsonValue& json, const TStringBuf& path, std::function<void(const std::vector<ui32>&, NJson::TJsonValue&)> onElem) {
    Y_ENSURE_BT(path.StartsWith('/'));
    Y_ENSURE_BT(!path.EndsWith('/'));
    TString pathCopy = TString(path);
    pathCopy.erase(0, 1);
    TVector<TString> pathPieces = StringSplitter(pathCopy).Split('/');
    Y_ENSURE_BT(pathPieces.size() > 0);
    std::vector<ui32> offsets;
    IterateMut(json, pathPieces, onElem, offsets);
}

} // namespace NKikimr::NYaml
