#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TDataManager {
        TBlobDepot* const Self;

        struct TCompareKey {
            bool operator ()(const TString& x, const TString& y) const { return x < y; }
            bool operator ()(const TStringBuf& x, const TString& y) const { return x < y; }
            bool operator ()(const TString& x, const TStringBuf& y) const { return x < y; }

            using is_transparent = void;
        };

        std::map<TString, TDataValue, TCompareKey> Data;
        std::set<TLogoBlobID> DataBlobIds;

    public:
        TDataManager(TBlobDepot *self)
            : Self(self)
        {
            (void)Self;
        }

        std::optional<TDataValue> FindKey(TStringBuf key) {
            const auto it = Data.find(key);
            return it != Data.end() ? std::make_optional(it->second) : std::nullopt;
        }

        void ScanRange(const std::optional<TStringBuf>& begin, const std::optional<TStringBuf>& end,
                TScanFlags flags, const std::function<bool(TStringBuf, const TDataValue&)>& callback) {
            auto beginIt = !begin ? Data.begin()
                : flags & EScanFlags::INCLUDE_BEGIN ? Data.lower_bound(*begin)
                : Data.upper_bound(*begin);

            auto endIt = !end ? Data.end()
                : flags & EScanFlags::INCLUDE_END ? Data.upper_bound(*end)
                : Data.lower_bound(*end);

            if (flags & EScanFlags::REVERSE) {
                while (beginIt != endIt) {
                    --endIt;
                    if (!callback(endIt->first, endIt->second)) {
                        break;
                    }
                }
            } else {
                while (beginIt != endIt) {
                    if (!callback(beginIt->first, beginIt->second)) {
                        break;
                    }
                    ++beginIt;
                }
            }
        }

        void DeleteKeys(const std::vector<TString>& keysToDelete) {
            for (const TString& key : keysToDelete) {
                Data.erase(key);
            }
        }

        void PutKey(TString key, TDataValue&& data) {
            Data[std::move(key)] = std::move(data);
        }

        void AddDataOnLoad(TString key, TString value) {
            NKikimrBlobDepot::TValue proto;
            const bool success = proto.ParseFromString(value);
            Y_VERIFY(success);
            PutKey(std::move(key), {
                .Meta = proto.GetMeta(),
                .ValueChain = std::move(*proto.MutableValueChain()),
                .KeepState = proto.GetKeepState(),
                .Public = proto.GetPublic(),
            });
        }

        std::optional<TString> UpdatesKeepState(TStringBuf key, NKikimrBlobDepot::EKeepState keepState) {
            if (const auto it = Data.find(key); it != Data.end()) {
                TDataValue value = it->second;
                value.KeepState = keepState;
                return ToValueProto(value);
            } else {
                return std::nullopt;
            }
        }

        void UpdateKeepState(const std::vector<std::pair<TString, NKikimrBlobDepot::EKeepState>>& data) {
            for (const auto& [key, keepState] : data) {
                auto& value = Data[std::move(key)];
                Y_VERIFY_DEBUG(value.KeepState < keepState);
                value.KeepState = keepState;
            }
        }

        static TString ToValueProto(const TDataValue& value) {
            NKikimrBlobDepot::TValue proto;
            if (value.Meta) {
                proto.SetMeta(value.Meta);
            }
            proto.MutableValueChain()->CopyFrom(value.ValueChain);
            if (proto.GetKeepState() != value.KeepState) {
                proto.SetKeepState(value.KeepState);
            }
            if (proto.GetPublic() != value.Public) {
                proto.SetPublic(value.Public);
            }

            TString s;
            const bool success = proto.SerializeToString(&s);
            Y_VERIFY(success);
            return s;
        }
    };

    TBlobDepot::TDataManagerPtr TBlobDepot::CreateDataManager() {
        return {new TDataManager{this}, std::default_delete<TDataManager>{}};
    }

    std::optional<TBlobDepot::TDataValue> TBlobDepot::FindKey(TStringBuf key) {
        return DataManager->FindKey(key);
    }

    void TBlobDepot::ScanRange(const std::optional<TStringBuf>& begin, const std::optional<TStringBuf>& end,
            TScanFlags flags, const std::function<bool(TStringBuf, const TDataValue&)>& callback) {
        return DataManager->ScanRange(begin, end, flags, callback);
    }

    void TBlobDepot::DeleteKeys(const std::vector<TString>& keysToDelete) {
        DataManager->DeleteKeys(keysToDelete);
    }

    void TBlobDepot::PutKey(TString key, TDataValue&& data) {
        DataManager->PutKey(std::move(key), std::move(data));
    }

    void TBlobDepot::AddDataOnLoad(TString key, TString value) {
        DataManager->AddDataOnLoad(std::move(key), std::move(value));
    }

    std::optional<TString> TBlobDepot::UpdatesKeepState(TStringBuf key, NKikimrBlobDepot::EKeepState keepState) {
        return DataManager->UpdatesKeepState(key, keepState);
    }

    void TBlobDepot::UpdateKeepState(const std::vector<std::pair<TString, NKikimrBlobDepot::EKeepState>>& data) {
        DataManager->UpdateKeepState(data);
    }

} // NKikimr::NBlobDepot
