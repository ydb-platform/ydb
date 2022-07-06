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
                if (beginIt != endIt) {
                    --endIt;
                    do {
                        auto& current = *endIt--;
                        if (!callback(current.first, current.second)) {
                            break;
                        }
                    } while (beginIt != endIt);
                }
            } else {
                while (beginIt != endIt) {
                    auto& current = *beginIt++;
                    if (!callback(current.first, current.second)) {
                        break;
                    }
                }
            }
        }

        void DeleteKey(TStringBuf key) {
            Data.erase(TString(key));
        }

        void PutKey(TString key, TDataValue&& data) {
            auto getKeyString = [&] {
                if (Self->Config.GetOperationMode() == NKikimrBlobDepot::VirtualGroup) {
                    return TLogoBlobID(reinterpret_cast<const ui64*>(key.data())).ToString();
                } else {
                    return key;
                }
            };
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "PutKey", (TabletId, Self->TabletID()), (Key, EscapeC(getKeyString())),
                (KeepState, NKikimrBlobDepot::EKeepState_Name(data.KeepState)));

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

        std::optional<TString> UpdateKeepState(TStringBuf key, NKikimrBlobDepot::EKeepState keepState) {
            auto& value = Data[TString(key)];
            if (value.KeepState < keepState) {
                value.KeepState = keepState;
                return ToValueProto(value);
            } else {
                return std::nullopt;
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

    void TBlobDepot::DeleteKey(TStringBuf key) {
        DataManager->DeleteKey(key);
    }

    void TBlobDepot::PutKey(TString key, TDataValue&& data) {
        DataManager->PutKey(std::move(key), std::move(data));
    }

    void TBlobDepot::AddDataOnLoad(TString key, TString value) {
        DataManager->AddDataOnLoad(std::move(key), std::move(value));
    }

    std::optional<TString> TBlobDepot::UpdateKeepState(TStringBuf key, NKikimrBlobDepot::EKeepState keepState) {
        return DataManager->UpdateKeepState(key, keepState);
    }

} // NKikimr::NBlobDepot
