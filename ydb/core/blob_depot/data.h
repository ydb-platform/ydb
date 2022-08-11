#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData {
        TBlobDepot* const Self;

    public:
        class alignas(TString) TKey {
            struct TData {
                ui8 Bytes[31];
                ui8 Type;
            } Data;

            static constexpr size_t TypeLenByteIdx = 31;
            static constexpr size_t MaxInlineStringLen = TypeLenByteIdx;
            static constexpr char BlobIdType = 32;
            static constexpr char StringType = 33;

            static_assert(sizeof(Data) == 32);

        public:
            TKey() {
                Data.Type = EncodeInlineStringLenAsTypeByte(0);
                Data.Bytes[0] = 0;
            }

            explicit TKey(TLogoBlobID id) {
                Data.Type = BlobIdType;
                reinterpret_cast<TLogoBlobID&>(Data.Bytes) = id;
            }

            template<typename T, typename = std::enable_if_t<std::is_constructible_v<TString, T&&>>>
            explicit TKey(T&& value) {
                if (value.size() <= MaxInlineStringLen) {
                    Data.Type = EncodeInlineStringLenAsTypeByte(value.size());
                    memcpy(Data.Bytes, value.data(), value.size());
                    if (value.size() != MaxInlineStringLen) {
                        Data.Bytes[value.size()] = 0;
                    }
                } else {
                    Data.Type = StringType;
                    new(Data.Bytes) TString(std::move(value));
                }
            }

            TKey(const TKey& other) {
                if (other.Data.Type == StringType) {
                    Data.Type = StringType;
                    new(Data.Bytes) TString(other.GetString());
                } else {
                    Data = other.Data;
                }
            }

            TKey(TKey&& other) {
                if (other.Data.Type == StringType) {
                    Data.Type = StringType;
                    new(Data.Bytes) TString(std::move(other.GetString()));
                    other.Reset();
                } else {
                    Data = other.Data;
                }
            }

            ~TKey() {
                Reset();
            }

            TKey& operator =(const TKey& other) {
                if (this != &other) {
                    if (Data.Type == StringType && other.Data.Type == StringType) {
                        GetString() = other.GetString();
                    } else if (Data.Type == StringType) {
                        GetString().~TString();
                        Data = other.Data;
                    } else if (other.Data.Type == StringType) {
                        Data.Type = StringType;
                        new(Data.Bytes) TString(other.GetString());
                    } else {
                        Data = other.Data;
                    }
                }
                return *this;
            }

            TKey& operator =(TKey&& other) {
                if (this != &other) {
                    if (Data.Type == StringType && other.Data.Type == StringType) {
                        GetString() = std::move(other.GetString());
                        other.Reset();
                    } else if (Data.Type == StringType) {
                        GetString().~TString();
                        Data = other.Data;
                    } else if (other.Data.Type == StringType) {
                        Data.Type = StringType;
                        new(Data.Bytes) TString(std::move(other.GetString()));
                        other.Reset();
                    } else {
                        Data = other.Data;
                    }
                }
                return *this;
            }

            std::variant<TLogoBlobID, TStringBuf> AsVariant() const {
                if (Data.Type == BlobIdType) {
                    return GetBlobId();
                } else {
                    return GetStringBuf();
                }
            }

            TString MakeBinaryKey() const {
                if (Data.Type == BlobIdType) {
                    return GetBlobId().AsBinaryString();
                } else {
                    return TString(GetStringBuf());
                }
            }

            static TKey FromBinaryKey(const TString& key, const NKikimrBlobDepot::TBlobDepotConfig& config) {
                if (config.GetOperationMode() == NKikimrBlobDepot::EOperationMode::VirtualGroup) {
                    return TKey(TLogoBlobID::FromBinary(key));
                } else {
                    return TKey(key);
                }
            }

            TString ToString() const {
                TStringStream s;
                Output(s);
                return s.Str();
            }

            void Output(IOutputStream& s) const {
                if (Data.Type == BlobIdType) {
                    s << GetBlobId();
                } else {
                    s << EscapeC(GetStringBuf());
                }
            }

            static int Compare(const TKey& x, const TKey& y) {
                if (x.Data.Type == BlobIdType && y.Data.Type == BlobIdType) {
                    return x.GetBlobId() < y.GetBlobId() ? -1 : y.GetBlobId() < x.GetBlobId() ? 1 : 0;
                } else if (x.Data.Type == BlobIdType) {
                    return -1;
                } else if (y.Data.Type == BlobIdType) {
                    return 1;
                } else {
                    const TStringBuf sbx = x.GetStringBuf();
                    const TStringBuf sby = y.GetStringBuf();
                    return sbx < sby ? -1 : sby < sbx ? 1 : 0;
                }
            }

            const TLogoBlobID& GetBlobId() const {
                Y_VERIFY_DEBUG(Data.Type == BlobIdType);
                return reinterpret_cast<const TLogoBlobID&>(Data.Bytes);
            }

            friend bool operator ==(const TKey& x, const TKey& y) { return Compare(x, y) == 0; }
            friend bool operator !=(const TKey& x, const TKey& y) { return Compare(x, y) != 0; }
            friend bool operator < (const TKey& x, const TKey& y) { return Compare(x, y) <  0; }
            friend bool operator <=(const TKey& x, const TKey& y) { return Compare(x, y) <= 0; }
            friend bool operator > (const TKey& x, const TKey& y) { return Compare(x, y) >  0; }
            friend bool operator >=(const TKey& x, const TKey& y) { return Compare(x, y) >= 0; }

        private:
            void Reset() {
                if (Data.Type == StringType) {
                    GetString().~TString();
                }
                Data.Type = EncodeInlineStringLenAsTypeByte(0);
                Data.Bytes[0] = 0;
            }

            TStringBuf GetStringBuf() const {
                if (Data.Type == StringType) {
                    return GetString();
                } else {
                    return TStringBuf(reinterpret_cast<const char*>(Data.Bytes), DecodeInlineStringLenFromTypeByte(Data.Type));
                }
            }

            const TString& GetString() const {
                Y_VERIFY_DEBUG(Data.Type == StringType);
                return reinterpret_cast<const TString&>(Data.Bytes);
            }

            TString& GetString() {
                Y_VERIFY_DEBUG(Data.Type == StringType);
                return reinterpret_cast<TString&>(Data.Bytes);
            }

            static ui8 EncodeInlineStringLenAsTypeByte(size_t len) {
                Y_VERIFY_DEBUG(len <= MaxInlineStringLen);
                return len == MaxInlineStringLen ? 0 : len ? len : MaxInlineStringLen;
            }

            static size_t DecodeInlineStringLenFromTypeByte(ui8 type) {
                return EncodeInlineStringLenAsTypeByte(type);
            }
        };

        struct TValue {
            TString Meta;
            TValueChain ValueChain;
            NKikimrBlobDepot::EKeepState KeepState;
            bool Public;
            bool Unconfirmed;
            std::optional<TLogoBlobID> OriginalBlobId;

            TValue() = delete;
            TValue(const TValue&) = delete;
            TValue(TValue&&) = default;

            TValue& operator =(const TValue&) = delete;
            TValue& operator =(TValue&&) = default;

            explicit TValue(NKikimrBlobDepot::TValue&& proto)
                : Meta(proto.GetMeta())
                , ValueChain(std::move(*proto.MutableValueChain()))
                , KeepState(proto.GetKeepState())
                , Public(proto.GetPublic())
                , Unconfirmed(proto.GetUnconfirmed())
                , OriginalBlobId(proto.HasOriginalBlobId()
                    ? std::make_optional(LogoBlobIDFromLogoBlobID(proto.GetOriginalBlobId()))
                    : std::nullopt)
            {}

            explicit TValue(NKikimrBlobDepot::EKeepState keepState)
                : KeepState(keepState)
                , Public(false)
                , Unconfirmed(false)
            {}
        };

        enum EScanFlags : ui32 {
            INCLUDE_BEGIN = 1,
            INCLUDE_END = 2,
            REVERSE = 4,
        };

        Y_DECLARE_FLAGS(TScanFlags, EScanFlags)

    private:
        struct TRecordWithTrash {};

        struct TRecordsPerChannelGroup
            : TIntrusiveListItem<TRecordsPerChannelGroup, TRecordWithTrash>
        {
            const ui64 TabletId;
            const ui8 Channel;
            const ui32 GroupId;

            std::set<TLogoBlobID> Used;
            std::set<TLogoBlobID> Trash; // committed trash
            std::vector<TLogoBlobID> TrashInFlight;
            ui32 PerGenerationCounter = 1;
            TGenStep IssuedGenStep; // currently in flight or already confirmed
            TGenStep LastConfirmedGenStep;
            bool CollectGarbageRequestInFlight = false;
            TBlobSeqId LeastExpectedBlobId;

            TRecordsPerChannelGroup(ui64 tabletId, ui8 channel, ui32 groupId)
                : TabletId(tabletId)
                , Channel(channel)
                , GroupId(groupId)
            {}

            void MoveToTrash(TData *self, TLogoBlobID id);
            void OnSuccessfulCollect(TData *self);
            void OnLeastExpectedBlobIdChange(TData *self, TBlobSeqId leastExpectedBlobId);
            void ClearInFlight(TData *self);
            void EnqueueForCollectionIfPossible(TData *self);
        };

        bool Loaded = false;
        std::map<TKey, TValue> Data;
        THashMap<TLogoBlobID, ui32> RefCount;
        THashMap<std::tuple<ui64, ui8, ui32>, TRecordsPerChannelGroup> RecordsPerChannelGroup;
        TIntrusiveList<TRecordsPerChannelGroup, TRecordWithTrash> RecordsWithTrash;
        std::optional<TKey> LastLoadedKey; // keys are being loaded in ascending order

        THashMultiMap<void*, TLogoBlobID> InFlightTrash; // being committed, but not yet confirmed

        class TTxIssueGC;
        class TTxConfirmGC;

        class TTxLoad;

        class TTxLoadSpecificKeys;
        class TTxResolve;

    public:
        TData(TBlobDepot *self)
            : Self(self)
        {}

        template<typename TCallback>
        void ScanRange(const TKey *begin, const TKey *end, TScanFlags flags, TCallback&& callback) {
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

        TRecordsPerChannelGroup& GetRecordsPerChannelGroup(TLogoBlobID id);

        void AddDataOnLoad(TKey key, TString value);
        void AddDataOnDecommit(const TBlobDepot::TBlob& blob, NTabletFlatExecutor::TTransactionContext& txc);
        void AddTrashOnLoad(TLogoBlobID id);
        void AddGenStepOnLoad(ui8 channel, ui32 groupId, TGenStep issuedGenStep, TGenStep confirmedGenStep);

        void PutKey(TKey key, TValue&& data);

        std::optional<TString> UpdateKeepState(TKey key, NKikimrBlobDepot::EKeepState keepState);
        void DeleteKey(const TKey& key, const std::function<void(TLogoBlobID)>& updateTrash, void *cookie);
        void CommitTrash(void *cookie);
        void HandleTrash();
        void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);
        void OnPushNotifyResult(TEvBlobDepot::TEvPushNotifyResult::TPtr ev);
        void OnCommitConfirmedGC(ui8 channel, ui32 groupId);

        void AccountBlob(TLogoBlobID id, bool add);

        bool CanBeCollected(ui32 groupId, TBlobSeqId id) const;

        void OnLeastExpectedBlobIdChange(ui8 channel);

        static TString ToValueProto(const TValue& value);

        template<typename TCallback>
        void EnumerateRefCount(TCallback&& callback) {
            for (const auto& [key, value] : RefCount) {
                callback(key, value);
            }
        }

        template<typename TCallback>
        void EnumerateTrash(TCallback&& callback) {
            for (const auto& [key, record] : RecordsPerChannelGroup) {
                THashSet<TLogoBlobID> inFlight(record.TrashInFlight.begin(), record.TrashInFlight.end());
                for (const TLogoBlobID& id : record.Trash) {
                    callback(record.GroupId, id, inFlight.contains(id));
                }
            }
        }

        void StartLoad();
        void OnLoadComplete();
        bool IsLoaded() const { return Loaded; }

        void Handle(TEvBlobDepot::TEvResolve::TPtr ev);

    private:
        void ExecuteIssueGC(ui8 channel, ui32 groupId, TGenStep issuedGenStep,
            std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage);

        void ExecuteConfirmGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, TGenStep confirmedGenStep);
    };

    Y_DECLARE_OPERATORS_FOR_FLAGS(TBlobDepot::TData::TScanFlags)

} // NKikimr::NBlobDepot
