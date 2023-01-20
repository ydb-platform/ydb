#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

#include <util/generic/hash_multi_map.h>

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData {
        TBlobDepot* const Self;

        using EKeepState = NKikimrBlobDepot::EKeepState;

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
                if (config.HasVirtualGroupId()) {
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

            struct THash {
                size_t operator ()(const TKey& key) const {
                    const auto v = key.AsVariant();
                    return std::visit([&](auto& value) { return MultiHash(v.index(), value); }, v);
                }
            };

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
            EKeepState KeepState = EKeepState::Default;
            bool Public = false;
            bool GoingToAssimilate = false;
            bool UncertainWrite = false;

            TValue() = default;
            TValue(const TValue&) = delete;
            TValue(TValue&&) = default;

            TValue& operator =(const TValue&) = delete;
            TValue& operator =(TValue&&) = default;

            explicit TValue(NKikimrBlobDepot::TValue&& proto, bool uncertainWrite)
                : Meta(proto.GetMeta())
                , ValueChain(std::move(*proto.MutableValueChain()))
                , KeepState(proto.GetKeepState())
                , Public(proto.GetPublic())
                , GoingToAssimilate(proto.GetGoingToAssimilate())
                , UncertainWrite(uncertainWrite)
            {}

            explicit TValue(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item)
                : Meta(item.GetMeta())
                , Public(false)
                , UncertainWrite(item.GetUncertainWrite())
            {
                auto *chain = ValueChain.Add();
                auto *locator = chain->MutableLocator();
                locator->CopyFrom(item.GetBlobLocator());
            }

            explicit TValue(EKeepState keepState)
                : KeepState(keepState)
                , Public(false)
                , UncertainWrite(false)
            {}

            bool IsWrittenUncertainly() const {
                return UncertainWrite && !ValueChain.empty();
            }

            void SerializeToProto(NKikimrBlobDepot::TValue *proto) const {
                if (Meta) {
                    proto->SetMeta(Meta);
                }
                if (!ValueChain.empty()) {
                    proto->MutableValueChain()->CopyFrom(ValueChain);
                }
                if (KeepState != proto->GetKeepState()) {
                    proto->SetKeepState(KeepState);
                }
                if (Public != proto->GetPublic()) {
                    proto->SetPublic(Public);
                }
                if (GoingToAssimilate != proto->GetGoingToAssimilate()) {
                    proto->SetGoingToAssimilate(GoingToAssimilate);
                }
            }

            static bool Validate(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item);
            bool SameValueChainAsIn(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item) const;

            TString SerializeToString() const {
                NKikimrBlobDepot::TValue proto;
                SerializeToProto(&proto);
                TString s;
                const bool success = proto.SerializeToString(&s);
                Y_VERIFY(success);
                return s;
            }

            TString ToString() const {
                TStringStream s;
                Output(s);
                return s.Str();
            }

            void Output(IOutputStream& s) const {
                s << "{Meta# '" << EscapeC(Meta) << "'"
                    << " ValueChain# " << FormatList(ValueChain)
                    << " KeepState# " << EKeepState_Name(KeepState)
                    << " Public# " << (Public ? "true" : "false")
                    << " GoingToAssimilate# " << (GoingToAssimilate ? "true" : "false")
                    << " UncertainWrite# " << (UncertainWrite ? "true" : "false")
                    << "}";
            }
        };

        enum EScanFlags : ui32 {
            INCLUDE_BEGIN = 1,
            INCLUDE_END = 2,
            REVERSE = 4,
        };

        Y_DECLARE_FLAGS(TScanFlags, EScanFlags)

    private:
        struct TRecordWithTrash {};

        struct TRecordsPerChannelGroup {
            const ui8 Channel;
            const ui32 GroupId;

            std::set<TLogoBlobID> Used;
            std::set<TLogoBlobID> Trash; // committed trash
            std::vector<TLogoBlobID> TrashInFlight;
            ui32 PerGenerationCounter = 1;
            TGenStep IssuedGenStep; // currently in flight or already confirmed
            TGenStep LastConfirmedGenStep;
            bool CollectGarbageRequestInFlight = false;

            TRecordsPerChannelGroup(ui8 channel, ui32 groupId)
                : Channel(channel)
                , GroupId(groupId)
            {}

            void MoveToTrash(TData *self, TLogoBlobID id);
            void OnSuccessfulCollect(TData *self);
            void OnLeastExpectedBlobIdChange(TData *self);
            void ClearInFlight(TData *self);
            void CollectIfPossible(TData *self);
        };

        bool Loaded = false;
        std::map<TKey, TValue> Data;
        std::set<TKey> LoadSkip; // keys to skip while loading
        THashMap<TLogoBlobID, ui32> RefCount;
        THashMap<std::tuple<ui8, ui32>, TRecordsPerChannelGroup> RecordsPerChannelGroup;
        std::optional<TKey> LastLoadedKey; // keys are being loaded in ascending order
        std::optional<TLogoBlobID> LastAssimilatedBlobId;
        ui64 TotalStoredDataSize = 0;
        ui64 TotalStoredTrashSize = 0;
        ui64 InFlightTrashSize = 0;

        friend class TGroupAssimilator;

        THashMultiMap<void*, TLogoBlobID> InFlightTrash; // being committed, but not yet confirmed

        struct TResolveDecommitContext {
            TEvBlobDepot::TEvResolve::TPtr Ev; // original resolve request
            ui32 NumRangesInFlight;
            std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob> DecommitBlobs = {};
            std::vector<TKey> ResolutionErrors = {};
            std::deque<TKey> DropNodataBlobs = {};
        };
        ui64 LastResolveQueryId = 0;
        THashMap<ui64, TResolveDecommitContext> ResolveDecommitContexts;

        class TTxIssueGC;
        class TTxConfirmGC;

        class TTxDataLoad;

        class TTxLoadSpecificKeys;
        class TTxResolve;
        class TResolveResultAccumulator;

        class TUncertaintyResolver;
        std::unique_ptr<TUncertaintyResolver> UncertaintyResolver;
        friend class TBlobDepot;

        std::deque<TKey> KeysMadeCertain; // but not yet committed
        bool CommitCertainKeysScheduled = false;

        struct TCollectCmd {
            ui64 QueryId;
            ui32 GroupId;
        };
        ui64 LastCollectCmdId = 0;
        std::unordered_map<ui64, TCollectCmd> CollectCmds;

    public:
        TData(TBlobDepot *self);
        ~TData();

        template<typename TCallback, typename T>
        bool ScanRange(const T& begin, const T& end, TScanFlags flags, TCallback&& callback) {
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
                            return false;
                        }
                    } while (beginIt != endIt);
                }
            } else {
                while (beginIt != endIt) {
                    auto& current = *beginIt++;
                    if (!callback(current.first, current.second)) {
                        return false;
                    }
                }
            }
            return true;
        }

        template<typename TCallback>
        void ShowRange(const std::optional<TKey>& seek, ui32 rowsBefore, ui32 rowsAfter, TCallback&& callback) {
            auto it = seek ? Data.lower_bound(*seek) : Data.begin();
            for (; it != Data.begin() && rowsBefore; --it, --rowsBefore, ++rowsAfter)
            {}
            for (; it != Data.end() && rowsAfter; ++it, --rowsAfter) {
                callback(it->first, it->second);
            }
        }

        const TValue *FindKey(const TKey& key) const;

        template<typename T, typename... TArgs>
        bool UpdateKey(TKey key, NTabletFlatExecutor::TTransactionContext& txc, void *cookie, const char *reason,
            T&& callback, TArgs&&... args);

        void UpdateKey(const TKey& key, const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie);

        void BindToBlob(const TKey& key, TBlobSeqId blobSeqId, NTabletFlatExecutor::TTransactionContext& txc, void *cookie);

        void MakeKeyCertain(const TKey& key);
        void HandleCommitCertainKeys();

        TRecordsPerChannelGroup& GetRecordsPerChannelGroup(TLogoBlobID id);
        TRecordsPerChannelGroup& GetRecordsPerChannelGroup(ui8 channel, ui32 groupId);

        void AddLoadSkip(TKey key);
        void AddDataOnLoad(TKey key, TString value, bool uncertainWrite, bool skip);
        bool AddDataOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlob& blob,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie, bool suppressLoadedCheck = false);
        void AddTrashOnLoad(TLogoBlobID id);
        void AddGenStepOnLoad(ui8 channel, ui32 groupId, TGenStep issuedGenStep, TGenStep confirmedGenStep);

        bool UpdateKeepState(TKey key, EKeepState keepState, NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void DeleteKey(const TKey& key, NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void CommitTrash(void *cookie);
        void HandleTrash(TRecordsPerChannelGroup& record);
        void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);
        void OnPushNotifyResult(TEvBlobDepot::TEvPushNotifyResult::TPtr ev);
        void OnCommitConfirmedGC(ui8 channel, ui32 groupId);
        bool OnBarrierShift(ui64 tabletId, ui8 channel, bool hard, TGenStep previous, TGenStep current, ui32& maxItems,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie);

        void AddFirstMentionedBlob(TLogoBlobID id);
        void AccountBlob(TLogoBlobID id, bool add);

        bool CanBeCollected(TBlobSeqId id) const;

        void OnLeastExpectedBlobIdChange(ui8 channel);

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
        bool IsKeyLoaded(const TKey& key) const { return key <= LastLoadedKey || Data.contains(key) || LoadSkip.contains(key); }

        void Handle(TEvBlobDepot::TEvResolve::TPtr ev);
        void Handle(TEvBlobStorage::TEvRangeResult::TPtr ev);
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);

        template<typename TCallback>
        void HandleResolutionResult(ui64 id, TCallback&& callback);

        ui64 GetTotalStoredDataSize() const {
            return TotalStoredDataSize;
        }

        void RenderMainPage(IOutputStream& s);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        bool BeginCommittingBlobSeqId(TAgent& agent, TBlobSeqId blobSeqId);
        void EndCommittingBlobSeqId(TAgent& agent, TBlobSeqId blobSeqId);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
        TMonotonic LastRecordsValidationTimestamp;

        void ValidateRecords();

    private:
        void ExecuteIssueGC(ui8 channel, ui32 groupId, TGenStep issuedGenStep,
            std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage, ui64 cookie);

        void ExecuteConfirmGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, TGenStep confirmedGenStep);
    };

    Y_DECLARE_OPERATORS_FOR_FLAGS(TBlobDepot::TData::TScanFlags)

} // NKikimr::NBlobDepot

template<> struct THash<NKikimr::NBlobDepot::TBlobDepot::TData::TKey> : NKikimr::NBlobDepot::TBlobDepot::TData::TKey::THash {};
template<> struct std::hash<NKikimr::NBlobDepot::TBlobDepot::TData::TKey> : THash<NKikimr::NBlobDepot::TBlobDepot::TData::TKey> {};
