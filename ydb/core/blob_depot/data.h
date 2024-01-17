#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "closed_interval_set.h"

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
            static constexpr ui8 MinType = 32;
            static constexpr ui8 BlobIdType = 33;
            static constexpr ui8 StringType = 34;
            static constexpr ui8 MaxType = 255;

            static_assert(sizeof(Data) == 32);

        private:
            explicit TKey(ui8 type) {
                Data.Type = type;
            }

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

            static TKey Min() { return TKey(MinType); }
            static TKey Max() { return TKey(MaxType); }

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
                } else if (Data.Type <= MaxInlineStringLen || Data.Type == StringType) {
                    return TString(GetStringBuf());
                } else if (Data.Type == MinType) {
                    return {};
                } else {
                    Y_ABORT();
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
                } else if (Data.Type == MinType) {
                    s << "<min>";
                } else if (Data.Type == MaxType) {
                    s << "<max>";
                } else {
                    s << EscapeC(GetStringBuf());
                }
            }

            static int Compare(const TKey& x, const TKey& y) {
                const ui8 xType = x.Data.Type <= MaxInlineStringLen ? StringType : x.Data.Type;
                const ui8 yType = y.Data.Type <= MaxInlineStringLen ? StringType : y.Data.Type;
                if (xType < yType) {
                    return -1;
                } else if (yType < xType) {
                    return 1;
                } else {
                    switch (xType) {
                        case StringType: {
                            const TStringBuf sbx = x.GetStringBuf();
                            const TStringBuf sby = y.GetStringBuf();
                            return sbx < sby ? -1 : sby < sbx ? 1 : 0;
                        }

                        case BlobIdType: {
                            const TLogoBlobID& xId = x.GetBlobId();
                            const TLogoBlobID& yId = y.GetBlobId();
                            return xId < yId ? -1 : yId < xId ? 1 : 0;
                        }

                        case MinType:
                        case MaxType:
                            return 0;

                        default:
                            Y_ABORT();
                    }
                }
            }

            const TLogoBlobID& GetBlobId() const {
                Y_DEBUG_ABORT_UNLESS(Data.Type == BlobIdType);
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
                } else if (Data.Type <= MaxInlineStringLen) {
                    return TStringBuf(reinterpret_cast<const char*>(Data.Bytes), DecodeInlineStringLenFromTypeByte(Data.Type));
                } else {
                    Y_ABORT();
                }
            }

            const TString& GetString() const {
                Y_DEBUG_ABORT_UNLESS(Data.Type == StringType);
                return reinterpret_cast<const TString&>(Data.Bytes);
            }

            TString& GetString() {
                Y_DEBUG_ABORT_UNLESS(Data.Type == StringType);
                return reinterpret_cast<TString&>(Data.Bytes);
            }

            static ui8 EncodeInlineStringLenAsTypeByte(size_t len) {
                Y_DEBUG_ABORT_UNLESS(len <= MaxInlineStringLen);
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
            ui32 ValueVersion = 0;
            bool UncertainWrite = false;

            TValue() = default;
            TValue(TValue&&) = default;

            TValue& operator =(const TValue&) = delete;
            TValue& operator =(TValue&&) = default;

            explicit TValue(const TValue&) = default;

            explicit TValue(NKikimrBlobDepot::TValue&& proto, bool uncertainWrite)
                : Meta(proto.GetMeta())
                , ValueChain(std::move(*proto.MutableValueChain()))
                , KeepState(proto.GetKeepState())
                , Public(proto.GetPublic())
                , GoingToAssimilate(proto.GetGoingToAssimilate())
                , ValueVersion(proto.GetValueVersion())
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
                if (ValueVersion != proto->GetValueVersion()) {
                    proto->SetValueVersion(ValueVersion);
                }
            }

            static bool Validate(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item);
            bool SameValueChainAsIn(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item) const;

            TString SerializeToString() const {
                NKikimrBlobDepot::TValue proto;
                SerializeToProto(&proto);
                TString s;
                const bool success = proto.SerializeToString(&s);
                Y_ABORT_UNLESS(success);
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
                    << " ValueVersion# " << ValueVersion
                    << " UncertainWrite# " << (UncertainWrite ? "true" : "false")
                    << "}";
            }

            bool Changed(const TValue& other) const {
                return Meta != other.Meta ||
                    !IsSameValueChain(ValueChain, other.ValueChain) ||
                    KeepState != other.KeepState ||
                    Public != other.Public ||
                    GoingToAssimilate != other.GoingToAssimilate ||
                    ValueVersion != other.ValueVersion ||
                    UncertainWrite != other.UncertainWrite;
            }
        };

        enum EScanFlags : ui32 {
            INCLUDE_BEGIN = 1,
            INCLUDE_END = 2,
            REVERSE = 4,
        };

        Y_DECLARE_FLAGS(TScanFlags, EScanFlags);

        struct TScanRange {
            TKey Begin;
            TKey End;
            TScanFlags Flags = {};
            ui64 MaxKeys = 0;
            ui32 PrechargeRows = 0;
            ui64 PrechargeBytes = 0;
#ifndef NDEBUG
            std::set<TKey> KeysInRange = {}; // runtime state
#endif
        };

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
            TGenStep HardGenStep; // last sucessfully confirmed (non-persistent value)
            ui32 CollectGarbageRequestsInFlight = 0;
            TBlobSeqId LastLeastBlobSeqId;
            bool InitialCollectionComplete = false;

            TRecordsPerChannelGroup(ui8 channel, ui32 groupId)
                : Channel(channel)
                , GroupId(groupId)
            {}

            void MoveToTrash(TData *self, TLogoBlobID id);
            void OnSuccessfulCollect(TData *self);
            void DeleteTrashRecord(TData *self, std::set<TLogoBlobID>::iterator& it);
            void OnLeastExpectedBlobIdChange(TData *self);
            void ClearInFlight(TData *self);
            void CollectIfPossible(TData *self);
            bool Collectible(TData *self);
            TGenStep GetHardGenStep(TData *self);
        };

        bool Loaded = false;
        std::map<TKey, TValue> Data;
        TClosedIntervalSet<TKey> LoadedKeys; // keys that are already scanned and loaded in the local database
        THashMap<TLogoBlobID, ui32> RefCount;
        THashMap<std::tuple<ui8, ui32>, TRecordsPerChannelGroup> RecordsPerChannelGroup;
        std::optional<TLogoBlobID> LastAssimilatedBlobId;
        THashSet<std::tuple<ui8, ui32>> AlreadyCutHistory;
        ui64 TotalStoredDataSize = 0;
        ui64 TotalStoredTrashSize = 0;
        ui64 InFlightTrashSize = 0;

        friend class TGroupAssimilator;

        THashMultiMap<void*, TLogoBlobID> InFlightTrash; // being committed, but not yet confirmed

        class TTxIssueGC;
        class TTxConfirmGC;
        class TTxHardGC;

        class TTxDataLoad;

        class TTxLoadSpecificKeys;
        class TResolveResultAccumulator;

        class TUncertaintyResolver;
        std::unique_ptr<TUncertaintyResolver> UncertaintyResolver;
        friend class TBlobDepot;

        std::deque<TKey> KeysMadeCertain; // but not yet committed
        bool CommitCertainKeysScheduled = false;

        struct TCollectCmd {
            ui64 QueryId;
            ui32 GroupId;
            bool Hard;
            TGenStep GenStep;
        };
        ui64 LastCollectCmdId = 0;
        std::unordered_map<ui64, TCollectCmd> CollectCmds;

        struct TLoadRangeFromDB {
            TData* const Data;
            const TScanRange& Range;
            bool* const Progress;
            bool Processing = true;
            std::optional<TKey> LastProcessedKey = {};

            static constexpr struct TReverse {} Reverse{};
            static constexpr struct TLeftBound {} LeftBound{};
            static constexpr struct TRightBound {} RightBound{};

            template<typename TCallback>
            bool operator ()(NTabletFlatExecutor::TTransactionContext& txc, const TKey& left, const TKey& right, TCallback&& callback) {
                auto table = NIceDb::TNiceDb(txc.DB).Table<Schema::Data>();
                return Range.Flags & EScanFlags::REVERSE
                    ? Load(Reverse, table.Reverse(), left, right, std::forward<TCallback>(callback))
                    : Load(Reverse, std::move(table), left, right, std::forward<TCallback>(callback));
            }

            template<typename TTable, typename TCallback>
            bool Load(TReverse, TTable&& table, const TKey& left, const TKey& right, TCallback&& callback) {
                return left != TKey::Min()
                    ? Load(LeftBound, table.GreaterOrEqual(left.MakeBinaryKey()), left, right, std::forward<TCallback>(callback))
                    : right != TKey::Max()
                    ? Load(RightBound, table.LessOrEqual(right.MakeBinaryKey()), left, right, std::forward<TCallback>(callback))
                    : Load(RightBound, table.All(), left, right, std::forward<TCallback>(callback));
            }

            template<typename TTable, typename TCallback>
            bool Load(TLeftBound, TTable&& table, const TKey& left, const TKey& right, TCallback&& callback) {
                return right != TKey::Max()
                    ? Load(RightBound, table.LessOrEqual(right.MakeBinaryKey()), left, right, std::forward<TCallback>(callback))
                    : Load(RightBound, std::forward<TTable>(table), left, right, std::forward<TCallback>(callback));
            }

            template<typename TTable, typename TCallback>
            bool Load(TRightBound, TTable&& table, const TKey& left, const TKey& right, TCallback&& callback) {
                if ((Range.PrechargeRows || Range.PrechargeBytes) && !table.Precharge(Range.PrechargeRows, Range.PrechargeBytes)) {
                    return false;
                }
                auto rowset = table.Select();
                if (!rowset.IsReady()) {
                    return false;
                }
                while (rowset.IsValid()) {
                    TKey key = TKey::FromBinaryKey(rowset.GetKey(), Data->Self->Config);
                    STLOG(PRI_TRACE, BLOB_DEPOT, BDT46, "ScanRange.Load", (Id, Data->Self->GetLogId()), (Left, left),
                        (Right, right), (Key, key));
                    if (left < key && key < right) {
                        TValue* const value = Data->AddDataOnLoad(key, rowset.template GetValue<Schema::Data::Value>(),
                            rowset.template GetValueOrDefault<Schema::Data::UncertainWrite>());
                        if (Processing) {
                            // we should not feed keys out of range when we are processing prefetched data outside the range
                            if (Range.Flags & EScanFlags::REVERSE) {
                                Processing = Range.Flags & EScanFlags::INCLUDE_BEGIN ? Range.Begin <= key : Range.Begin < key;
                            } else {
                                Processing = Range.Flags & EScanFlags::INCLUDE_END ? key <= Range.End : key < Range.End;
                            }
                        }
                        Processing = Processing && callback(key, *value);
                        *Progress = true;
                    } else {
                        Y_DEBUG_ABORT_UNLESS(key == left || key == right);
                    }
                    LastProcessedKey.emplace(std::move(key));
                    if (!rowset.Next()) {
                        return false; // we break iteration anyway, because we can't read more data
                    }
                }
                return Processing;
            };
        };

    public:
        TData(TBlobDepot *self);
        ~TData();

        template<typename TCallback>
        bool ScanRange(TScanRange& range, NTabletFlatExecutor::TTransactionContext *txc, bool *progress, TCallback&& callback) {
            STLOG(PRI_TRACE, BLOB_DEPOT, BDT76, "ScanRange", (Id, Self->GetLogId()), (Begin, range.Begin), (End, range.End),
                (Flags, range.Flags), (MaxKeys, range.MaxKeys));

            const bool reverse = range.Flags & EScanFlags::REVERSE;
            TLoadRangeFromDB loader{this, range, progress};

            bool res = true;

            auto issue = [&](const TKey& key, const TValue& value) {
                Y_DEBUG_ABORT_UNLESS(range.Flags & EScanFlags::INCLUDE_BEGIN ? range.Begin <= key : range.Begin < key);
                Y_DEBUG_ABORT_UNLESS(range.Flags & EScanFlags::INCLUDE_END ? key <= range.End : key < range.End);
#ifndef NDEBUG
                Y_ABORT_UNLESS(range.KeysInRange.insert(key).second); // ensure that the generated key is unique
#endif
                if (!callback(key, value) || !--range.MaxKeys) {
                    return false; // scan aborted by user or finished scanning the required range
                } else {
                    // remove already scanned items from the range query
                    return true;
                }
            };

            const auto& from = reverse ? TKey::Min() : range.Begin;
            const auto& to = reverse ? range.End : TKey::Max();
            LoadedKeys.EnumInRange(from, to, reverse, [&](const TKey& left, const TKey& right, bool isRangeLoaded) {
                STLOG(PRI_TRACE, BLOB_DEPOT, BDT83, "ScanRange.Step", (Id, Self->GetLogId()), (Left, left), (Right, right),
                    (IsRangeLoaded, isRangeLoaded), (From, from), (To, to));
                if (!isRangeLoaded) {
                    // we have to load range (left, right), not including both ends
                    Y_ABORT_UNLESS(txc && progress);
                    if (!loader(*txc, left, right, issue)) {
                        res = !loader.Processing;
                        return false; // break the iteration
                    }
                } else if (reverse) {
                    for (auto it = Data.upper_bound(right); it != Data.begin(); ) {
                        const auto& [key, value] = *--it;
                        if (key < left) {
                            break;
                        } else if (range.Flags & EScanFlags::INCLUDE_BEGIN ? key < range.Begin : key <= range.Begin) {
                            return false; // just left the left side of the range
                        } else if ((key != range.End || range.Flags & EScanFlags::INCLUDE_END) && !issue(key, value)) {
                            return false; // enough keys processed
                        }
                    }
                } else {
                    // we have a range of loaded keys in the interval [left, right], including both ends -- load
                    // data from memory
                    for (auto it = Data.lower_bound(left); it != Data.end() && it->first <= right; ++it) {
                        const auto& [key, value] = *it;
                        if (range.Flags & EScanFlags::INCLUDE_END ? range.End < key : range.End <= key) {
                            return false; // just left the right side of the range
                        } else if ((key != range.Begin || range.Flags & EScanFlags::INCLUDE_BEGIN) && !issue(key, value)) {
                            return false; // enough keys processed
                        }
                    }
                }
                if (!loader.LastProcessedKey || (reverse & EScanFlags::REVERSE ? left < *loader.LastProcessedKey :
                        *loader.LastProcessedKey < right)) {
                    loader.LastProcessedKey.emplace(reverse ? left : right);
                }
                return true;
            });

            if (loader.LastProcessedKey) {
                if (reverse) {
                    LoadedKeys.AddRange(std::make_tuple(*loader.LastProcessedKey, range.End));
                } else {
                    LoadedKeys.AddRange(std::make_tuple(range.Begin, *loader.LastProcessedKey));
                }
                (reverse ? range.End : range.Begin) = std::move(*loader.LastProcessedKey);
                range.Flags.RemoveFlags(reverse ? EScanFlags::INCLUDE_END : EScanFlags::INCLUDE_BEGIN);
            }

            return res;
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

        void BindToBlob(const TKey& key, TBlobSeqId blobSeqId, bool keep, bool doNotKeep,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie);

        void MakeKeyCertain(const TKey& key);
        void HandleCommitCertainKeys();

        TRecordsPerChannelGroup& GetRecordsPerChannelGroup(TLogoBlobID id);
        TRecordsPerChannelGroup& GetRecordsPerChannelGroup(ui8 channel, ui32 groupId);

        TValue *AddDataOnLoad(TKey key, TString value, bool uncertainWrite);
        bool AddDataOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlob& blob,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void AddTrashOnLoad(TLogoBlobID id);
        void AddGenStepOnLoad(ui8 channel, ui32 groupId, TGenStep issuedGenStep, TGenStep confirmedGenStep);

        bool UpdateKeepState(TKey key, EKeepState keepState, NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void DeleteKey(const TKey& key, NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void CommitTrash(void *cookie);
        void HandleTrash(TRecordsPerChannelGroup& record);
        void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev);
        void OnPushNotifyResult(TEvBlobDepot::TEvPushNotifyResult::TPtr ev);
        void OnCommitConfirmedGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted);
        bool OnBarrierShift(ui64 tabletId, ui8 channel, bool hard, TGenStep previous, TGenStep current, ui32& maxItems,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void CollectTrashByHardBarrier(ui8 channel, ui32 groupId, TGenStep hardGenStep,
            const std::function<bool(TLogoBlobID)>& callback);
        void OnCommitHardGC(ui8 channel, ui32 groupId, TGenStep hardGenStep);
        void TrimChannelHistory(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted);

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
        bool LoadTrash(NTabletFlatExecutor::TTransactionContext& txc, TString& from, bool& progress);
        void OnLoadComplete();
        bool IsLoaded() const { return Loaded; }
        bool IsKeyLoaded(const TKey& key) const { return Loaded || LoadedKeys[key]; }

        bool EnsureKeyLoaded(const TKey& key, NTabletFlatExecutor::TTransactionContext& txc);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        class TResolveDecommitActor;
        IActor *CreateResolveDecommitActor(TEvBlobDepot::TEvResolve::TPtr ev);

        class TTxCommitAssimilatedBlob;
        void ExecuteTxCommitAssimilatedBlob(NKikimrProto::EReplyStatus status, TBlobSeqId blobSeqId, TData::TKey key,
            ui32 notifyEventType, TActorId parentId, ui64 cookie, bool keep = false, bool doNotKeep = false);

        class TTxResolve;
        void ExecuteTxResolve(TEvBlobDepot::TEvResolve::TPtr ev, THashSet<TLogoBlobID>&& resolutionErrors = {});

        void Handle(TEvBlobDepot::TEvResolve::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

        void ExecuteConfirmGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, size_t index,
            TGenStep confirmedGenStep);

        void ExecuteHardGC(ui8 channel, ui32 groupId, TGenStep hardGenStep);
    };

    Y_DECLARE_OPERATORS_FOR_FLAGS(TBlobDepot::TData::TScanFlags);

} // NKikimr::NBlobDepot

template<> struct THash<NKikimr::NBlobDepot::TBlobDepot::TData::TKey> : NKikimr::NBlobDepot::TBlobDepot::TData::TKey::THash {};
template<> struct std::hash<NKikimr::NBlobDepot::TBlobDepot::TData::TKey> : THash<NKikimr::NBlobDepot::TBlobDepot::TData::TKey> {};
