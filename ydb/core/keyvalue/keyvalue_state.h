#pragma once
#include "defs.h"
#include "keyvalue.h"
#include "keyvalue_collect_operation.h"
#include "keyvalue_const.h"
#include "keyvalue_data_header.h"
#include "keyvalue_events.h"
#include "keyvalue_helpers.h"
#include "keyvalue_index_record.h"
#include "keyvalue_intermediate.h"
#include "keyvalue_item_type.h"
#include "keyvalue_stored_state_data.h"
#include "keyvalue_simple_db.h"
#include "channel_balancer.h"
#include <util/generic/set.h>
#include <util/generic/hash_multi_map.h>
#include <ydb/core/base/appdata.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_metrics.h>
#include <ydb/core/keyvalue/protos/events.pb.h>
#include <library/cpp/time_provider/time_provider.h>
#include <bitset>

namespace NActors {
    struct TActorContext;
}

namespace NKikimr {
namespace NKeyValue {

class TKeyValueState {
public:
    using TIndex = TMap<TString, TIndexRecord>;
    using TCommand = NKikimrKeyValue::ExecuteTransactionRequest::Command;

    class TIncrementalKeySet {
        TMap<TString, TIndexRecord>& Index;
        TSet<TString>                AddedKeys;
        TSet<TString>                DeletedKeys;

        class TIterator {
            TIncrementalKeySet&                  KeySet;
            TMap<TString, TIndexRecord>::iterator IndexIterator;
            TSet<TString>::iterator               AddedKeysIterator;
            TSet<TString>::iterator               DeletedKeysIterator;

        public:
            TIterator(TIncrementalKeySet& keySet, TMap<TString, TIndexRecord>::iterator indexIterator,
                    TSet<TString>::iterator addedKeysIterator, TSet<TString>::iterator deletedKeysIterator)
                : KeySet(keySet)
                , IndexIterator(indexIterator)
                , AddedKeysIterator(addedKeysIterator)
                , DeletedKeysIterator(deletedKeysIterator)
            {
                AdvanceToNonDeleted();
            }

            TIterator(const TIterator&) = default;

            TIterator& operator=(const TIterator& other) {
                IndexIterator = other.IndexIterator;
                AddedKeysIterator = other.AddedKeysIterator;
                DeletedKeysIterator = other.DeletedKeysIterator;
                return *this;
            }

            const TString& operator*() const {
                if (IndexIterator != KeySet.Index.end() && AddedKeysIterator != KeySet.AddedKeys.end()) {
                    return std::min(IndexIterator->first, *AddedKeysIterator);
                } else if (IndexIterator != KeySet.Index.end()) {
                    return IndexIterator->first;
                } else if (AddedKeysIterator != KeySet.AddedKeys.end()) {
                    return *AddedKeysIterator;
                } else {
                    Y_ABORT("operator*() called on invalid iterator");
                }
            }

            const TString *operator->() const {
                return &**this;
            }

            TIterator& operator++() {
                MoveNext();
                AdvanceToNonDeleted();
                return *this;
            }

            void AdvanceToNonDeleted() {
                for (;;) {
                    if (IndexIterator == KeySet.Index.end() && AddedKeysIterator == KeySet.AddedKeys.end()) {
                        DeletedKeysIterator = KeySet.DeletedKeys.end();
                        break;
                    } else {
                        const TString& currentKey = **this;
                        while (DeletedKeysIterator != KeySet.DeletedKeys.end() && *DeletedKeysIterator < currentKey) {
                            ++DeletedKeysIterator; // FIXME: optimize
                        }
                        if (DeletedKeysIterator == KeySet.DeletedKeys.end() || *DeletedKeysIterator != currentKey) {
                            break;
                        }
                        MoveNext();
                        ++DeletedKeysIterator;
                    }
                }
            }

            void MoveNext() {
                if (IndexIterator != KeySet.Index.end() && AddedKeysIterator != KeySet.AddedKeys.end()) {
                    const TString& key = std::min(IndexIterator->first, *AddedKeysIterator);
                    if (IndexIterator->first == key) {
                        ++IndexIterator;
                    }
                    if (*AddedKeysIterator == key) {
                        ++AddedKeysIterator;
                    }
                } else if (IndexIterator != KeySet.Index.end()) {
                    ++IndexIterator;
                } else if (AddedKeysIterator != KeySet.AddedKeys.end()) {
                    ++AddedKeysIterator;
                } else {
                    Y_ABORT("operator++() called on invalid iterator");
                }
            }

            friend bool operator==(const TIterator& left, const TIterator& right) {
                return left.IndexIterator == right.IndexIterator
                    && left.AddedKeysIterator == right.AddedKeysIterator
                    && left.DeletedKeysIterator == right.DeletedKeysIterator;
            }

            friend bool operator!=(const TIterator& left, const TIterator& right) {
                return !(left == right);
            }
        };

    public:
        using iterator = TIterator;

    public:
        TIncrementalKeySet(TMap<TString, TIndexRecord>& index)
            : Index(index)
        {}

        template<typename Iterator>
        void insert(Iterator first, Iterator last) {
            AddedKeys.insert(first, last);
            if (last != first) {
                --last;
                TSet<TString>::iterator begin = DeletedKeys.lower_bound(*first);
                TSet<TString>::iterator end = DeletedKeys.upper_bound(*last);
                DeletedKeys.erase(begin, end);
            }
        }

        void insert(const TString& key) {
            AddedKeys.insert(key);
            DeletedKeys.erase(key);
        }

        void erase(const iterator& iter) {
            const TString& key = *iter;
            DeletedKeys.insert(key);
            AddedKeys.erase(key);
        }

        void erase(iterator first, const iterator& last) {
            // FIXME: optimize
            while (first != last) {
                iterator temp = first;
                ++first;
                erase(temp);
            }
        }

        iterator find(const TString& key) {
            TSet<TString>::iterator deleted = DeletedKeys.lower_bound(key);
            if (deleted != DeletedKeys.end() && *deleted == key) {
                return end();
            }

            TMap<TString, TIndexRecord>::iterator index = Index.lower_bound(key);
            TSet<TString>::iterator added = AddedKeys.lower_bound(key);

            if ((index != Index.end() && index->first == key) || (added != AddedKeys.end() && *added == key)) {
                return TIterator{*this, index, added, deleted};
            }

            return end();
        }

        iterator begin() {
            return TIterator{*this, Index.begin(), AddedKeys.begin(), DeletedKeys.begin()};
        }

        iterator end() {
            return TIterator{*this, Index.end(), AddedKeys.end(), DeletedKeys.end()};
        }

        iterator lower_bound(const TString& key) {
            return TIterator{*this, Index.lower_bound(key), AddedKeys.lower_bound(key), DeletedKeys.lower_bound(key)};
        }

        iterator upper_bound(const TString& key) {
            return TIterator{*this, Index.upper_bound(key), AddedKeys.upper_bound(key), DeletedKeys.upper_bound(key)};
        }
    };

    enum class ECollectCookie {
        Hard = 1,
        SoftInitial = 2,
        Soft = 3,
    };

    ui32 GetGeneration() const {
        return StoredState.UserGeneration;
    }

protected:
    TKeyValueStoredStateData StoredState;
    ui32 NextLogoBlobStep;
    ui32 NextLogoBlobCookie;

    using TKeySet = TIncrementalKeySet;

    TIndex Index;
    THashMap<TLogoBlobID, ui32> RefCounts;
    TSet<TLogoBlobID> Trash;
    TMap<ui64, ui64> InFlightForStep;
    TMap<std::tuple<ui64, ui32>, ui32> RequestUidStepToCount;
    THashSet<ui64> CmdTrimLeakedBlobsUids;
    std::vector<THolder<TIntermediate>> CmdTrimLeakedBlobsPostponed;
    THashMap<ui64, TInstant> RequestInputTime;
    ui64 NextRequestUid = 1;
    TIntrusivePtr<TCollectOperation> CollectOperation;
    bool IsCollectEventSent;
    bool InitWithoutCollect;
    std::array<ui64, 256> ChannelDataUsage;
    std::bitset<256> UsedChannels;
    THolder<TChannelBalancer::TWeightManager> WeightManager;

    ui64 TabletId;
    TActorId KeyValueActorId;
    ui32 ExecutorGeneration;
    bool IsStatePresent;
    bool IsEmptyDbStart;
    bool IsDamaged;
    bool IsTabletYellowMove;
    bool IsTabletYellowStop;
    TActorId ChannelBalancerActorId;
    ui64 InitialCollectsSent = 0;

    TDeque<TAutoPtr<TIntermediate>> Queue;
    ui64 IntermediatesInFlight;
    ui64 IntermediatesInFlightLimit;
    ui64 RoInlineIntermediatesInFlight;
    ui64 DeletesPerRequestLimit;

    TTabletCountersBase *TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;

    TInstant LastCollectStartedAt;

    ui32 PerGenerationCounter; // for garbage collection

    NMetrics::TResourceMetrics* ResourceMetrics = nullptr;

    TMaybe<NKeyValue::THelpers::TGenerationStep> PartialCollectedGenerationStep;
    TVector<TLogoBlobID> PartialCollectedDoNotKeep;
    bool RepeatGCTX = false;

    ui64 TotalTrashSize = 0;

public:
    TKeyValueState();
    void Clear();
    void SetupTabletCounters(TAutoPtr<TTabletCountersBase> counters);
    void ClearTabletCounters();
    TAutoPtr<TTabletCountersBase> TakeTabletCounters();
    TTabletCountersBase& GetTabletCounters();
    void SetupResourceMetrics(NMetrics::TResourceMetrics* metrics);
    void CountRequestComplete(NMsgBusProxy::EResponseStatus status, const TRequestStat &stat, const TActorContext &ctx);
    void CountRequestTakeOffOrEnqueue(TRequestType::EType requestType);
    void CountRequestOtherError(TRequestType::EType requestType);
    void CountRequestIncoming(TRequestType::EType requestType);
    void CountTrashRecord(const TLogoBlobID& id);
    void CountWriteRecord(const TLogoBlobID& id);
    void CountDeleteInline(ui32 sizeBytes);
    void CountInitialTrashRecord(const TLogoBlobID& id);
    void CountUncommittedTrashRecord(const TLogoBlobID& id);
    void CountTrashCollected(const TLogoBlobID& id);
    void CountTrashCommitted(const TLogoBlobID& id);
    void CountTrashDeleted(const TLogoBlobID& id);
    void CountOverrun();
    void CountLatencyBsOps(const TRequestStat &stat);
    void CountLatencyBsCollect();
    void CountLatencyQueue(const TRequestStat &stat);
    void CountLatencyLocalBase(const TIntermediate &intermediate);
    void CountStarting();
    void CountProcessingInitQueue();
    void CountOnline();

    void Terminate(const TActorContext& ctx);
    void Load(const TString &key, const TString& value);
    void InitExecute(ui64 tabletId, TActorId keyValueActorId, ui32 executorGeneration, ISimpleDb &db,
        const TActorContext &ctx, const TTabletStorageInfo *info);
    void InitComplete(const TActorContext& ctx, const TTabletStorageInfo *info);
    bool RegisterInitialCollectResult(const TActorContext &ctx, const TTabletStorageInfo *info);
    void RegisterInitialGCCompletionExecute(ISimpleDb &db, const TActorContext &ctx);
    void RegisterInitialGCCompletionComplete(const TActorContext &ctx, const TTabletStorageInfo *info);
    void SendCutHistory(const TActorContext &ctx, const TTabletStorageInfo *info);
    void OnInitQueueEmpty(const TActorContext &ctx);
    void OnStateWork(const TActorContext &ctx);
    void RequestExecute(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx,
        const TTabletStorageInfo *info);
    void RequestComplete(THolder<TIntermediate> &intermediate, const TActorContext &ctx, const TTabletStorageInfo *info);
    void DropRefCountsOnErrorInTx(std::deque<std::pair<TLogoBlobID, bool>>&& refCountsIncr, ISimpleDb& db, const TActorContext& ctx);
    void DropRefCountsOnError(std::deque<std::pair<TLogoBlobID, bool>>& refCountsIncr /*in-out*/, bool writesMade,
        const TActorContext& ctx);
    void ProcessPostponedTrims(const TActorContext& ctx, const TTabletStorageInfo *info);

    // garbage collection methods
    void PrepareCollectIfNeeded(const TActorContext &ctx);
    bool RemoveCollectedTrash(ISimpleDb &db, const TActorContext &ctx);
    void UpdateStoredState(ISimpleDb &db, const TActorContext &ctx, const NKeyValue::THelpers::TGenerationStep &genStep);
    void CompleteGCExecute(ISimpleDb &db, const TActorContext &ctx);
    void CompleteGCComplete(const TActorContext &ctx, const TTabletStorageInfo *info);
    void StartGC(const TActorContext &ctx, TVector<TLogoBlobID> &keep, TVector<TLogoBlobID> &doNotKeep,
        TVector<TLogoBlobID>& trashGoingToCollect);
    void StartCollectingIfPossible(const TActorContext &ctx);
    bool OnEvCollect(const TActorContext &ctx);
    void OnEvCollectDone(const TActorContext &ctx);
    void OnEvCompleteGC(bool repeat);


    void Reply(THolder<TIntermediate> &intermediate, const TActorContext &ctx, const TTabletStorageInfo *info);
    void ProcessCmd(TIntermediate::TRead &read,
        NKikimrClient::TKeyValueResponse::TReadResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(TIntermediate::TRangeRead &request,
        NKikimrClient::TKeyValueResponse::TReadRangeResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(TIntermediate::TWrite &request,
        NKikimrClient::TKeyValueResponse::TWriteResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(TIntermediate::TPatch &request,
        NKikimrClient::TKeyValueResponse::TPatchResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(const TIntermediate::TDelete &request,
        NKikimrClient::TKeyValueResponse::TDeleteRangeResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(const TIntermediate::TRename &request,
        NKikimrClient::TKeyValueResponse::TRenameResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(const TIntermediate::TCopyRange &request,
        NKikimrClient::TKeyValueResponse::TCopyRangeResult *legacyResponse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void ProcessCmd(const TIntermediate::TConcat &request,
        NKikimrClient::TKeyValueResponse::TConcatResult *resplegacyResponseonse,
        NKikimrKeyValue::StorageChannel *response,
        ISimpleDb &db, const TActorContext &ctx, TRequestStat &stat, ui64 unixTime, TIntermediate *intermediate);
    void CmdRead(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdReadRange(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdRename(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdDelete(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdWrite(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdGetStatus(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdCopyRange(THolder<TIntermediate>& intermediate, ISimpleDb& db, const TActorContext& ctx);
    void CmdConcat(THolder<TIntermediate>& intermediate, ISimpleDb& db, const TActorContext& ctx);
    void CmdTrimLeakedBlobs(THolder<TIntermediate>& intermediate, ISimpleDb& db, const TActorContext& ctx);
    void CmdSetExecutorFastLogPolicy(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void CmdCmds(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);
    void ProcessCmds(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx,
        const TTabletStorageInfo *info);
    bool IncrementGeneration(THolder<TIntermediate> &intermediate, ISimpleDb &db, const TActorContext &ctx);

    struct TCheckResult {
        NKikimrKeyValue::Statuses::ReplyStatus Status = NKikimrKeyValue::Statuses::RSTATUS_OK;
        TString ErrorMsg;
    };

    TCheckResult CheckCmd(const TIntermediate::TRename &cmd, TKeySet& keys, ui32 index) const;
    TCheckResult CheckCmd(const TIntermediate::TDelete &cmd, TKeySet& keys, ui32 index) const;
    TCheckResult CheckCmd(const TIntermediate::TWrite &cmd, TKeySet& keys, ui32 index) const;
    TCheckResult CheckCmd(const TIntermediate::TPatch &cmd, TKeySet& keys, ui32 index) const;
    TCheckResult CheckCmd(const TIntermediate::TCopyRange &cmd, TKeySet& keys, ui32 index) const;
    TCheckResult CheckCmd(const TIntermediate::TConcat &cmd, TKeySet& keys, ui32 index) const;
    TCheckResult CheckCmd(const TIntermediate::TGetStatus &cmd, TKeySet& keys, ui32 index) const;

    template<class Cmd>
    bool CheckCmds(THolder<TIntermediate>& intermediate, const TDeque<Cmd>& cmds, const TActorContext& ctx,
        TKeySet& keys, const TTabletStorageInfo* info);
    bool CheckCmds(THolder<TIntermediate>& intermediate, const TActorContext& /*ctx*/, TKeySet& keys,
            const TTabletStorageInfo* /*info*/);

    void Step();
    TLogoBlobID AllocateLogoBlobId(ui32 size, ui32 storageChannelIdx, ui64 requestUid);
    TLogoBlobID AllocatePatchedLogoBlobId(ui32 size, ui32 storageChannelIdx, TLogoBlobID originalBlobId, ui64 requestUid);
    TIntrusivePtr<TCollectOperation>& GetCollectOperation() {
        return CollectOperation;
    }

    void Dereference(const TIndexRecord& record, ISimpleDb& db, const TActorContext& ctx);
    void UpdateKeyValue(const TString& key, const TIndexRecord& record, ISimpleDb& db, const TActorContext& ctx);
    void Dereference(const TLogoBlobID& id, ISimpleDb& db, const TActorContext& ctx, bool initial);

    ui32 GetPerGenerationCounter() {
        return PerGenerationCounter;
    }

    void PushTrashBeingCommitted(TVector<TLogoBlobID>& trashBeingCommitted, const TActorContext& ctx);

    void OnEvReadRequest(TEvKeyValue::TEvRead::TPtr &ev, const TActorContext &ctx,
            const TTabletStorageInfo *info);
    void OnEvReadRangeRequest(TEvKeyValue::TEvReadRange::TPtr &ev, const TActorContext &ctx,
            const TTabletStorageInfo *info);
    void OnEvExecuteTransaction(TEvKeyValue::TEvExecuteTransaction::TPtr &ev, const TActorContext &ctx,
            const TTabletStorageInfo *info);
    void OnEvGetStorageChannelStatus(TEvKeyValue::TEvGetStorageChannelStatus::TPtr &ev, const TActorContext &ctx,
            const TTabletStorageInfo *info);
    void OnEvAcquireLock(TEvKeyValue::TEvAcquireLock::TPtr &ev, const TActorContext &ctx,
            const TTabletStorageInfo *info);

    void OnPeriodicRefresh(const TActorContext &ctx);
    void OnUpdateWeights(TChannelBalancer::TEvUpdateWeights::TPtr ev);

    void OnRequestComplete(ui64 requestUid, ui64 generation, ui64 step, const TActorContext &ctx,
        const TTabletStorageInfo *info, NMsgBusProxy::EResponseStatus status, const TRequestStat &stat);
    void CancelInFlight(ui64 requestUid);

    void OnEvIntermediate(TIntermediate &intermediate, const TActorContext &ctx);
    void OnEvRequest(TEvKeyValue::TEvRequest::TPtr &ev, const TActorContext &ctx, const TTabletStorageInfo *info);
    bool PrepareIntermediate(TEvKeyValue::TEvRequest::TPtr &ev, THolder<TIntermediate> &intermediate,
        TRequestType::EType &inOutRequestType, const TActorContext &ctx, const TTabletStorageInfo *info);
    void RenderHTMLPage(IOutputStream &out) const;
    void MonChannelStat(NJson::TJsonValue& out) const;

    bool CheckDeadline(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate);

    template <typename TRequest>
    bool CheckDeadline(const TActorContext &ctx, TRequest *request,
            THolder<TIntermediate> &intermediate)
    {
        ui64 deadlineInstantMs = request->Record.deadline_instant_ms();
        if (!deadlineInstantMs) {
            return false;
        }
        intermediate->Deadline = TInstant::MicroSeconds(deadlineInstantMs * 1000ull);

        TInstant now = TAppData::TimeProvider->Now();
        if (intermediate->Deadline <= now) {
            TStringStream str;
            str << "KeyValue# " << TabletId;
            str << " Deadline reached before processing the request!";
            str << " DeadlineInstantMs# " << deadlineInstantMs;
            str << " < Now# " << (ui64)now.MilliSeconds();
            ReplyError<typename TRequest::TResponse>(ctx, str.Str(),
                    NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT, intermediate);
            return true;
        }

        return false;
    }

    bool CheckGeneration(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate);

    template <typename TGrpcRequestWithLockGeneration>
    bool CheckGeneration(const TActorContext &ctx, TGrpcRequestWithLockGeneration *kvRequest,
            THolder<TIntermediate> &intermediate)
    {
        auto &record = kvRequest->Record;
        if (record.has_lock_generation()) {
            intermediate->HasGeneration = true;
            intermediate->Generation = record.lock_generation();
            if (record.lock_generation() != StoredState.GetUserGeneration()) {
                TStringStream str;
                str << "KeyValue# " << TabletId;
                str << " Generation mismatch! Requested# " << record.lock_generation();
                str << " Actual# " << StoredState.GetUserGeneration();
                str << " Marker# KV05";
                ReplyError<typename TGrpcRequestWithLockGeneration::TResponse>(ctx, str.Str(),
                        NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION, intermediate);
                return true;
            }
        } else {
            intermediate->HasGeneration = false;
        }
        return false;
    }

    void RegisterRequestActor(const TActorContext &ctx, THolder<TIntermediate> &&intermediate,
        const TTabletStorageInfo *info, ui32 tabletGeneration);

    void SplitIntoBlobs(TIntermediate::TWrite &cmd, bool isInline, ui32 storageChannelIdx);

    bool PrepareCmdRead(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, bool &outIsInlineOnly);
    bool PrepareCmdReadRange(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, bool &inOutIsInlineOnly);
    bool PrepareCmdRename(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate);
    bool PrepareCmdDelete(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate);
    bool PrepareCmdWrite(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest, TEvKeyValue::TEvRequest& ev,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info);
    bool PrepareCmdPatch(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest, TEvKeyValue::TEvRequest& ev,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info);
    bool PrepareCmdGetStatus(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info);
    bool PrepareCmdCopyRange(const TActorContext& ctx, NKikimrClient::TKeyValueRequest& kvRequest,
        THolder<TIntermediate>& intermediate);
    bool PrepareCmdConcat(const TActorContext& ctx, NKikimrClient::TKeyValueRequest& kvRequest,
        THolder<TIntermediate>& intermediate);
    bool PrepareCmdTrimLeakedBlobs(const TActorContext& ctx, NKikimrClient::TKeyValueRequest& kvRequest,
        THolder<TIntermediate>& intermediate, const TTabletStorageInfo *info);
    bool PrepareCmdSetExecutorFastLogPolicy(const TActorContext &ctx, NKikimrClient::TKeyValueRequest &kvRequest,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info);


    struct TPrepareResult {
        bool WithError = false;
        TString ErrorMsg;
    };
    TPrepareResult PrepareOneCmd(const TCommand::Rename &request, THolder<TIntermediate> &intermediate);
    TPrepareResult PrepareOneCmd(const TCommand::Concat &request, THolder<TIntermediate> &intermediate);
    TPrepareResult PrepareOneCmd(const TCommand::CopyRange &request, THolder<TIntermediate> &intermediate);
    TPrepareResult PrepareOneCmd(const TCommand::Write &request, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo *info);
    TPrepareResult PrepareOneCmd(const TCommand::DeleteRange &request, THolder<TIntermediate> &intermediate,
        const TActorContext &ctx);
    TPrepareResult PrepareOneCmd(const TCommand &request, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo *info, const TActorContext &ctx);
    TPrepareResult PrepareCommands(NKikimrKeyValue::ExecuteTransactionRequest &kvRequest,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info, const TActorContext &ctx);
    TPrepareResult InitGetStatusCommand(TIntermediate::TGetStatus &cmd,
        NKikimrClient::TKeyValueRequest::EStorageChannel storageChannel, const TTabletStorageInfo *info);
    void ReplyError(const TActorContext &ctx, TString errorDescription,
        NMsgBusProxy::EResponseStatus oldStatus, NKikimrKeyValue::Statuses::ReplyStatus newStatus,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info = nullptr);

    template <typename TResponse>
    void ReplyError(const TActorContext &ctx, TString errorDescription,
        NKikimrKeyValue::Statuses::ReplyStatus status, THolder<TIntermediate> &intermediate,
        const TTabletStorageInfo *info = nullptr)
    {
        LOG_INFO_S(ctx, NKikimrServices::KEYVALUE, errorDescription);
        Y_ABORT_UNLESS(!intermediate->IsReplied);
        std::unique_ptr<TResponse> response = std::make_unique<TResponse>();
        response->Record.set_status(status);

        if constexpr (std::is_same_v<TResponse, TEvKeyValue::TEvReadResponse>) {
            auto &cmd = *intermediate->ReadCommand;
            Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TRead>(cmd));
            TIntermediate::TRead &interRead = std::get<TIntermediate::TRead>(cmd);
            response->Record.set_requested_offset(interRead.Offset);
            response->Record.set_requested_size(interRead.ValueSize);
            response->Record.set_requested_key(interRead.Key);
        }
        if constexpr (!std::is_same_v<TResponse, TEvKeyValue::TEvGetStorageChannelStatusResponse>) {
            if (intermediate->HasCookie) {
                response->Record.set_cookie(intermediate->Cookie);
            }
        }

        if (errorDescription) {
            response->Record.set_msg(errorDescription);
        }

        ResourceMetrics->Network.Increment(response->Record.ByteSize());

        intermediate->IsReplied = true;
        ctx.Send(intermediate->RespondTo, response.release());
        if (info) {
            intermediate->UpdateStat();
            OnRequestComplete(intermediate->RequestUid, intermediate->CreatedAtGeneration, intermediate->CreatedAtStep,
                    ctx, info, TEvKeyValue::TEvNotify::ConvertStatus(status), intermediate->Stat);
        } else { //metrics change report in OnRequestComplete is not done
            ResourceMetrics->TryUpdate(ctx);
            RequestInputTime.erase(intermediate->RequestUid);
        }
    }

    bool PrepareReadRequest(const TActorContext &ctx, TEvKeyValue::TEvRead::TPtr &ev,
        THolder<TIntermediate> &intermediate, TRequestType::EType *outRequestTyp);
    bool PrepareReadRangeRequest(const TActorContext &ctx, TEvKeyValue::TEvReadRange::TPtr &ev,
        THolder<TIntermediate> &intermediate, TRequestType::EType *outRequestType);
    bool PrepareExecuteTransactionRequest(const TActorContext &ctx, TEvKeyValue::TEvExecuteTransaction::TPtr &ev,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info);
    TPrepareResult PrepareOneGetStatus(TIntermediate::TGetStatus &cmd, ui64 publicStorageChannel,
        const TTabletStorageInfo *info);
    bool PrepareGetStorageChannelStatusRequest(const TActorContext &ctx, TEvKeyValue::TEvGetStorageChannelStatus::TPtr &ev,
        THolder<TIntermediate> &intermediate, const TTabletStorageInfo *info);
    bool PrepareAcquireLockRequest(const TActorContext &ctx, TEvKeyValue::TEvAcquireLock::TPtr &ev,
        THolder<TIntermediate> &intermediate);

    template <typename TRequestType>
    void PostponeIntermediate(THolder<TIntermediate> &&intermediate) {
        intermediate->Stat.EnqueuedAs = Queue.size() + 1;
        Queue.push_back(std::move(intermediate));
    }
    void ProcessPostponedIntermediate(const TActorContext& ctx, THolder<TIntermediate> &&intermediate,
             const TTabletStorageInfo *info);

    bool ConvertRange(const NKikimrClient::TKeyValueRequest::TKeyRange& from, TKeyRange *to,
                      const TActorContext& ctx, THolder<TIntermediate>& intermediate, const char *cmd, ui32 index);

    struct TConvertRangeResult {
        TString ErrorMsg;
        bool WithError = false;
    };

    TConvertRangeResult ConvertRange(const NKikimrKeyValue::KVRange& range, TKeyRange *to, const char *cmd)
    {
        if (range.has_from_key_inclusive()) {
            to->HasFrom = true;
            to->KeyFrom = range.from_key_inclusive();
            to->IncludeFrom = true;
        } else if (range.has_from_key_exclusive()) {
            to->HasFrom = true;
            to->KeyFrom = range.from_key_exclusive();
            to->IncludeFrom = false;
        } else {
            to->HasFrom = false;
        }

        if (range.has_to_key_inclusive()) {
            to->HasTo = true;
            to->KeyTo = range.to_key_inclusive();
            to->IncludeTo = true;
        } else if (range.has_to_key_exclusive()) {
            to->HasTo = true;
            to->KeyTo = range.to_key_exclusive();
            to->IncludeTo = false;
        } else {
            to->HasTo = false;
        }

        if (to->HasFrom && to->HasTo) {
            if (!to->IncludeFrom && !to->IncludeTo && to->KeyFrom >= to->KeyTo) {
                TString msg = TStringBuilder() << "KeyValue# " << TabletId
                        << " Range.KeyFrom >= Range.KeyTo and both exclusive in " << cmd
                        << " Marker# KV31";
                return TConvertRangeResult{msg, true};
            }
            if (to->KeyFrom > to->KeyTo) {
                TString msg = TStringBuilder() << "KeyValue# " << TabletId
                        << " Range.KeyFrom > Range.KeyTo and both exclusive in " << cmd
                        << " Marker# KV36";
                return TConvertRangeResult{msg, true};
            }
        }

        return {};
    }

    template<typename Container, typename Iterator = typename Container::iterator>
    static std::pair<Iterator, Iterator> GetRange(const TKeyRange& range, Container& container) {
        auto first = !range.HasFrom ? container.begin()
                                    : range.IncludeFrom ? container.lower_bound(range.KeyFrom)
                                                        : container.upper_bound(range.KeyFrom);

        auto last = !range.HasTo ? container.end()
                                 : range.IncludeTo ? container.upper_bound(range.KeyTo)
                                                   : container.lower_bound(range.KeyTo);

        return {first, last};
    }

    template<typename Func>
    void TraverseRange(const TKeyRange& range, Func&& callback) {
        std::pair<TIndex::iterator, TIndex::iterator> r = GetRange(range, Index);
        TIndex::iterator nextIt;
        for (TIndex::iterator it = r.first; it != r.second; it = nextIt) {
            nextIt = std::next(it);
            callback(it);
        }
    }
    void UpdateResourceMetrics(const TActorContext& ctx);

    bool GetIsDamaged() const {
        return IsDamaged;
    }

    bool GetIsTabletYellowMove() const {
        return IsTabletYellowMove;
    }

    bool GetIsTabletYellowStop() const {
        return IsTabletYellowStop;
    }

    void SetTabletYellowStop(bool isTabletYellow) {
        IsTabletYellowStop = isTabletYellow;
    }

    void SetTabletYellowMove(bool isTabletYellow) {
        IsTabletYellowMove = isTabletYellow;
    }

    ui64 GetTrashTotalBytes() const {
        return TotalTrashSize;
    }

    ui32 GetTrashCount() const {
        return Trash.size();
    }

public: // For testing
    TString Dump() const;
    void VerifyEqualIndex(const TKeyValueState& state) const;
};

} // NKeyValue
} // NKikimr
