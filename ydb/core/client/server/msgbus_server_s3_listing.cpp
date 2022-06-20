#include "msgbus_server_request.h"
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/path.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NMsgBusProxy {


template <NKikimrServices::TActivity::EType acitvityType>
class TS3ListingRequestBase : public TActorBootstrapped<TS3ListingRequestBase<acitvityType>> {
private:
    typedef TS3ListingRequestBase<acitvityType> TSelf;
    typedef TActorBootstrapped<TSelf> TBase;

    static constexpr ui32 DEFAULT_MAX_KEYS = 1001;
    static constexpr ui32 DEFAULT_TIMEOUT_SEC = 5*60;

    const NKikimrClient::TS3ListingRequest* Request;
    THolder<const NACLib::TUserToken> UserToken;
    ui32 MaxKeys;
    TActorId SchemeCache;
    TActorId LeaderPipeCache;
    TDuration Timeout;
    TActorId TimeoutTimerActorId;
    TAutoPtr<TKeyDesc> KeyRange;
    bool WaitingResolveReply;
    bool Finished;
    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> ResolveNamesResult;
    TVector<NScheme::TTypeId> KeyColumnTypes;
    TSysTables::TTableColumnInfo PathColumnInfo;
    TVector<TSysTables::TTableColumnInfo> CommonPrefixesColumns;
    TVector<TSysTables::TTableColumnInfo> ContentsColumns;
    TSerializedCellVec PrefixColumns;
    TSerializedCellVec StartAfterSuffixColumns;
    TSerializedCellVec KeyRangeFrom;
    TSerializedCellVec KeyRangeTo;
    ui32 CurrentShardIdx;
    TVector<TSerializedCellVec> CommonPrefixesRows;
    TVector<TSerializedCellVec> ContentsRows;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return acitvityType;
    }

    TS3ListingRequestBase(TActorId schemeCache, THolder<const NACLib::TUserToken>&& userToken)
        : Request(nullptr)
        , UserToken(std::move(userToken))
        , MaxKeys(DEFAULT_MAX_KEYS)
        , SchemeCache(schemeCache)
        , LeaderPipeCache(MakePipePeNodeCacheID(false))
        , Timeout(TDuration::Seconds(DEFAULT_TIMEOUT_SEC))
        , WaitingResolveReply(false)
        , Finished(false)
        , CurrentShardIdx(0)
    {
    }

    virtual const NKikimrClient::TS3ListingRequest* ExtractRequest(TString& errDescr) = 0;

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString errDescr;
        Request = ExtractRequest(errDescr);
        if (!Request) {
            return ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::WrongRequest, errDescr, ctx);
        }

        if (Request->GetMaxKeys() > 0 && Request->GetMaxKeys() <= DEFAULT_MAX_KEYS) {
            MaxKeys = Request->GetMaxKeys();
        }

        ui32 userTimeoutMillisec = Request->GetTimeout();
        if (userTimeoutMillisec > 0 && TDuration::MilliSeconds(userTimeoutMillisec) < Timeout) {
            Timeout = TDuration::MilliSeconds(userTimeoutMillisec);
        }

        ResolveTable(Request->GetTableName(), ctx);
    }

    void Die(const NActors::TActorContext& ctx) override {
        Y_VERIFY(Finished);
        Y_VERIFY(!WaitingResolveReply);
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        if (TimeoutTimerActorId) {
            ctx.Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }
        TBase::Die(ctx);
    }

protected:
    virtual void SendReplyMessage(NKikimrClient::TS3ListingResponse&& response) = 0;

private:
    STFUNC(StateWaitResolveTable) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void ResolveTable(const TString& table, const NActors::TActorContext& ctx) {
        // TODO: check all params;

        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(table);
        if (entry.Path.empty()) {
            return ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::ResolveError, "Invalid table path specified", ctx);
        }
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(entry);
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

        TimeoutTimerActorId = CreateLongTimer(ctx, Timeout,
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));

        TBase::Become(&TSelf::StateWaitResolveTable);
        WaitingResolveReply = true;
    }

    void ReplyWithError(EResponseStatus status, NTxProxy::TResultStatus::EStatus errorCode, const TString& message, const TActorContext& ctx) {
        NKikimrClient::TS3ListingResponse response;
        response.SetStatus(status);
        response.SetDescription(message);
        response.SetErrorCode(errorCode);
        SendReplyMessage(std::move(response));
        Finished = true;

        // We cannot Die() while scheme cache request is in flight because that request has pointer to
        // KeyRange member so we must not destroy it before we get the response
        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(MSTATUS_TIMEOUT, NTxProxy::TResultStatus::EStatus::ExecTimeout, "Request timed out", ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_VERIFY(request.ResultSet.size() == 1);
        if (request.ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyWithError(MSTATUS_ERROR,  NTxProxy::TResultStatus::EStatus::ResolveError,
                                  ToString(request.ResultSet.front().Status), ctx);
        }
        ResolveNamesResult = ev->Get()->Request;

        if (!BuildSchema(ctx)) {
            return;
        }

        if (!BuildKeyRange(ctx)) {
            return;
        }

        ResolveShards(ctx);
    }

    bool BuildSchema(const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto& entry = ResolveNamesResult->ResultSet.front();

        TVector<ui32> keyColumnIds;
        THashMap<TString, ui32> columnByName;
        for (const auto& ci : entry.Columns) {
            columnByName[ci.second.Name] = ci.second.Id;
            i32 keyOrder = ci.second.KeyOrder;
            if (keyOrder != -1) {
                Y_VERIFY(keyOrder >= 0);
                KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), keyOrder + 1));
                KeyColumnTypes[keyOrder] = ci.second.PType;
                keyColumnIds.resize(Max<size_t>(keyColumnIds.size(), keyOrder + 1));
                keyColumnIds[keyOrder] = ci.second.Id;
            }
        }

        TString errStr;
        TVector<TCell> prefixCells;
        TConstArrayRef<NScheme::TTypeId> prefixTypes(KeyColumnTypes.data(), KeyColumnTypes.size() - 1); // -1 for path column
        NMiniKQL::CellsFromTuple(&Request->GetKeyPrefix().GetType(), Request->GetKeyPrefix().GetValue(),
                                 prefixTypes, true, prefixCells, errStr);
        if (!errStr.empty()) {
            ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::WrongRequest, "Invalid KeyPrefix: " + errStr, ctx);
            return false;
        }

        PrefixColumns.Parse(TSerializedCellVec::Serialize(prefixCells));

        // Check path column
        ui32 pathColPos = prefixCells.size();
        Y_VERIFY(pathColPos < KeyColumnTypes.size());
        PathColumnInfo = entry.Columns[keyColumnIds[pathColPos]];
        if (PathColumnInfo.PType != NScheme::NTypeIds::Utf8) {
            ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::WrongRequest,
                           Sprintf("Value for path column '%s' has type %s, expected Utf8",
                                   PathColumnInfo.Name.data(), NScheme::TypeName(PathColumnInfo.PType)), ctx);
            return false;
        }

        CommonPrefixesColumns.push_back(PathColumnInfo);

        TVector<TCell> suffixCells;
        TConstArrayRef<NScheme::TTypeId> suffixTypes(KeyColumnTypes.data() + pathColPos, KeyColumnTypes.size() - pathColPos); // starts at path column
        NMiniKQL::CellsFromTuple(&Request->GetStartAfterKeySuffix().GetType(), Request->GetStartAfterKeySuffix().GetValue(),
                                 suffixTypes, true, suffixCells, errStr);
        if (!errStr.empty()) {
            ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::WrongRequest,
                           "Invalid StartAfterKeySuffix: " + errStr, ctx);
            return false;
        }

        StartAfterSuffixColumns.Parse(TSerializedCellVec::Serialize(suffixCells));

        if (!StartAfterSuffixColumns.GetCells().empty()) {
            TString startAfterPath = TString(StartAfterSuffixColumns.GetCells()[0].Data(), StartAfterSuffixColumns.GetCells()[0].Size());
            if (!startAfterPath.StartsWith(Request->GetPathColumnPrefix())) {
                ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::WrongRequest,
                               "Invalid StartAfterKeySuffix: StartAfter parameter doesn't match PathPrefix", ctx);
                return false;
            }
        }

        // Check ColumsToReturn
        TSet<TString> requestedColumns(Request->GetColumnsToReturn().begin(), Request->GetColumnsToReturn().end());

        // Always request all suffix columns starting from path column
        for (size_t i = pathColPos; i < keyColumnIds.size(); ++i) {
            ui32 colId = keyColumnIds[i];
            requestedColumns.erase(entry.Columns[colId].Name);
            ContentsColumns.push_back(entry.Columns[colId]);
        }

        for (const auto& name : requestedColumns) {
            if (!columnByName.contains(name)) {
                ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::WrongRequest,
                               Sprintf("Unknown column '%s'", name.data()), ctx);
                return false;
            }
            ContentsColumns.push_back(entry.Columns[columnByName[name]]);
        }

        return true;
    }

    bool BuildKeyRange(const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);

        TVector<TCell> fromValues(PrefixColumns.GetCells().begin(), PrefixColumns.GetCells().end());
        TVector<TCell> toValues(PrefixColumns.GetCells().begin(), PrefixColumns.GetCells().end());

        TString pathPrefix = Request->GetPathColumnPrefix();
        TString endPathPrefix;

        if (pathPrefix.empty()) {
            fromValues.resize(KeyColumnTypes.size());
        } else {
            // TODO: check for valid UTF-8

            fromValues.push_back(TCell(pathPrefix.data(), pathPrefix.size()));
            fromValues.resize(KeyColumnTypes.size());

            endPathPrefix = pathPrefix;
            // pathPrefix must be a valid Utf8 string, so it cannot contain 0xff byte and its safe to add 1
            // to make end of range key
            endPathPrefix.back() = endPathPrefix.back() + 1;
            toValues.push_back(TCell(endPathPrefix.data(), endPathPrefix.size()));
            toValues.resize(KeyColumnTypes.size());
        }

        if (!StartAfterSuffixColumns.GetCells().empty()) {
            // TODO: check for valid UTF-8
            for (size_t i = 0; i < StartAfterSuffixColumns.GetCells().size(); ++i) {
                fromValues[PathColumnInfo.KeyOrder + i] = StartAfterSuffixColumns.GetCells()[i];
            }
        }

        KeyRangeFrom.Parse(TSerializedCellVec::Serialize(fromValues));
        KeyRangeTo.Parse(TSerializedCellVec::Serialize(toValues));

        TTableRange range(KeyRangeFrom.GetCells(), true,
                          KeyRangeTo.GetCells(), false,
                          false);

        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& ci : ContentsColumns) {
            TKeyDesc::TColumnOp op = { ci.Id, TKeyDesc::EColumnOperation::Read, ci.PType, 0, 0 };
            columns.push_back(op);
        }

        auto& entry = ResolveNamesResult->ResultSet.front();

        KeyRange.Reset(new TKeyDesc(entry.TableId, range, TKeyDesc::ERowOperation::Read, KeyColumnTypes, columns));
        return true;
    }

    void ResolveShards(const NActors::TActorContext& ctx) {
        TAutoPtr<NSchemeCache::TSchemeCacheRequest> request(new NSchemeCache::TSchemeCacheRequest());

        request->ResultSet.emplace_back(std::move(KeyRange));

        TAutoPtr<TEvTxProxySchemeCache::TEvResolveKeySet> resolveReq(new TEvTxProxySchemeCache::TEvResolveKeySet(request));
        ctx.Send(SchemeCache, resolveReq.Release());

        TBase::Become(&TSelf::StateWaitResolveShards);
        WaitingResolveReply = true;
    }

    STFUNC(StateWaitResolveShards) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    bool CheckAccess(TString& errorMessage) {
        const ui32 access = NACLib::EAccessRights::SelectRow;
        if (access != 0
                && UserToken != nullptr
                && KeyRange->Status == TKeyDesc::EStatus::Ok
                && KeyRange->SecurityObject != nullptr
                && !KeyRange->SecurityObject->CheckAccess(access, *UserToken))
        {
            TStringStream explanation;
            explanation << "Access denied for " << UserToken->GetUserSID()
                        << " with access " << NACLib::AccessRightsToString(access)
                        << " to table [" << Request->GetTableName() << "]";

            errorMessage = explanation.Str();
            return false;
        }
        return true;
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev, const TActorContext &ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        TEvTxProxySchemeCache::TEvResolveKeySetResult *msg = ev->Get();
        Y_VERIFY(msg->Request->ResultSet.size() == 1);
        KeyRange = std::move(msg->Request->ResultSet[0].KeyDescription);

        if (msg->Request->ErrorCount > 0) {
            return ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::ResolveError,
                                  Sprintf("Unknown table '%s'", Request->GetTableName().data()), ctx);
        }

        TString accessCheckError;
        if (!CheckAccess(accessCheckError)) {
            return ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::AccessDenied, accessCheckError, ctx);
        }

        auto getShardsString = [] (const TVector<TKeyDesc::TPartitionInfo>& partitions) {
            TVector<ui64> shards;
            shards.reserve(partitions.size());
            for (auto& partition : partitions) {
                shards.push_back(partition.ShardId);
            }

            return JoinVectorIntoString(shards, ", ");
        };

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Range shards: "
            << getShardsString(KeyRange->GetPartitions()));

        if (KeyRange->GetPartitions().size() > 0) {
            CurrentShardIdx = 0;
            MakeShardRequest(CurrentShardIdx, ctx);
        } else {
            ReplySuccess(ctx);
        }
    }

    void MakeShardRequest(ui32 idx, const NActors::TActorContext& ctx) {
        ui64 shardId = KeyRange->GetPartitions()[idx].ShardId;

        THolder<TEvDataShard::TEvS3ListingRequest> ev(new TEvDataShard::TEvS3ListingRequest());
        ev->Record.SetTableId(KeyRange->TableId.PathId.LocalPathId);
        ev->Record.SetSerializedKeyPrefix(PrefixColumns.GetBuffer());
        ev->Record.SetPathColumnPrefix(Request->GetPathColumnPrefix());
        ev->Record.SetPathColumnDelimiter(Request->GetPathColumnDelimiter());
        ev->Record.SetSerializedStartAfterKeySuffix(StartAfterSuffixColumns.GetBuffer());
        ev->Record.SetMaxKeys(MaxKeys - ContentsRows.size() - CommonPrefixesRows.size());
        if (!CommonPrefixesRows.empty()) {
            // Next shard might have the same common prefix, need to skip it
            ev->Record.SetLastCommonPrefix(CommonPrefixesRows.back().GetBuffer());
        }

        for (const auto& ci : ContentsColumns) {
            ev->Record.AddColumnsToReturn(ci.Id);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_REQUEST, "Sending request to shards " << shardId);

        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.Release(), shardId, true), IEventHandle::FlagTrackDelivery);

        TBase::Become(&TSelf::StateWaitResults);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        ReplyWithError(MSTATUS_INTERNALERROR, NTxProxy::TResultStatus::EStatus::Unknown,
                       "Internal error: pipe cache is not available, the cluster might not be configured properly", ctx);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        // Invalidate scheme cache in case of partitioning change
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(KeyRange->TableId, TActorId()));
        ReplyWithError(MSTATUS_NOTREADY, NTxProxy::TResultStatus::EStatus::ProxyShardNotAvailable, "Failed to connect to shard", ctx);
    }

    STFUNC(StateWaitResults) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDataShard::TEvS3ListingResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            HFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void Handle(TEvDataShard::TEvS3ListingResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        const auto& shardResponse = ev->Get()->Record;

        // Notify the cache that we are done with the pipe
        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(shardResponse.GetTabletID()));

        if (shardResponse.GetStatus() == NKikimrTxDataShard::TError::WRONG_SHARD_STATE) {
            // Invalidate scheme cache in case of partitioning change
            ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(KeyRange->TableId, TActorId()));
            ReplyWithError(MSTATUS_NOTREADY, NTxProxy::TResultStatus::EStatus::ProxyShardNotAvailable, shardResponse.GetErrorDescription(), ctx);
            return;
        }

        if (shardResponse.GetStatus() != NKikimrTxDataShard::TError::OK) {
            ReplyWithError(MSTATUS_ERROR, NTxProxy::TResultStatus::EStatus::ExecError, shardResponse.GetErrorDescription(), ctx);
            return;
        }

        for (size_t i = 0; i < shardResponse.CommonPrefixesRowsSize(); ++i) {
            if (!CommonPrefixesRows.empty() && CommonPrefixesRows.back().GetBuffer() == shardResponse.GetCommonPrefixesRows(i)) {
                LOG_ERROR_S(ctx, NKikimrServices::MSGBUS_REQUEST, "S3 listing got duplicate common prefix from shard " << shardResponse.GetTabletID());
            }
            CommonPrefixesRows.emplace_back(shardResponse.GetCommonPrefixesRows(i));
        }

        for (size_t i = 0; i < shardResponse.ContentsRowsSize(); ++i) {
            ContentsRows.emplace_back(shardResponse.GetContentsRows(i));
        }

        if (CurrentShardIdx+1 < KeyRange->GetPartitions().size() &&
            MaxKeys > ContentsRows.size() + CommonPrefixesRows.size() &&
            shardResponse.GetMoreRows())
        {
            ++CurrentShardIdx;
            MakeShardRequest(CurrentShardIdx, ctx);
        } else {
            ReplySuccess(ctx);
        }
    }

    void AddResultColumn(NKikimrMiniKQL::TStructType& row, const TSysTables::TTableColumnInfo& colInfo) const {
        auto* col = row.AddMember();
        col->SetName(colInfo.Name);
        col->MutableType()->SetKind(NKikimrMiniKQL::Optional);
        auto* item = col->MutableType()->MutableOptional()->MutableItem();
        item->SetKind(NKikimrMiniKQL::Data);
        item->MutableData()->SetScheme(colInfo.PType);
    }

    void BuildResultType(NKikimrMiniKQL::TType& type) const {
        // CommonPrefixes: list struct { Path : String }
        type.SetKind(NKikimrMiniKQL::Struct);
        auto* st = type.MutableStruct();
        {
            auto* prefixes = st->AddMember();
            prefixes->SetName("CommonPrefixes");
            auto* rowList = prefixes->MutableType();
            rowList->SetKind(NKikimrMiniKQL::List);
            auto* row = rowList->MutableList()->MutableItem();
            row->SetKind(NKikimrMiniKQL::Struct);
            auto* rowSt = row->MutableStruct();
            for (const auto& ci : CommonPrefixesColumns) {
                AddResultColumn(*rowSt, ci);
            }
        }

        // Contents: list of struct { Path : String Col1 : Type1 ... }
        {
            auto* contents = st->AddMember();
            contents->SetName("Contents");
            auto* rowList = contents->MutableType();
            rowList->SetKind(NKikimrMiniKQL::List);
            auto* row = rowList->MutableList()->MutableItem();
            row->SetKind(NKikimrMiniKQL::Struct);
            auto* rowSt = row->MutableStruct();

            for (const auto& ci : ContentsColumns) {
                AddResultColumn(*rowSt, ci);
            }
        }
    }

    void AddResultRow(NKikimrMiniKQL::TValue& listOfRows, const TVector<TSysTables::TTableColumnInfo>& rowScheme, const TSerializedCellVec& cells) const {
        Y_VERIFY(rowScheme.size() >= cells.GetCells().size());
        TString errStr;

        auto& mkqlRow = *listOfRows.AddList();
        for (ui32 i = 0; i < cells.GetCells().size(); ++i) {
            const TCell& c = cells.GetCells()[i];
            auto* val = mkqlRow.AddStruct();

            bool ok = NMiniKQL::CellToValue(rowScheme[i].PType, c, *val, errStr);
            Y_VERIFY(ok, "Failed to build result position %" PRIu32 " error: %s", i, errStr.data());
        }
    }

    void ReplySuccess(const NActors::TActorContext& ctx) {
        NKikimrClient::TS3ListingResponse resp;
        resp.SetStatus(MSTATUS_OK);
        resp.SetKeySuffixSize(KeyColumnTypes.size() - PathColumnInfo.KeyOrder);

        NKikimrMiniKQL::TResult& result = *resp.MutableResult();

        BuildResultType(*result.MutableType());

        auto* prefixes = result.MutableValue()->AddStruct();
        for (size_t i = 0; i < CommonPrefixesRows.size(); ++i) {
            AddResultRow(*prefixes, CommonPrefixesColumns, CommonPrefixesRows[i]);
        }

        auto* contents = result.MutableValue()->AddStruct();
        for (size_t i = 0; i < ContentsRows.size(); ++i) {
            AddResultRow(*contents, ContentsColumns, ContentsRows[i]);
        }

        SendReplyMessage(std::move(resp));
        Finished = true;
        Die(ctx);
    }
};


//////////////////////////////////////////////////////
// MsgBus and old GRPC API implementation

class TS3ListingRequestMsgbus : public TMessageBusSessionIdentHolder, public TS3ListingRequestBase<NKikimrServices::TActivity::MSGBUS_COMMON> {
private:
    TAutoPtr<TBusS3ListingRequest> RequestHolder;

public:
    TS3ListingRequestMsgbus(NMsgBusProxy::TBusMessageContext& msgCtx, TActorId schemeCache)
        : TMessageBusSessionIdentHolder(msgCtx)
        , TS3ListingRequestBase(schemeCache, nullptr)
        , RequestHolder(static_cast<TBusS3ListingRequest*>(msgCtx.ReleaseMessage()))
    {}

    const NKikimrClient::TS3ListingRequest* ExtractRequest(TString& errDescr) override {
        errDescr.clear();
        return &RequestHolder->Record;
    }

protected:
    void SendReplyMessage(NKikimrClient::TS3ListingResponse&& response) override {
        TAutoPtr<TBusS3ListingResponse> responseMsg(new TBusS3ListingResponse());
        responseMsg->Record = std::move(response);
        TMessageBusSessionIdentHolder::SendReplyAutoPtr(responseMsg);
    }
};

IActor* CreateMessageBusS3ListingRequest(TBusMessageContext& msg) {
    TActorId schemeCache = MakeSchemeCacheID();
    return new TS3ListingRequestMsgbus(msg, schemeCache);
}

} // namespace NMsgBusProxy


//////////////////////////////////////////////////////
// new GRPC API implementation

namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

class TMessageConverter {
protected:
    static Ydb::S3Internal::S3ListingResult ConvertResult(NKikimrClient::TS3ListingResponse& msgbusResponse) {
        Ydb::S3Internal::S3ListingResult grpcResult;
        Y_VERIFY(msgbusResponse.GetStatus() == NMsgBusProxy::MSTATUS_OK);
        //Cerr << msgbusResponse << Endl;

        Y_VERIFY(msgbusResponse.GetResult().GetType().GetStruct().GetMember(0).GetName() == "CommonPrefixes");
        ConvertMiniKQLRowsToResultSet(
                    msgbusResponse.GetResult().GetType().GetStruct().GetMember(0).GetType(),
                    msgbusResponse.GetResult().GetValue().GetStruct(0),
                    *grpcResult.Mutablecommon_prefixes());

        Y_VERIFY(msgbusResponse.GetResult().GetType().GetStruct().GetMember(1).GetName() == "Contents");
        ConvertMiniKQLRowsToResultSet(
                    msgbusResponse.GetResult().GetType().GetStruct().GetMember(1).GetType(),
                    msgbusResponse.GetResult().GetValue().GetStruct(1),
                    *grpcResult.Mutablecontents());

        grpcResult.Setkey_suffix_size(msgbusResponse.GetKeySuffixSize());

        return grpcResult;
    }

    static Ydb::StatusIds::StatusCode ConvertMsgBusProxyStatusToYdb(ui32 msgBusStatus, ui32 errorCode) {
        switch (msgBusStatus) {
        case NMsgBusProxy::MSTATUS_OK:
            return Ydb::StatusIds::SUCCESS;
        case NMsgBusProxy::MSTATUS_TIMEOUT:
            return Ydb::StatusIds::TIMEOUT;
        case NMsgBusProxy::MSTATUS_INTERNALERROR:
            return Ydb::StatusIds::INTERNAL_ERROR;
        case NMsgBusProxy::MSTATUS_NOTREADY:
            return Ydb::StatusIds::UNAVAILABLE;

        case NMsgBusProxy::MSTATUS_ERROR: {
            switch (errorCode) {
            case NTxProxy::TResultStatus::EStatus::ResolveError:
                return Ydb::StatusIds::SCHEME_ERROR;
            case NTxProxy::TResultStatus::EStatus::ProxyShardNotAvailable:
                return Ydb::StatusIds::UNAVAILABLE;
            case NTxProxy::TResultStatus::EStatus::WrongRequest:
                return Ydb::StatusIds::BAD_REQUEST;
            case NTxProxy::TResultStatus::AccessDenied:
                return Ydb::StatusIds::UNAUTHORIZED;
            default:
                return Ydb::StatusIds::GENERIC_ERROR;
            }
        }

        default:
            return Ydb::StatusIds::GENERIC_ERROR;
        }
    }

    static NKikimrClient::TS3ListingRequest ConvertRequest(const Ydb::S3Internal::S3ListingRequest* proto) {
        NKikimrClient::TS3ListingRequest msgbusRequest;
        msgbusRequest.SetTableName(proto->Gettable_name());
        if (proto->Haskey_prefix()) {
            ValueToParams(proto->Getkey_prefix(), *msgbusRequest.MutableKeyPrefix());
        }
        msgbusRequest.SetPathColumnPrefix(proto->Getpath_column_prefix());
        msgbusRequest.SetPathColumnDelimiter(proto->Getpath_column_delimiter());
        if (proto->Hasstart_after_key_suffix()) {
            ValueToParams(proto->Getstart_after_key_suffix(), *msgbusRequest.MutableStartAfterKeySuffix());
        }
        msgbusRequest.SetMaxKeys(proto->Getmax_keys());
        msgbusRequest.MutableColumnsToReturn()->CopyFrom(proto->Getcolumns_to_return());

        // TODO: operaiton params
        return msgbusRequest;
    }

    static void ValueToParams(const Ydb::TypedValue& tv, NKikimrMiniKQL::TParams& params) {
        ConvertYdbTypeToMiniKQLType(tv.Gettype(), *params.MutableType());
        ConvertYdbValueToMiniKQLValue(tv.Gettype(), tv.Getvalue(), *params.MutableValue());
    }

    static void ConvertMiniKQLRowsToResultSet(const NKikimrMiniKQL::TType& rowsListType, const NKikimrMiniKQL::TValue& rowsList, Ydb::ResultSet& resultSet) {
        TStackVec<NKikimrMiniKQL::TType> columnTypes;
        Y_VERIFY(rowsListType.GetKind() == NKikimrMiniKQL::ETypeKind::List);
        for (const auto& column : rowsListType.GetList().GetItem().GetStruct().GetMember()) {
            auto columnMeta = resultSet.add_columns();
            columnMeta->set_name(column.GetName());
            columnTypes.push_back(column.GetType());
            ConvertMiniKQLTypeToYdbType(column.GetType(), *columnMeta->mutable_type());
        }

        for (const auto& row : rowsList.GetList()) {
            auto newRow = resultSet.add_rows();
            ui32 columnCount = static_cast<ui32>(row.StructSize());
            Y_VERIFY(columnCount == columnTypes.size());
            for (ui32 i = 0; i < columnCount; i++) {
                const auto& column = row.GetStruct(i);
                ConvertMiniKQLValueToYdbValue(columnTypes[i], column, *newRow->add_items());
            }
        }
        resultSet.set_truncated(false);
    }
};


class TS3ListingRequestGrpc : protected TMessageConverter, public NMsgBusProxy::TS3ListingRequestBase<NKikimrServices::TActivity::GRPC_REQ> {
private:
    TAutoPtr<TEvS3ListingRequest> GrpcRequest;
    NKikimrClient::TS3ListingRequest MsgbusRequest;

public:
    TS3ListingRequestGrpc(TAutoPtr<TEvS3ListingRequest> request, TActorId schemeCache)
        : TS3ListingRequestBase(schemeCache,
                                THolder<const NACLib::TUserToken>(request->GetInternalToken() ? new NACLib::TUserToken(request->GetInternalToken()) : nullptr))
        , GrpcRequest(request)
    {}


    const NKikimrClient::TS3ListingRequest* ExtractRequest(TString& errDescr) override {
        try {
            MsgbusRequest = TMessageConverter::ConvertRequest(GrpcRequest->GetProtoRequest());
        } catch (std::exception& e) {
            errDescr = e.what();
            return nullptr;
        }

        return &MsgbusRequest;
    }

protected:
    void SendReplyMessage(NKikimrClient::TS3ListingResponse&& msgbusResponse) override {
        if (msgbusResponse.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            Ydb::StatusIds::StatusCode grpcStatus =
                    TMessageConverter::ConvertMsgBusProxyStatusToYdb(msgbusResponse.GetStatus(), msgbusResponse.GetErrorCode());
            TString description = msgbusResponse.GetDescription();

            if (!description.empty()) {
                GrpcRequest->RaiseIssue(NYql::TIssue(description));
            }
            GrpcRequest->ReplyWithYdbStatus(grpcStatus);
        } else {
            Ydb::S3Internal::S3ListingResult grpcResult = TMessageConverter::ConvertResult(msgbusResponse);
            GrpcRequest->SendResult(grpcResult, Ydb::StatusIds::SUCCESS);
        }
    }
};


IActor* CreateGrpcS3ListingRequest(TAutoPtr<TEvS3ListingRequest> request) {
    TActorId schemeCache = MakeSchemeCacheID();
    return new TS3ListingRequestGrpc(request, schemeCache);
}

} // namespace NGRpcService
} // namespace NKikimr
