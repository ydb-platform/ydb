#include "grpc_request_proxy.h"
#include "rpc_calls.h"

#include "util/string/vector.h"
#include "ydb/library/yql/minikql/mkql_type_ops.h"
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/path.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <util/system/unaligned_mem.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NKikimr {
namespace NGRpcService {

using TEvObjectStorageListingRequest = TGrpcRequestOperationCall<Ydb::ObjectStorage::ListingRequest, Ydb::ObjectStorage::ListingResponse>;

// NOTE: TCell's can reference memomry from tupleValue
bool CellsFromTuple(const Ydb::Type* tupleType,
                    const Ydb::Value& tupleValue,
                    const TConstArrayRef<NScheme::TTypeInfo>& types,
                    bool allowCastFromString,
                    TVector<TCell>& key,
                    TString& errStr,
                    TVector<TString>& memoryOwner)
{

#define CHECK_OR_RETURN_ERROR(cond, descr) \
    if (!(cond)) { \
        errStr = descr; \
        return false; \
    }

    if (tupleType) {
        Ydb::Type::TypeCase typeCase = tupleType->type_case();
        CHECK_OR_RETURN_ERROR(typeCase == Ydb::Type::kTupleType ||
                              (typeCase == Ydb::Type::TYPE_NOT_SET && tupleType->tuple_type().elementsSize() == 0), "Must be a tuple");
        CHECK_OR_RETURN_ERROR(tupleType->tuple_type().elementsSize() <= types.size(),
            "Tuple size " + ToString(tupleType->tuple_type().elementsSize()) + " is greater that expected size " + ToString(types.size()));

        for (size_t i = 0; i < tupleType->tuple_type().elementsSize(); ++i) {
            const auto& ti = tupleType->tuple_type().Getelements(i);
            CHECK_OR_RETURN_ERROR(ti.type_case() == Ydb::Type::kTypeId, "Element at index " + ToString(i) + " in not a TypeId");
            const auto& typeId = ti.Gettype_id();
            CHECK_OR_RETURN_ERROR(typeId == types[i].GetTypeId() ||
                allowCastFromString && (typeId == NScheme::NTypeIds::Utf8),
                "Element at index " + ToString(i) + " has type " + Type_PrimitiveTypeId_Name(typeId) + " but expected type is " + ToString(types[i].GetTypeId()));
        }

        CHECK_OR_RETURN_ERROR(tupleType->Gettuple_type().elementsSize() == tupleValue.itemsSize(),
            Sprintf("Tuple value length %" PRISZT " doesn't match the length in type %" PRISZT, tupleValue.itemsSize(), tupleType->Gettuple_type().elementsSize()));
    } else {
        CHECK_OR_RETURN_ERROR(types.size() >= tupleValue.itemsSize(),
            Sprintf("Tuple length %" PRISZT " is greater than key column count %" PRISZT, tupleValue.itemsSize(), types.size()));
    }

    for (ui32 i = 0; i < tupleValue.itemsSize(); ++i) {
        auto& v = tupleValue.Getitems(i);

        auto value_case = v.value_case();

        CHECK_OR_RETURN_ERROR(value_case != Ydb::Value::VALUE_NOT_SET,
                              Sprintf("Data must be present at position %" PRIu32, i));

        CHECK_OR_RETURN_ERROR(v.itemsSize() == 0 &&
                              v.pairsSize() == 0,
                              Sprintf("Simple type is expected in tuple at position %" PRIu32, i));

        TCell c;
        auto typeId = types[i].GetTypeId();
        switch (typeId) {

#define CASE_SIMPLE_TYPE(name, type, protoField) \
        case NScheme::NTypeIds::name: \
        { \
            bool valuePresent = v.Has##protoField##_value(); \
            if (valuePresent) { \
                type val = v.Get##protoField##_value(); \
                c = TCell((const char*)&val, sizeof(val)); \
            } else if (allowCastFromString && v.Hastext_value()) { \
                const auto slot = NUdf::GetDataSlot(typeId); \
                const auto out = NMiniKQL::ValueFromString(slot, v.Gettext_value()); \
                CHECK_OR_RETURN_ERROR(out, Sprintf("Cannot parse value of type " #name " from text '%s' in tuple at position %" PRIu32, v.Gettext_value().data(), i)); \
                const auto val = out.Get<type>(); \
                c = TCell((const char*)&val, sizeof(val)); \
            } else { \
                CHECK_OR_RETURN_ERROR(false, Sprintf("Value of type " #name " expected in tuple at position %" PRIu32, i)); \
            } \
            Y_ABORT_UNLESS(c.IsInline()); \
            break; \
        }

        CASE_SIMPLE_TYPE(Bool,   bool,  bool);
        CASE_SIMPLE_TYPE(Int8,   i8,    int32);
        CASE_SIMPLE_TYPE(Uint8,  ui8,   uint32);
        CASE_SIMPLE_TYPE(Int16,  i16,   int32);
        CASE_SIMPLE_TYPE(Uint16, ui16,  uint32);
        CASE_SIMPLE_TYPE(Int32,  i32,   int32);
        CASE_SIMPLE_TYPE(Uint32, ui32,  uint32);
        CASE_SIMPLE_TYPE(Int64,  i64,   int64);
        CASE_SIMPLE_TYPE(Uint64, ui64,  uint64);
        CASE_SIMPLE_TYPE(Float,  float, float);
        CASE_SIMPLE_TYPE(Double, double, double);
        CASE_SIMPLE_TYPE(Date,   ui16,  uint32);
        CASE_SIMPLE_TYPE(Datetime, ui32, uint32);
        CASE_SIMPLE_TYPE(Timestamp, ui64, uint64);
        CASE_SIMPLE_TYPE(Interval, i64, int64);


#undef CASE_SIMPLE_TYPE

        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Utf8:
        {
            c = TCell(v.Gettext_value().data(), v.Gettext_value().size());
            break;
        }
        case NScheme::NTypeIds::JsonDocument:
        case NScheme::NTypeIds::DyNumber:
        {
            c = TCell(v.Getbytes_value().data(), v.Getbytes_value().size());
            break;
        }
        case NScheme::NTypeIds::String:
        {
            if (v.Hasbytes_value()) {
                c = TCell(v.Getbytes_value().data(), v.Getbytes_value().size());
            } else if (allowCastFromString && v.Hastext_value()) {
                c = TCell(v.Gettext_value().data(), v.Gettext_value().size());
            } else {
                CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type String in tuple at position %" PRIu32, i));
            }
            break;
        }
        case NScheme::NTypeIds::Pg:
        {
            if (v.Hasbytes_value()) {
                c = TCell(v.Getbytes_value().data(), v.Getbytes_value().size());
            } else if (v.Hastext_value()) {
                auto typeDesc = types[i].GetTypeDesc();
                auto convert = NPg::PgNativeBinaryFromNativeText(v.Gettext_value(), NPg::PgTypeIdFromTypeDesc(typeDesc));
                if (convert.Error) {
                    CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type Pg: %s in tuple at position %" PRIu32, convert.Error->data(), i));
                } else {
                    auto &data = memoryOwner.emplace_back(convert.Str);
                    c = TCell(data);
                }
            } else {
                CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type Pg in tuple at position %" PRIu32, i));
            }
            break;
        }
        case NScheme::NTypeIds::Uuid:
        {
            if (v.Haslow_128()) {
                auto &data = memoryOwner.emplace_back();
                data.resize(NUuid::UUID_LEN);
                NUuid::UuidHalfsToBytes(data.Detach(), data.size(), v.Gethigh_128(), v.Getlow_128());
                c = TCell(data);
            } else if (v.Hasbytes_value()) {
                Y_ABORT_UNLESS(v.Getbytes_value().size() == NUuid::UUID_LEN);
                c = TCell(v.Getbytes_value().data(), v.Getbytes_value().size());
            } else {
                CHECK_OR_RETURN_ERROR(false, Sprintf("Cannot parse value of type Uuid in tuple at position %" PRIu32, i));
            }
            break;
        }
        default:
            CHECK_OR_RETURN_ERROR(false, Sprintf("Unsupported typeId %" PRIu16 " at index %" PRIu32, typeId, i));
            break;
        }

        CHECK_OR_RETURN_ERROR(!c.IsNull(), Sprintf("Invalid non-NULL value at index %" PRIu32, i));
        key.push_back(c);
    }

#undef CHECK_OR_RETURN_ERROR

    return true;
}

class TObjectStorageListingRequestGrpc : public TActorBootstrapped<TObjectStorageListingRequestGrpc> {
private:
    typedef TActorBootstrapped<TThis> TBase;

    static constexpr i32 DEFAULT_MAX_KEYS = 1001;
    static constexpr ui32 DEFAULT_TIMEOUT_SEC = 5*60;

    std::unique_ptr<IRequestOpCtx> GrpcRequest;
    const Ydb::ObjectStorage::ListingRequest* Request;
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
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    TSysTables::TTableColumnInfo PathColumnInfo;
    TVector<TSysTables::TTableColumnInfo> CommonPrefixesColumns;
    TVector<TSysTables::TTableColumnInfo> ContentsColumns;
    TSerializedCellVec PrefixColumns;
    TSerializedCellVec StartAfterSuffixColumns;
    TSerializedCellVec KeyRangeFrom;
    TSerializedCellVec KeyRangeTo;
    ui32 CurrentShardIdx;
    TVector<TString> CommonPrefixesRows;
    TVector<TSerializedCellVec> ContentsRows;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TObjectStorageListingRequestGrpc(std::unique_ptr<IRequestOpCtx> request, TActorId schemeCache, THolder<const NACLib::TUserToken>&& userToken)
        : GrpcRequest(std::move(request))
        , Request(TEvObjectStorageListingRequest::GetProtoRequest(GrpcRequest.get()))
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

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString errDescr;
        if (!Request) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, errDescr, ctx);
        }

        if (Request->Getmax_keys() > 0 && Request->Getmax_keys() <= DEFAULT_MAX_KEYS) {
            MaxKeys = Request->Getmax_keys();
        }

        // TODO: respect timeout parameter
        // ui32 userTimeoutMillisec = Request->GetTimeout();
        // if (userTimeoutMillisec > 0 && TDuration::MilliSeconds(userTimeoutMillisec) < Timeout) {
        //     Timeout = TDuration::MilliSeconds(userTimeoutMillisec);
        // }

        ResolveTable(Request->Gettable_name(), ctx);
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
        entry.Path = NKikimr::SplitPath(table);
        if (entry.Path.empty()) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Invalid table path specified", ctx);
        }
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(entry);
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));

        TimeoutTimerActorId = CreateLongTimer(ctx, Timeout,
            new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));

        TBase::Become(&TThis::StateWaitResolveTable);
        WaitingResolveReply = true;
    }

    void ReplyWithError(Ydb::StatusIds::StatusCode grpcStatus, const TString& message, const TActorContext& ctx) {
        if (!message.empty()) {
            GrpcRequest->RaiseIssue(NYql::TIssue(message));
        }
        GrpcRequest->ReplyWithYdbStatus(grpcStatus);

        Finished = true;

        // We cannot Die() while scheme cache request is in flight because that request has pointer to
        // KeyRange member so we must not destroy it before we get the response
        if (!WaitingResolveReply) {
            Die(ctx);
        }
    }

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, "Request timed out", ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        WaitingResolveReply = false;
        if (Finished) {
            return Die(ctx);
        }

        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_VERIFY(request.ResultSet.size() == 1);
        if (request.ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
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
        TVector<TString> prefixMemoryOwner;
        TConstArrayRef<NScheme::TTypeInfo> prefixTypes(KeyColumnTypes.data(), KeyColumnTypes.size() - 1); // -1 for path column
        CellsFromTuple(&Request->Getkey_prefix().Gettype(), Request->Getkey_prefix().Getvalue(),
                prefixTypes, true, prefixCells, errStr, prefixMemoryOwner);

        if (!errStr.empty()) {
            ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "Invalid KeyPrefix: " + errStr, ctx);
            return false;
        }

        PrefixColumns.Parse(TSerializedCellVec::Serialize(prefixCells));

        // Check path column
        ui32 pathColPos = prefixCells.size();
        Y_VERIFY(pathColPos < KeyColumnTypes.size());
        PathColumnInfo = entry.Columns[keyColumnIds[pathColPos]];
        if (PathColumnInfo.PType.GetTypeId() != NScheme::NTypeIds::Utf8) {
            ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                           Sprintf("Value for path column '%s' has type %s, expected Utf8",
                                   PathColumnInfo.Name.data(), NScheme::TypeName(PathColumnInfo.PType).c_str()), ctx);
            return false;
        }

        CommonPrefixesColumns.push_back(PathColumnInfo);

        TVector<TCell> suffixCells;
        TVector<TString> suffixMemoryOwner;
        TConstArrayRef<NScheme::TTypeInfo> suffixTypes(KeyColumnTypes.data() + pathColPos, KeyColumnTypes.size() - pathColPos); // starts at path column
        CellsFromTuple(&Request->Getstart_after_key_suffix().Gettype(), Request->Getstart_after_key_suffix().Getvalue(),
                                 suffixTypes, true, suffixCells, errStr, suffixMemoryOwner);
        if (!errStr.empty()) {
            ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                           "Invalid StartAfterKeySuffix: " + errStr, ctx);
            return false;
        }

        StartAfterSuffixColumns.Parse(TSerializedCellVec::Serialize(suffixCells));

        if (!StartAfterSuffixColumns.GetCells().empty()) {
            TString startAfterPath = TString(StartAfterSuffixColumns.GetCells()[0].Data(), StartAfterSuffixColumns.GetCells()[0].Size());
            if (!startAfterPath.StartsWith(Request->Getpath_column_prefix())) {
                ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                               "Invalid StartAfterKeySuffix: StartAfter parameter doesn't match PathPrefix", ctx);
                return false;
            }
        }

        // Check ColumsToReturn
        TSet<TString> requestedColumns(Request->Getcolumns_to_return().begin(), Request->Getcolumns_to_return().end());

        // Always request all suffix columns starting from path column
        for (size_t i = pathColPos; i < keyColumnIds.size(); ++i) {
            ui32 colId = keyColumnIds[i];
            requestedColumns.erase(entry.Columns[colId].Name);
            ContentsColumns.push_back(entry.Columns[colId]);
        }

        for (const auto& name : requestedColumns) {
            if (!columnByName.contains(name)) {
                ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
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

        TString pathPrefix = Request->Getpath_column_prefix();
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

        TBase::Become(&TThis::StateWaitResolveShards);
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
                        << " to table [" << Request->Gettable_name() << "]";

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
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                                  Sprintf("Unknown table '%s'", Request->Gettable_name().data()), ctx);
        }

        TString accessCheckError;
        if (!CheckAccess(accessCheckError)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, accessCheckError, ctx);
        }

        auto getShardsString = [] (const TVector<TKeyDesc::TPartitionInfo>& partitions) {
            TVector<ui64> shards;
            shards.reserve(partitions.size());
            for (auto& partition : partitions) {
                shards.push_back(partition.ShardId);
            }

            return JoinVectorIntoString(shards, ", ");
        };

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Range shards: "
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
        ev->Record.SetPathColumnPrefix(Request->Getpath_column_prefix());
        ev->Record.SetPathColumnDelimiter(Request->Getpath_column_delimiter());
        ev->Record.SetSerializedStartAfterKeySuffix(StartAfterSuffixColumns.GetBuffer());
        ev->Record.SetMaxKeys(MaxKeys - ContentsRows.size() - CommonPrefixesRows.size());
        if (!CommonPrefixesRows.empty()) {
            // Next shard might have the same common prefix, need to skip it
            ev->Record.SetLastCommonPrefix(CommonPrefixesRows.back());
        }

        for (const auto& ci : ContentsColumns) {
            ev->Record.AddColumnsToReturn(ci.Id);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::RPC_REQUEST, "Sending request to shards " << shardId);

        ctx.Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.Release(), shardId, true), IEventHandle::FlagTrackDelivery);

        TBase::Become(&TThis::StateWaitResults);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR,
                       "Internal error: pipe cache is not available, the cluster might not be configured properly", ctx);
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        // Invalidate scheme cache in case of partitioning change
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvInvalidateTable(KeyRange->TableId, TActorId()));
        ReplyWithError(Ydb::StatusIds::UNAVAILABLE, "Failed to connect to shard", ctx);
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
            ReplyWithError(Ydb::StatusIds::UNAVAILABLE, shardResponse.GetErrorDescription(), ctx);
            return;
        }

        if (shardResponse.GetStatus() != NKikimrTxDataShard::TError::OK) {
            ReplyWithError(Ydb::StatusIds::GENERIC_ERROR, shardResponse.GetErrorDescription(), ctx);
            return;
        }

        for (size_t i = 0; i < shardResponse.CommonPrefixesRowsSize(); ++i) {
            if (!CommonPrefixesRows.empty() && CommonPrefixesRows.back() == shardResponse.GetCommonPrefixesRows(i)) {
                LOG_ERROR_S(ctx, NKikimrServices::RPC_REQUEST, "S3 listing got duplicate common prefix from shard " << shardResponse.GetTabletID());
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

    void FillResultRows(Ydb::ResultSet &resultSet, TVector<TSysTables::TTableColumnInfo> &columns, TVector<TSerializedCellVec> resultRows) {
        const auto getPgTypeFromColMeta = [](const auto &colMeta) {
            return NYdb::TPgType(NPg::PgTypeNameFromTypeDesc(colMeta.PType.GetTypeDesc()),
                                 colMeta.PTypeMod);
        };

        const auto getTypeFromColMeta = [&](const auto &colMeta) {
            if (colMeta.PType.GetTypeId() == NScheme::NTypeIds::Pg) {
                return NYdb::TTypeBuilder().Pg(getPgTypeFromColMeta(colMeta)).Build();
            } else {
                return NYdb::TTypeBuilder()
                    .Primitive((NYdb::EPrimitiveType)colMeta.PType.GetTypeId())
                    .Build();
            }
        };

        for (const auto& colMeta : columns) {
            const auto type = getTypeFromColMeta(colMeta);
            auto* col = resultSet.Addcolumns();
            
            *col->mutable_type()->mutable_optional_type()->mutable_item() = NYdb::TProtoAccessor::GetProto(type);
            *col->mutable_name() = colMeta.Name;
        }

        for (auto& row : resultRows) {
            NYdb::TValueBuilder vb;
            vb.BeginStruct();
            for (size_t i = 0; i < columns.size(); ++i) {
                const auto& colMeta = columns[i];

                const auto& cell = row.GetCells()[i];
                vb.AddMember(colMeta.Name);
                if (colMeta.PType.GetTypeId() == NScheme::NTypeIds::Pg)
                {
                    const NPg::TConvertResult& pgResult = NPg::PgNativeTextFromNativeBinary(cell.AsBuf(), colMeta.PType.GetTypeDesc());
                    if (pgResult.Error) {
                        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "PgNativeTextFromNativeBinary error " << *pgResult.Error);
                    }
                    const NYdb::TPgValue pgValue{cell.IsNull() ? NYdb::TPgValue::VK_NULL : NYdb::TPgValue::VK_TEXT, pgResult.Str, getPgTypeFromColMeta(colMeta)};
                    vb.Pg(pgValue);
                }
                else
                {
                    const NScheme::TTypeInfo& typeInfo = colMeta.PType;

                    if (cell.IsNull()) {
                        vb.EmptyOptional((NYdb::EPrimitiveType)typeInfo.GetTypeId());
                    } else {
                        vb.BeginOptional();
                        ProtoValueFromCell(vb, typeInfo, cell);
                        vb.EndOptional();
                    }
                }
            }
            vb.EndStruct();
            auto proto = NYdb::TProtoAccessor::GetProto(vb.Build());
            *resultSet.add_rows() = std::move(proto);
        }
    }

    void ReplySuccess(const NActors::TActorContext& ctx) {
        Ydb::ObjectStorage::ListingResult resp;

        for (auto commonPrefix : CommonPrefixesRows) {
            resp.add_common_prefixes(commonPrefix);
        }

        auto &contents = *resp.mutable_contents();
        contents.set_truncated(false);
        FillResultRows(contents, ContentsColumns, ContentsRows);

        try {
            GrpcRequest->SendResult(resp, Ydb::StatusIds::SUCCESS);
        } catch(std::exception ex) {
            GrpcRequest->RaiseIssue(NYql::ExceptionToIssue(ex));
            GrpcRequest->ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
        }
        
        Finished = true;
        Die(ctx);
    }
};

IActor* CreateGrpcObjectStorageListingHandler(std::unique_ptr<IRequestOpCtx> request) {
    TActorId schemeCache = MakeSchemeCacheID();
    auto token = THolder<const NACLib::TUserToken>(request->GetInternalToken() ? new NACLib::TUserToken(request->GetSerializedToken()) : nullptr);
    return new TObjectStorageListingRequestGrpc(std::move(request), schemeCache, std::move(token));
}

void DoObjectStorageListingRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(CreateGrpcObjectStorageListingHandler(std::move(p)));
}

} // namespace NKikimr
} // namespace NGRpcService
