#include "rpc_common.h"
#include "rpc_deferrable.h"
#include "service_longtx.h"

#include <ydb/public/api/grpc/draft/ydb_long_tx_v1.pb.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/formats/sharding.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

#include <library/cpp/actors/wilson/wilson_profile_span.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

namespace NKikimr {

namespace {

using TEvLongTxBeginRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::LongTx::BeginTransactionRequest,
    Ydb::LongTx::BeginTransactionResponse>;
using TEvLongTxCommitRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::LongTx::CommitTransactionRequest,
    Ydb::LongTx::CommitTransactionResponse>;
using TEvLongTxRollbackRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::LongTx::RollbackTransactionRequest,
    Ydb::LongTx::RollbackTransactionResponse>;
using TEvLongTxWriteRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::LongTx::WriteRequest,
    Ydb::LongTx::WriteResponse>;
using TEvLongTxReadRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::LongTx::ReadRequest,
    Ydb::LongTx::ReadResponse>;

std::shared_ptr<arrow::Schema> ExtractArrowSchema(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    TVector<std::pair<TString, NScheme::TTypeInfo>> columns;
    for (auto& col : schema.GetColumns()) {
        Y_VERIFY(col.HasTypeId());
        auto typeInfo = NScheme::TypeInfoFromProtoColumnType(col.GetTypeId(),
            col.HasTypeInfo() ? &col.GetTypeInfo() : nullptr);
        columns.emplace_back(col.GetName(), typeInfo);
    }

    return NArrow::MakeArrowSchema(columns);
}

class TShardInfo {
private:
    const TString Data;
    const ui32 RowsCount;
public:
    TShardInfo(const TString& data, const ui32 rowsCount)
        : Data(data)
        , RowsCount(rowsCount)
    {

    }
    const TString& GetData() const {
        return Data;
    }
    ui32 GetRowsCount() const {
        return RowsCount;
    }
};

class TFullSplitData {
private:
    ui32 ShardsCount = 0;
    THashMap<ui64, TShardInfo> ShardsInfo;

public:
    TString ErrorString;

    TFullSplitData(const ui32 shardsCount, TString errString = {})
        : ShardsCount(shardsCount)
        , ErrorString(errString)
    {}

    const THashMap<ui64, TShardInfo>& GetShardsInfo() const {
        return ShardsInfo;
    }

    ui32 GetShardsCount() const {
        return ShardsCount;
    }

    void AddShardInfo(const ui64 tabletId, TShardInfo&& info) {
        ShardsInfo.emplace(tabletId, std::move(info));
    }
};

TFullSplitData SplitData(const std::shared_ptr<arrow::RecordBatch>& batch,
    const NKikimrSchemeOp::TColumnTableDescription& description)
{
    Y_VERIFY(batch);
    Y_VERIFY(description.HasSharding() && description.GetSharding().HasHashSharding());

    auto& descSharding = description.GetSharding();
    auto& hashSharding = descSharding.GetHashSharding();

    TVector<ui64> tabletIds(descSharding.GetColumnShards().begin(), descSharding.GetColumnShards().end());
    TVector<TString> shardingColumns(hashSharding.GetColumns().begin(), hashSharding.GetColumns().end());
    ui32 numShards = tabletIds.size();
    Y_VERIFY(numShards);
    TFullSplitData result(numShards);

    if (numShards == 1) {
        TShardInfo splitInfo(NArrow::SerializeBatchNoCompression(batch), batch->num_rows());
        result.AddShardInfo(tabletIds[0], std::move(splitInfo));
        return result;
    }

    std::vector<ui32> rowSharding;
    if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N) {
        NArrow::THashSharding sharding(numShards);
        rowSharding = sharding.MakeSharding(batch, shardingColumns);
    } else if (hashSharding.GetFunction() == NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS) {
        ui32 activeShards = NArrow::TLogsSharding::DEFAULT_ACITVE_SHARDS;
        if (hashSharding.HasActiveShardsCount()) {
            activeShards = hashSharding.GetActiveShardsCount();
        }
        NArrow::TLogsSharding sharding(numShards, activeShards);
        rowSharding = sharding.MakeSharding(batch, shardingColumns);
    }

    if (rowSharding.empty()) {
        result.ErrorString = "empty "
            + NKikimrSchemeOp::TColumnTableSharding::THashSharding::EHashFunction_Name(hashSharding.GetFunction())
            + " sharding";
        for (auto& column : shardingColumns) {
            if (batch->schema()->GetFieldIndex(column) < 0) {
                result.ErrorString += ", no column '" + column + "'";
            }
        }
        return result;
    }

    std::vector<std::shared_ptr<arrow::RecordBatch>> sharded = NArrow::ShardingSplit(batch, rowSharding, numShards);
    Y_VERIFY(sharded.size() == numShards);

    THashMap<ui64, TString> out;
    for (size_t i = 0; i < sharded.size(); ++i) {
        if (sharded[i]) {
            TShardInfo splitInfo(NArrow::SerializeBatchNoCompression(sharded[i]), sharded[i]->num_rows());
            result.AddShardInfo(tabletIds[i], std::move(splitInfo));
        }
    }

    Y_VERIFY(result.GetShardsInfo().size());
    return result;
}

// Deserialize arrow batch and splits it
TFullSplitData SplitData(const TString& data, const NKikimrSchemeOp::TColumnTableDescription& description) {
    Y_VERIFY(description.HasSchema());
    auto& olapSchema = description.GetSchema();
    Y_VERIFY(olapSchema.GetEngine() == NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);

    std::shared_ptr<arrow::Schema> schema = ExtractArrowSchema(olapSchema);
    std::shared_ptr<arrow::RecordBatch> batch = NArrow::DeserializeBatch(data, schema);
    if (!batch) {
        return TFullSplitData(0, TString("cannot deserialize batch with schema ") + schema->ToString());
    }

    auto res = batch->ValidateFull();
    if (!res.ok()) {
        return TFullSplitData(0, TString("deserialize batch is not valid: ") + res.ToString());
    }
    return SplitData(batch, description);
}

}

namespace NGRpcService {
using namespace NActors;
using namespace NLongTxService;

class TLongTxBeginRPC : public TActorBootstrapped<TLongTxBeginRPC> {
    using TBase = TActorBootstrapped<TLongTxBeginRPC>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TLongTxBeginRPC(std::unique_ptr<IRequestOpCtx> request)
        : TBase()
        , Request(std::move(request))
        , DatabaseName(Request->GetDatabaseName().GetOrElse(DatabaseFromDomain(AppData())))
    {}

    void Bootstrap() {
        const auto* req = TEvLongTxBeginRequest::GetProtoRequest(Request);

        NKikimrLongTxService::TEvBeginTx::EMode mode = {};
        switch (req->tx_type()) {
            case Ydb::LongTx::BeginTransactionRequest::READ:
                mode = NKikimrLongTxService::TEvBeginTx::MODE_READ_ONLY;
                break;
            case Ydb::LongTx::BeginTransactionRequest::WRITE:
                mode = NKikimrLongTxService::TEvBeginTx::MODE_WRITE_ONLY;
                break;
            default:
                // TODO: report error
                break;
        }

        Send(MakeLongTxServiceID(SelfId().NodeId()), new TEvLongTxService::TEvBeginTx(DatabaseName, mode));
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLongTxService::TEvBeginTxResult, Handle);
        }
    }

    void Handle(TEvLongTxService::TEvBeginTxResult::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);
            if (issues) {
                Request->RaiseIssues(std::move(issues));
            }
            Request->ReplyWithYdbStatus(msg->Record.GetStatus());
            return PassAway();
        }

        Ydb::LongTx::BeginTransactionResult result;
        result.set_tx_id(msg->GetLongTxId().ToString());
        ReplySuccess(result);
    }

    void ReplySuccess(const Ydb::LongTx::BeginTransactionResult& result) {
        Request->SendResult(result, Ydb::StatusIds::SUCCESS);
        PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    TString DatabaseName;
};

//

class TLongTxCommitRPC : public TActorBootstrapped<TLongTxCommitRPC> {
    using TBase = TActorBootstrapped<TLongTxCommitRPC>;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TLongTxCommitRPC(std::unique_ptr<IRequestOpCtx> request)
        : TBase()
        , Request(std::move(request))
    {
    }

    void Bootstrap() {
        const auto* req = TEvLongTxCommitRequest::GetProtoRequest(Request);

        TString errMsg;
        if (!LongTxId.ParseString(req->tx_id(), &errMsg)) {
            return ReplyError(Ydb::StatusIds::BAD_REQUEST, errMsg);
        }

        Send(MakeLongTxServiceID(SelfId().NodeId()), new TEvLongTxService::TEvCommitTx(LongTxId));
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLongTxService::TEvCommitTxResult, Handle);
        }
    }

    void Handle(TEvLongTxService::TEvCommitTxResult::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);
            if (issues) {
                Request->RaiseIssues(std::move(issues));
            }
            Request->ReplyWithYdbStatus(msg->Record.GetStatus());
            return PassAway();
        }

        Ydb::LongTx::CommitTransactionResult result;
        const auto* req = TEvLongTxCommitRequest::GetProtoRequest(Request);
        result.set_tx_id(req->tx_id());
        ReplySuccess(result);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message) {
        if (!message.empty()) {
            Request->RaiseIssue(NYql::TIssue(message));
        }
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }

    void ReplySuccess(const Ydb::LongTx::CommitTransactionResult& result) {
        Request->SendResult(result, Ydb::StatusIds::SUCCESS);
        PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    TLongTxId LongTxId;
};

//

class TLongTxRollbackRPC : public TActorBootstrapped<TLongTxRollbackRPC> {
    using TBase = TActorBootstrapped<TLongTxRollbackRPC>;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TLongTxRollbackRPC(std::unique_ptr<IRequestOpCtx> request)
        : TBase()
        , Request(std::move(request))
    {
    }

    void Bootstrap() {
        const auto* req = TEvLongTxRollbackRequest::GetProtoRequest(Request);

        TString errMsg;
        if (!LongTxId.ParseString(req->tx_id(), &errMsg)) {
            return ReplyError(Ydb::StatusIds::BAD_REQUEST, errMsg);
        }

        Send(MakeLongTxServiceID(SelfId().NodeId()), new TEvLongTxService::TEvRollbackTx(LongTxId));
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLongTxService::TEvRollbackTxResult, Handle);
        }
    }

    void Handle(TEvLongTxService::TEvRollbackTxResult::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);
            if (issues) {
                Request->RaiseIssues(std::move(issues));
            }
            Request->ReplyWithYdbStatus(msg->Record.GetStatus());
            return PassAway();
        }

        Ydb::LongTx::RollbackTransactionResult result;
        const auto* req = TEvLongTxRollbackRequest::GetProtoRequest(Request);
        result.set_tx_id(req->tx_id());
        ReplySuccess(result);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message) {
        if (!message.empty()) {
            Request->RaiseIssue(NYql::TIssue(message));
        }
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }

    void ReplySuccess(const Ydb::LongTx::RollbackTransactionResult& result) {
        Request->SendResult(result, Ydb::StatusIds::SUCCESS);
        PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    TLongTxId LongTxId;
};

// Common logic of LongTx Write that takes care of splitting the data according to the sharding scheme,
// sending it to shards and collecting their responses
template <class TLongTxWriteImpl>
class TLongTxWriteBase : public TActorBootstrapped<TLongTxWriteImpl> {
    using TBase = TActorBootstrapped<TLongTxWriteImpl>;
protected:
    using TThis = typename TBase::TThis;

public:
    struct TRetryData {
        static const constexpr ui32 MaxRetriesPerShard = 10;
        static const constexpr ui32 OverloadedDelayMs = 200;

        ui64 TableId;
        TString DedupId;
        TString Data;
        ui32 NumRetries;

        static TDuration OverloadTimeout() {
            return TDuration::MilliSeconds(OverloadedDelayMs);
        }
    };

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TLongTxWriteBase(const TString& databaseName, const TString& path, const TString& token,
        const TLongTxId& longTxId, const TString& dedupId)
        : TBase()
        , DatabaseName(databaseName)
        , Path(path)
        , DedupId(dedupId)
        , LongTxId(longTxId)
        , LeaderPipeCache(MakePipePeNodeCacheID(false))
        , ActorSpan(0, NWilson::TTraceId::NewTraceId(0, Max<ui32>()), "TLongTxWriteBase")
    {
        if (token) {
            UserToken.emplace(token);
        }
    }

    void PassAway() override {
        this->Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

protected:
    void ProceedWithSchema(const NSchemeCache::TSchemeCacheNavigate& resp) {
        NWilson::TProfileSpan pSpan = ActorSpan.BuildChildrenSpan("ProceedWithSchema");
        if (resp.ErrorCount > 0) {
            // TODO: map to a correct error
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "There was an error during table query");
        }

        auto& entry = resp.ResultSet[0];

        if (UserToken && entry.SecurityObject) {
            const ui32 access = NACLib::UpdateRow;
            if (!entry.SecurityObject->CheckAccess(access, *UserToken)) {
                RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, TStringBuilder()
                    << "User has no permission to perform writes to this table"
                    << " user: " << UserToken->GetUserSID()
                    << " path: " << Path));
                return ReplyError(Ydb::StatusIds::UNAUTHORIZED);
            }
        }

        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "The specified path is not an column table");
        }

        if (!entry.ColumnTableInfo || !entry.ColumnTableInfo->Description.HasSharding()
            || !entry.ColumnTableInfo->Description.HasSchema()) {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "Column table expected");
        }

        const auto& description = entry.ColumnTableInfo->Description;
        const auto& schema = description.GetSchema();
        const auto& sharding = description.GetSharding();

        if (sharding.ColumnShardsSize() == 0) {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "No shards to write to");
        }

        if (!schema.HasEngine() || schema.GetEngine() == NKikimrSchemeOp::COLUMN_ENGINE_NONE ||
            (schema.GetEngine() == NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES && !sharding.HasHashSharding())) {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "Wrong column table configuration");
        }

        ui64 tableId = entry.TableId.PathId.LocalPathId;

        if (sharding.HasRandomSharding()) {
            ui64 shard = sharding.GetColumnShards(0);
            SendWriteRequest(shard, tableId, DedupId, GetSerializedData());
        } else if (sharding.HasHashSharding()) {

            const TFullSplitData batches = HasDeserializedBatch() ?
                SplitData(GetDeserializedBatch(), description) :
                SplitData(GetSerializedData(), description);
            if (batches.GetShardsInfo().empty()) {
                return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "Input data sharding error: " + batches.ErrorString);
            }
            ui32 sumBytes = 0;
            ui32 rowsCount = 0;
            for (auto& [shard, info] : batches.GetShardsInfo()) {
                sumBytes += info.GetData().size();
                rowsCount += info.GetRowsCount();
                SendWriteRequest(shard, tableId, DedupId, info.GetData());
            }
            pSpan.Attribute("affected_shards_count", (long)batches.GetShardsInfo().size());
            pSpan.Attribute("bytes", (long)sumBytes);
            pSpan.Attribute("rows", (long)rowsCount);
            pSpan.Attribute("shards_count", (long)batches.GetShardsCount());
        } else {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "Sharding method is not supported");
        }

        this->Become(&TThis::StateWrite);
    }

private:
    void SendWriteRequest(ui64 shardId, ui64 tableId, const TString& dedupId, const TString& data) {
        TRetryData retry{
            .TableId = tableId,
            .DedupId = dedupId,
            .Data = data,
            .NumRetries = 0
        };
        WaitShards.emplace(shardId, std::move(retry));
        SendToTablet(shardId, MakeHolder<TEvColumnShard::TEvWrite>(this->SelfId(), LongTxId, tableId, dedupId, data));
    }

    bool RetryWriteRequest(ui64 shardId, bool delayed = true) {
        if (!WaitShards.count(shardId)) {
            return false;
        }

        auto& retry = WaitShards[shardId];
        if (retry.NumRetries < TRetryData::MaxRetriesPerShard) {
            if (delayed) {
                if (ShardsToRetry.empty()) {
                    TimeoutTimerActorId = CreateLongTimer(TRetryData::OverloadTimeout(),
                        new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup()));
                }
                ShardsToRetry.insert(shardId);
            } else {
                ++retry.NumRetries;
                SendToTablet(shardId, MakeHolder<TEvColumnShard::TEvWrite>(this->SelfId(), LongTxId,
                                                                           retry.TableId, retry.DedupId, retry.Data));
            }
            return true;
        }
        return false;
    }

    STFUNC(StateWrite) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvColumnShard::TEvWriteResult, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvColumnShard::TEvWriteResult::TPtr& ev) {
        auto gProfile = ActorSpan.StartStackTimeGuard("WriteResult");
        const auto* msg = ev->Get();
        ui64 shardId = msg->Record.GetOrigin();
        Y_VERIFY(WaitShards.count(shardId) || ShardsWrites.count(shardId));

        const auto status = (NKikimrTxColumnShard::EResultStatus)msg->Record.GetStatus();
        if (status == NKikimrTxColumnShard::OVERLOADED) {
            if (RetryWriteRequest(shardId)) {
                return;
            }
        }
        if (status != NKikimrTxColumnShard::SUCCESS) {
            auto ydbStatus = NColumnShard::ConvertToYdbStatus(status);
            return ReplyError(ydbStatus,
                TStringBuilder() << "Cannot write data into shard " << shardId << " in longTx " << LongTxId.ToString());
        }

        if (!WaitShards.count(shardId)) {
            return;
        }

        ShardsWrites[shardId] = msg->Record.GetWriteId();
        WaitShards.erase(shardId);
        if (WaitShards.empty()) {
            SendAttachWrite();
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        NWilson::TProfileSpan pSpan(0, ActorSpan.GetTraceId(), "DeliveryProblem");
        const auto* msg = ev->Get();
        ui64 shardId = msg->TabletId;

        if (!WaitShards.count(shardId)) {
            return;
        }

        if (RetryWriteRequest(shardId)) {
            return;
        }

        TString errMsg = TStringBuilder() << "Shard " << shardId << " is not available after "
            << WaitShards[shardId].NumRetries << " retries";
        if (msg->NotDelivered) {
            return ReplyError(Ydb::StatusIds::UNAVAILABLE, errMsg);
        } else {
            return ReplyError(Ydb::StatusIds::UNDETERMINED, errMsg);
        }
    }

    void HandleTimeout(const TActorContext& /*ctx*/) {
        TimeoutTimerActorId = {};
        for (ui64 shardId : ShardsToRetry) {
            RetryWriteRequest(shardId, false);
        }
        ShardsToRetry.clear();
    }

private:
    void SendAttachWrite() {
        auto req = MakeHolder<TEvLongTxService::TEvAttachColumnShardWrites>(LongTxId);
        for (auto& [shardId, writeId] : ShardsWrites) {
            req->AddWrite(shardId, writeId);
        }
        this->Send(MakeLongTxServiceID(this->SelfId().NodeId()), req.Release());
        this->Become(&TThis::StateAttachWrite);
    }


    STFUNC(StateAttachWrite) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLongTxService::TEvAttachColumnShardWritesResult, Handle);
        }
    }

    void Handle(TEvLongTxService::TEvAttachColumnShardWritesResult::TPtr& ev) {
        NWilson::TProfileSpan pSpan(0, ActorSpan.GetTraceId(), "AttachColumnShardWritesResult");
        const auto* msg = ev->Get();

        if (msg->Record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(msg->Record.GetIssues(), issues);
            for (auto& issue : issues) {
                RaiseIssue(issue);
            }
            return ReplyError(msg->Record.GetStatus());
        }

        ReplySuccess();
    }

private:
    void SendToTablet(ui64 tabletId, THolder<IEventBase> event) {
        this->Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), tabletId, true),
                IEventHandle::FlagTrackDelivery, 0, ActorSpan.GetTraceId());
    }

protected:
    virtual bool HasDeserializedBatch() const {
         return false;
    }

    virtual std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const {
        return nullptr;
    }
    virtual TString GetSerializedData() = 0;
    virtual void RaiseIssue(const NYql::TIssue& issue) = 0;
    virtual void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) = 0;
    virtual void ReplySuccess() = 0;

protected:
    const TString DatabaseName;
    const TString Path;
    const TString DedupId;
    TLongTxId LongTxId;
private:
    const TActorId LeaderPipeCache;
    std::optional<NACLib::TUserToken> UserToken;
    THashMap<ui64, TRetryData> WaitShards;
    THashMap<ui64, ui64> ShardsWrites;
    THashSet<ui64> ShardsToRetry;
    NWilson::TProfileSpan ActorSpan;
    TActorId TimeoutTimerActorId;
};


// GRPC call implementation of LongTx Write
class TLongTxWriteRPC : public TLongTxWriteBase<TLongTxWriteRPC> {
    using TBase = TLongTxWriteBase<TLongTxWriteRPC>;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TLongTxWriteRPC(std::unique_ptr<IRequestOpCtx> request)
        : TBase(request->GetDatabaseName().GetOrElse(DatabaseFromDomain(AppData())),
            TEvLongTxWriteRequest::GetProtoRequest(request)->path(),
            request->GetSerializedToken(),
            TLongTxId(),
            TEvLongTxWriteRequest::GetProtoRequest(request)->dedup_id())
        , Request(std::move(request))
        , SchemeCache(MakeSchemeCacheID())
    {
    }

    void Bootstrap() {
        const auto* req = GetProtoRequest();

        TString errMsg;
        if (!LongTxId.ParseString(req->tx_id(), &errMsg)) {
            return ReplyError(Ydb::StatusIds::BAD_REQUEST, errMsg);
        }

        if (GetProtoRequest()->data().format() != Ydb::LongTx::Data::APACHE_ARROW) {
            return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Only APACHE_ARROW data format is supported");
        }

        SendNavigateRequest();
    }

    void SendNavigateRequest() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = this->DatabaseName;
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = ::NKikimr::SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TThis::StateNavigate);
    }

    STFUNC(StateNavigate) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();
        Y_VERIFY(resp);
        ProceedWithSchema(*resp);
    }

private:
    const TEvLongTxWriteRequest::TRequest* GetProtoRequest() const {
        return TEvLongTxWriteRequest::GetProtoRequest(Request);
    }

protected:
    TString GetSerializedData() override {
        return GetProtoRequest()->data().data();
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        Request->RaiseIssue(issue);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) override {
        if (!message.empty()) {
            Request->RaiseIssue(NYql::TIssue(message));
        }
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }

    void ReplySuccess() override {
        Ydb::LongTx::WriteResult result;
        result.set_tx_id(GetProtoRequest()->tx_id());
        result.set_path(Path);
        result.set_dedup_id(DedupId);

        Request->SendResult(result, Ydb::StatusIds::SUCCESS);
        PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    TActorId SchemeCache;
};


template<>
IActor* TEvLongTxWriteRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TLongTxWriteRPC(std::unique_ptr<NKikimr::NGRpcService::IRequestOpCtx>(msg));
}

// LongTx Write implementation called from the inside of YDB (e.g. as a part of BulkUpsert call)
// NOTE: permission checks must have been done by the caller
class TLongTxWriteInternal : public TLongTxWriteBase<TLongTxWriteInternal> {
    using TBase = TLongTxWriteBase<TLongTxWriteInternal>;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TLongTxWriteInternal(const TActorId& replyTo, const TLongTxId& longTxId, const TString& dedupId,
            const TString& databaseName, const TString& path,
            std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult,
            std::shared_ptr<arrow::RecordBatch> batch,
            std::shared_ptr<NYql::TIssues> issues)
        : TBase(databaseName, path, TString(), longTxId, dedupId)
        , ReplyTo(replyTo)
        , NavigateResult(navigateResult)
        , Batch(batch)
        , Issues(issues)
    {
        Y_VERIFY(Issues);
    }

    void Bootstrap() {
        Y_VERIFY(NavigateResult);
        ProceedWithSchema(*NavigateResult);
    }

protected:
    bool HasDeserializedBatch() const override {
         return true;
    }

    std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const override {
        return Batch;
    }

    TString GetSerializedData() override {
        return NArrow::SerializeBatchNoCompression(Batch);
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        Issues->AddIssue(issue);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) override {
        if (!message.empty()) {
            Issues->AddIssue(NYql::TIssue(message));
        }
        this->Send(ReplyTo, new TEvents::TEvCompleted(0, status));
        PassAway();
    }

    void ReplySuccess() override {
        this->Send(ReplyTo, new TEvents::TEvCompleted(0, Ydb::StatusIds::SUCCESS));
        PassAway();
    }

private:
    const TActorId ReplyTo;
    std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> NavigateResult;
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::shared_ptr<NYql::TIssues> Issues;
};


TActorId DoLongTxWriteSameMailbox(const TActorContext& ctx, const TActorId& replyTo,
    const NLongTxService::TLongTxId& longTxId, const TString& dedupId,
    const TString& databaseName, const TString& path,
    std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult,
    std::shared_ptr<arrow::RecordBatch> batch, std::shared_ptr<NYql::TIssues> issues)
{
    return ctx.RegisterWithSameMailbox(
        new TLongTxWriteInternal(replyTo, longTxId, dedupId, databaseName, path, navigateResult, batch, issues));
}


class TLongTxReadRPC : public TActorBootstrapped<TLongTxReadRPC> {
    using TBase = TActorBootstrapped<TLongTxReadRPC>;

private:
    static const constexpr ui32 MaxRetriesPerShard = 10;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TLongTxReadRPC(std::unique_ptr<IRequestOpCtx> request)
        : TBase()
        , Request(std::move(request))
        , DatabaseName(Request->GetDatabaseName().GetOrElse(DatabaseFromDomain(AppData())))
        , SchemeCache(MakeSchemeCacheID())
        , LeaderPipeCache(MakePipePeNodeCacheID(false))
        , TableId(0)
        , OutChunkNumber(0)
    {
    }

    void Bootstrap() {
        const auto* req = TEvLongTxReadRequest::GetProtoRequest(Request);

        if (const TString& internalToken = Request->GetSerializedToken()) {
            UserToken.emplace(internalToken);
        }

        TString errMsg;
        if (!LongTxId.ParseString(req->tx_id(), &errMsg)) {
            return ReplyError(Ydb::StatusIds::BAD_REQUEST, errMsg);
        }

        Path = req->path();
        SendNavigateRequest();
    }

    void PassAway() override {
        Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    void SendNavigateRequest() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = this->DatabaseName;
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = ::NKikimr::SplitPath(Path);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TThis::StateNavigate);
    }

    STFUNC(StateNavigate) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        NSchemeCache::TSchemeCacheNavigate* resp = ev->Get()->Request.Get();

        if (resp->ErrorCount > 0) {
            // TODO: map to a correct error
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "There was an error during table query");
        }

        auto& entry = resp->ResultSet[0];

        if (UserToken && entry.SecurityObject) {
            const ui32 access = NACLib::SelectRow;
            if (!entry.SecurityObject->CheckAccess(access, *UserToken)) {
                Request->RaiseIssue(MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, TStringBuilder()
                    << "User has no permission to perform reads from this table"
                    << " user: " << UserToken->GetUserSID()
                    << " path: " << Path));
                return ReplyError(Ydb::StatusIds::UNAUTHORIZED);
            }
        }

        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindColumnTable) {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "The specified path is not an column table");
        }

        Y_VERIFY(entry.ColumnTableInfo);
        Y_VERIFY(entry.ColumnTableInfo->Description.HasSharding());
        const auto& sharding = entry.ColumnTableInfo->Description.GetSharding();

        TableId = entry.TableId.PathId.LocalPathId;
        for (ui64 shardId : sharding.GetColumnShards()) {
            ShardChunks[shardId] = {};
        }
        for (ui64 shardId : sharding.GetAdditionalColumnShards()) {
            ShardChunks[shardId] = {};
        }

        if (ShardChunks.empty()) {
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "No shards to read");
        }

        SendReadRequests();
    }

private:
    void SendReadRequests() {
        for (auto& [shard, chunk] : ShardChunks) {
            Y_UNUSED(chunk);
            SendRequest(shard);
        }
        Become(&TThis::StateWork);
    }

    void SendRequest(ui64 shard) {
        Y_VERIFY(shard != 0);
        Waits.insert(shard);
        SendToTablet(shard, MakeRequest());
    }

    STFUNC(StateWork) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvColumnShard::TEvReadResult, Handle);
        }
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        Y_UNUSED(ev);
        ReplyError(Ydb::StatusIds::INTERNAL_ERROR,
                   "Internal error: node pipe cache is not available, check cluster configuration");
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        ui64 shard = ev->Get()->TabletId;
        if (!Waits.contains(shard)) {
            return;
        }

        if (!ShardRetries.contains(shard)) {
            ShardRetries[shard] = 0;
        }

        ui32 retries = ++ShardRetries[shard];
        if (retries > MaxRetriesPerShard) {
            return ReplyError(Ydb::StatusIds::UNAVAILABLE, Sprintf("Failed to connect to shard %lu", shard));
        }

        SendRequest(shard);
    }

    void Handle(TEvColumnShard::TEvReadResult::TPtr& ev) {
        const auto& record = Proto(ev->Get());
        ui64 shard = record.GetOrigin();
        ui64 chunk = record.GetBatch();
        bool finished = record.GetFinished();

        { // Filter duplicates and allow messages reorder
            if (!ShardChunks.contains(shard)) {
                return ReplyError(Ydb::StatusIds::GENERIC_ERROR, "Response from unexpected shard");
            }

            if (!Waits.contains(shard) || ShardChunks[shard].contains(chunk)) {
                return;
            }

            if (finished) {
                ShardChunkCounts[shard] = chunk + 1; // potential int overflow but pofig
            }

            ShardChunks[shard].insert(chunk);
            if (ShardChunkCounts.count(shard) && ShardChunkCounts[shard] == ShardChunks[shard].size()) {
                Waits.erase(shard);
                ShardChunks[shard].clear();
                Send(LeaderPipeCache, new TEvPipeCache::TEvUnlink(shard));
            }
        }

        ui32 status = record.GetStatus();
        if (status == NKikimrTxColumnShard::EResultStatus::SUCCESS) {
            auto result = MakeResult(OutChunkNumber, Waits.empty());
            if (record.HasData()) {
                result->mutable_data()->set_data(record.GetData());
            }
            ++OutChunkNumber;
            return ReplySuccess(*result);
        }

        return ReplyError(Ydb::StatusIds::GENERIC_ERROR, "");
    }

    THolder<TEvColumnShard::TEvRead> MakeRequest() const {
        return MakeHolder<TEvColumnShard::TEvRead>(
            SelfId(), 0, LongTxId.Snapshot.Step, LongTxId.Snapshot.TxId, TableId);
    }

    Ydb::LongTx::ReadResult* MakeResult(ui64 outChunk, bool finished) const {
        auto result = TEvLongTxReadRequest::AllocateResult<Ydb::LongTx::ReadResult>(Request);

        const auto* req = TEvLongTxReadRequest::GetProtoRequest(Request);
        result->set_tx_id(req->tx_id());
        result->set_path(req->path());
        result->set_chunk(outChunk);
        result->set_finished(finished);
        return result;
    }

private:
    void SendToTablet(ui64 tabletId, THolder<IEventBase> event) {
        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(event.Release(), tabletId, true),
                IEventHandle::FlagTrackDelivery);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) {
        if (!message.empty()) {
            Request->RaiseIssue(NYql::TIssue(message));
        }
        Request->ReplyWithYdbStatus(status);
        PassAway();
    }

    void ReplySuccess(const Ydb::LongTx::ReadResult& result) {
        Request->SendResult(result, Ydb::StatusIds::SUCCESS);
        PassAway();
    }

private:
    std::unique_ptr<IRequestOpCtx> Request;
    TString DatabaseName;
    TActorId SchemeCache;
    TActorId LeaderPipeCache;
    std::optional<NACLib::TUserToken> UserToken;
    TLongTxId LongTxId;
    TString Path;
    ui64 TableId;
    THashMap<ui64, THashSet<ui32>> ShardChunks;
    THashMap<ui64, ui32> ShardChunkCounts;
    THashMap<ui64, ui32> ShardRetries;
    THashSet<ui64> Waits;
    ui64 OutChunkNumber;
};

//

void DoLongTxBeginRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TLongTxBeginRPC(std::move(p)));
}

void DoLongTxCommitRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TLongTxCommitRPC(std::move(p)));
}

void DoLongTxRollbackRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TLongTxRollbackRPC(std::move(p)));
}

void DoLongTxWriteRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TLongTxWriteRPC(std::move(p)));
}

void DoLongTxReadRPC(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TLongTxReadRPC(std::move(p)));
}

}
}
