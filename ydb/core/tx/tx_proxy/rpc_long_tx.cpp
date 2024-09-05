#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/data_events/shard_writer.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/services/ext_index/common/service.h>

#include <ydb/library/actors/wilson/wilson_profile_span.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

namespace NKikimr {

namespace NTxProxy {
using namespace NActors;
using namespace NLongTxService;

// Common logic of LongTx Write that takes care of splitting the data according to the sharding scheme,
// sending it to shards and collecting their responses
template <class TLongTxWriteImpl>
class TLongTxWriteBase : public TActorBootstrapped<TLongTxWriteImpl> {
    using TBase = TActorBootstrapped<TLongTxWriteImpl>;
protected:
    using TThis = typename TBase::TThis;

public:
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
        , ActorSpan(0, NWilson::TTraceId::NewTraceId(0, Max<ui32>()), "TLongTxWriteBase")
    {
        if (token) {
            UserToken.emplace(token);
        }
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

        if (NCSIndex::TServiceOperator::IsEnabled()) {
            TBase::Send(NCSIndex::MakeServiceId(TBase::SelfId().NodeId()),
                new NCSIndex::TEvAddData(GetDataAccessor().GetDeserializedBatch(), Path, std::make_shared<NCSIndex::TNaiveDataUpsertController>(TBase::SelfId())));
        } else {
            IndexReady = true;
        }

        auto shardsSplitter = NEvWrite::IShardsSplitter::BuildSplitter(entry);
        if (!shardsSplitter) {
            return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Shard splitter not implemented for table kind");
        }

        auto initStatus = shardsSplitter->SplitData(entry, GetDataAccessor());
        if (!initStatus.Ok()) {
            return ReplyError(initStatus.GetStatus(), initStatus.GetErrorMessage());
        }

        const auto& splittedData = shardsSplitter->GetSplitData();
        InternalController = std::make_shared<NEvWrite::TWritersController>(splittedData.GetShardRequestsCount(), this->SelfId(), LongTxId);
        ui32 sumBytes = 0;
        ui32 rowsCount = 0;
        ui32 writeIdx = 0;
        for (auto& [shard, infos] : splittedData.GetShardsInfo()) {
            for (auto&& shardInfo : infos) {
                InternalController->GetCounters()->OnRequest(shardInfo->GetRowsCount(), shardInfo->GetBytes());
                sumBytes += shardInfo->GetBytes();
                rowsCount += shardInfo->GetRowsCount();
                this->Register(new NEvWrite::TShardWriter(shard, shardsSplitter->GetTableId(), DedupId, shardInfo, ActorSpan, InternalController, ++writeIdx, NEvWrite::EModificationType::Replace));
            }
        }
        pSpan.Attribute("affected_shards_count", (long)splittedData.GetShardsInfo().size());
        pSpan.Attribute("bytes", (long)sumBytes);
        pSpan.Attribute("rows", (long)rowsCount);
        pSpan.Attribute("shards_count", (long)splittedData.GetShardsCount());
        AFL_DEBUG(NKikimrServices::LONG_TX_SERVICE)("affected_shards_count", splittedData.GetShardsInfo().size())("shards_count", splittedData.GetShardsCount())
            ("path", Path)("shards_info", splittedData.ShortLogString(32));
        this->Become(&TThis::StateMain);
    }

private:
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvWrite::TWritersController::TEvPrivate::TEvShardsWriteResult, Handle)
            hFunc(TEvLongTxService::TEvAttachColumnShardWritesResult, Handle);
            hFunc(NCSIndex::TEvAddDataResult, Handle);
        }
    }

    void Handle(NEvWrite::TWritersController::TEvPrivate::TEvShardsWriteResult::TPtr& ev) {
        NWilson::TProfileSpan pSpan(0, ActorSpan.GetTraceId(), "ShardsWriteResult");
        const auto* msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status != Ydb::StatusIds::SUCCESS);
        for (auto& issue : msg->Issues) {
            RaiseIssue(issue);
        }
        ReplyError(msg->Status);
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
        if (IndexReady) {
            ReplySuccess();
        } else {
            ColumnShardReady = true;
        }
    }

    void Handle(NCSIndex::TEvAddDataResult::TPtr& ev) {
        const auto* msg = ev->Get();
        if (msg->GetErrorMessage()) {
            NWilson::TProfileSpan pSpan(0, ActorSpan.GetTraceId(), "NCSIndex::TEvAddDataResult");
            RaiseIssue(NYql::TIssue(msg->GetErrorMessage()));
            return ReplyError(Ydb::StatusIds::GENERIC_ERROR, msg->GetErrorMessage());
        } else {
            if (ColumnShardReady) {
                ReplySuccess();
            } else {
                IndexReady = true;
            }
        }

    }

protected:
    virtual NEvWrite::IShardsSplitter::IEvWriteDataAccessor& GetDataAccessor() const = 0;
    virtual void RaiseIssue(const NYql::TIssue& issue) = 0;
    virtual void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) = 0;
    virtual void ReplySuccess() = 0;

protected:
    const TString DatabaseName;
    const TString Path;
    const TString DedupId;
    TLongTxId LongTxId;
private:
    std::optional<NACLib::TUserToken> UserToken;
    NWilson::TProfileSpan ActorSpan;
    NEvWrite::TWritersController::TPtr InternalController;
    bool ColumnShardReady = false;
    bool IndexReady = false;
};

// LongTx Write implementation called from the inside of YDB (e.g. as a part of BulkUpsert call)
// NOTE: permission checks must have been done by the caller
class TLongTxWriteInternal : public TLongTxWriteBase<TLongTxWriteInternal> {
    using TBase = TLongTxWriteBase<TLongTxWriteInternal>;

    class TParsedBatchData : public NEvWrite::IShardsSplitter::IEvWriteDataAccessor {
        std::shared_ptr<arrow::RecordBatch> Batch;
    public:
        TParsedBatchData(std::shared_ptr<arrow::RecordBatch> batch)
            : Batch(batch)
        {}

        std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const override {
            return Batch;
        }

        TString GetSerializedData() const override {
            return NArrow::SerializeBatchNoCompression(Batch);
        }
    };

    NEvWrite::IShardsSplitter::IEvWriteDataAccessor::TPtr DataAccessor;
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
        Y_ABORT_UNLESS(Issues);
        DataAccessor = std::make_shared<TParsedBatchData>(Batch);
    }

    void Bootstrap() {
        Y_ABORT_UNLESS(NavigateResult);
        ProceedWithSchema(*NavigateResult);
    }

protected:
    NEvWrite::IShardsSplitter::IEvWriteDataAccessor& GetDataAccessor() const override {
        return *DataAccessor;
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

//


}
}
