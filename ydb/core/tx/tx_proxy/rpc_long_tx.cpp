#include "global.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/data_events/shard_writer.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/wilson/wilson_profile_span.h>
#include <ydb/services/ext_index/common/service.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

namespace NKikimr {

namespace NTxProxy {
using namespace NActors;
using namespace NLongTxService;

// Common logic of LongTx Write that takes care of splitting the data according to the sharding scheme,
// sending it to shards and collecting their responses
template <class TLongTxWriteImpl>
class TLongTxWriteBase: public TActorBootstrapped<TLongTxWriteImpl>,
                        NColumnShard::TMonitoringObjectsCounter<TLongTxWriteBase<TLongTxWriteImpl>> {
    using TBase = TActorBootstrapped<TLongTxWriteImpl>;
    static inline TAtomicCounter MemoryInFlight = 0;

protected:
    using TThis = typename TBase::TThis;
    const bool NoTxWrite = false;

public:
    TLongTxWriteBase(const TString& databaseName, const std::vector<TString>& tables, const std::vector<ui64> &numrows, const TString& token, const TLongTxId& longTxId, const TString& dedupId,
        const bool noTxWrite)
        : NoTxWrite(noTxWrite)
        , DatabaseName(databaseName)
        , Tables(tables)
        , NumRows(numrows)
        , DedupId(dedupId)
        , LongTxId(longTxId)
        , ActorSpan(0, NWilson::TTraceId::NewTraceId(0, Max<ui32>()), "TLongTxWriteBase") {
        if (token) {
            UserToken.emplace(token);
        }
    }

    virtual ~TLongTxWriteBase() {
        AFL_VERIFY(MemoryInFlight.Sub(InFlightSize) >= 0);
    }

protected:
    void ProceedWithSchema(const NSchemeCache::TSchemeCacheNavigate& resp) {
        NWilson::TProfileSpan pSpan = ActorSpan.BuildChildrenSpan("ProceedWithSchema");
        if (resp.ErrorCount > 0) {
            // TODO: map to a correct error
            return ReplyError(Ydb::StatusIds::SCHEME_ERROR, "There was an error during table query");
        }

        struct SplitInfo {
            ui64 tableId;
            ui64 schemaVersion;
            NEvWrite::IShardsSplitter::IShardInfo::TPtr shardInfo;
        };

        std::unordered_map<ui64, std::vector<SplitInfo>> splits;

        InFlightSize += ExtractDataAccessor(0)->GetSize();

        const i64 sizeInFlight = MemoryInFlight.Add(InFlightSize);
        if (TLimits::MemoryInFlightWriting < (ui64)sizeInFlight && sizeInFlight != InFlightSize) {
            return ReplyError(Ydb::StatusIds::OVERLOADED, "a lot of memory in flight");
        }


        int tableNum = 0;
        for(auto& entry: resp.ResultSet) {
            if (UserToken && entry.SecurityObject) {
                const ui32 access = NACLib::UpdateRow;
                if (!entry.SecurityObject->CheckAccess(access, *UserToken)) {
                    // RaiseIssue(MakeIssue(
                    //     NKikimrIssues::TIssuesIds::ACCESS_DENIED, TStringBuilder() << "User has no permission to perform writes to this table"
                    //                                                                << " user: " << UserToken->GetUserSID() << " path: " << Path));
                    return ReplyError(Ydb::StatusIds::UNAUTHORIZED);
                }
            }

            auto accessor = ExtractDataAccessor(tableNum);

            if (NCSIndex::TServiceOperator::IsEnabled()) {
                // AFL_VERIFY(false);
                // TBase::Send(
                //     NCSIndex::MakeServiceId(TBase::SelfId().NodeId()), new NCSIndex::TEvAddData(accessor->GetDeserializedBatch(), Tables, NumRows,
                //                                                            std::make_shared<NCSIndex::TNaiveDataUpsertController>(TBase::SelfId())));
            } else {
                IndexReady = true;
            }

            auto shardsSplitter = NEvWrite::IShardsSplitter::BuildSplitter(entry);
            if (!shardsSplitter) {
                return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Shard splitter not implemented for table kind");
            }

            auto initStatus = shardsSplitter->SplitData(entry, *accessor);
            if (!initStatus.Ok()) {
                return ReplyError(initStatus.GetStatus(), initStatus.GetErrorMessage());
            }
            accessor.reset();

            const auto& splittedData = shardsSplitter->GetSplitData();

            ui64 tableId = entry.TableId.PathId.LocalPathId;

            for (auto& [shard, infos] : splittedData.GetShardsInfo()) {
                for (auto&& shardInfo : infos) {
                    SplitInfo split;
                    split.tableId = tableId;
                    split.schemaVersion = shardsSplitter->GetSchemaVersion();
                    split.shardInfo = shardInfo;
                    splits[shard].push_back(split);
                }
            }

            tableNum++;
        }

        ui64 writeIdx = 0;

        ui64 writesCount = 0;
        for(auto &[shard, infos]: splits) {
            writesCount += infos.size();
        }

        InternalController =
                std::make_shared<NEvWrite::TWritersController>(writesCount, this->SelfId(), LongTxId, NoTxWrite);

        InternalController->GetCounters()->OnSplitByShards(splits.size());

        for(auto &[shard, infos]: splits) {
            for(auto &info: infos) {
                this->Register(
                    new NEvWrite::TShardWriter(shard, info.tableId, info.schemaVersion, DedupId, info.shardInfo,
                        ActorSpan, InternalController, writeIdx++, NEvWrite::EModificationType::Replace, NoTxWrite, TDuration::Seconds(20)));
            }
        }

        // pSpan.Attribute("affected_shards_count", (long)splittedData.GetShardsInfo().size());
        // pSpan.Attribute("bytes", (long)sumBytes);
        // pSpan.Attribute("rows", (long)rowsCount);
        // pSpan.Attribute("shards_count", (long)splittedData.GetShardsCount());
        // AFL_DEBUG(NKikimrServices::LONG_TX_SERVICE)("affected_shards_count", splittedData.GetShardsInfo().size())(
        //     "shards_count", splittedData.GetShardsCount())("path", Path)("shards_info", splittedData.ShortLogString(32));
        this->Become(&TThis::StateMain);
    }

private:
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEvWrite::TWritersController::TEvPrivate::TEvShardsWriteResult, Handle);
            hFunc(TEvLongTxService::TEvAttachColumnShardWritesResult, Handle);
            hFunc(NCSIndex::TEvAddDataResult, Handle);
        }
    }

    void Handle(NEvWrite::TWritersController::TEvPrivate::TEvShardsWriteResult::TPtr& ev) {
        NWilson::TProfileSpan pSpan(0, ActorSpan.GetTraceId(), "ShardsWriteResult");
        const auto* msg = ev->Get();
        if (msg->Status == Ydb::StatusIds::SUCCESS) {
            if (IndexReady) {
                ReplySuccess();
            } else {
                ColumnShardReady = true;
            }
        } else {
            Y_ABORT_UNLESS(msg->Status != Ydb::StatusIds::SUCCESS);
            for (auto& issue : msg->Issues) {
                RaiseIssue(issue);
            }
            ReplyError(msg->Status);
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
    virtual std::unique_ptr<NEvWrite::IShardsSplitter::IEvWriteDataAccessor> ExtractDataAccessor(int table) = 0;
    virtual void RaiseIssue(const NYql::TIssue& issue) = 0;
    virtual void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) = 0;
    virtual void ReplySuccess() = 0;

protected:
    const TString DatabaseName;
    const std::vector<TString> Tables;
    const std::vector<ui64> NumRows;
    const TString DedupId;
    TLongTxId LongTxId;

private:
    i64 InFlightSize = 0;
    std::optional<NACLib::TUserToken> UserToken;
    NWilson::TProfileSpan ActorSpan;
    NEvWrite::TWritersController::TPtr InternalController;
    bool ColumnShardReady = false;
    bool IndexReady = false;
};

// LongTx Write implementation called from the inside of YDB (e.g. as a part of BulkUpsert call)
// NOTE: permission checks must have been done by the caller
class TLongTxWriteInternal: public TLongTxWriteBase<TLongTxWriteInternal> {
    using TBase = TLongTxWriteBase<TLongTxWriteInternal>;

    class TParsedBatchData: public NEvWrite::IShardsSplitter::IEvWriteDataAccessor {
    private:
        using TBase = NEvWrite::IShardsSplitter::IEvWriteDataAccessor;
        std::shared_ptr<arrow::RecordBatch> Batch;

    public:
        TParsedBatchData(std::shared_ptr<arrow::RecordBatch> batch)
            : TBase(NArrow::GetBatchMemorySize(batch))
            , Batch(batch) {
        }

        std::shared_ptr<arrow::RecordBatch> GetDeserializedBatch() const override {
            return Batch;
        }

        TString GetSerializedData() const override {
            return NArrow::SerializeBatchNoCompression(Batch);
        }
    };

public:
    explicit TLongTxWriteInternal(const TActorId& replyTo, const TLongTxId& longTxId, const TString& dedupId, const TString& databaseName,
        const std::vector<TString>& tables, const std::vector<ui64> &numrows, std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult, std::shared_ptr<arrow::RecordBatch> batch,
        std::shared_ptr<NYql::TIssues> issues, const bool noTxWrite, const ui32 cookie)
        : TBase(databaseName, tables, numrows, TString(), longTxId, dedupId, noTxWrite)
        , ReplyTo(replyTo)
        , NavigateResult(navigateResult)
        , Batch(batch)
        , Issues(issues)
        , Cookie(cookie) {
        Y_ABORT_UNLESS(Issues);
    }

    void Bootstrap() {
        Y_ABORT_UNLESS(NavigateResult);
        ProceedWithSchema(*NavigateResult);
    }

protected:
    std::unique_ptr<NEvWrite::IShardsSplitter::IEvWriteDataAccessor> ExtractDataAccessor(int table) override {
        size_t start = 0;
        for(int i=0; i<table;i++) {
            start += NumRows[i];
        }
        return std::make_unique<TParsedBatchData>(Batch->Slice(start, NumRows[table]));
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        Issues->AddIssue(issue);
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& message = TString()) override {
        if (!message.empty()) {
            Issues->AddIssue(NYql::TIssue(message));
        }
        this->Send(ReplyTo, new TEvents::TEvCompleted(0, status), 0, Cookie);
        PassAway();
    }

    void ReplySuccess() override {
        this->Send(ReplyTo, new TEvents::TEvCompleted(0, Ydb::StatusIds::SUCCESS), 0, Cookie);
        PassAway();
    }

private:
    const TActorId ReplyTo;
    std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> NavigateResult;
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::shared_ptr<NYql::TIssues> Issues;
    const ui32 Cookie;
};

TActorId DoLongTxWriteSameMailbox(const TActorContext& ctx, const TActorId& replyTo, const NLongTxService::TLongTxId& longTxId,
    const TString& dedupId, const TString& databaseName, const std::vector<TString>& tables, const std::vector<ui64> &numrows,
    std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult, std::shared_ptr<arrow::RecordBatch> batch,
    std::shared_ptr<NYql::TIssues> issues, const bool noTxWrite, const ui32 cookie) {
    return ctx.RegisterWithSameMailbox(
        new TLongTxWriteInternal(replyTo, longTxId, dedupId, databaseName, tables, numrows, navigateResult, batch, issues, noTxWrite, cookie));
}

//

}   // namespace NTxProxy
}   // namespace NKikimr
