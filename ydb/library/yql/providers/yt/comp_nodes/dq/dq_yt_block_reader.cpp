#include "dq_yt_block_reader.h"
#include "stream_decoder.h"
#include "dq_yt_rpc_helpers.h"

#include <ydb/library/yql/public/udf/arrow/block_builder.h>

#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <arrow/compute/cast.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/result.h>
#include <arrow/buffer.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/size_literals.h>
#include <util/stream/output.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

namespace {
struct TResultBatch {
    using TPtr = std::shared_ptr<TResultBatch>;
    size_t RowsCnt;
    std::vector<arrow::Datum> Columns;
    TResultBatch(int64_t cnt, decltype(Columns)&& columns) : RowsCnt(cnt), Columns(std::move(columns)) {}
    TResultBatch(std::shared_ptr<arrow::RecordBatch> batch) : RowsCnt(batch->num_rows()), Columns(batch->columns().begin(), batch->columns().end()) {}
};

template<typename T>
class TBlockingQueueWithLimit {
    struct Poison {
        TString Error;
    };
    using TPoisonOr = std::variant<T, Poison>;
public:
    TBlockingQueueWithLimit(size_t limit) : Limit_(limit) {}
    void Push(T&& val) {
        PushInternal(std::move(val));
    }

    void PushPoison(const TString& err) {
        PushInternal(Poison{err});
    }

    T Get() {
        auto res = GetInternal();
        if (std::holds_alternative<Poison>(res)) {
            throw std::runtime_error(std::get<Poison>(res).Error);
        }
        return std::move(std::get<T>(res));
    }
private:
    template<typename X>
    void PushInternal(X&& val) {
        NYT::TPromise<void> promise;
        {
            std::lock_guard _(Mtx_);
            if (!Awaiting_.empty()) {
                Awaiting_.front().Set(std::move(val));
                Awaiting_.pop();
                return;
            }
            if (Ready_.size() >= Limit_) {
                promise = NYT::NewPromise<void>();
                BlockedPushes_.push(promise);
            } else {
                Ready_.emplace(std::move(val));
                return;
            }
        }
        YQL_ENSURE(NYT::NConcurrency::WaitFor(promise.ToFuture()).IsOK());
        std::lock_guard _(Mtx_);
        Ready_.emplace(std::move(val));
    }

    TPoisonOr GetInternal() {
        NYT::TPromise<TPoisonOr> awaiter;
        {
            std::lock_guard _(Mtx_);
            if (!BlockedPushes_.empty()) {
                BlockedPushes_.front().Set();
                BlockedPushes_.pop();
            }
            if (!Ready_.empty()) {
                auto res = std::move(Ready_.front());
                Ready_.pop();
                return res;
            }
            awaiter = NYT::NewPromise<TPoisonOr>();
            Awaiting_.push(awaiter);
        }
        auto awaitResult = NYT::NConcurrency::WaitFor(awaiter.ToFuture());
        if (!awaitResult.IsOK()) {
            throw std::runtime_error(awaitResult.GetMessage());
        }
        return std::move(awaitResult.Value());
    }
    std::mutex Mtx_;
    std::queue<TPoisonOr> Ready_;
    std::queue<NYT::TPromise<void>> BlockedPushes_;
    std::queue<NYT::TPromise<TPoisonOr>> Awaiting_;
    size_t Limit_;
};

class TListener {
    using TBatchPtr = std::shared_ptr<arrow::RecordBatch>;
public:
    using TPromise = NYT::TPromise<TResultBatch>;
    using TPtr = std::shared_ptr<TListener>;
    TListener(size_t initLatch, size_t inflight) : Latch_(initLatch), Queue_(inflight) {}

    void OnEOF() {
        bool excepted = 0;
        if (GotEOF_.compare_exchange_strong(excepted, 1)) {
            // block poining to nullptr is marker of EOF
            HandleResult(nullptr);
        } else {
            // can't get EOF more than one time
            HandleError("EOS already got");
        }
    }

    // Handles result
    void HandleResult(TResultBatch::TPtr&& res) {
        Queue_.Push(std::move(res));
    }

    void OnRecordBatchDecoded(TBatchPtr record_batch) {
        YQL_ENSURE(record_batch);
        // decode dictionary
        record_batch = NKikimr::NArrow::DictionaryToArray(record_batch);
        // and handle result
        HandleResult(std::make_shared<TResultBatch>(record_batch));
    }

    TResultBatch::TPtr Get() {
        return Queue_.Get();
    }

    void HandleError(const TString& msg) {
        Queue_.PushPoison(msg);
    }

    void HandleFallback(TResultBatch::TPtr&& block) {
        HandleResult(std::move(block));
    }

    void InputDone() {
        // EOF comes when all inputs got EOS
        if (!--Latch_) {
            OnEOF();
        }
    }
private:
    std::atomic<size_t> Latch_;
    std::atomic<bool> GotEOF_;
    TBlockingQueueWithLimit<TResultBatch::TPtr> Queue_;
};

class TBlockBuilder {
public:
    void Init(std::shared_ptr<std::vector<TType*>> columnTypes, arrow::MemoryPool& pool, const NUdf::IPgBuilder* pgBuilder) {
        ColumnTypes_ = columnTypes;
        ColumnBuilders_.reserve(ColumnTypes_->size());
        size_t maxBlockItemSize = 0;
        for (auto& type: *ColumnTypes_) {
            maxBlockItemSize = std::max(maxBlockItemSize, CalcMaxBlockItemSize(type));
        }
        size_t maxBlockLen = CalcBlockLen(maxBlockItemSize);
        for (size_t i = 0; i < ColumnTypes_->size(); ++i) {
            ColumnBuilders_.push_back(
                std::move(NUdf::MakeArrayBuilder(
                    TTypeInfoHelper(), ColumnTypes_->at(i),
                    pool,
                    maxBlockLen,
                    pgBuilder
                ))
            );
        }
    }

    void Add(const NUdf::TUnboxedValue& val) {
        for (ui32 i = 0; i < ColumnBuilders_.size(); ++i) {
            auto v = val.GetElement(i);
            ColumnBuilders_[i]->Add(v);
        }
        ++RowsCnt_;
    }

    std::vector<TResultBatch::TPtr> Build() {
        std::vector<arrow::Datum> columns;
        columns.reserve(ColumnBuilders_.size());
        for (size_t i = 0; i < ColumnBuilders_.size(); ++i) {
            columns.emplace_back(std::move(ColumnBuilders_[i]->Build(false)));
        }
        std::vector<std::shared_ptr<TResultBatch>> blocks;
        int64_t offset = 0;
        std::vector<int64_t> currentChunk(columns.size()), inChunkOffset(columns.size());
        while (RowsCnt_) {
            int64_t max_curr_len = RowsCnt_;
            for (size_t i = 0; i < columns.size(); ++i) {
                if (arrow::Datum::Kind::CHUNKED_ARRAY == columns[i].kind()) {
                    auto& c_arr = columns[i].chunked_array();
                    while (currentChunk[i] < c_arr->num_chunks() && !c_arr->chunk(currentChunk[i])) {
                        ++currentChunk[i];
                    }
                    YQL_ENSURE(currentChunk[i] < c_arr->num_chunks());
                    max_curr_len = std::min(max_curr_len, c_arr->chunk(currentChunk[i])->length() - inChunkOffset[i]);
                }
            }
            RowsCnt_ -= max_curr_len;
            decltype(columns) result_columns;
            result_columns.reserve(columns.size());
            offset += max_curr_len;
            for (size_t i = 0; i < columns.size(); ++i) {
                auto& e = columns[i];
                if (arrow::Datum::Kind::CHUNKED_ARRAY == e.kind()) {
                    result_columns.emplace_back(e.chunked_array()->chunk(currentChunk[i])->Slice(inChunkOffset[i], max_curr_len));
                    if (max_curr_len + inChunkOffset[i] == e.chunked_array()->chunk(currentChunk[i])->length()) {
                        ++currentChunk[i];
                        inChunkOffset[i] = 0;
                    } else {
                        inChunkOffset[i] += max_curr_len;
                    }
                } else {
                    result_columns.emplace_back(e.array()->Slice(offset - max_curr_len, max_curr_len));
                }
            }
            blocks.emplace_back(std::make_shared<TResultBatch>(max_curr_len, std::move(result_columns)));
        }
        return blocks;
    }

private:
    int64_t RowsCnt_ = 0;
    std::vector<std::unique_ptr<NUdf::IArrayBuilder>> ColumnBuilders_;
    std::shared_ptr<std::vector<TType*>> ColumnTypes_;
};

class TLocalListener : public arrow::ipc::Listener {
public:
    TLocalListener(std::shared_ptr<TListener> consumer) 
        : Consumer_(consumer) {}

    void Init(std::shared_ptr<TLocalListener> self) {
        Self_ = self;
        Decoder_ = std::make_shared<arrow::ipc::NDqs::StreamDecoder2>(self, arrow::ipc::IpcReadOptions{.use_threads=false});
    }

    arrow::Status OnEOS() override {
        Decoder_->Reset();
        return arrow::Status::OK();
    }

    arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) override {
        Consumer_->OnRecordBatchDecoded(batch);
        return arrow::Status::OK();
    }

    void Consume(std::shared_ptr<arrow::Buffer> buff) {
        ARROW_OK(Decoder_->Consume(buff));
    }

    void Finish() {
        Self_ = nullptr;
    }
private:
    std::shared_ptr<TLocalListener> Self_;
    std::shared_ptr<TListener> Consumer_;
    std::shared_ptr<arrow::ipc::NDqs::StreamDecoder2> Decoder_;
};

class TSource : public TNonCopyable {
public:
    using TPtr = std::shared_ptr<TSource>;
    TSource(std::unique_ptr<TSettingsHolder>&& settings,
        size_t inflight, TType* type, const THolderFactory& holderFactory)
        : Settings_(std::move(settings))
        , Inputs_(std::move(Settings_->RawInputs))
        , Listener_(std::make_shared<TListener>(Inputs_.size(), inflight))
        , HolderFactory_(holderFactory)
    {
        auto structType = AS_TYPE(TStructType, type);
        std::vector<TType*> columnTypes_(structType->GetMembersCount());
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            columnTypes_[i] = structType->GetMemberType(i);
        }
        auto ptr = std::make_shared<decltype(columnTypes_)>(std::move(columnTypes_));
        Inflight_ = std::min(inflight, Inputs_.size());

        LocalListeners_.reserve(Inputs_.size());
        for (size_t i = 0; i < Inputs_.size(); ++i) {
            InputsQueue_.emplace(i);
            LocalListeners_.emplace_back(std::make_shared<TLocalListener>(Listener_));
            LocalListeners_.back()->Init(LocalListeners_.back());
        }
        BlockBuilder_.Init(ptr, *Settings_->Pool, Settings_->PgBuilder);
        FallbackReader_.SetSpecs(*Settings_->Specs, HolderFactory_);
    }

    void RunRead() {
        size_t inputIdx;
        {
            std::lock_guard _(Mtx_);
            if (InputsQueue_.empty()) {
                return;
            }
            inputIdx = InputsQueue_.front();
            InputsQueue_.pop();
        }
        Inputs_[inputIdx]->Read().SubscribeUnique(BIND([inputIdx = inputIdx, self = Self_](NYT::TErrorOr<NYT::TSharedRef>&& res) {
            self->Pool_->GetInvoker()->Invoke(BIND([inputIdx, self, res = std::move(res)] () mutable {
                try {
                    self->Accept(inputIdx, std::move(res));
                    self->RunRead();
                } catch (std::exception& e) {
                    self->Listener_->HandleError(e.what());
                }
            }));
        }));
    }

    void Accept(size_t inputIdx, NYT::TErrorOr<NYT::TSharedRef>&& res) {
        if (res.IsOK() && !res.Value()) {
            // End Of Stream
            Listener_->InputDone();
            return;
        }

        if (!res.IsOK()) {
            // Propagate error
            Listener_->HandleError(res.GetMessage());
            return;
        }

        NYT::NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
        NYT::NApi::NRpcProxy::NProto::TRowsetStatistics statistics;
        NYT::TSharedRef currentPayload = NYT::NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(res.Value(), &descriptor, &statistics);
        if (descriptor.rowset_format() != NYT::NApi::NRpcProxy::NProto::RF_ARROW) {
            auto promise = NYT::NewPromise<std::vector<TResultBatch::TPtr>>();
            MainInvoker_->Invoke(BIND([inputIdx, currentPayload, self = Self_, promise] {
                try {
                    promise.Set(self->FallbackHandler(inputIdx, currentPayload));
                } catch (std::exception& e) {
                    promise.Set(NYT::TError(e.what()));
                }
            }));
            auto result = NYT::NConcurrency::WaitFor(promise.ToFuture());
            if (!result.IsOK()) {
                Listener_->HandleError(result.GetMessage());
                return;
            }
            for (auto& e: result.Value()) {
                Listener_->HandleFallback(std::move(e));
            }
            InputDone(inputIdx);
            return;
        }

        if (!currentPayload.Size()) {
            // EOS
            Listener_->InputDone();
            return;
        }
        // TODO(): support row and range indexes
        auto payload = TMemoryInput(currentPayload.Begin(), currentPayload.Size());
        arrow::BufferBuilder bb;
        ARROW_OK(bb.Reserve(currentPayload.Size()));
        ARROW_OK(bb.Append((const uint8_t*)payload.Buf(), currentPayload.Size()));
        LocalListeners_[inputIdx]->Consume(*bb.Finish());
        InputDone(inputIdx);
    }

    // Return input back to queue
    void InputDone(auto input) {
        std::lock_guard _(Mtx_);
        InputsQueue_.emplace(input);
    }

    TResultBatch::TPtr Next() {
        auto result = Listener_->Get();
        return result;
    }

    std::vector<TResultBatch::TPtr> FallbackHandler(size_t idx, NYT::TSharedRef payload) {
        if (!payload.Size()) {
            return {};
        }
        // We're have only one mkql reader, protect it if 2 fallbacks happen at the same time
        std::lock_guard _(FallbackMtx_);
        auto currentReader_ = std::make_shared<TPayloadRPCReader>(std::move(payload));

        // TODO(): save and recover row indexes
        FallbackReader_.SetReader(*currentReader_, 1, 4_MB, ui32(Settings_->OriginalIndexes[idx]), true);
        // If we don't save the reader, after exiting FallbackHandler it will be destroyed,
        // but FallbackReader points on it yet.
        Reader_ = currentReader_;
        FallbackReader_.Next();
        while (FallbackReader_.IsValid()) {
            auto currentRow = std::move(FallbackReader_.GetRow());
            if (!Settings_->Specs->InputGroups.empty()) {
                currentRow = std::move(HolderFactory_.CreateVariantHolder(currentRow.Release(), Settings_->Specs->InputGroups.at(Settings_->OriginalIndexes[idx])));
            }
            BlockBuilder_.Add(currentRow);
            FallbackReader_.Next();
        }
        return BlockBuilder_.Build();
    }

    void Finish() {
        FallbackReader_.Finish();
        Pool_->Shutdown();
        for (auto& e: LocalListeners_) {
            e->Finish();
        }
        Self_ = nullptr;
    }

    void SetSelfAndRun(TPtr self) {
        MainInvoker_ = NYT::GetCurrentInvoker();
        Self_ = self;
        Pool_ = NYT::NConcurrency::CreateThreadPool(Inflight_, "rpc_reader_inflight");
        // Run Inflight_ reads at the same time
        for (size_t i = 0; i < Inflight_; ++i) {
            RunRead();
        }
    }

private:
    NYT::IInvoker* MainInvoker_;
    NYT::NConcurrency::IThreadPoolPtr Pool_;
    std::mutex Mtx_;
    std::mutex FallbackMtx_;
    std::unique_ptr<TSettingsHolder> Settings_;
    std::vector<std::shared_ptr<TLocalListener>> LocalListeners_;
    std::vector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> Inputs_;
    TMkqlReaderImpl FallbackReader_;
    TBlockBuilder BlockBuilder_;
    std::shared_ptr<TPayloadRPCReader> Reader_;
    std::queue<size_t> InputsQueue_;
    TListener::TPtr Listener_;
    TPtr Self_;
    size_t Inflight_;
    const THolderFactory& HolderFactory_;
};

class TState: public TComputationValue<TState> {
    using TBase = TComputationValue<TState>;
public:
    TState(TMemoryUsageInfo* memInfo, TSource::TPtr source, size_t width, TType* type)
        : TBase(memInfo)
        , Source_(std::move(source))
        , Width_(width)
        , Type_(type)
        , Types_(width)
    {
        for (size_t i = 0; i < Width_; ++i) {
            Types_[i] = NArrow::GetArrowType(AS_TYPE(TStructType, Type_)->GetMemberType(i));
        }
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        auto batch = Source_->Next();
        if (!batch) {
            Source_->Finish();
            return EFetchResult::Finish;
        }
        for (size_t i = 0; i < Width_; ++i) {
            if (!output[i]) {
                continue;
            }
            if(!batch->Columns[i].type()->Equals(Types_[i])) {
                *(output[i]) = ctx.HolderFactory.CreateArrowBlock(ARROW_RESULT(arrow::compute::Cast(batch->Columns[i], Types_[i])));
                continue;
            }
            *(output[i]) = ctx.HolderFactory.CreateArrowBlock(std::move(batch->Columns[i]));
        }
        if (output[Width_]) {
            *(output[Width_]) = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(ui64(batch->RowsCnt)));
        }
        return EFetchResult::One;
    }

private:
    TSource::TPtr Source_;
    const size_t Width_;
    TType* Type_;
    std::vector<std::shared_ptr<arrow::DataType>> Types_;
};
};

class TDqYtReadBlockWrapper : public TStatefulWideFlowComputationNode<TDqYtReadBlockWrapper> {
using TBaseComputation =  TStatefulWideFlowComputationNode<TDqYtReadBlockWrapper>;
public:
    
    TDqYtReadBlockWrapper(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables, NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight,
        size_t timeout) : TBaseComputation(ctx.Mutables, this, EValueRepresentation::Boxed)
        , Width(AS_TYPE(TStructType, itemType)->GetMembersCount())
        , CodecCtx(ctx.Env, ctx.FunctionRegistry, &ctx.HolderFactory)
        , ClusterName(clusterName)
        , Token(token)
        , SamplingSpec(samplingSpec)
        , Tables(std::move(tables))
        , Inflight(inflight)
        , Timeout(timeout)
        , Type(itemType)
    {
        // TODO() Enable range indexes
        Specs.SetUseSkiff("", TMkqlIOSpecs::ESystemField::RowIndex);
        Specs.Init(CodecCtx, inputSpec, inputGroups, tableNames, itemType, {}, {}, jobStats);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        auto settings = CreateInputStreams(true, Token, ClusterName, Timeout, Inflight > 1, Tables, SamplingSpec);
        settings->Specs = &Specs;
        settings->Pool = arrow::default_memory_pool();
        settings->PgBuilder = &ctx.Builder->GetPgBuilder();
        auto source = std::make_shared<TSource>(std::move(settings), Inflight, Type, ctx.HolderFactory);
        source->SetSelfAndRun(source);
        state = ctx.HolderFactory.Create<TState>(source, Width, Type);
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }
        return static_cast<TState&>(*state.AsBoxed()).FetchValues(ctx, output);
    }

    void RegisterDependencies() const final {}

    const ui32 Width;
    NCommon::TCodecContext CodecCtx;
    TMkqlIOSpecs Specs;

    TString ClusterName;
    TString Token;
    NYT::TNode SamplingSpec;
    TVector<std::pair<NYT::TRichYPath, NYT::TFormat>> Tables;
    size_t Inflight;
    size_t Timeout;
    TType* Type;
};

IComputationNode* CreateDqYtReadBlockWrapper(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables, NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight,
        size_t timeout) 
{
    return new TDqYtReadBlockWrapper(ctx, clusterName, token, inputSpec, samplingSpec, inputGroups, itemType, tableNames, std::move(tables), jobStats, inflight, timeout);
}
}