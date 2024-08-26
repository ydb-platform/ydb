#include "dq_yt_block_reader.h"
#include "stream_decoder.h"
#include "dq_yt_rpc_helpers.h"
#include "arrow_converter.h"

#include <ydb/library/yql/public/udf/arrow/block_builder.h>

#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/threading/thread.h>
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
#include <arrow/chunked_array.h>
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

TStatKey FallbackCount("YtBlockReader_Fallbacks", true);
TStatKey BlockCount("YtBlockReader_Blocks", true);

namespace {
struct TResultBatch {
    using TPtr = std::shared_ptr<TResultBatch>;
    size_t RowsCnt;
    std::vector<arrow::Datum> Columns;
    TResultBatch(int64_t cnt) : RowsCnt(cnt) {}
    TResultBatch(int64_t cnt, decltype(Columns)&& columns) : RowsCnt(cnt), Columns(std::move(columns)) {}
};

template<typename T>
class TBlockingQueueWithLimit {
    struct Poison {
        TString Error;
    };
    struct TFallbackNotify {};
    using TPoisonOr = std::variant<T, Poison, TFallbackNotify>;
public:
    TBlockingQueueWithLimit(size_t limit) : Limit_(limit) {}
    void Push(T&& val, bool imm) {
        PushInternal(std::move(val), imm);
    }

    void PushPoison(const TString& err) {
        PushInternal(Poison{err}, true);
    }

    void PushNotify() {
        PushInternal(TFallbackNotify{}, true);
    }

    TMaybe<T> Get() {
        auto res = GetInternal();
        if (std::holds_alternative<Poison>(res)) {
            throw std::runtime_error(std::get<Poison>(res).Error);
        }
        if (std::holds_alternative<TFallbackNotify>(res)) {
            return {};
        }
        return std::move(std::get<T>(res));
    }
private:
    template<typename X>
    void PushInternal(X&& val, bool imm) {
        for (;;) {
            NYT::TPromise<void> promise;
            {
                std::lock_guard guard(Mtx_);
                if (!Awaiting_.empty()) {
                    Awaiting_.front().Set(std::move(val));
                    Awaiting_.pop();
                    return;
                }
                if (!imm && Ready_.size() >= Limit_) {
                    promise = NYT::NewPromise<void>();
                    BlockedPushes_.push(promise);
                } else {
                    Ready_.emplace(std::move(val));
                    return;
                }
            }
            YQL_ENSURE(NYT::NConcurrency::WaitFor(promise.ToFuture()).IsOK());
        }
    }

    TPoisonOr GetInternal() {
        NYT::TPromise<TPoisonOr> awaiter;
        {
            std::lock_guard guard(Mtx_);
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
    TListener(size_t initLatch, size_t inflight)
        : Latch_(initLatch)
        , Queue_(inflight) {}

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
    void HandleResult(TResultBatch::TPtr&& res, bool immediatly = false) {
        Queue_.Push(std::move(res), immediatly);
    }

    TMaybe<TResultBatch::TPtr> Get() {
        return Queue_.Get();
    }

    void HandleError(const TString& msg) {
        Queue_.PushPoison(msg);
    }

    void HandleFallback(TResultBatch::TPtr&& block) {
        HandleResult(std::move(block), true);
    }

    void NotifyFallback() {
        Queue_.PushNotify();
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
    TLocalListener(std::shared_ptr<TListener> consumer
        , std::unordered_map<std::string, ui32>& columnOrderMapping
        , std::shared_ptr<std::vector<TType*>> columnTypes
        , std::shared_ptr<std::vector<std::shared_ptr<arrow::DataType>>> arrowTypes
        , arrow::MemoryPool& pool, const NUdf::IPgBuilder* pgBuilder
        , bool isNative, NKikimr::NMiniKQL::IStatsRegistry* jobStats) 
        : Consumer_(consumer)
        , ColumnTypes_(columnTypes)
        , JobStats_(jobStats)
        , ColumnOrderMapping(columnOrderMapping)
    {
        ColumnConverters_.reserve(columnTypes->size());
        for (size_t i = 0; i < columnTypes->size(); ++i) {
            ColumnConverters_.emplace_back(MakeYtColumnConverter(columnTypes->at(i), pgBuilder, pool, isNative));
        }
    }

    void Init(std::shared_ptr<TLocalListener> self) {
        Self_ = self;
        Decoder_ = std::make_shared<arrow::ipc::NDqs::StreamDecoder2>(self, arrow::ipc::IpcReadOptions{.use_threads=false});
    }

    arrow::Status OnEOS() override {
        Decoder_->Reset();
        return arrow::Status::OK();
    }

    arrow::Status OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> batch) override {
        NKikimr::NMiniKQL::TScopedAlloc scope(__LOCATION__);
        TThrowingBindTerminator t;
        YQL_ENSURE(batch);
        MKQL_ADD_STAT(JobStats_, BlockCount, 1);
        std::vector<arrow::Datum> result;
        YQL_ENSURE((size_t)batch->num_columns() == ColumnConverters_.size());
        result.resize(ColumnConverters_.size());
        size_t matchedColumns = 0;
        for (size_t i = 0; i < ColumnConverters_.size(); ++i) {
            auto columnIdxIt = ColumnOrderMapping.find(batch->schema()->field_names()[i]);
            if (ColumnOrderMapping.end() == columnIdxIt) {
                continue;
            }
            ++matchedColumns;
            auto columnIdx =  columnIdxIt->second;
            result[columnIdx] = std::move(ColumnConverters_[columnIdx]->Convert(batch->column(i)->data()));
        }
        Y_ENSURE(matchedColumns == ColumnOrderMapping.size());
        Consumer_->HandleResult(std::make_shared<TResultBatch>(batch->num_rows(), std::move(result)));
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
    std::shared_ptr<std::vector<TType*>> ColumnTypes_;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_;
    std::vector<std::unique_ptr<IYtColumnConverter>> ColumnConverters_;
    std::unordered_map<std::string, ui32>& ColumnOrderMapping;
};

class TSource : public TNonCopyable {
public:
    using TPtr = std::shared_ptr<TSource>;
    TSource(std::unique_ptr<TSettingsHolder>&& settings,
        size_t inflight, TType* type, std::shared_ptr<std::vector<std::shared_ptr<arrow::DataType>>> types, const THolderFactory& holderFactory, NKikimr::NMiniKQL::IStatsRegistry* jobStats)
        : HolderFactory(holderFactory)
        , Settings_(std::move(settings))
        , Inputs_(Settings_->Requests.size())
        , Listener_(std::make_shared<TListener>(Inputs_.size(), inflight))
        , JobStats_(jobStats)
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
            auto& decoder = Settings_->Specs->Inputs[Settings_->OriginalIndexes[i]];
            bool native = decoder->NativeYtTypeFlags && !decoder->FieldsVec[i].ExplicitYson;
            LocalListeners_.emplace_back(std::make_shared<TLocalListener>(Listener_, Settings_->ColumnNameMapping, ptr, types, *Settings_->Pool, Settings_->PgBuilder, native, jobStats));
            LocalListeners_.back()->Init(LocalListeners_.back());
        }
        BlockBuilder_.Init(ptr, *Settings_->Pool, Settings_->PgBuilder);
        FallbackReader_.SetSpecs(*Settings_->Specs, HolderFactory);
    }

    void RunRead() {
        size_t inputIdx;
        {
            std::lock_guard guard(Mtx_);
            if (InputsQueue_.empty()) {
                if (NextFreeInputIdx >= Inputs_.size()) {
                    return;
                }
                inputIdx = NextFreeInputIdx++;
            } else {
                inputIdx = InputsQueue_.front();
                InputsQueue_.pop();
            }
        }
        if (!Inputs_[inputIdx]) {
            CreateInputStream(Settings_->Requests[inputIdx]).SubscribeUnique(BIND([self = Self_, inputIdx] (NYT::TErrorOr<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>&& stream) {
                self->Pool_->GetInvoker()->Invoke(BIND([inputIdx, self, stream = std::move(stream)]() mutable {
                    try {
                        self->Inputs_[inputIdx] = std::move(stream.ValueOrThrow());
                        self->InputDone(inputIdx);
                        self->RunRead();
                    } catch (...) {
                        self->Listener_->HandleError(CurrentExceptionMessage());
                    }
                }));
            }));
            return;
        }
        Inputs_[inputIdx]->Read().SubscribeUnique(BIND([inputIdx = inputIdx, self = Self_](NYT::TErrorOr<NYT::TSharedRef>&& res) {
            self->Pool_->GetInvoker()->Invoke(BIND([inputIdx, self, res = std::move(res)]() mutable {
                try {
                    self->Accept(inputIdx, std::move(res));
                    self->RunRead();
                } catch (...) {
                    self->Listener_->HandleError(CurrentExceptionMessage());
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
            if (currentPayload.Size()) {
                std::lock_guard guard(FallbackMtx_);
                Fallbacks_.push({inputIdx, currentPayload});
            } else {
                InputDone(inputIdx);
            }
            Listener_->NotifyFallback();
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
        std::lock_guard guard(Mtx_);
        InputsQueue_.emplace(input);
    }

    TResultBatch::TPtr Next() {
        for(;;) {
            size_t inputIdx = 0;
            NYT::TSharedRef payload;
            {
                std::lock_guard guard(FallbackMtx_);
                if (Fallbacks_.size()) {
                    inputIdx = Fallbacks_.front().first;
                    payload = Fallbacks_.front().second;
                    Fallbacks_.pop();
                }
            }
            if (payload) {
                for (auto &e: FallbackHandler(inputIdx, payload)) {
                    Listener_->HandleFallback(std::move(e));
                }
                InputDone(inputIdx);
                RunRead();
            }
            auto result = Listener_->Get();
            if (!result) { // Falled back
                continue;
            }
            return *result;
        }
    }

    std::vector<TResultBatch::TPtr> FallbackHandler(size_t idx, NYT::TSharedRef payload) {
        if (!payload.Size()) {
            return {};
        }
        auto currentReader_ = std::make_shared<TPayloadRPCReader>(std::move(payload));
        MKQL_ADD_STAT(JobStats_, FallbackCount, 1);
        // TODO(): save and recover row indexes
        FallbackReader_.SetReader(*currentReader_, 1, 4_MB, ui32(Settings_->OriginalIndexes[idx]), true);
        // If we don't save the reader, after exiting FallbackHandler it will be destroyed,
        // but FallbackReader points on it yet.
        Reader_ = currentReader_;
        FallbackReader_.Next();
        while (FallbackReader_.IsValid()) {
            auto currentRow = std::move(FallbackReader_.GetRow());
            if (!Settings_->Specs->InputGroups.empty()) {
                currentRow = std::move(HolderFactory.CreateVariantHolder(currentRow.Release(), Settings_->Specs->InputGroups.at(Settings_->OriginalIndexes[idx])));
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
        Self_ = self;
        Pool_ = NYT::NConcurrency::CreateThreadPool(Inflight_, "block_reader");
        // Run Inflight_ reads at the same time
        for (size_t i = 0; i < Inflight_; ++i) {
            RunRead();
        }
    }

    const THolderFactory& HolderFactory;
private:
    NYT::NConcurrency::IThreadPoolPtr Pool_;
    std::mutex Mtx_;
    std::mutex FallbackMtx_;
    std::queue<std::pair<size_t, NYT::TSharedRef>> Fallbacks_;
    std::queue<TResultBatch::TPtr> FallbackBlocks_;
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
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_;
    size_t NextFreeInputIdx = 0;
};

class TReaderState: public TComputationValue<TReaderState> {
    using TBase = TComputationValue<TReaderState>;
public:
    TReaderState(TMemoryUsageInfo* memInfo, TSource::TPtr source, size_t width, std::shared_ptr<std::vector<std::shared_ptr<arrow::DataType>>> arrowTypes)
        : TBase(memInfo)
        , Source_(std::move(source))
        , Width_(width)
        , Types_(arrowTypes)
        , Result_(width)
    {
    }
    
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) {
        if (GotFinish_) {
            return NUdf::EFetchStatus::Finish;
        }
        YQL_ENSURE(width == Width_ + 1);
        try {
            auto batch = Source_->Next();
            if (!batch) {
                GotFinish_ = 1;
                Source_->Finish();
                return NUdf::EFetchStatus::Finish;
            }
            
            for (size_t i = 0; i < Width_; ++i) {
                YQL_ENSURE(batch->Columns[i].type()->Equals(Types_->at(i)));
                output[i] = Source_->HolderFactory.CreateArrowBlock(std::move(batch->Columns[i]));
            }
            output[Width_] = Source_->HolderFactory.CreateArrowBlock(arrow::Datum(ui64(batch->RowsCnt)));
        } catch (...) {
            Cerr << "YT RPC Reader exception:\n";
            throw;
        }
        return NUdf::EFetchStatus::Ok;
    }

private:
    TSource::TPtr Source_;
    const size_t Width_;
    std::shared_ptr<std::vector<std::shared_ptr<arrow::DataType>>> Types_;
    std::vector<NUdf::TUnboxedValue*> Result_;
    bool GotFinish_ = 0;
};
};

class TDqYtReadBlockWrapper : public TMutableComputationNode<TDqYtReadBlockWrapper> {
using TBaseComputation =  TMutableComputationNode<TDqYtReadBlockWrapper>;
public:

    TDqYtReadBlockWrapper(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight, size_t timeout, const TVector<ui64>& tableOffsets)
        : TBaseComputation(ctx.Mutables, EValueRepresentation::Boxed)
        , Width_(AS_TYPE(TStructType, itemType)->GetMembersCount())
        , CodecCtx_(ctx.Env, ctx.FunctionRegistry, &ctx.HolderFactory)
        , ClusterName_(clusterName)
        , Token_(token)
        , SamplingSpec_(samplingSpec)
        , Tables_(std::move(tables))
        , Inflight_(inflight)
        , Timeout_(timeout)
        , Type_(itemType)
        , JobStats_(jobStats)
    {
        // TODO() Enable range indexes + row indexes
        Specs_.SetUseSkiff("", 0);
        Specs_.Init(CodecCtx_, inputSpec, inputGroups, tableNames, itemType, {}, {}, jobStats);
        Specs_.SetTableOffsets(tableOffsets);
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
    }

     NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto settings = CreateInputStreams(true, Token_, ClusterName_, Timeout_, Inflight_ > 1, Tables_, SamplingSpec_);
        settings->Specs = &Specs_;
        settings->Pool = arrow::default_memory_pool();
        settings->PgBuilder = &ctx.Builder->GetPgBuilder();
        auto types = std::make_shared<std::vector<std::shared_ptr<arrow::DataType>>>(Width_);
        TVector<TString> columnNames;
        for (size_t i = 0; i < Width_; ++i) {
            columnNames.emplace_back(AS_TYPE(TStructType, Type_)->GetMemberName(i));
            YQL_ENSURE(ConvertArrowType(AS_TYPE(TStructType, Type_)->GetMemberType(i), types->at(i)), "Can't convert type to arrow");
        }
        settings->SetColumns(columnNames);
        auto source = std::make_shared<TSource>(std::move(settings), Inflight_, Type_, types, ctx.HolderFactory, JobStats_);
        source->SetSelfAndRun(source);
        return ctx.HolderFactory.Create<TReaderState>(source, Width_, types);
    }

    void RegisterDependencies() const final {}
private:
    const ui32 Width_;
    NCommon::TCodecContext CodecCtx_;
    TMkqlIOSpecs Specs_;

    TString ClusterName_;
    TString Token_;
    NYT::TNode SamplingSpec_;
    TVector<std::pair<NYT::TRichYPath, NYT::TFormat>> Tables_;
    size_t Inflight_;
    size_t Timeout_;
    TType* Type_;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_;
};

IComputationNode* CreateDqYtReadBlockWrapper(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight, size_t timeout, const TVector<ui64>& tableOffsets) 
{
    return new TDqYtReadBlockWrapper(ctx, clusterName, token, inputSpec, samplingSpec, inputGroups, itemType,
                                                tableNames, std::move(tables), jobStats, inflight, timeout, tableOffsets);
}
}
