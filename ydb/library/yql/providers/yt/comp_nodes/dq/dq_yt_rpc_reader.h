#pragma once

#include "dq_yt_reader_impl.h"

#include <yt/yt/core/actions/future.h>
#include <mutex>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

struct TSettingsHolder;

class TParallelFileInputState: public IInputState {
public:
    struct TResult {
        size_t Input_ = 0;
        NYT::TSharedRef Value_;
    };
    struct TReaderState {
        TMaybe<ui64> CurrentRow;
        TMaybe<ui32> CurrentRange;
    };
    // Used to pass struct in lambdas. std::shared_ptr copying is thread-safe
    struct TInnerState {
        TInnerState(size_t inputsCount, size_t inflight) : RawInputs(inputsCount), Inflight(inflight) {};
        void InputReady(size_t idx);
        void InputDone();
        TMaybe<size_t> GetFreeReader();
        bool AllReadersDone();

        std::mutex Lock;
        std::queue<TResult> Results;
        NYT::TError Error{};

        NYT::TPromise<void> WaitPromise = NYT::NewPromise<void>();
        TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> RawInputs;
        std::queue<size_t> ReadyReaders;
        size_t NextFreeReaderIdx = 0;
        size_t ReadersInflight = 0;
        size_t Inflight;
    };
    TParallelFileInputState(const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        size_t blockSize,
        size_t inflight,
        std::unique_ptr<TSettingsHolder>&& settings);

    size_t GetTableIndex() const;

    size_t GetRecordIndex() const;

    void SetNextBlockCallback(std::function<void()> cb);
protected:
    virtual bool IsValid() const override;

    virtual NUdf::TUnboxedValue GetCurrent() override;

    virtual void Next() override;

    void Finish();

    void RunNext();

    bool NextValue();

private:
    void CheckError() const;
    std::shared_ptr<TInnerState> InnerState_;
    std::vector<TReaderState> StateByReader_;
    NYT::TRawTableReaderPtr CurrentReader_ = nullptr;
    bool Eof_ = false;
    const TMkqlIOSpecs* Spec_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    const size_t BlockSize_;
    TMkqlReaderImpl MkqlReader_;
    size_t CurrentInput_ = 0;
    size_t CurrentRecord_ = 1;
    bool Valid_ = true;
    NUdf::TUnboxedValue CurrentValue_;
    std::function<void()> OnNextBlockCallback_;
    NKikimr::NMiniKQL::TSamplingStatTimer TimerAwaiting_;
    TVector<size_t> OriginalIndexes_;
    std::unique_ptr<TSettingsHolder> Settings_;
};


class TDqYtReadWrapperRPC : public TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState> {
public:
using TInputType = NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr;
    TDqYtReadWrapperRPC(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups, TType* itemType, const TVector<TString>& tableNames,
        TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight, size_t timeout,
        const TVector<ui64>& tableOffsets)
            : TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>(ctx, clusterName, token,
                inputSpec, samplingSpec, inputGroups, itemType, tableNames, std::move(tables), jobStats,
                inflight, timeout, tableOffsets) {}

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const;
};
}
