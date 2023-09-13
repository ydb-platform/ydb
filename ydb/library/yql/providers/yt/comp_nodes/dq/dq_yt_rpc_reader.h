#pragma once

#include "dq_yt_reader_impl.h"

#include <yt/yt/core/actions/future.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

struct TSettingsHolder;

class TParallelFileInputState: public IInputState {
struct TResult {
    size_t Input_ = 0;
    NYT::TSharedRef Value_;
};
struct TReaderState {
    TMaybe<ui64> CurrentRow;
    TMaybe<ui32> CurrentRange;
};
public:
    TParallelFileInputState(const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>&& rawInputs,
        size_t blockSize,
        size_t inflight,
        std::unique_ptr<TSettingsHolder>&& client,
        TVector<size_t>&& originalIndexes);

    size_t GetTableIndex() const;

    size_t GetRecordIndex() const;

    void SetNextBlockCallback(std::function<void()> cb);
protected:
    virtual bool IsValid() const override;

    virtual NUdf::TUnboxedValue GetCurrent() override;

    virtual void Next() override;

    void Finish();

    bool RunNext();

    bool NextValue();

private:
    // Used to pass struct in lambdas. std::shared_ptr copying is thread-safe
    struct TInnerState {
        TInnerState(size_t inputsCount) : IsInputDone(inputsCount) {};
        std::mutex Lock;
        std::queue<TResult> Results;
        std::vector<bool> IsInputDone;
        NYT::TError Error{};
        size_t CurrentInputIdx = 0;
        size_t CurrentInflight = 0;
        NYT::TPromise<void> WaitPromise = NYT::NewPromise<void>();
    };
    std::shared_ptr<TInnerState> InnerState_;
    std::vector<TReaderState> StateByReader_;
    NYT::TRawTableReaderPtr CurrentReader_ = nullptr;
    bool Eof_ = false;
    const TMkqlIOSpecs* Spec_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> RawInputs_;
    const size_t BlockSize_;
    TMkqlReaderImpl MkqlReader_;
    size_t CurrentInput_ = 0;
    size_t CurrentRecord_ = 1;
    size_t Inflight_ = 1;
    bool Valid_ = true;
    std::unique_ptr<TSettingsHolder> Settings_;
    NUdf::TUnboxedValue CurrentValue_;
    std::function<void()> OnNextBlockCallback_;
    NKikimr::NMiniKQL::TSamplingStatTimer TimerAwaiting_;
    TVector<size_t> OriginalIndexes_;

};


class TDqYtReadWrapperRPC : public TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState> {
public:
using TInputType = NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr;
    TDqYtReadWrapperRPC(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables, NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight, size_t timeout)
            : TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>(ctx, clusterName, token, inputSpec, samplingSpec, inputGroups, itemType, tableNames, std::move(tables), jobStats, inflight, timeout) {}

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const;
};
}
