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
public:
    TParallelFileInputState(const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr>&& rawInputs,
        size_t blockSize,
        size_t inflight,
        std::unique_ptr<TSettingsHolder>&& client);

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
    std::mutex Lock_;
    std::queue<TResult> Results_;
    std::vector<size_t> RecordByReader_;
    std::vector<bool> IsInputDone_;
    NYT::TPromise<void> WaitPromise_ = NYT::NewPromise<void>();
    NYT::TRawTableReaderPtr CurrentReader_ = nullptr;
    size_t CurrentInflight_ = 0;
    bool Eof_ = false;
    const TMkqlIOSpecs* Spec_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    TVector<NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr> RawInputs_;
    const size_t BlockSize_;
    TMkqlReaderImpl MkqlReader_;
    size_t CurrentInput_ = 0;
    size_t CurrentRecord_ = 1;
    size_t Inflight_ = 1;
    size_t CurrentInputIdx_ = 0;
    bool Valid_ = true;
    std::unique_ptr<TSettingsHolder> Settings_;
    NUdf::TUnboxedValue CurrentValue_;
    std::function<void()> OnNextBlockCallback_;
};


class TDqYtReadWrapperRPC : public TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState> {
public:
using TInputType = NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr;
    TDqYtReadWrapperRPC(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables, NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight)
            : TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>(ctx, clusterName, token, inputSpec, samplingSpec, inputGroups, itemType, tableNames, std::move(tables), jobStats, inflight) {}

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const;
};
}
