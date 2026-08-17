#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

enum class ESqueezeState: ui8 {
    Idle = 0,
    Work,
    Finished,
    NeedInit
};

struct TSqueezeState {
    TSqueezeState(
        IComputationExternalNode* item,
        IComputationExternalNode* state,
        IComputationNode* outSwitch,
        IComputationNode* initState,
        IComputationNode* updateState,
        IComputationExternalNode* inSave,
        IComputationNode* outSave,
        IComputationExternalNode* inLoad,
        IComputationNode* outLoad,
        const TType* stateType);

    TSqueezeState(const TSqueezeState& state);

    NUdf::TUnboxedValue Save(TComputationContext& ctx) const;

    void Load(TComputationContext& ctx, const NUdf::TStringRef& state);

    ESqueezeState Stage = ESqueezeState::Idle;

    IComputationExternalNode* const Item;
    IComputationExternalNode* const State;
    IComputationNode* const Switch;
    IComputationNode* const InitState;
    IComputationNode* const UpdateState;

    IComputationExternalNode* const InSave;
    IComputationNode* const OutSave;
    IComputationExternalNode* const InLoad;
    IComputationNode* const OutLoad;

private:
    const TValuePacker& GetPacker() const;

    const TType* StateType_;

    mutable THolder<TValuePacker> Packer_;
};

class TSqueezeCodegenValue: public TComputationValue<TSqueezeCodegenValue> {
public:
    using TBase = TComputationValue<TSqueezeCodegenValue>;

    using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&, ESqueezeState&);

    TSqueezeCodegenValue(TMemoryUsageInfo* memInfo, const TSqueezeState& state, TFetchPtr fetch, TComputationContext& ctx, NUdf::TUnboxedValue&& stream);

private:
    ui32 GetTraverseCount() const final;

    NUdf::TUnboxedValue GetTraverseItem(ui32) const final;

    NUdf::TUnboxedValue Save() const final;

    void Load(const NUdf::TStringRef& state) final;

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final;

    const TFetchPtr FetchFunc_;
    const NUdf::TUnboxedValue Stream_;
    TComputationContext& Ctx_;
    TSqueezeState State_;
};

} // namespace NKikimr::NMiniKQL
