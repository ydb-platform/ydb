#include "mkql_squeeze_state.h"
#include "mkql_saveload.h"

#include <ydb/library/yql/minikql/mkql_string_util.h> 
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h> 
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h> 

namespace NKikimr {
namespace NMiniKQL {

TSqueezeState::TSqueezeState(
    IComputationExternalNode* item,
    IComputationExternalNode* state,
    IComputationNode* outSwitch,
    IComputationNode* initState,
    IComputationNode* updateState,
    IComputationExternalNode* inSave,
    IComputationNode* outSave,
    IComputationExternalNode* inLoad,
    IComputationNode* outLoad,
    const TType* stateType
)
    : Item(item)
    , State(state)
    , Switch(outSwitch)
    , InitState(initState)
    , UpdateState(updateState)
    , InSave(inSave)
    , OutSave(outSave)
    , InLoad(inLoad)
    , OutLoad(outLoad)
    , StateType(stateType)
{}

TSqueezeState::TSqueezeState(const TSqueezeState& state)
    : Item(state.Item)
    , State(state.State)
    , Switch(state.Switch)
    , InitState(state.InitState)
    , UpdateState(state.UpdateState)
    , InSave(state.InSave)
    , OutSave(state.OutSave)
    , InLoad(state.InLoad)
    , OutLoad(state.OutLoad)
    , StateType(state.StateType)
{}

NUdf::TUnboxedValuePod TSqueezeState::Save(TComputationContext& ctx) const {
    TString out;
    WriteByte(out, static_cast<ui8>(Stage));
    if (ESqueezeState::Work == Stage) {
        InSave->SetValue(ctx, State->GetValue(ctx));
        WriteUnboxedValue(out, GetPacker(), OutSave->GetValue(ctx));
    }
    return MakeString(out);
}

void TSqueezeState::Load(TComputationContext& ctx, const NUdf::TStringRef& state) {
    TStringBuf in(state.Data(), state.Size());
    Stage = static_cast<ESqueezeState>(ReadByte(in));
    if (ESqueezeState::Work == Stage) {
        InLoad->SetValue(ctx, ReadUnboxedValue(in, GetPacker(), ctx));
        State->SetValue(ctx, OutLoad->GetValue(ctx));
    }
}

const TValuePacker& TSqueezeState::GetPacker() const {
    if (!Packer && StateType)
        Packer = MakeHolder<TValuePacker>(false, StateType); 
    return *Packer;
}

TSqueezeCodegenValue::TSqueezeCodegenValue(TMemoryUsageInfo* memInfo, const TSqueezeState& state, TFetchPtr fetch, TComputationContext& ctx, NUdf::TUnboxedValue&& stream)
    : TBase(memInfo)
    , FetchFunc(fetch)
    , Stream(std::move(stream))
    , Ctx(ctx)
    , State(state)
{}

ui32 TSqueezeCodegenValue::GetTraverseCount() const {
    return 1U;
}

NUdf::TUnboxedValue TSqueezeCodegenValue::GetTraverseItem(ui32) const {
    return Stream;
}

NUdf::TUnboxedValue TSqueezeCodegenValue::Save() const {
    return State.Save(Ctx);
}

void TSqueezeCodegenValue::Load(const NUdf::TStringRef& state) {
    State.Load(Ctx, state);
}

NUdf::EFetchStatus TSqueezeCodegenValue::Fetch(NUdf::TUnboxedValue& result) {
    if (ESqueezeState::Finished == State.Stage)
        return NUdf::EFetchStatus::Finish;
    return FetchFunc(&Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), result, State.Stage);
}

}
}
