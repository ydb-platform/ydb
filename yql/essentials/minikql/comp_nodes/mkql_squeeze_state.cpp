#include "mkql_squeeze_state.h"
#include "mkql_saveload.h"

#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

constexpr ui32 StateVersion = 1;

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
    const TType* stateType)
    : Item(item)
    , State(state)
    , Switch(outSwitch)
    , InitState(initState)
    , UpdateState(updateState)
    , InSave(inSave)
    , OutSave(outSave)
    , InLoad(inLoad)
    , OutLoad(outLoad)
    , StateType_(stateType)
{
}

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
    , StateType_(state.StateType_)
{
}

NUdf::TUnboxedValue TSqueezeState::Save(TComputationContext& ctx) const {
    TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, ctx);
    out.Write(static_cast<ui8>(Stage));
    if (ESqueezeState::Work == Stage) {
        InSave->SetValue(ctx, State->GetValue(ctx));
        out.WriteUnboxedValue(GetPacker(), OutSave->GetValue(ctx));
    }
    return out.MakeState();
}

void TSqueezeState::Load(TComputationContext& ctx, const NUdf::TStringRef& state) {
    TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);

    const auto loadStateVersion = in.GetStateVersion();
    if (loadStateVersion != StateVersion) {
        THROW yexception() << "Invalid state version " << loadStateVersion;
    }

    Stage = static_cast<ESqueezeState>(in.Read<ui8>());
    if (ESqueezeState::Work == Stage) {
        InLoad->SetValue(ctx, in.ReadUnboxedValue(GetPacker(), ctx));
        State->SetValue(ctx, OutLoad->GetValue(ctx));
    }
}

const TValuePacker& TSqueezeState::GetPacker() const {
    if (!Packer_ && StateType_) {
        Packer_ = MakeHolder<TValuePacker>(false, StateType_);
    }
    return *Packer_;
}

TSqueezeCodegenValue::TSqueezeCodegenValue(TMemoryUsageInfo* memInfo, const TSqueezeState& state, TFetchPtr fetch, TComputationContext& ctx, NUdf::TUnboxedValue&& stream)
    : TBase(memInfo)
    , FetchFunc_(fetch)
    , Stream_(std::move(stream))
    , Ctx_(ctx)
    , State_(state)
{
}

ui32 TSqueezeCodegenValue::GetTraverseCount() const {
    return 1U;
}

NUdf::TUnboxedValue TSqueezeCodegenValue::GetTraverseItem(ui32) const {
    return Stream_;
}

NUdf::TUnboxedValue TSqueezeCodegenValue::Save() const {
    return State_.Save(Ctx_);
}

void TSqueezeCodegenValue::Load(const NUdf::TStringRef& state) {
    State_.Load(Ctx_, state);
}

NUdf::EFetchStatus TSqueezeCodegenValue::Fetch(NUdf::TUnboxedValue& result) {
    if (ESqueezeState::Finished == State_.Stage) {
        return NUdf::EFetchStatus::Finish;
    }
    return FetchFunc_(&Ctx_, static_cast<const NUdf::TUnboxedValuePod&>(Stream_), result, State_.Stage);
}

} // namespace NKikimr::NMiniKQL
