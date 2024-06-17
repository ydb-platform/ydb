#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_path.h"

#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define RETURN_RESULT_UNLESS(x) if (!(x)) return result;


namespace NKikimr::NSchemeShard {

namespace NResourcePool {

TPath::TChecker IsParentPathValid(const TPath& parentPath);

bool IsParentPathValid(const THolder<TProposeResponse>& result, const TPath& parentPath);

bool Validate(const NKikimrSchemeOp::TResourcePoolDescription& description, TString& errorStr);

TResourcePoolInfo::TPtr CreateResourcePool(const NKikimrSchemeOp::TResourcePoolDescription& description, ui64 alterVersion);

}  // namespace NResourcePool

class TResourcePoolSubOperation : public TSubOperation {
public:
    using TSubOperation::TSubOperation;

protected:
    static TTxState::ETxState NextState();
    virtual TTxState::ETxState NextState(TTxState::ETxState state) const override;
    virtual TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override;
    virtual TSubOperationState::TPtr GetProposeOperationState() = 0;

    bool IsApplyIfChecksPassed(const THolder<TProposeResponse>& result, const TOperationContext& context) const;
    static bool IsDescriptionValid(const THolder<TProposeResponse>& result, const NKikimrSchemeOp::TResourcePoolDescription& description);

    static void AddPathInSchemeShard(const THolder<TProposeResponse>& result, const TPath& dstPath);
    TTxState& CreateTransaction(const TOperationContext& context, const TPathId& resourcePoolPathId, TTxState::ETxType txType) const;
    void RegisterParentPathDependencies(const TOperationContext& context, const TPath& parentPath) const;

    void AdvanceTransactionStateToPropose(const TOperationContext& context, NIceDb::TNiceDb& db) const;
    void PersistResourcePool(const TOperationContext& context, NIceDb::TNiceDb& db, const TPathElement::TPtr& resourcePoolPath, const TResourcePoolInfo::TPtr& resourcePoolInfo, const TString& acl) const;
};

}  // namespace NKikimr::NSchemeShard
