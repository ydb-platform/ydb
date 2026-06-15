#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

using TModifyKeysList = TVector<std::tuple<TCoAtom, TCoAtom, ui32, const TTypeAnnotationNode*>>;
enum EJoinSide { Right, Left, Both };

class TPhysicalJoinBuilder: public TPhysicalBinaryOpBuilderWithParams {
public:
    TPhysicalJoinBuilder(TIntrusivePtr<TOpJoin> join, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalBinaryOpBuilderWithParams(ctx, pos)
        , Join(join) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, bool useBlockHashJoin) override;

private:
    TExprNode::TPtr BuildPhysicalJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, bool useBlockHashJoins);
    TExprNode::TPtr BuildCrossJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput);
    TString GetValidJoinKind(const TString& joinKind) const;
    void PrepareJoinKeys(TVector<TString>& leftJoinKeys, TVector<TString>& rightJoinKeys, TModifyKeysList& remapLeft, TModifyKeysList& remapRight,
                         THashMap<TString, TString>& leftColumnRemap, THashMap<TString, TString>& rightColumnRemap, TVector<TString>& leftJoinKeyRenames,
                         TVector<TString>& rightJoinKeyRenames, const TStructExprType* leftInputType, const TStructExprType* rightInputType, const bool outer,
                         const EJoinSide joinSide);
    TExprNode::TPtr PrepareJoinSide(TExprNode::TPtr input, const TVector<TInfoUnit>& colNames, TVector<TString>& joinKeys, const TModifyKeysList& remap,
                                    const bool filterNulls);
    TExprNode::TPtr SqueezeJoinInputToDict(TExprNode::TPtr input, const ui32 width, const TVector<ui32>& joinKeys, const bool withPayloads);
    TExprNode::TPtr BuildMapJoin(const TString& joinType, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, TVector<TCoAtom>& leftColumnIdxs, TVector<TCoAtom>& rightColumnIdxs,
                                 TVector<TCoAtom>& leftRenames, TVector<TCoAtom>& rightRenames, TVector<TCoAtom>& leftKeyColumnNames,
                                 TVector<TCoAtom>& rightKeyColumnNames);
    TExprNode::TPtr BuildGraceJoin(const TString& joinType, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput, TVector<TCoAtom>& leftColumnIdxs, TVector<TCoAtom>& rightColumnIdxs,
                                   TVector<TCoAtom>& leftRenames, TVector<TCoAtom>& rightRenames, TVector<TCoAtom>& leftKeyColumnNames,
                                   TVector<TCoAtom>& rightKeyColumnNames);
    TExprNode::TPtr BuildBlockHashJoin(const TString& joinType, TExprNode::TPtr leftInput, TExprNode::TPtr rightInput,
                                       const TVector<TCoAtom>& leftKeyColumnIdxs, const TVector<TCoAtom>& rightKeyColumnIdsx,
                                       const TVector<TCoAtom>& leftKeyColumnNames, const TVector<TCoAtom>& rightKeyColumnNames, bool isReverseBlockJoin);

    TIntrusivePtr<TOpJoin> Join;
};
