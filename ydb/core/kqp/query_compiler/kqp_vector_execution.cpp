#include "kqp_vector_execution.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;
using namespace NYql::NNodes;

bool IsMember(const TExprNode& node, const TExprNode* row, TStringBuf name) {
    return node.IsCallable("Member") && node.ChildrenSize() == 2
        && node.Child(0) == row && node.Child(1)->IsAtom(name);
}

bool CompileTarget(const TExprNode& node, NKqpProto::TKqpPhyVectorExecution& plan) {
    const TExprNode* value = &node;
    if (node.IsCallable("Member") && node.ChildrenSize() == 2) {
        value = node.Child(0);
        const TTypeAnnotationNode* type = value->GetTypeAnn();
        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            plan.SetTargetStructOptional(true);
            type = type->Cast<TOptionalExprType>()->GetItemType();
        }
        if (type->GetKind() != ETypeAnnotationKind::Struct) {
            return false;
        }
        const auto* structType = type->Cast<TStructExprType>();
        const auto index = structType->FindItem(node.Child(1)->Content());
        if (!index) {
            return false;
        }
        plan.SetTargetMemberIndex(*index);
    }
    if (value->IsCallable("Parameter")) {
        plan.MutableTarget()->MutableParamValue()->SetParamName(TString(value->Child(0)->Content()));
        return true;
    }
    if (value->IsCallable("String") && !plan.HasTargetMemberIndex()) {
        auto* literal = plan.MutableTarget()->MutableLiteralValue();
        literal->mutable_type()->set_type_id(Ydb::Type::STRING);
        literal->mutable_value()->set_bytes_value(TString(value->Child(0)->Content()));
        return true;
    }
    return false;
}

bool CompileProjection(const TExprNode& expression, const TExprNode* row,
    const NKqpProto::TKqpPhyCnVectorSearch& search, NKqpProto::TKqpPhyVectorExecution& plan) {
    // An identity stream is useful for direct PK/score projections as well.
    const TTypeAnnotationNode* resultType = expression.GetTypeAnn();
    if (resultType->GetKind() != ETypeAnnotationKind::Struct) {
        return false;
    }
    const auto* structType = resultType->Cast<TStructExprType>();
    for (const auto* member : structType->GetItems()) {
        const TExprNode* value = nullptr;
        if (expression.IsCallable("AsStruct")) {
            for (const auto& item : expression.Children()) {
                if (item->Child(0)->IsAtom(member->GetName())) {
                    value = item->Child(1);
                    break;
                }
            }
        } else {
            return false;
        }
        if (!value) {
            return false;
        }
        auto* output = plan.AddProjection();
        if (value->IsCallable("SafeCast") && value->ChildrenSize() == 2) {
            // All supported integer values fit their decimal string; casts with
            // null/error handling or other source types must execute the SQL program.
            const TTypeAnnotationNode* from = value->Child(0)->GetTypeAnn();
            const TTypeAnnotationNode* to = value->GetTypeAnn();
            if (from->GetKind() != ETypeAnnotationKind::Data || to->GetKind() != ETypeAnnotationKind::Data
                || to->Cast<TDataExprType>()->GetSlot() != NUdf::EDataSlot::String) {
                return false;
            }
            const auto slot = from->Cast<TDataExprType>()->GetSlot();
            if (slot != NUdf::EDataSlot::Int64 && slot != NUdf::EDataSlot::Uint64) {
                return false;
            }
            output->SetIntegerToString(true);
            output->SetSignedInteger(slot == NUdf::EDataSlot::Int64);
            value = value->Child(0);
        }
        bool found = false;
        for (ui32 i = 0; i < search.ColumnsSize(); ++i) {
            if (IsMember(*value, row, search.GetColumns(i))) {
                // The initial fast path emits scalar keys/scores, never vectors
                // or arbitrary covered payloads with unbounded result sizes.
                const TTypeAnnotationNode* type = value->GetTypeAnn();
                if (type->GetKind() == ETypeAnnotationKind::Optional) {
                    type = type->Cast<TOptionalExprType>()->GetItemType();
                }
                if (type->GetKind() != ETypeAnnotationKind::Data) {
                    return false;
                }
                const auto slot = type->Cast<TDataExprType>()->GetSlot();
                if (slot != NUdf::EDataSlot::Uint64 && slot != NUdf::EDataSlot::Int64 && slot != NUdf::EDataSlot::Float) {
                    return false;
                }
                output->SetSourceIndex(i);
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return plan.ProjectionSize() > 0;
}

} // namespace

void TryCompileVectorExecution(const NYql::NNodes::TKqpPhysicalTx& tx, NKqpProto::TKqpPhyTx& proto) {
    if (proto.GetType() != NKqpProto::TKqpPhyTx::TYPE_GENERIC || proto.GetHasEffects()
        || proto.StagesSize() != 3 || proto.ResultsSize() != 1 || !proto.GetResults(0).GetIsStream()) {
        return;
    }
    // Match exactly: singleton vector -> vector search + TopSort/projection ->
    // identity result collector. No other stage may have side effects or I/O.
    for (const auto& stage : proto.GetStages()) {
        if (stage.SourcesSize() || stage.SinksSize() || stage.OutputTransformsSize()
            || stage.TableOpsSize() || stage.GetIsEffectsStage() || stage.WasmUdfModulesSize()
            || !stage.GetSecureParams().empty() || !stage.GetStageControlPlaneActors().empty()) {
            return;
        }
    }
    const auto& source = proto.GetStages(0);
    const auto& stage = proto.GetStages(1);
    const auto& collector = proto.GetStages(2);
    if (source.InputsSize() || stage.InputsSize() != 1 || collector.InputsSize() != 1
        || !stage.GetInputs(0).HasVectorSearch() || stage.GetInputs(0).GetStageIndex() != 0
        || stage.GetInputs(0).GetOutputIndex() != 0
        || !collector.GetInputs(0).HasUnionAll() || collector.GetInputs(0).GetStageIndex() != 1
        || collector.GetInputs(0).GetOutputIndex() != 0
        || proto.GetResults(0).GetConnection().GetStageIndex() != 2
        || proto.GetResults(0).GetConnection().GetOutputIndex() != 0) {
        return;
    }
    const auto& search = stage.GetInputs(0).GetVectorSearch();
    if (search.GetHasPrefix() || !search.HasScoreColumnName()
        || search.GetIndexSettings().vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT
        || search.GetIndexSettings().metric() != Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN) {
        return;
    }
    const auto sourceProgram = tx.Stages().Item(0).Program();
    const auto searchProgram = tx.Stages().Item(1).Program();
    const auto resultProgram = tx.Stages().Item(2).Program();
    if (sourceProgram.Args().Size() || searchProgram.Args().Size() != 1 || resultProgram.Args().Size() != 1
        || resultProgram.Body().Raw() != resultProgram.Args().Arg(0).Raw()) {
        return;
    }
    const TExprNode* target = sourceProgram.Body().Raw();
    if (!target->IsCallable("ToStream") || !target->Child(0)->IsCallable("Just")) {
        return;
    }
    target = target->Child(0)->Child(0);
    if (!target->IsCallable("AsStruct") || target->ChildrenSize() != 1) {
        return;
    }
    target = target->Child(0)->Child(1);
    const TTypeAnnotationNode* targetType = target->GetTypeAnn();
    if (targetType->GetKind() == ETypeAnnotationKind::Optional) {
        targetType = targetType->Cast<TOptionalExprType>()->GetItemType();
    }
    if (targetType->GetKind() != ETypeAnnotationKind::Data
        || targetType->Cast<TDataExprType>()->GetSlot() != NUdf::EDataSlot::String) {
        return;
    }
    NKqpProto::TKqpPhyVectorExecution plan;
    plan.SetSearchStageIndex(1);
    if (!CompileTarget(*target, plan)) {
        return;
    }
    const auto* body = searchProgram.Body().Raw();
    if (!(body->IsCallable("Map") || body->IsCallable("OrderedMap")) || body->ChildrenSize() != 2) {
        return;
    }
    const auto* top = body->Child(0);
    const auto* projection = body->Child(1);
    if (!top->IsCallable("TopSort") || top->ChildrenSize() != 4
        || top->Child(0) != searchProgram.Args().Arg(0).Raw()
        || !top->Child(2)->IsCallable("Bool") || !top->Child(2)->Child(0)->IsAtom("true")
        || !projection->IsLambda() || projection->Child(0)->ChildrenSize() != 1) {
        return;
    }
    // The native search already returns the very same top-K ordered by this
    // score. Match LIMIT structurally to prevent truncating a different window.
    const auto* sortKey = top->Child(3);
    if (!sortKey->IsLambda() || sortKey->Child(0)->ChildrenSize() != 1
        || !IsMember(*sortKey->Child(1), sortKey->Child(0)->Child(0), search.GetScoreColumnName())) {
        return;
    }
    NKqpProto::TKqpPhyValue limit;
    if (top->Child(1)->IsCallable("Parameter")) {
        limit.MutableParamValue()->SetParamName(TString(top->Child(1)->Child(0)->Content()));
    } else if (top->Child(1)->IsCallable("Uint64")) {
        FillLiteralProto(TCoDataCtor(top->ChildPtr(1)), *limit.MutableLiteralValue());
    } else {
        return;
    }
    if (limit.SerializeAsString() != search.GetTopK().SerializeAsString()
        || !CompileProjection(*projection->Child(1), projection->Child(0)->Child(0), search, plan)) {
        return;
    }
    *proto.MutableVectorExecution() = std::move(plan);
}

} // namespace NKikimr::NKqp
