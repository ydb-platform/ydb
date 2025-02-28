#include "tree.h"

#include <ydb/core/formats/arrow/program/assign_const.h>
#include <ydb/core/formats/arrow/program/assign_internal.h>

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

std::shared_ptr<IRequestNode> IRequestNode::Copy() const {
    auto selfCopy = DoCopy();
    selfCopy->Parent = nullptr;
    selfCopy->NodeId = NodeId.BuildCopy();
    AFL_VERIFY(selfCopy);
    for (auto&& i : Children) {
        selfCopy->Children.emplace_back(i->Copy());
    }
    for (auto&& i : selfCopy->Children) {
        i->Parent = selfCopy.get();
    }
    return selfCopy;
}

void IRequestNode::RemoveChildren(const TNodeId nodeId) {
    auto nameCopy = nodeId;
    const auto pred = [nameCopy](const std::shared_ptr<IRequestNode>& child) {
        if (child->GetNodeId() == nameCopy) {
            child->Parent = nullptr;
            return true;
        } else {
            return false;
        }
    };
    const ui32 sizeBefore = Children.size();
    Children.erase(std::remove_if(Children.begin(), Children.end(), pred), Children.end());
    AFL_VERIFY(sizeBefore == Children.size() + 1);
}

NJson::TJsonValue IRequestNode::SerializeToJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("id", NodeId.ToString());
    result.InsertValue("internal", DoSerializeToJson());
    if (Children.size()) {
        auto& childrenJson = result.InsertValue("children", NJson::JSON_ARRAY);
        for (auto&& i : Children) {
            childrenJson.AppendValue(i->SerializeToJson());
        }
    }
    return result;
}

void IRequestNode::Attach(const std::shared_ptr<IRequestNode>& children) {
    auto copy = children;
    if (copy->Parent) {
        copy->Parent->RemoveChildren(copy->GetNodeId());
    }
    copy->Parent = this;
    for (auto&& i : Children) {
        AFL_VERIFY(i->GetNodeId() != copy->GetNodeId());
    }
    Children.emplace_back(copy);
}

void IRequestNode::Exchange(const TNodeId& nodeId, const std::shared_ptr<IRequestNode>& children) {
    auto copy = children;
    for (auto&& i : Children) {
        if (i->GetNodeId() == nodeId) {
            i = copy;
            i->Parent = this;
            return;
        }
    }
    AFL_VERIFY(false);
}

NJson::TJsonValue TPackAnd::DoSerializeToJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("type", "pack_and");
    if (IsEmptyFlag) {
        result.InsertValue("empty", true);
    }
    {
        auto& arrJson = result.InsertValue("equals", NJson::JSON_ARRAY);
        for (auto&& i : Equals) {
            auto& jsonCondition = arrJson.AppendValue(NJson::JSON_MAP);
            jsonCondition.InsertValue(i.first.DebugString(), i.second->ToString());
        }
    }
    {
        auto& arrJson = result.InsertValue("likes", NJson::JSON_ARRAY);
        for (auto&& i : Likes) {
            auto& jsonCondition = arrJson.AppendValue(NJson::JSON_MAP);
            jsonCondition.InsertValue(i.first.DebugString(), i.second.ToString());
        }
    }
    return result;
}

void TPackAnd::AddEqual(const TOriginalDataAddress& originalDataAddress, const std::shared_ptr<arrow::Scalar>& value) {
    AFL_VERIFY(value);
    auto it = Equals.find(originalDataAddress);
    if (it == Equals.end()) {
        Equals.emplace(originalDataAddress, value);
    } else if (it->second->Equals(*value)) {
        return;
    } else {
        IsEmptyFlag = true;
    }
}

bool TOperationNode::DoCollapse() {
    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::Coalesce) {
        AFL_VERIFY(Children.size() == 2);
        AFL_VERIFY(Children[1]->Is<TConstantNode>());
        Parent->Attach(Children[0]);
        Parent->RemoveChildren(GetNodeId());
        return true;
    }
    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::Equals && Children.size() == 2 && Children[1]->Is<TConstantNode>() &&
        Children[0]->Is<TOriginalColumn>()) {
        Parent->Exchange(GetNodeId(), std::make_shared<TPackAnd>(Children[0]->As<TOriginalColumn>()->GetNodeId().BuildOriginalDataAddress(),
                                          Children[1]->As<TConstantNode>()->GetConstant()));
        return true;
    }
    const bool isLike =
        (Operation == NYql::TKernelRequestBuilder::EBinaryOp::StringContains ||
            Operation == NYql::TKernelRequestBuilder::EBinaryOp::StartsWith || Operation == NYql::TKernelRequestBuilder::EBinaryOp::EndsWith);
    if (isLike && Children.size() == 2 && Children[1]->Is<TConstantNode>() && Children[0]->Is<TOriginalColumn>()) {
        auto scalar = Children[1]->As<TConstantNode>()->GetConstant();
        AFL_VERIFY(scalar->type->id() == arrow::binary()->id());
        auto scalarString = static_pointer_cast<arrow::BinaryScalar>(scalar);
        std::optional<TLikePart::EOperation> op;
        if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::StringContains) {
            op = TLikePart::EOperation::Contains;
        } else if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::EndsWith) {
            op = TLikePart::EOperation::EndsWith;
        } else if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::StartsWith) {
            op = TLikePart::EOperation::StartsWith;
        }
        AFL_VERIFY(op);
        TLikePart likePart(*op, TString((const char*)scalarString->value->data(), scalarString->value->size()));
        Parent->Exchange(
            GetNodeId(), std::make_shared<TPackAnd>(Children[0]->As<TOriginalColumn>()->GetNodeId().BuildOriginalDataAddress(), likePart));
        return true;
    }
    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
        if (Parent->Is<TOperationNode>() && Parent->As<TOperationNode>()->Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
            Parent->Attach(Children);
            Parent->RemoveChildren(GetNodeId());
            return true;
        }
    }
    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
        if (Parent->Is<TOperationNode>() && Parent->As<TOperationNode>()->Operation == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
            Parent->Attach(Children);
            Parent->RemoveChildren(GetNodeId());
            return true;
        }
    }
    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
        auto copy = Children;
        TPackAnd* baseSet = nullptr;
        bool changed = false;
        for (auto&& c : copy) {
            if (c->Is<TPackAnd>()) {
                if (baseSet) {
                    baseSet->Merge(*c->As<TPackAnd>());
                    RemoveChildren(c->GetNodeId());
                    changed = true;
                } else {
                    baseSet = c->As<TPackAnd>();
                }
            }
        }
        if (changed) {
            return true;
        }
    }

    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And && Children.size() == 1) {
        AFL_VERIFY(Children.front()->Is<TPackAnd>());
        Parent->Exchange(GetNodeId(), Children.front());
        return true;
    }

    if (Operation == NYql::TKernelRequestBuilder::EBinaryOp::And) {
        std::vector<std::shared_ptr<IRequestNode>> newNodes;
        std::set<TString> cNames;
        for (auto&& i : Children) {
            if (i->Is<TOperationNode>() && i->As<TOperationNode>()->Operation == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
                auto orNode = i;
                RemoveChildren(i->GetNodeId());
                auto copy = orNode->GetChildren();
                auto copyChildren = Children;
                for (auto&& orNodeChildren : copy) {
                    std::vector<std::shared_ptr<IRequestNode>> producedChildren;
                    for (auto&& c : copyChildren) {
                        producedChildren.emplace_back(c->Copy());
                    }
                    producedChildren.emplace_back(orNodeChildren->Copy());
                    newNodes.emplace_back(std::make_shared<TOperationNode>(0, NYql::TKernelRequestBuilder::EBinaryOp::And, producedChildren));
                }
                Parent->Exchange(GetNodeId(), std::make_shared<TOperationNode>(0, NYql::TKernelRequestBuilder::EBinaryOp::Or, newNodes));
                return true;
            }
        }
    }
    return false;
}

bool TKernelNode::DoCollapse() {
    if (KernelName == "JsonValue" && Children.size() == 2 && Children[1]->Is<TConstantNode>() && Children[0]->Is<TOriginalColumn>()) {
        auto scalar = Children[1]->As<TConstantNode>()->GetConstant();
        AFL_VERIFY(scalar->type->id() == arrow::binary()->id());
        auto scalarString = static_pointer_cast<arrow::BinaryScalar>(scalar);
        const TString jsonPath((const char*)scalarString->value->data(), scalarString->value->size());
        Parent->Exchange(GetNodeId(), std::make_shared<TOriginalColumn>(Children[0]->As<TOriginalColumn>()->GetNodeId().GetColumnId(), jsonPath));
        return true;
    }
    return false;
}

bool TNormalForm::Add(const NArrow::NSSA::IResourceProcessor& processor, const TProgramContainer& program) {
    if (processor.GetProcessorType() == NArrow::NSSA::EProcessorType::Filter) {
        return true;
    }
    std::vector<std::shared_ptr<IRequestNode>> argNodes;
    for (auto&& arg : processor.GetInput()) {
        if (program.IsGenerated(arg.GetColumnId())) {
            auto it = Nodes.find(arg.GetColumnId());
            std::shared_ptr<IRequestNode> data;
            if (it == Nodes.end()) {
                it = NodesGlobal.find(arg.GetColumnId());
                if (it == NodesGlobal.end()) {
                    AFL_CRIT(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "program_arg_is_missing")("program", program.DebugString())(
                        "column_id", arg.GetColumnId());
                    return false;
                }
                data = it->second->Copy();
            } else {
                data = it->second;
            }
            argNodes.emplace_back(data);
        } else {
            argNodes.emplace_back(std::make_shared<TOriginalColumn>(arg.GetColumnId()));
        }
    }
    for (auto&& i : argNodes) {
        Nodes.erase(i->GetNodeId().GetColumnId());
    }

    if (processor.GetProcessorType() == NArrow::NSSA::EProcessorType::Const) {
        const auto* constProcessor = static_cast<const NArrow::NSSA::TConstProcessor*>(&processor);
        AFL_VERIFY(processor.GetInput().size() == 0);
        auto node = std::make_shared<TConstantNode>(processor.GetOutputColumnIdOnce(), constProcessor->GetScalarConstant());
        Nodes.emplace(processor.GetOutputColumnIdOnce(), node);
        NodesGlobal.emplace(processor.GetOutputColumnIdOnce(), node);
    } else if (processor.GetProcessorType() == NArrow::NSSA::EProcessorType::Calculation) {
        const auto* calcProcessor = static_cast<const NArrow::NSSA::TCalculationProcessor*>(&processor);
        if (!!calcProcessor->GetYqlOperationId()) {
            auto node = std::make_shared<TOperationNode>(
                processor.GetOutputColumnIdOnce(), (NYql::TKernelRequestBuilder::EBinaryOp)*calcProcessor->GetYqlOperationId(), argNodes);
            Nodes.emplace(processor.GetOutputColumnIdOnce(), node);
            NodesGlobal.emplace(processor.GetOutputColumnIdOnce(), node);
        } else if (calcProcessor->GetKernelLogic()) {
            auto node = std::make_shared<TKernelNode>(processor.GetOutputColumnIdOnce(), calcProcessor->GetKernelLogic()->GetClassName(), argNodes);
            Nodes.emplace(processor.GetOutputColumnIdOnce(), node);
            NodesGlobal.emplace(processor.GetOutputColumnIdOnce(), node);
        }
    } else {
        return false;
    }
    return true;
}

std::shared_ptr<TRootNode> TNormalForm::GetRootNode() {
    auto result = std::make_shared<TRootNode>();

    if (Nodes.size() != 1) {
        std::vector<std::shared_ptr<IRequestNode>> nodes;
        for (auto&& i : Nodes) {
            nodes.emplace_back(i.second);
        }
        result->Attach(std::make_shared<TOperationNode>(Max<ui32>(), NYql::TKernelRequestBuilder::EBinaryOp::And, nodes));
    } else {
        result->Attach(Nodes.begin()->second);
    }
    return result;
}

}   // namespace NKikimr::NOlap::NIndexes::NRequest
