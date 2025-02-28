#include "composite.h"
#include "coverage.h"
#include "tree.h"

namespace NKikimr::NOlap::NIndexes::NRequest {

std::shared_ptr<TDataForIndexesCheckers> TDataForIndexesCheckers::Build(const TProgramContainer& program) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("program", program.DebugString());
    if (!program.GetSourceColumns().size()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "no_data_in_program");
        return nullptr;
    }
    if (!program.GetChainVerified()->GetLastOriginalDataFilter()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "no_filter_in_program");
        return nullptr;
    }
    TNormalForm nForm;
    for (ui32 stepIdx = 0; stepIdx <= *program.GetChainVerified()->GetLastOriginalDataFilter();  ++stepIdx) {
        auto& s = program.GetChainVerified()->GetProcessors()[stepIdx];
        if (s->GetProcessorType() == NArrow::NSSA::EProcessorType::Filter) {
            continue;
        }
        if (!nForm.Add(*s, program)) {
            return nullptr;
        }
    }
    auto rootNode = nForm.GetRootNode();
    AFL_VERIFY(rootNode);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("original_program", rootNode->SerializeToJson());
    while (rootNode->Collapse()) {
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("collapsed_program", rootNode->SerializeToJson());
    if (rootNode->GetChildren().size() != 1) {
        return nullptr;
    }
    std::shared_ptr<TDataForIndexesCheckers> result = std::make_shared<TDataForIndexesCheckers>();
    if (auto* orNode = rootNode->GetChildren().front()->As<TOperationNode>()) {
        if (orNode->GetOperation() == NYql::TKernelRequestBuilder::EBinaryOp::Or) {
            for (auto&& i : orNode->GetChildren()) {
                if (auto* andPackNode = i->As<TPackAnd>()) {
                    result->AddBranch(andPackNode->GetEquals(), andPackNode->GetLikes());
                } else if (auto* operationNode = i->As<TOperationNode>()) {
                    if (operationNode->GetOperation() == NYql::TKernelRequestBuilder::EBinaryOp::And) {
                        TPackAnd* pack = operationNode->FindFirst<TPackAnd>();
                        if (!pack) {
                            return nullptr;
                        }
                        result->AddBranch(pack->GetEquals(), pack->GetLikes());
                    }
                } else {
                    return nullptr;
                }
            }
        }
    } else if (auto* andPackNode = rootNode->GetChildren().front()->As<TPackAnd>()) {
        result->AddBranch(andPackNode->GetEquals(), andPackNode->GetLikes());
    } else {
        return nullptr;
    }
    return result;
}

TIndexCheckerContainer TDataForIndexesCheckers::GetCoverChecker() const {
    std::vector<std::shared_ptr<IIndexChecker>> andCheckers;
    for (auto&& i : Branches) {
        auto andChecker = i->GetAndChecker();
        if (!andChecker) {
            return TIndexCheckerContainer();
        }
        andCheckers.emplace_back(andChecker);
    }
    if (andCheckers.size() == 0) {
        return TIndexCheckerContainer();
    } else if (andCheckers.size() == 1) {
        return andCheckers.front();
    } else {
        return TIndexCheckerContainer(std::make_shared<TOrIndexChecker>(andCheckers));
    }
}

std::shared_ptr<NKikimr::NOlap::NIndexes::IIndexChecker> TBranchCoverage::GetAndChecker() const {
    if (Indexes.empty()) {
        return nullptr;
    }
    return std::make_shared<TAndIndexChecker>(Indexes);
}

}   // namespace NKikimr::NOlap::NIndexes::NRequest
