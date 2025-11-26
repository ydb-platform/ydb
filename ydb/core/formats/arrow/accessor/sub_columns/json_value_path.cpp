#include "json_value_path.h"

#include <arrow/array/array_binary.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>
#include <yql/essentials/types/binary_json/read.h>

#include <algorithm>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

void TJsonPathAccessor::VisitValues(const TValuesVisitor& visitor) const {
    if (!ChunkedArrayAccessor) {
        return;
    }

    // TODO: Write test and refactor

    ChunkedArrayAccessor->VisitValues([&](std::shared_ptr<arrow::Array> arr) {
        AFL_VERIFY(arr);
        AFL_VERIFY(arr->type_id() == arrow::binary()->id());
        const auto& binaryArray = static_cast<const arrow::BinaryArray&>(*arr);
        for (int64_t i = 0; i < binaryArray.length(); ++i) {
            auto value = binaryArray.Value(i);
            if (value.empty()) {
                visitor(std::nullopt);
                continue;
            }

            auto reader = NBinaryJson::TBinaryJsonReader::Make(TStringBuf(value.data(), value.size()));
            auto rootCursor = reader->GetRootCursor();
            if (rootCursor.GetType() == NBinaryJson::EContainerType::TopLevelScalar) {
                switch (rootCursor.GetElement(0).GetType()) {
                    case NBinaryJson::EEntryType::String:
                        visitor(rootCursor.GetElement(0).GetString());
                        break;
                    case NBinaryJson::EEntryType::Number:
                        visitor(::ToString(rootCursor.GetElement(0).GetNumber()));
                        break;
                    case NBinaryJson::EEntryType::BoolTrue:
                        visitor("true");
                        break;
                    case NBinaryJson::EEntryType::BoolFalse:
                        visitor("false");
                        break;
                    default:
                        visitor(std::nullopt);
                        break;
                }
            } else if (!RemainingPath.empty()) {
                auto binaryJsonRoot = NYql::NJsonPath::TValue(reader->GetRootCursor());
                NYql::TIssues issues;
                auto path = NYql::NJsonPath::ParseJsonPath(RemainingPath, issues, 5); // TODO: move it to path parsing
                if (!issues.Empty()) {
                    visitor(std::nullopt);
                } else {
                    const auto result = NYql::NJsonPath::ExecuteJsonPath(path, binaryJsonRoot, NYql::NJsonPath::TVariablesMap{}, nullptr);
                    if (result.IsError()) {
                        visitor(std::nullopt);
                    } else {
                        const auto& nodes = result.GetNodes();
                        if (nodes.size() != 1) {
                            visitor(std::nullopt);
                        } else {
                            switch (nodes[0].GetType()) {
                                case NYql::NJsonPath::EValueType::Bool:
                                    visitor(::ToString(nodes[i].GetBool()));
                                    break;
                                case NYql::NJsonPath::EValueType::Number:
                                    visitor(::ToString(nodes[i].GetNumber()));
                                    break;
                                case NYql::NJsonPath::EValueType::String:
                                    visitor(nodes[i].GetString());
                                    break;
                                case NYql::NJsonPath::EValueType::Null:
                                case NYql::NJsonPath::EValueType::Object:
                                case NYql::NJsonPath::EValueType::Array:
                                    visitor(std::nullopt);
                                    break;
                            }
                        }
                    }
                }
            } else {
                visitor(std::nullopt);
            }
        }
    });
}

TConclusionStatus TJsonPathAccessorTrie::Insert(TJsonPathBuf jsonPath, std::shared_ptr<IChunkedArray> accessor) {
    auto splittedPathResult = NSubColumns::SplitJsonPath(jsonPath, NSubColumns::TJsonPathSplitSettings{.FillTypes = true, .FillStartPositions = false});
    if (!splittedPathResult.IsSuccess()) {
        return splittedPathResult;
    }

    auto [pathItems, pathTypes, _] = splittedPathResult.DetachResult();
    AFL_VERIFY(pathItems.size() == pathTypes.size());

    auto currentNode = &root;
    for (decltype(pathItems)::size_type i = 0; i < pathItems.size(); ++i) {
        AFL_VERIFY(pathTypes[i] == NYql::NJsonPath::EJsonPathItemType::MemberAccess);
        if (auto found = currentNode->Children.find(pathItems[i]); found != currentNode->Children.end()) {
            currentNode = found->second.get();
        } else {
            currentNode = currentNode->Children.emplace(pathItems[i], std::make_unique<TrieNode>()).first->second.get();
        }
    }

    currentNode->accessor = std::move(accessor);

    return TConclusionStatus::Success();
}

TConclusion<TJsonPathAccessor> TJsonPathAccessorTrie::GetAccessor(TJsonPathBuf jsonPath) const {
    auto splittedPathResult = SplitJsonPath(jsonPath, NSubColumns::TJsonPathSplitSettings{.FillTypes = false, .FillStartPositions = true});
    if (!splittedPathResult.IsSuccess()) {
        return splittedPathResult;
    }

    auto [pathItems, _, startPositions] = splittedPathResult.DetachResult();
    AFL_VERIFY(pathItems.size() == startPositions.size());
    auto currentNode = &root;
    for (decltype(pathItems)::size_type i = 0; i < pathItems.size(); ++i) {
        if (auto found = currentNode->Children.find(pathItems[i]); found != currentNode->Children.end()) {
            currentNode = found->second.get();
        } else if (currentNode->accessor) {
            auto remainingPath = jsonPath.substr(startPositions[i]);
            return TJsonPathAccessor(currentNode->accessor, remainingPath.empty() ? TString{} :
                "$" + TString(remainingPath.data(), remainingPath.size()));
        } else {
            return TJsonPathAccessor(nullptr, {});
        }
    }

    if (currentNode->accessor) {
        return TJsonPathAccessor(currentNode->accessor, {});
    }

    return TJsonPathAccessor(nullptr, {});
}

TConclusion<TSplittedJsonPath> SplitJsonPath(TJsonPathBuf jsonPath, const TJsonPathSplitSettings& settings) {
    NYql::TIssues issues;
    auto path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 5);
    if (!path) {
        return TConclusionStatus::Fail(issues.ToOneLineString());
    }

    NYql::NJsonPath::TJsonPathReader reader(path);
    auto it = &reader.ReadFirst();
    auto prevPos = jsonPath.size();
    TSplittedJsonPath result;

    while (it->Type == NYql::NJsonPath::EJsonPathItemType::MemberAccess || it->Type == NYql::NJsonPath::EJsonPathItemType::ArrayAccess) {
        auto currentPos = it->Pos.Column;
        if (settings.FillTypes) {
            result.PathTypes.push_back(it->Type);
        }
        if (settings.FillStartPositions) {
            result.StartPositions.push_back(currentPos);
        }

        if (it->Type == NYql::NJsonPath::EJsonPathItemType::ArrayAccess) {
            if (currentPos >= jsonPath.size() || prevPos < currentPos) {
                return TConclusionStatus::Fail("Invalid path: " + TString(jsonPath.data(), jsonPath.size()));
            }
            result.PathItems.push_back(TString(jsonPath.data() + currentPos, prevPos - currentPos));
        } else {
            auto val = it->GetString();
            result.PathItems.push_back(TString(val.data(), val.size()));
        }

        prevPos = currentPos;
        it = &reader.ReadInput(*it);
    }

    if (it->Type != NYql::NJsonPath::EJsonPathItemType::ContextObject) {
        return TConclusionStatus::Fail("Unsupported path: " + TString(jsonPath.data(), jsonPath.size()));
    }

    std::ranges::reverse(result.PathItems);
    if (settings.FillTypes) {
        std::ranges::reverse(result.PathTypes);
    }
    if (settings.FillStartPositions) {
        std::ranges::reverse(result.StartPositions);
    }

    return result;
}

} // namespace NKikimr::NArrow::NAccessor::NSubColumns
