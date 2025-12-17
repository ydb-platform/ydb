#include "json_value_path.h"

#include <arrow/array/array_binary.h>
#include <ydb/core/formats/arrow/accessor/common/binary_json_value_view.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>
#include <yql/essentials/types/binary_json/read.h>

#include <algorithm>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TString QuoteJsonItem(TStringBuf item) {
    TStringBuilder builder;

    builder << '"';

    for (char ch : item) {
        if (ch == '"') {
            builder << "\\\"";
        } else if (ch == '\\') {
            builder << "\\\\";
        } else {
            builder << ch;
        }
    }

    builder << '"';

    return builder;
}

TJsonPath ToJsonPath(TStringBuf path) {
    return TString("$.") + path;
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

TString ToSubcolumnName(TStringBuf path) {
    auto pathItemsResult = SplitJsonPath(path, NSubColumns::TJsonPathSplitSettings{.FillTypes = true, .FillStartPositions = false});
    if (pathItemsResult.IsFail()) {
        pathItemsResult = SplitJsonPath(ToJsonPath(path), NSubColumns::TJsonPathSplitSettings{.FillTypes = true, .FillStartPositions = false});
        if (pathItemsResult.IsFail()) {
            return TString(path);
        }
    }
    auto [pathItems, pathTypes, _] = pathItemsResult.DetachResult();
    TString result;
    result.reserve(path.size() + 2 * pathItems.size());
    for (decltype(pathItems)::size_type i = 0; i < pathItems.size(); ++i) {
        if (pathTypes[i] == NYql::NJsonPath::EJsonPathItemType::ArrayAccess) {
            result.append(pathItems[i]);
        } else {
            if (!result.empty()) {
                result.append(".");
            }
            result.append(QuoteJsonItem(pathItems[i]));
        }
    }

    return result;
}

TJsonPathAccessor::TJsonPathAccessor(std::shared_ptr<IChunkedArray> accessor, TString remainingPath, const std::optional<ui64>& cookie)
    : ChunkedArrayAccessor(std::move(accessor))
    , RemainingPath(std::move(remainingPath))
    , Cookie(cookie) {
    if (!RemainingPath.empty()) {
        NYql::TIssues issues;
        RemainingPathPtr = NYql::NJsonPath::ParseJsonPath(RemainingPath, issues, 5);
        AFL_VERIFY(issues.Empty())("RemainingPath", RemainingPath)("issues", issues.ToString());
    }
}

void TJsonPathAccessor::VisitValues(const TValuesVisitor& visitor) const {
    if (!ChunkedArrayAccessor) {
        return;
    }

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

            TStringBuf binaryJsonBuffer(value.data(), value.size());
            AFL_VERIFY(NBinaryJson::IsValidBinaryJson(binaryJsonBuffer));
            auto reader = NBinaryJson::TBinaryJsonReader::Make(binaryJsonBuffer);
            auto rootCursor = reader->GetRootCursor();
            if (rootCursor.GetType() == NBinaryJson::EContainerType::TopLevelScalar) {
                if (RemainingPathPtr) {
                    visitor(std::nullopt);
                    continue;
                }
                switch (rootCursor.GetElement(0).GetType()) {
                    case NBinaryJson::EEntryType::String:
                        visitor(rootCursor.GetElement(0).GetString());
                        break;
                    case NBinaryJson::EEntryType::Number:
                        visitor(TBinaryJsonValueView::JsonNumberToString(rootCursor.GetElement(0).GetNumber()));
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
            } else if (RemainingPathPtr) {
                auto binaryJsonRoot = NYql::NJsonPath::TValue(reader->GetRootCursor());
                const auto result = NYql::NJsonPath::ExecuteJsonPath(RemainingPathPtr, binaryJsonRoot, NYql::NJsonPath::TVariablesMap{}, nullptr);
                if (result.IsError()) {
                    visitor(std::nullopt);
                    continue;
                }
                const auto& nodes = result.GetNodes();
                if (nodes.size() != 1) {
                    // TODO: Find case when it is needed and maybe support
                    visitor(std::nullopt);
                    continue;
                }
                const auto& node = nodes[0];
                switch (node.GetType()) {
                    case NYql::NJsonPath::EValueType::Bool:
                        visitor(node.GetBool() ? "true" : "false");
                        break;
                    case NYql::NJsonPath::EValueType::Number:
                        visitor(::ToString(node.GetNumber()));
                        break;
                    case NYql::NJsonPath::EValueType::String:
                        visitor(node.GetString());
                        break;
                    case NYql::NJsonPath::EValueType::Null:
                    case NYql::NJsonPath::EValueType::Object:
                    case NYql::NJsonPath::EValueType::Array:
                        visitor(std::nullopt);
                        break;
                }
            } else {
                visitor(std::nullopt);
            }
        }
    });
}

TConclusionStatus TJsonPathAccessorTrie::Insert(TJsonPathBuf jsonPath, std::shared_ptr<IChunkedArray> accessor, const std::optional<ui64>& cookie) {
    auto splittedPathResult = NSubColumns::SplitJsonPath(jsonPath, NSubColumns::TJsonPathSplitSettings{.FillTypes = true, .FillStartPositions = false});
    if (!splittedPathResult.IsSuccess()) {
        return splittedPathResult;
    }

    auto [pathItems, pathTypes, _] = splittedPathResult.DetachResult();
    AFL_VERIFY(pathItems.size() == pathTypes.size());

    auto currentNode = &Root;
    for (decltype(pathItems)::size_type i = 0; i < pathItems.size(); ++i) {
        AFL_VERIFY(pathTypes[i] == NYql::NJsonPath::EJsonPathItemType::MemberAccess);
        if (auto found = currentNode->Children.find(pathItems[i]); found != currentNode->Children.end()) {
            currentNode = found->second.get();
        } else {
            currentNode = currentNode->Children.emplace(pathItems[i], std::make_unique<TrieNode>()).first->second.get();
        }
    }

    AFL_VERIFY(!currentNode->Accessor);

    currentNode->Accessor = std::move(accessor);
    currentNode->Cookie = std::move(cookie);

    return TConclusionStatus::Success();
}

TConclusion<std::shared_ptr<TJsonPathAccessor>> TJsonPathAccessorTrie::GetAccessor(TJsonPathBuf jsonPath) const {
    auto splittedPathResult = SplitJsonPath(jsonPath, NSubColumns::TJsonPathSplitSettings{.FillTypes = false, .FillStartPositions = true});
    if (!splittedPathResult.IsSuccess()) {
        return splittedPathResult;
    }

    auto [pathItems, _, startPositions] = splittedPathResult.DetachResult();
    AFL_VERIFY(pathItems.size() == startPositions.size());
    auto currentNode = &Root;
    for (decltype(pathItems)::size_type i = 0; i < pathItems.size(); ++i) {
        if (auto found = currentNode->Children.find(pathItems[i]); found != currentNode->Children.end()) {
            currentNode = found->second.get();
        } else if (currentNode->Accessor || currentNode->Cookie) {
            auto remainingPath = jsonPath.substr(startPositions[i]);
            // strict is required, because there is a memory problem in NYql::NJsonPath::ExecuteJsonPath with lax and BinaryJson
            return std::make_shared<TJsonPathAccessor>(currentNode->Accessor, remainingPath.empty() ? TString{} : "strict $" + TString(remainingPath.data(), remainingPath.size()), currentNode->Cookie);
        } else {
            return std::make_shared<TJsonPathAccessor>(nullptr, TString{});
        }
    }

    return std::make_shared<TJsonPathAccessor>(currentNode->Accessor, TString{}, currentNode->Cookie);
}

} // namespace NKikimr::NArrow::NAccessor::NSubColumns
