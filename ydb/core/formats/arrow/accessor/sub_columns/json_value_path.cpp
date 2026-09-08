#include "json_value_path.h"
#include "types.h"

#include <arrow/array/array_binary.h>
#include <ydb/core/formats/arrow/accessor/common/json_value_view.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>
#include <yql/essentials/types/binary_json/read.h>

#include <algorithm>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

void AppendQuotedJsonItem(TString& result, const TStringBuf item) {
    result.append("\"");

    for (char ch : item) {
        if (ch == '"') {
            result.append("\\\"");
        } else if (ch == '\\') {
            result.append("\\\\");
        } else {
            result.append(1, ch);
        }
    }

    result.append("\"");
}

}

TString QuoteJsonItem(const TStringBuf item) {
    TString result;
    result.reserve(item.size() + 2);
    AppendQuotedJsonItem(result, item);
    return result;
}

void AppendSubcolumnName(TString& currentPrefix, const TStringBuf item) {
    if (currentPrefix) {
        currentPrefix.append(".");
    }
    AppendQuotedJsonItem(currentPrefix, item);
}

TString BuildSubcolumnName(const TStringBuf currentPrefix, const TStringBuf item) {
    TString result(currentPrefix);
    AppendSubcolumnName(result, item);
    return result;
}

size_t EstimateSubcolumnNameSize(const TStringBuf path, const size_t pathItemsCount) {
    // Every canonical member adds a pair of quotes.
    return path.size() + 2 * pathItemsCount;
}

TJsonPath ToJsonPath(TStringBuf path) {
    if (!path.StartsWith('"')) {
        return TString("$.") + QuoteJsonItem(path);
    }
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

TConclusion<TParsedJsonPath> ParseJsonPath(const TJsonPathBuf jsonPath) {
    auto result = SplitJsonPath(jsonPath, TJsonPathSplitSettings{.FillTypes = true, .FillStartPositions = true});
    if (result.IsFail()) {
        return TConclusionStatus::Fail(result.GetErrorMessage());
    }
    return TParsedJsonPath{jsonPath, result.DetachResult()};
}

TConclusionStatus ValidateJsonPath(TJsonPathBuf jsonPath) {
    const auto result = SplitJsonPath(jsonPath, TJsonPathSplitSettings{.FillTypes = false, .FillStartPositions = false});
    if (result.IsSuccess()) {
        return TConclusionStatus::Success();
    }
    return TConclusionStatus::Fail(result.GetErrorMessage());
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
    result.reserve(EstimateSubcolumnNameSize(path, pathItems.size()));
    for (decltype(pathItems)::size_type i = 0; i < pathItems.size(); ++i) {
        if (pathTypes[i] == NYql::NJsonPath::EJsonPathItemType::ArrayAccess) {
            result.append(pathItems[i]);
        } else {
            AppendSubcolumnName(result, pathItems[i]);
        }
    }

    return result;
}

TJsonPathAccessor::TJsonPathAccessor(std::shared_ptr<IChunkedArray> accessor, TString remainingPath, const EValueType valueType,
    const std::optional<ui64>& cookie)
    : ChunkedArrayAccessor(std::move(accessor))
    , RemainingPath(std::move(remainingPath))
    , ValueType(valueType)
    , Cookie(cookie) {
    if (!RemainingPath.empty()) {
        NYql::TIssues issues;
        RemainingPathPtr = NYql::NJsonPath::ParseJsonPath(RemainingPath, issues, 5);
        AFL_VERIFY(issues.Empty())("RemainingPath", RemainingPath)("issues", issues.ToString());
    }
}

std::shared_ptr<IChunkedArray> TJsonPathAccessor::GetNativeStringArray() const {
    if (!RemainingPath.empty() || ValueType != EValueType::String || !ChunkedArrayAccessor ||
        ChunkedArrayAccessor->GetType() != IChunkedArray::EType::Array || ChunkedArrayAccessor->GetDataType()->id() != arrow::Type::STRING) {
        return nullptr;
    }
    return ChunkedArrayAccessor;
}

void TJsonPathAccessor::VisitValues(const TValuesVisitor& visitor) const {
    if (!ChunkedArrayAccessor) {
        return;
    }

    ChunkedArrayAccessor->VisitValues([&](std::shared_ptr<arrow::Array> arr) {
        AFL_VERIFY(arr);
        for (int64_t i = 0; i < arr->length(); ++i) {
            if (arr->IsNull(i)) {
                visitor(std::nullopt);
                continue;
            }

            const auto value = ArrayElementToJsonValueView(*arr, i, ValueType);
            if (auto scalar = value.GetScalarOptional()) {
                // A scalar has no sub-structure, so a remaining path cannot resolve against it.
                visitor(RemainingPathPtr ? std::nullopt : scalar);
                continue;
            }

            const auto blob = value.GetBinaryJsonBlobOptional();
            if (!RemainingPathPtr || !blob) {
                visitor(std::nullopt);
                continue;
            }

            auto reader = NBinaryJson::TBinaryJsonReader::Make(*blob);
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
        }
    });
}

} // namespace NKikimr::NArrow::NAccessor::NSubColumns
