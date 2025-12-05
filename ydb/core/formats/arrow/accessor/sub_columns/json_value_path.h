#pragma once

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <functional>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Should be in JSONPath (Query Expressions for JSON) format (RFC: https://datatracker.ietf.org/doc/html/rfc9535)
using TJsonPath = TString;
using TJsonPathBuf = TStringBuf;

TString QuoteJsonItem(TStringBuf item);
TJsonPath ToJsonPath(TStringBuf path);

struct TSplittedJsonPath {
    TVector<TString> PathItems;
    TVector<NYql::NJsonPath::EJsonPathItemType> PathTypes;
    TVector<TJsonPathBuf::size_type> StartPositions;
};

struct TJsonPathSplitSettings {
    bool FillTypes = false;
    bool FillStartPositions = false;
};


TConclusion<TSplittedJsonPath> SplitJsonPath(TJsonPathBuf jsonPath, const TJsonPathSplitSettings& settings = {});

TString ToSubcolumnName(TStringBuf path);


class TJsonPathAccessor {
    YDB_READONLY_DEF(std::shared_ptr<IChunkedArray>, ChunkedArrayAccessor);
    YDB_READONLY_DEF(TString, RemainingPath);
    YDB_READONLY_DEF(std::optional<ui64>, Cookie);
    NYql::NJsonPath::TJsonPathPtr RemainingPathPtr;

public:
    using TValuesVisitor = std::function<void(const std::optional<TStringBuf>& value)>;

    TJsonPathAccessor(std::shared_ptr<IChunkedArray> accessor, TString remainingPath, const std::optional<ui64>& cookie = std::nullopt);

    void VisitValues(const TValuesVisitor& visitor) const;

    bool IsValid() const {
        return ChunkedArrayAccessor != nullptr || Cookie.has_value();
    }

    ui64 GetRecordsCount() const {
        return ChunkedArrayAccessor ? ChunkedArrayAccessor->GetRecordsCount() : 0;
    }
};

class TJsonPathAccessorTrie {
    struct TrieNode {
        TMap<TString, std::unique_ptr<TrieNode>> Children;
        std::shared_ptr<IChunkedArray> Accessor;
        std::optional<ui64> Cookie;
    };

    TrieNode Root;

public:
    TConclusionStatus Insert(TJsonPathBuf jsonPath, std::shared_ptr<IChunkedArray> accessor, const std::optional<ui64>& cookie = std::nullopt);
    TConclusion<std::shared_ptr<TJsonPathAccessor>> GetAccessor(TJsonPathBuf jsonPath) const;
};

} // namespace NKikimr::NArrow::NAccessor::NSubColumns
