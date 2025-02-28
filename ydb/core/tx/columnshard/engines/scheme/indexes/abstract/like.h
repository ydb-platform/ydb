#pragma once
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexes::NRequest {

class TLikePart {
public:
    enum class EOperation {
        StartsWith,
        EndsWith,
        Contains
    };

private:
    YDB_READONLY(EOperation, Operation, EOperation::Contains);
    YDB_READONLY_DEF(TString, Value);

public:
    TLikePart(const EOperation op, const TString& value)
        : Operation(op)
        , Value(value) {
    }

    static TLikePart MakeStart(const TString& value) {
        return TLikePart(EOperation::StartsWith, value);
    }
    static TLikePart MakeEnd(const TString& value) {
        return TLikePart(EOperation::EndsWith, value);
    }
    static TLikePart MakeContains(const TString& value) {
        return TLikePart(EOperation::Contains, value);
    }

    TString ToString() const;
};

class TLikeDescription {
private:
    THashMap<TString, TLikePart> LikeSequences;

public:
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& jsonSeq = result.InsertValue("sequences", NJson::JSON_ARRAY);
        for (auto&& i : LikeSequences) {
            jsonSeq.AppendValue(i.second.ToString());
        }
        return result;
    }

    TLikeDescription(const TLikePart& likePart) {
        LikeSequences.emplace(likePart.ToString(), likePart);
    }

    const THashMap<TString, TLikePart>& GetLikeSequences() const {
        return LikeSequences;
    }

    void Merge(const TLikeDescription& d) {
        for (auto&& i : d.LikeSequences) {
            LikeSequences.emplace(i.first, i.second);
        }
    }

    TString ToString() const;
};

}   // namespace NKikimr::NOlap::NIndexes::NRequest
