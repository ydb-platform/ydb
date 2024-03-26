#pragma once

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimrColumnShardStatisticsProto {
class TScalar;
class TPortionStorage;
}

namespace NKikimr::NOlap::NStatistics {
class TPortionStorageCursor {
private:
    YDB_READONLY(ui32, ScalarsPosition, 0);
public:
    TPortionStorageCursor() = default;

    void AddScalarsPosition(const ui32 shift) {
        ScalarsPosition += shift;
    }
};

class TPortionStorage {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<arrow::Scalar>>, Data);
    static NKikimrColumnShardStatisticsProto::TScalar ScalarToProto(const arrow::Scalar& value);
    static std::shared_ptr<arrow::Scalar> ProtoToScalar(const NKikimrColumnShardStatisticsProto::TScalar& proto);
    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardStatisticsProto::TPortionStorage& proto);

public:
    bool IsEmpty() const {
        return Data.empty();
    }

    std::shared_ptr<arrow::Scalar> GetScalarVerified(const TPortionStorageCursor& cursor) const;

    void AddScalar(const std::shared_ptr<arrow::Scalar>& scalar);

    NKikimrColumnShardStatisticsProto::TPortionStorage SerializeToProto() const;

    static TConclusion<TPortionStorage> BuildFromProto(const NKikimrColumnShardStatisticsProto::TPortionStorage& proto) {
        TPortionStorage result;
        auto parse = result.DeserializeFromProto(proto);
        if (!parse) {
            return parse;
        }
        return result;
    }
};
}