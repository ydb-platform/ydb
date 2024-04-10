#pragma once
#include <ydb/library/accessor/accessor.h>
#include <vector>
#include <set>

namespace NKikimr::NOlap::NStatistics {
enum class EType {
    Undefined /* "undefined" */,
    Max /* "max" */,
    Variability /* "variability" */
};

class TIdentifier {
private:
    YDB_READONLY(EType, Type, EType::Undefined);
    YDB_READONLY_DEF(std::vector<ui32>, EntityIds);
public:
    TIdentifier(const EType type, const std::vector<ui32>& entities);

    bool operator<(const TIdentifier& item) const;
    bool operator==(const TIdentifier& item) const;
};

}