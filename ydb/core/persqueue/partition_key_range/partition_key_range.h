#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/maybe.h>

// forward declarations
namespace NKikimrPQ {
    class TPartitionKeyRange;
}

namespace NKikimr {
namespace NPQ {

struct TPartitionKeyRange {
    TMaybe<TSerializedCellVec> FromBound; // inclusive
    TMaybe<TSerializedCellVec> ToBound; // exclusive

    static TPartitionKeyRange Parse(const NKikimrPQ::TPartitionKeyRange& proto);
    void Serialize(NKikimrPQ::TPartitionKeyRange& proto) const;

private:
    static void ParseBound(const TString& data, TMaybe<TSerializedCellVec>& bound);

}; // TPartitionKeyRange

} // NPQ
} // NKikimr
