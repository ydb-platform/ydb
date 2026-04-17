#include "partition_id.h"

#include <util/digest/multi.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ {

    size_t TPartitionId::GetHash() const {
        return MultiHash(OriginalPartitionId, WriteId, InternalPartitionId);
    }

    void TPartitionId::ToStream(IOutputStream& s) const {
        if (WriteId.Defined()) {
            s << '{' << OriginalPartitionId << ", " << *WriteId << ", " << InternalPartitionId << '}';
        } else {
            s << OriginalPartitionId;
        }
    }

    TString TPartitionId::ToString() const {
        return TStringBuilder() << *this;
    }
} // namespace NKikimr::NPQ

template <>
void Out<NKikimr::NPQ::TPartitionId>(IOutputStream& s, const NKikimr::NPQ::TPartitionId& v) {
    v.ToStream(s);
}
