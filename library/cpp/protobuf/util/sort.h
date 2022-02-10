#pragma once

#include <google/protobuf/message.h> 

#include <util/generic/vector.h>
#include <util/generic/algorithm.h>

namespace NProtoBuf {
    // TComparePtr is something like:
    // typedef bool (*TComparePtr)(const Message* msg1, const Message* msg2);
    // typedef bool (*TComparePtr)(const TProto* msg1, const TProto* msg2);

    template <typename TProto, typename TComparePtr>
    void SortMessages(RepeatedPtrField<TProto>& msgs, TComparePtr cmp) {
        TVector<TProto*> ptrs;
        ptrs.reserve(msgs.size());
        while (msgs.size()) {
            ptrs.push_back(msgs.ReleaseLast());
        }

        ::StableSort(ptrs.begin(), ptrs.end(), cmp);

        for (size_t i = 0; i < ptrs.size(); ++i) {
            msgs.AddAllocated(ptrs[i]);
        }
    }

}
