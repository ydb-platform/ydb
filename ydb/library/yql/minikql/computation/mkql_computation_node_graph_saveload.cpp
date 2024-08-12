#include "mkql_computation_node_graph_saveload.h"
#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/minikql/pack_num.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

void TraverseGraph(const NUdf::TUnboxedValue* roots, ui32 rootCount, TVector<NUdf::TUnboxedValue>& values) {
    THashSet<NUdf::IBoxedValue*> dedup;

    for (ui32 i = 0; i < rootCount; ++i) {
        const auto& value = roots[i];
        if (!value.IsBoxed()) {
            continue;
        }
        auto* ptr = value.AsBoxed().Get();
        if (dedup.contains(ptr)) {
            continue;
        }
        dedup.insert(ptr);
        values.push_back(value);
    }

    for (ui32 from = 0, to = values.size(); from != to; ++from) {
        auto current = values[from];
        auto count = current.GetTraverseCount();

        for (ui32 i = 0; i < count; ++i) {
            auto value = current.GetTraverseItem(i);
            if (!value.IsBoxed()) {
                continue;
            }
            auto* ptr = value.AsBoxed().Get();
            if (dedup.contains(ptr)) {
                continue;
            }
            dedup.insert(ptr);
            values.push_back(value);
            ++to;
        }
    }
}

}

void SaveGraphState(const NUdf::TUnboxedValue* roots, ui32 rootCount, ui64 hash, TString& out) {
    out.clear();
    out.AppendNoAlias((const char*)&hash, sizeof(hash));

    TVector<NUdf::TUnboxedValue> values;
    TraverseGraph(roots, rootCount, values);

    for (ui32 i = 0; i < values.size(); ++i) {
        auto state = values[i].Save();
        if (state.IsString() || state.IsEmbedded()) {
            auto strRef = state.AsStringRef();
            auto size = strRef.Size();
            WriteUi64(out, size);
            if (size) {
                out.AppendNoAlias(strRef.Data(), size);
            }
        }
        else if (state.IsBoxed()) {
            TString taskState;
            auto listIt = state.GetListIterator();
            NUdf::TUnboxedValue str;
            while (listIt.Next(str)) {
                const TStringBuf strRef = str.AsStringRef();
                taskState.AppendNoAlias(strRef.Data(), strRef.Size());
            }
            WriteUi64(out, taskState.size());
            if (!taskState.empty()) {
                out.AppendNoAlias(taskState.Data(), taskState.size());
            }
        }
    }
}

void LoadGraphState(const NUdf::TUnboxedValue* roots, ui32 rootCount, ui64 hash, const TStringBuf& in) {
    TStringBuf state(in);

    MKQL_ENSURE(state.size() >= sizeof(ui64), "Serialized state is corrupted - no hash");
    ui64 storedHash = *(ui64*)state.data();
    state.Skip(sizeof(storedHash));

    MKQL_ENSURE(hash == storedHash, "Unable to load graph state, different hashes");

    TVector<NUdf::TUnboxedValue> values;
    TraverseGraph(roots, rootCount, values);

    for (ui32 i = 0; i < values.size(); ++i) {
        auto size = ReadUi64(state);
        if (size) {
            MKQL_ENSURE(size <= state.size(), "Serialized state is corrupted");
            values[i].Load(NUdf::TStringRef(state.data(), size));
            state.Skip(size);
        }
    }

    MKQL_ENSURE(state.size() == 0, "State was not loaded correctly");
}

} // namespace NMiniKQL
} // namespace NKikimr
