#include <ydb/core/kqp/opt/rbo/rules/map/map_output_utils.h>

namespace NKikimr {
namespace NKqp {

TVector<TInfoUnit> BuildMapOutput(const TVector<TInfoUnit>& inputOutput, const TVector<TMapElement>& elements) {
    TVector<TInfoUnit> output = inputOutput;
    TInfoUnitSet renameSources;
    for (const auto& element : elements) {
        if (element.IsRename()) {
            AddInfoUnit(renameSources, element.GetRename());
        }
    }

    if (!renameSources.empty()) {
        TVector<TInfoUnit> kept;
        kept.reserve(output.size());
        for (const auto& iu : output) {
            if (!renameSources.contains(iu)) {
                kept.push_back(iu);
            }
        }
        output = std::move(kept);
    }

    for (const auto& element : elements) {
        output.push_back(element.GetElementName());
    }

    return output;
}

TVector<TInfoUnit> BuildMapOutput(const TIntrusivePtr<TOpMap>& map, const TVector<TMapElement>& elements) {
    return BuildMapOutput(map->GetInput()->GetOutputIUs(), elements);
}

} // namespace NKqp
} // namespace NKikimr
