#pragma once
#include "abstract.h"

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NArrow::NSSA {

class TProgramChain {
private:
    std::vector<TResourceProcessorStep> Processors;
    THashMap<ui32, THashSet<ui32>> SourcesByColumnId;
    THashSet<ui32> SourceColumns;
    THashSet<ui32> FilterColumns;

    [[nodiscard]] TConclusionStatus Initialize();
    YDB_READONLY_DEF(std::optional<ui32>, LastOriginalDataFilter);
    YDB_READONLY_DEF(std::optional<ui32>, FirstAggregation);

public:
    TProgramChain() = default;

    bool HasAggregations() const {
        return !!FirstAggregation;
    }

    bool IsGenerated(const ui32 columnId) const {
        auto it = SourcesByColumnId.find(columnId);
        AFL_VERIFY(it != SourcesByColumnId.end());
        return it->second.size() != 1 || !it->second.contains(columnId);
    }

    const std::vector<TResourceProcessorStep>& GetProcessors() const {
        return Processors;
    }

    const THashSet<ui32>& GetSourceColumns() const {
        return SourceColumns;
    }

    const THashSet<ui32>& GetFilterColumns() const {
        return FilterColumns;
    }

    TString DebugString() const {
        return DebugJson().GetStringRobust();
    }

    NJson::TJsonValue DebugJson() const;

    class TBuilder {
    private:
        std::vector<std::shared_ptr<IResourceProcessor>> Processors;
        const IColumnResolver& Resolver;
        bool Finished = false;

    public:
        TBuilder(const IColumnResolver& resolver)
            : Resolver(resolver) {
        }

        void Add(const std::shared_ptr<IResourceProcessor>& processor) {
            AFL_VERIFY(!Finished);
            Processors.emplace_back(processor);
        }

        TConclusion<std::shared_ptr<TProgramChain>> Finish() {
            AFL_VERIFY(!Finished);
            Finished = true;
            auto result = TProgramChain::Build(std::move(Processors), Resolver);
            if (result.IsFail()) {
                return result;
            }
            return std::make_shared<TProgramChain>(result.DetachResult());
        }
    };

    [[nodiscard]] TConclusionStatus Apply(const std::shared_ptr<TAccessorsCollection>& resources) const;

    static TConclusion<TProgramChain> Build(std::vector<std::shared_ptr<IResourceProcessor>>&& processors, const IColumnResolver& resolver);
};

}   // namespace NKikimr::NArrow::NSSA
