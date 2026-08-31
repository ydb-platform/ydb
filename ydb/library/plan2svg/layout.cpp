#include "visualizer.h"

#include "format.h"

#include <util/generic/size_literals.h>
#include <util/stream/output.h>

namespace NPlan2Svg {

void TPlan::MarkStageIndent(ui32 indent, ui32& offsetY, std::shared_ptr<TStage> stage) {
    if (stage->IndentX < indent) {
        stage->IndentX = indent;
    }

    stage->OffsetY = offsetY;
    ui32 height = std::max<ui32>(
        (   (stage->EgressBytes != nullptr) + (stage->OutputBytes != nullptr)
            + 2 /* MEM, CPU */
            + stage->Connections.size() + stage->BuiltInIngress
        ),
        stage->Operators.size()
    ) * (INTERNAL_HEIGHT + INTERNAL_GAP_Y) + INTERNAL_GAP_Y;

    stage->Height = height;
    stage->IndentY = stage->OffsetY + GAP_Y + height;
    offsetY += GAP_Y + height;

    if (stage->Connections.size() > 1) {
        indent += (INDENT_X + GAP_X);
    }

    for (auto c : stage->Connections) {
        if (c->CteConnection) {
            c->CteIndentX = indent;
            offsetY += GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2;
            stage->IndentY = std::max(stage->IndentY, offsetY);
        } else {
            MarkStageIndent(indent, offsetY, c->FromStage);
            stage->IndentY = std::max(stage->IndentY, c->FromStage->IndentY);
        }
    }

    Height = std::max(Height, stage->IndentY);
}

void TPlan::MarkLayout() {
    Height = 0;
    if (!Stages.empty()) {
        ui32 offsetY = 0;
        MarkStageIndent(0, offsetY, Stages.front());
    }
    if (!Nodes.empty()) {
        ui32 nodeOffsetY = GAP_Y + INTERNAL_HEIGHT + INTERNAL_GAP_Y * 2;
        Height += nodeOffsetY; // only node header
        for (auto& node : Nodes) {
            node->OffsetY = nodeOffsetY;
            node->Height =
                (   // (node->OutputBytes != nullptr)
                    + 4 /* 3xMEM, CPU */
                    // + (node->InputBytes != nullptr)
                    // + (node->IngressBytes != nullptr)
                ) * (INTERNAL_HEIGHT + INTERNAL_GAP_Y) + INTERNAL_GAP_Y;
            nodeOffsetY += (GAP_Y + node->Height);
        }
        NodeIndentY = nodeOffsetY;
    }
}

struct TCpuSample {
    ui64 Time;
    ui64 Value;
    ui32 StageId;
};

// A critical path is a chain of stages, each reached from the one before it
// through a single connection. Which connection that is depends on what is being
// measured, so the caller names the TStage member holding it. The result is the
// group id list the SVG uses to highlight the whole chain at once.
static TString CriticalPathGroups(const std::vector<std::shared_ptr<TStage>>& stages,
    std::shared_ptr<TConnection> TStage::* critical)
{
    TStringBuilder builder;
    if (!stages.empty()) {
        auto* stage = stages[0].get();
        builder << 'g' << stage->GroupId;
        while (stage) {
            auto& connection = stage->*critical;
            if (connection) {
                builder << ",g" << connection->GroupId;
                stage = connection->FromStage.get();
                builder << ",g" << stage->GroupId;
            } else {
                stage = nullptr;
            }
        }
    }
    return builder;
}

TString TPlan::GetCriticalCpuGroups() {
    return CriticalPathGroups(Stages, &TStage::CriticalCpuConnection);
}

TString TPlan::GetCriticalTimeGroups() {
    return CriticalPathGroups(Stages, &TStage::CriticalTimeConnection);
}

void TPlan::CalcCriticals(TStage& stage) {
    if (stage.CriticalCpuTotal == 0 && stage.CriticalTimeTotal == 0) {
        for (auto& connection : stage.Connections) {
            if (connection->FromStage) {
                CalcCriticals(*connection->FromStage);
                if (!stage.CriticalCpuConnection || stage.CriticalCpuTotal < connection->FromStage->CriticalCpuTotal) {
                    stage.CriticalCpuConnection = connection;
                    stage.CriticalCpuTotal = connection->FromStage->CriticalCpuTotal;
                }
                if (!stage.CriticalTimeConnection || stage.CriticalTimeTotal < connection->FromStage->CriticalTimeTotal) {
                    stage.CriticalTimeConnection = connection;
                    stage.CriticalTimeTotal = connection->FromStage->CriticalTimeTotal;
                }
            }
        }
        if (stage.CpuTime) {
            auto cpu = stage.CpuTime->Details.Sum;
            if (stage.Tasks) {
                cpu /= stage.Tasks;
            }
            stage.CriticalCpuTotal += cpu;
        }
        stage.CriticalTimeTotal += stage.MaxTime - stage.MinTime;
    }
}

void TPlan::CalcHotPath() {
    std::vector<TCpuSample> cpuTimes;
    std::unordered_map<ui32, TStage*> StageIdToStage;
    if (!Stages.empty()) {
        CalcCriticals(*Stages[0]);
    }
    for (auto s : Stages) {
        if (!s->External && s->CpuTime && s->Tasks && !s->CpuTime->History.Values.empty()) {
            auto stageId = s->PhysicalStageId;
            StageIdToStage.emplace(stageId, s.get());
            for (const auto& [t, v] : s->CpuTime->History.Values) {
                cpuTimes.push_back(TCpuSample{.Time = t, .Value = v / s->Tasks, .StageId = stageId});
            }
            cpuTimes.push_back(TCpuSample{.Time = s->CpuTime->History.Values.back().first + 1, .Value = 0, .StageId = stageId});
        }
    }
    if (cpuTimes.size() < 2) {
        return;
    }
    std::sort(cpuTimes.begin(), cpuTimes.end(), [](const TCpuSample& a, const TCpuSample& b) { return a.Time < b.Time; });
    auto first = true;
    ui32 currentStageId = 0;
    ui64 leftTime = 0;
    std::unordered_map<ui32, ui64> cpuPerStageTask;
    for (const auto& tvs : cpuTimes) {
        if (cpuPerStageTask.contains(tvs.StageId)) {
            cpuPerStageTask[tvs.StageId] = tvs.Value;
        } else {
            cpuPerStageTask.emplace(tvs.StageId, tvs.Value);
        }
        if (first) {
            currentStageId = tvs.StageId;
            leftTime = tvs.Time;
            first = false;
        } else {
            if (leftTime == tvs.Time) {
                continue;
            }
            ui32 hotStageId = 0;
            ui64 hotStageCpu = 0;
            for (const auto& [stageId, cpu] : cpuPerStageTask) {
                if (cpu >= hotStageCpu) {
                    hotStageCpu = cpu;
                    hotStageId = stageId;
                }
            }
            if (currentStageId != hotStageId) {
                StageIdToStage.at(currentStageId)->HotRegions.emplace_back(leftTime, tvs.Time);
                currentStageId = hotStageId;
                leftTime = tvs.Time;
            }
        }
    }
    auto& last = cpuTimes.back();
    if (last.Time != leftTime) {
        StageIdToStage.at(currentStageId)->HotRegions.emplace_back(leftTime, last.Time);
    }
}

} // namespace NPlan2Svg
