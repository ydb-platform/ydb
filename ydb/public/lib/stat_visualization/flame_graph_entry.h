#pragma once

#include "flame_graph_builder.h"
#include "svg_script.h"
#include "stat_visalization_error.h"

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NKikimr::NVisual {

constexpr double RECT_HEIGHT = 15;
constexpr double INTER_ELEMENT_OFFSET = 3;

// Offsets from image limits
constexpr double HORIZONTAL_OFFSET = 10;
constexpr double VERTICAL_OFFSET = 30;

constexpr double TEXT_SIDE_OFFSET = 3;
constexpr double TEXT_TOP_OFFSET = 10.5;

constexpr double VIEWPORT_WIDTH = 1200;

struct TWeight {
    explicit TWeight(ui64 self)
            : Self(self) {};
    ui64 Self;
    ui64 Total = 0;
};

struct TCombinedWeights {
    // Cpu usage from this step stats
    TWeight Cpu;
    // Output bytes of step
    TWeight Bytes;
    // Time spent
    TWeight Ms;
    // Number of tasks used
    TWeight Tasks;

    TCombinedWeights()
            : Cpu(0), Bytes(0), Ms(0), Tasks(0) {}

    TCombinedWeights(ui64 cpu, ui64 bytes, ui64 ms, ui64 tasks)
            : Cpu(cpu), Bytes(bytes), Ms(ms), Tasks(tasks) {}

    void AddSelfToTotal() {
        Cpu.Self = (Cpu.Total + Cpu.Self) ? Cpu.Self : 1;
        Bytes.Self = (Bytes.Total + Bytes.Self) ? Bytes.Self : 1;
        Ms.Self = (Ms.Total + Ms.Self) ? Ms.Self : 1;
        Tasks.Self = (Tasks.Total + Tasks.Self) ? Tasks.Self : 1;

        Cpu.Total += Cpu.Self;
        Bytes.Total += Bytes.Self;
        Ms.Total += Ms.Self;
        Tasks.Total += Tasks.Self;
    }

    TCombinedWeights operator+(const TCombinedWeights &rhs) {
        Cpu.Total += rhs.Cpu.Total;
        Bytes.Total += rhs.Bytes.Total;
        Ms.Total += rhs.Ms.Total;
        Tasks.Total += rhs.Tasks.Total;
        return *this;
    }

    TWeight operator[](EFlameGraphType type) const {
        switch (type) {
            case CPU:
                return Cpu;
            case TIME:
                return Ms;
            case BYTES_OUTPUT:
                return Bytes;
            case TASKS:
                return Tasks;
            case ALL:
                throw yexception() << "Unsupported value for FlameGraphType";
        }
    }
};

struct TTaskInfo {
    ui64 TaskId = 0;
    TMap<EFlameGraphType, double> TaskStats;
};

class TPlanGraphEntry {
public:
    TPlanGraphEntry(const TString &name, ui32 stageId,
                    ui64 weightCpu, ui64 weightBytes, ui64 weightMs, ui64 weightTasks,
                    TVector<TTaskInfo> &&taskInfo)
            : Name(name)
            , StageId(stageId)
            , Weights(weightCpu, weightBytes, weightMs, weightTasks)
            , Tasks(std::move(taskInfo)) {};

    void AddChild(THolder<TPlanGraphEntry> &&child);


    /// Builds svg for graph, starting from this node
    void SerializeToSvg(TOFStream &stream, TStringOutput &taskStream, double viewportHeight, EFlameGraphType type) const;

    /// Returns current depth of graph
    ui32 CalculateDepth(ui32 curDepth);

    /// After graph is build, we can recalculate weights considering children
    ///
    /// Not all plan entries has own statistics, for such entries we recalculate the weight as
    /// weight of all children
    TCombinedWeights CalculateWeight();

    /// Returns Svg element corresponding to current graph and calls itself recursively for children
    /// Writes stage and task elements to different streams, as we have to sort it later.
    /// Task streams should be placed in the end of svg file, to give us correct Z axis alignment
    double SerializeToSvgImpl(TOFStream &stageStream,
                              TStringOutput &taskStream,
                              double xOffset,
                              double yOffset,
                              double parentWeight,
                              double visibleWeight,
                              EFlameGraphType type,
                              double parentWidth) const;

    static TMap<EFlameGraphType, TString> &PlanGraphTypeName();

private:
    void SerializeTaskProfile(TStringOutput &taskStream,
                              double xOffset,
                              double yOffset,
                              EFlameGraphType type,
                              double parentWidth) const;

    void SerializeStage(TOFStream &stream,
                        double xOffset,
                        double yOffset,
                        EFlameGraphType type,
                        const TWeight &weight,
                        double width) const;

    static TString CutTextForAvailableWidth(const TString &text, double width);

public:
    TString Name;
    ui32 StageId;
    TCombinedWeights Weights;
    TVector<TTaskInfo> Tasks;

    TVector<THolder<TPlanGraphEntry>> Children;
};


}
