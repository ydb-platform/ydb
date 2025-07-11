#pragma once

#include <util/string/builder.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql {

class TPlanPrinter {
public:
    TStringBuilder b;

    void DescribeChannel(const auto& ch, bool spilling) {
        if (spilling) {
            b << "Ch" << ch.GetId() << " [shape=diamond, label=\"Ch" << ch.GetId() << "\", color=\"red\"];";
        } else {
            b << "Ch" << ch.GetId() << " [shape=diamond, label=\"Ch" << ch.GetId() << "\"];";
        }
    }

    void PrintInputChannel(const auto& ch, const auto& type) {
        b << "Ch" << ch.GetId() << " -> T" << ch.GetDstTaskId() << " [label=" << "\"" << type << "\"];\n";
    }

    void PrintOutputChannel(const auto& ch, const auto& type) {
        b << "T" << ch.GetSrcTaskId() << " -> Ch" << ch.GetId() << " [label=" << "\"" << type << "\"];\n";
    }

    void PrintSource(auto taskId, auto sourceIndex) {
        b << "S" << taskId << "_" << sourceIndex << " -> T" << taskId << " [label=" << "\"S" << sourceIndex << "\"];\n";
    }

    void DescribeSource(auto taskId, auto sourceIndex) {
        b << "S" << taskId << "_" << sourceIndex << " ";
        b << "[shape=square, label=\"" << taskId << "/" << sourceIndex << "\"];\n";
    }

    void PrintTask(const auto& task) {
        int index = 0;
        for (const auto& input : task.GetInputs()) {
            TString inputName = "Unknown";
            bool isSource = false;
            if (input.HasUnionAll()) { inputName = "UnionAll"; }
            else if (input.HasMerge()) { inputName = "Merge"; }
            else if (input.HasSource()) { inputName = "Source"; isSource = true; }
            if (isSource) {
                PrintSource(task.GetId(), index);
            } else {
                for (const auto& ch : input.GetChannels()) {
                    PrintInputChannel(ch, inputName);
                }
            }
            index ++;
        }
        for (const auto& output : task.GetOutputs()) {
            TString outputName = "Unknown";
            if (output.HasMap()) { outputName = "Map"; }
            else if (output.HasRangePartition()) { outputName = "Range"; }
            else if (output.HasHashPartition()) { outputName = "Hash"; }
            else if (output.HasBroadcast()) { outputName = "Broadcast"; }
            // TODO: effects, sink
            for (const auto& ch : output.GetChannels()) {
                PrintOutputChannel(ch, outputName);
            }
        }
    }

    void DescribeTask(const auto& task) {
        b << "T" << task.GetId() << " [shape=circle, label=\"" << task.GetId() << "/" << task.GetStageId() << "\"];\n";
        int index = 0;
        for (const auto& input : task.GetInputs()) {
            if (input.HasSource()) {
                DescribeSource(task.GetId(), index);
            }
            index ++;
        }
        for (const auto& output : task.GetOutputs()) {
            for (const auto& ch : output.GetChannels()) {
                DescribeChannel(ch, task.GetEnableSpilling());
            }
        }
    }
 
    TString Print(const auto& tasks) {
        b.clear();
        b << "digraph G {\n";
        for (const auto& task : tasks) {
            static_assert(std::is_same_v<NDqProto::TDqTask, std::decay_t<decltype(task)>>);
            DescribeTask(task);
        }
        b << "\n";
        for (const auto& task : tasks) {
            PrintTask(task);
        }
        b << "}\n";
        return b;
    }
};
} // namespace NYql

