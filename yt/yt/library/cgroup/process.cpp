#include "process.h"

#include <util/stream/file.h>

#include <util/string/split.h>
#include <util/string/vector.h>

namespace NYT::NCGroups {

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, std::string> ParseProcessCGroups(const TString& str)
{
    THashMap<std::string, std::string> result;

    // /proc/<pid>/cgroup format:
    //   v1: "<hierarchy_id>:<subsystems>:<path>" — one line per controller.
    //   v2: "0::<path>" — single line, empty subsystems field.
    for (const auto& line : SplitString(str, "\n")) {
        std::vector<std::string> tokens = StringSplitter(line).Split(':').Limit(3);
        if (tokens.size() != 3) {
            continue;
        }

        const auto& subsystemsSet = tokens[1];
        auto name = tokens[2];
        if (subsystemsSet.empty()) {
            // v2 unified hierarchy.
            result[""] = name;
            continue;
        }

        std::vector<std::string> subsystems;
        StringSplitter(subsystemsSet.data()).Split(',').SkipEmpty().Collect(&subsystems);
        for (const auto& subsystem : subsystems) {
            // Skip named hierarchies without controllers.
            if (!subsystem.starts_with("name=")) {
                result[subsystem] = name;
            }
        }
    }

    return result;
}

THashMap<std::string, std::string> GetProcessCGroups(pid_t pid)
{
    auto rawCgroups = TFileInput(Format("/proc/%v/cgroup", pid)).ReadAll();
    return ParseProcessCGroups(rawCgroups);
}

THashMap<std::string, std::string> GetSelfProcessCGroups()
{
    auto rawCgroups = TFileInput("/proc/self/cgroup").ReadAll();
    return ParseProcessCGroups(rawCgroups);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroups
