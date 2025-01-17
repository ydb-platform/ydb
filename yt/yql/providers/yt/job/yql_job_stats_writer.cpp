#include "yql_job_stats_writer.h"
#include "yql_job_base.h"

#include <yt/cpp/mapreduce/library/user_job_statistics/user_job_statistics.h>
#include <yt/cpp/mapreduce/common/helpers.h>

using NKikimr::NMiniKQL::IStatsRegistry;
using NKikimr::NMiniKQL::TStatKey;

namespace NYql {

void WriteJobStats(const IStatsRegistry* stats, const TJobCountersProvider& countersProvider, IOutputStream* out) {
    NYtTools::TUserJobStatsProxy statsWriter;
    statsWriter.Init(out);
    auto& stream = *statsWriter.GetStream();

    stats->ForEachStat([&stream](const TStatKey& key, i64 value) {
        auto node = NYT::TNode{}(TString(key.GetName()), value);
        stream << NYT::NodeToYsonString(node, NYson::EYsonFormat::Text) << ";\n";
    });

    for (const auto& x : countersProvider.Counters_) {
        auto moduleMap = NYT::TNode{}(x.first.second, x.second);
        auto counterMap = NYT::TNode{}(x.first.first, moduleMap);
        auto udfMap = NYT::TNode{}("Counter", counterMap);
        auto node = NYT::TNode{}("Udf", udfMap);
        stream << NYT::NodeToYsonString(node, NYson::EYsonFormat::Text) << ";\n";
    }

    for (const auto& x : countersProvider.Probes_) {
        auto moduleMap = NYT::TNode{}(x.first.second, i64(1000.0 * x.second.TotalCycles / GetCyclesPerMillisecond()));
        auto timeMap = NYT::TNode{}(x.first.first, moduleMap);
        auto udfMap = NYT::TNode{}("TimeUsec", timeMap);
        auto node = NYT::TNode{}("Udf", udfMap);
        stream << NYT::NodeToYsonString(node, NYson::EYsonFormat::Text) << ";\n";
    }

    stream.Flush();
}

} // namspace NYql
