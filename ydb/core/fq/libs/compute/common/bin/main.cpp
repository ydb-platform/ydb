#include <ydb/core/fq/libs/compute/common/utils.h>

int main(int, char **) {
    double cpuUsage;
    TFileInput in("/home/hor911/q21plan.json");
    TString plan = in.ReadAll();
    auto processor = NFq::CreateStatProcessor("stat_tree");
    plan = processor->ConvertPlan(plan);
    {
        TUnbufferedFileOutput output("/home/hor911/q21plan_tree.json");
        output.Write(plan.c_str(), plan.size());
    }
    auto stats = processor->GetQueryStat(plan, cpuUsage);
    TUnbufferedFileOutput output("/home/hor911/q21stat_tree.json");
    output.Write(stats.c_str(), stats.size());
}
