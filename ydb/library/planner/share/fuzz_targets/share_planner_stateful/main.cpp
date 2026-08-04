#include <ydb/library/planner/share/history.h>
#include <ydb/library/planner/share/shareplanner.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <map>
#include <vector>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t MaxAccounts = 8;
constexpr size_t MaxGroups = 4;
constexpr double Eps = 1e-7;

TString ToTString(const char* value) {
    return TString(value);
}

double ConsumeAmount(FuzzedDataProvider& fdp, ui32 maxMilli = 16000) {
    return static_cast<double>(fdp.ConsumeIntegralInRange<ui32>(0, maxMilli)) / 1000.0;
}

bool NearlyEqual(double lhs, double rhs, double eps = Eps) {
    return std::fabs(lhs - rhs) <= eps * std::max({1.0, std::fabs(lhs), std::fabs(rhs)});
}

void CheckFinite(double value) {
    Y_ABORT_UNLESS(std::isfinite(value));
}

class TFuzzAccount : public NScheduling::TShareAccount {
public:
    TFuzzAccount(const TString& name, NScheduling::FWeight weight, NScheduling::FWeight maxWeight, NScheduling::TEnergy volume = 1)
        : TShareAccount(name, weight, maxWeight, volume)
    {}
};

class TFuzzGroup : public NScheduling::TShareGroup {
public:
    TFuzzGroup(const TString& name, NScheduling::FWeight weight, NScheduling::FWeight maxWeight, NScheduling::TEnergy volume = 1)
        : TShareGroup(name, weight, maxWeight, volume)
    {}
};

using TFuzzPlanner = NScheduling::TCustomSharePlanner<TFuzzAccount, TFuzzGroup>;

struct TNodeModel {
    TString Parent;
    double Weight = 1.0;
    double MaxWeight = 2.0;
    double Volume = 1.0;
    double Done = 0.0;
    ui64 DoneCount = 0;
};

TString AccountName(size_t id) {
    return "acc_" + ToString(id);
}

TString GroupName(size_t id) {
    return "grp_" + ToString(id);
}

std::vector<TString> GroupParents(const std::map<TString, TNodeModel>& groups) {
    std::vector<TString> result{ToTString("/")};
    for (const auto& [name, _] : groups) {
        result.push_back(name);
    }
    return result;
}

bool HasChildren(const TString& group, const std::map<TString, TNodeModel>& accounts, const std::map<TString, TNodeModel>& groups) {
    for (const auto& [_, node] : accounts) {
        if (node.Parent == group) {
            return true;
        }
    }
    for (const auto& [name, node] : groups) {
        if (name != group && node.Parent == group) {
            return true;
        }
    }
    return false;
}

void CheckSharePlanner(
        TFuzzPlanner& planner,
        const std::map<TString, TNodeModel>& accounts,
        const std::map<TString, TNodeModel>& groups,
        double used,
        double wasted)
{
    Y_ABORT_UNLESS(planner.GetRoot());
    Y_ABORT_UNLESS(planner.GetRoot()->GetParent() == nullptr);
    Y_ABORT_UNLESS(planner.GetRoot()->GetPlanner() == &planner);
    Y_ABORT_UNLESS(planner.AccountsCount() == accounts.size());
    Y_ABORT_UNLESS(planner.GroupsCount() == groups.size() + 1);
    Y_ABORT_UNLESS(NearlyEqual(planner.D(), used));
    Y_ABORT_UNLESS(NearlyEqual(planner.W(), wasted));
    CheckFinite(planner.dD());
    CheckFinite(planner.dW());

    NScheduling::TSharePlannerStats stats;
    planner.GetStats(stats);
    Y_ABORT_UNLESS(NearlyEqual(stats.ResUsed, used));
    Y_ABORT_UNLESS(NearlyEqual(stats.ResWasted, wasted));

    std::map<TString, double> childShareSum;
    std::map<TString, double> childWeightSum;
    std::map<TString, double> childVolumeSum;

    auto checkNodeCommon = [&](const auto* node, const TNodeModel& model) {
        Y_ABORT_UNLESS(node);
        Y_ABORT_UNLESS(node->GetPlanner() == &planner);
        Y_ABORT_UNLESS(node->GetParent());
        Y_ABORT_UNLESS(node->GetParent()->GetName() == model.Parent);
        Y_ABORT_UNLESS(NearlyEqual(node->w0(), model.Weight));
        Y_ABORT_UNLESS(NearlyEqual(node->wmax(), model.MaxWeight));
        Y_ABORT_UNLESS(NearlyEqual(node->V(), model.Volume));
        Y_ABORT_UNLESS(node->s0() > 0.0 && node->s0() <= NScheduling::gs + Eps);
        Y_ABORT_UNLESS(node->CalcGlobalShare() > 0.0 && node->CalcGlobalShare() <= NScheduling::gs + Eps);
        CheckFinite(node->D());
        CheckFinite(node->S());
        CheckFinite(node->E());
        CheckFinite(node->s0());
        CheckFinite(node->CalcGlobalShare());
        if (const auto* ctx = node->Ctx()) {
            const double dynamicWeight = ctx->GetWeight();
            CheckFinite(dynamicWeight);
            Y_ABORT_UNLESS(dynamicWeight + Eps >= node->w0());
            Y_ABORT_UNLESS(dynamicWeight <= node->wmax() + Eps);
        }
        childShareSum[model.Parent] += node->s0();
        childWeightSum[model.Parent] += model.Weight;
        childVolumeSum[model.Parent] += model.Volume;
    };

    for (const auto& [name, model] : accounts) {
        auto account = planner.FindAccountByName(name);
        checkNodeCommon(account.get(), model);
        const auto& nodeStats = account->GetStats();
        Y_ABORT_UNLESS(NearlyEqual(nodeStats.ResDone, model.Done));
        Y_ABORT_UNLESS(nodeStats.NDone == model.DoneCount);
    }

    for (const auto& [name, model] : groups) {
        auto group = planner.FindGroupByName(name);
        checkNodeCommon(group.get(), model);
    }

    for (const TString& parentName : GroupParents(groups)) {
        const auto group = planner.FindGroupByName(parentName);
        Y_ABORT_UNLESS(group);
        const bool hasChildren = childWeightSum.contains(parentName);
        Y_ABORT_UNLESS(NearlyEqual(group->GetTotalWeight(), hasChildren ? childWeightSum[parentName] : 0.0));
        Y_ABORT_UNLESS(NearlyEqual(group->GetTotalVolume(), hasChildren ? childVolumeSum[parentName] : 0.0));
        if (hasChildren) {
            Y_ABORT_UNLESS(NearlyEqual(childShareSum[parentName], NScheduling::gs));
        }
        CheckFinite(group->GetTotalCredit());
    }

    NScheduling::TSharePlannerSensors sensors;
    planner.GetSensors(sensors, true, true, true);
    Y_ABORT_UNLESS(static_cast<size_t>(sensors.AccountsSize()) == accounts.size());
    Y_ABORT_UNLESS(static_cast<size_t>(sensors.GroupsSize()) == groups.size() + 1);
    Y_ABORT_UNLESS(static_cast<size_t>(sensors.NodesSize()) == accounts.size() + groups.size() + 1);
    Y_ABORT_UNLESS(NearlyEqual(sensors.GetResUsed(), used));
    Y_ABORT_UNLESS(NearlyEqual(sensors.GetResWasted(), wasted));
}

void ConfigureSharePlanner(TFuzzPlanner& planner, FuzzedDataProvider& fdp) {
    NScheduling::TSharePlannerConfig cfg;
    cfg.SetName("planner_runtime_stateful");
    cfg.SetPull(static_cast<NScheduling::EPullType>(fdp.ConsumeIntegralInRange<int>(0, 2)));
    cfg.SetModel(static_cast<NScheduling::EPlanningModel>(fdp.ConsumeIntegralInRange<int>(0, 2)));
    cfg.SetBilling(static_cast<NScheduling::EBillingType>(fdp.ConsumeIntegralInRange<int>(0, 1)));
    cfg.SetPullLength(0.1 + ConsumeAmount(fdp, 10000));
    cfg.SetDenseLength(0.05 + ConsumeAmount(fdp, 10000));
    cfg.SetAveragingLength(0.05 + ConsumeAmount(fdp, 5000));
    cfg.SetStaticTariff(ConsumeAmount(fdp, 4000));
    cfg.SetBillingIsMemoryless(fdp.ConsumeBool());
    planner.Configure(cfg);
}

void ExerciseSharePlanner(FuzzedDataProvider& fdp) {
    TFuzzPlanner planner;
    std::map<TString, TNodeModel> accounts;
    std::map<TString, TNodeModel> groups;
    double used = 0.0;
    double wasted = 0.0;
    TStringStream savedHistory;
    bool haveHistory = false;

    ConfigureSharePlanner(planner, fdp);

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 11)) {
            case 0:
                ConfigureSharePlanner(planner, fdp);
                break;
            case 1: {
                const size_t id = fdp.ConsumeIntegralInRange<size_t>(0, MaxAccounts - 1);
                const TString name = AccountName(id);
                if (!accounts.contains(name)) {
                    const auto parents = GroupParents(groups);
                    const TString parent = parents[fdp.ConsumeIntegralInRange<size_t>(0, parents.size() - 1)];
                    const double weight = 1.0 + fdp.ConsumeIntegralInRange<ui32>(0, 15);
                    const double maxWeight = weight + 1.0 + fdp.ConsumeIntegralInRange<ui32>(0, 64);
                    const double volume = ConsumeAmount(fdp, 8000);
                    auto parentGroup = planner.FindGroupByName(parent);
                    Y_ABORT_UNLESS(parentGroup);
                    planner.Add<TFuzzAccount>(parentGroup.get(), name, weight, maxWeight, volume);
                    accounts.emplace(name, TNodeModel{parent, weight, maxWeight, volume, 0.0, 0});
                }
                break;
            }
            case 2: {
                const size_t id = fdp.ConsumeIntegralInRange<size_t>(0, MaxGroups - 1);
                const TString name = GroupName(id);
                if (!groups.contains(name)) {
                    const auto parents = GroupParents(groups);
                    const TString parent = parents[fdp.ConsumeIntegralInRange<size_t>(0, parents.size() - 1)];
                    if (parent != name) {
                        const double weight = 1.0 + fdp.ConsumeIntegralInRange<ui32>(0, 15);
                        const double maxWeight = weight + 1.0 + fdp.ConsumeIntegralInRange<ui32>(0, 64);
                        const double volume = ConsumeAmount(fdp, 8000);
                        auto parentGroup = planner.FindGroupByName(parent);
                        Y_ABORT_UNLESS(parentGroup);
                        planner.Add<TFuzzGroup>(parentGroup.get(), name, weight, maxWeight, volume);
                        groups.emplace(name, TNodeModel{parent, weight, maxWeight, volume, 0.0, 0});
                    }
                }
                break;
            }
            case 3:
                if (!accounts.empty()) {
                    auto it = std::next(accounts.begin(), fdp.ConsumeIntegralInRange<size_t>(0, accounts.size() - 1));
                    auto account = planner.FindAccountByName(it->first);
                    Y_ABORT_UNLESS(account);
                    planner.Delete(account.get());
                    accounts.erase(it);
                }
                break;
            case 4:
                if (!groups.empty()) {
                    auto it = std::next(groups.begin(), fdp.ConsumeIntegralInRange<size_t>(0, groups.size() - 1));
                    if (!HasChildren(it->first, accounts, groups)) {
                        auto group = planner.FindGroupByName(it->first);
                        Y_ABORT_UNLESS(group);
                        planner.Delete(group.get());
                        groups.erase(it);
                    }
                }
                break;
            case 5:
                planner.Clear();
                accounts.clear();
                groups.clear();
                break;
            case 6:
                if (!accounts.empty()) {
                    auto it = std::next(accounts.begin(), fdp.ConsumeIntegralInRange<size_t>(0, accounts.size() - 1));
                    auto account = planner.FindAccountByName(it->first);
                    Y_ABORT_UNLESS(account);
                    const double cost = ConsumeAmount(fdp);
                    planner.Done(account.get(), cost);
                    it->second.Done += cost;
                    ++it->second.DoneCount;
                    used += cost;
                }
                break;
            case 7: {
                const double cost = ConsumeAmount(fdp);
                planner.Waste(cost);
                wasted += cost;
                break;
            }
            case 8:
                planner.Commit(fdp.ConsumeBool());
                break;
            case 9:
                savedHistory.Clear();
                NScheduling::THistorySaver::Save(&planner, savedHistory);
                haveHistory = true;
                break;
            case 10:
                if (haveHistory) {
                    TStringInput input(savedHistory.Str());
                    Y_ABORT_UNLESS(NScheduling::THistoryLoader::Load(&planner, input));
                    planner.Commit(fdp.ConsumeBool());
                }
                break;
            default:
                if (fdp.ConsumeBool()) {
                    const TString info = planner.PrintInfo(true, fdp.ConsumeBool(), fdp.ConsumeBool(), true);
                    Y_ABORT_UNLESS(!info.empty());
                }
                break;
        }
        CheckSharePlanner(planner, accounts, groups, used, wasted);
    }
}


} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    ExerciseSharePlanner(fdp);

    return 0;
}
