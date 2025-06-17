#include "kafka_state_name_to_int.h"

namespace NKafka {
    const std::map<int, TString> numbersToStatesMapping = {{0, "Unknown"},
                                                    {1, "PreparingRebalance"},
                                                    {2, "CompletingRebalance"},
                                                    {3, "Stable"},
                                                    {4, "Dead"},
                                                    {5, "Empty"}};

    const std::map<TString, int> statesToNumbersMapping = {{"Unknown", 0},
                                                    {"PreparingRebalance", 1},
                                                    {"CompletingRebalance", 2},
                                                    {"Stable", 3},
                                                    {"Dead", 4},
                                                    {"Empty", 5}};
}
