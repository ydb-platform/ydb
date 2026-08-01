/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef REPORT_H
#define REPORT_H

#include <sstream>
#include <string>

#include "meta_analysis.h"
#include "experiments.h"
#include "experimentconf.h"
#include "space.h"

namespace similarity {
  using std::string;
  using std::endl;
  using std::stringstream;

  template <class dist_t>
  inline string produceHumanReadableReport(
                    const ExperimentConfig<dist_t>& config,
                    MetaAnalysis& ExpRes,
                    const string& MethDescStr,
                    const string& IndexParamStr,
                    const string& QueryTimeParamStr) {
    stringstream  Print;

    Print << std::endl << 
            "===================================" << std::endl;
    Print << MethDescStr << std::endl;
    Print << "Index parameters:      " << IndexParamStr     << std::endl;
    Print << "Query-time parameters: " << QueryTimeParamStr << std::endl;
    Print << "===================================" << std::endl;
    Print << "# of points: " << config.GetDataObjects().size() << std::endl;
    Print << "# of queries: " << config.GetQueryToRunQty() << std::endl;
    Print << "------------------------------------" << std::endl;
    Print << "Recall:            " << round3(ExpRes.GetRecallAvg())  << " -> " << "[" << round3(ExpRes.GetRecallConfMin()) << " " << round3(ExpRes.GetRecallConfMax()) << "]" << std::endl;
    Print << "Recall@1:          " << round3(ExpRes.GetRecallAt1Avg())  << " -> " << "[" << round3(ExpRes.GetRecallAt1ConfMin()) << " " << round3(ExpRes.GetRecallAt1ConfMax()) << "]" << std::endl;
    Print << "PrecisionOfApprox: " << round3(ExpRes.GetPrecisionOfApproxAvg())                << " -> " << "[" << round3(ExpRes.GetPrecisionOfApproxConfMin()) << " " << round3(ExpRes.GetPrecisionOfApproxConfMax()) << "]" << std::endl;
    Print << "ClassAccuracy:     " << ExpRes.GetClassAccuracyAvg() << " -> " << "[" << ExpRes.GetClassAccuracyConfMin() << " " << ExpRes.GetClassAccuracyConfMax() << "]" << std::endl;
    Print << "RelPosError:       " << round2(ExpRes.GetRelPosErrorAvg())  << " -> " << "[" << round2(ExpRes.GetRelPosErrorConfMin()) << " \t" << round2(ExpRes.GetRelPosErrorConfMax()) << "]" << std::endl;
    Print << "NumCloser:         " << round2(ExpRes.GetNumCloserAvg())    << " -> " << "[" << round2(ExpRes.GetNumCloserConfMin()) << " \t" << round2(ExpRes.GetNumCloserConfMax()) << "]" << std::endl;
    Print << "------------------------------------" << std::endl;
    Print << "QueryTime:         " << round2(ExpRes.GetQueryTimeAvg())    << " -> " << "[" << round2(ExpRes.GetQueryTimeConfMin()) << " \t" << round2(ExpRes.GetQueryTimeConfMax()) << "]" << std::endl;
    Print << "QueryPerSec:       " << round2(ExpRes.GetQueryPerSecAvg())    << " -> " << "[" << round2(ExpRes.GetQueryPerSecConfMin()) << " \t" << round2(ExpRes.GetQueryPerSecConfMax()) << "]" << std::endl;
    Print << "DistComp:          " << round2(ExpRes.GetDistCompAvg())     << " -> " << "[" << round2(ExpRes.GetDistCompConfMin()) << " \t" << round2(ExpRes.GetDistCompConfMax()) << "]" << std::endl;
    Print << "------------------------------------" << std::endl;
    Print << "ImprEfficiency:    " << round2(ExpRes.GetImprEfficiencyAvg()) << " -> " << "["  <<  round2(ExpRes.GetImprEfficiencyConfMin()) << " \t" << round2(ExpRes.GetImprEfficiencyConfMax()) << "]" << std::endl;
    Print << "ImprDistComp:      " << round2(ExpRes.GetImprDistCompAvg()) << " -> " << "[" << round2(ExpRes.GetImprDistCompAvg()) << " \t"<< round2(ExpRes.GetImprDistCompConfMax()) << "]" << std::endl;
    Print << "------------------------------------" << std::endl;
    Print << "Memory Usage:      " << round2(ExpRes.GetMemAvg()) << " MB" << std::endl;
    Print << "Index Time:        " << round2(ExpRes.GetIndexTimeAvg()) << " sec" << std::endl;
    Print << "------------------------------------" << std::endl;
    
    return Print.str();
  }

};

#endif
