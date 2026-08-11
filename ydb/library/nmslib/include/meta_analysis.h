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
#ifndef _META_ANALYSIS_H
#define _META_ANALYSIS_H

#include <vector>
#include <string>

#include "utils.h"

namespace similarity {

using std::vector;
using std::string;

/*
 * A helper class to integrate results
 */
class MetaAnalysis {
public:
  MetaAnalysis(size_t TestSetQty, double zVal = 1.96) : zVal_(zVal) {
    PrecisionOfApprox_        .resize(TestSetQty);
    Recall_         .resize(TestSetQty);
    ClassAccuracy_  .resize(TestSetQty);
    LogRelPosError_ .resize(TestSetQty);
    NumCloser_      .resize(TestSetQty);
    RecallAt1_      .resize(TestSetQty);
    QueryTime_      .resize(TestSetQty);
    DistComp_       .resize(TestSetQty);
    ImprEfficiency_ .resize(TestSetQty);
    ImprDistComp_   .resize(TestSetQty);
    Mem_            .resize(TestSetQty);
    IndexTime_      .resize(TestSetQty);
    LoadTime_       .resize(TestSetQty);
    SaveTime_       .resize(TestSetQty);
    QueryPerSec_    .resize(TestSetQty);
  }

  // Let's protect Add* functions, b/c them can be called from different threads  
  void AddRecall(size_t SetId, double Recall) {
    Recall_[SetId].push_back(Recall);
  }
  void AddPrecisionOfApprox(size_t SetId, double PrecisionOfApprox) {
    PrecisionOfApprox_[SetId].push_back(PrecisionOfApprox);
  }
  void AddClassAccuracy(size_t SetId, double ClassAccuracy) {
    ClassAccuracy_[SetId].push_back(ClassAccuracy);
  }
  void AddLogRelPosError(size_t SetId, double LogRelPosError) {
    LogRelPosError_[SetId].push_back(LogRelPosError);
  }
  void AddNumCloser(size_t SetId, double NumCloser) {
    NumCloser_[SetId].push_back(NumCloser);
  }
  void AddRecallAt1(size_t SetId, double RecallAt1) {
    RecallAt1_[SetId].push_back(RecallAt1);
  }
  void AddQueryTime(size_t SetId, double QueryTime) {
    QueryTime_[SetId].push_back(QueryTime);
  }
  void AddDistComp(size_t SetId, double DistComp) {
    DistComp_[SetId].push_back(DistComp);
  }
  void SetMem(size_t SetId, double Mem) {
    Mem_[SetId] = Mem;
  }
  void SetIndexTime(size_t SetId, double IndexTime) {
    IndexTime_[SetId] = IndexTime;
  }
  void SetLoadTime(size_t SetId, double LoadTime) {
    LoadTime_[SetId] = LoadTime;
  }
  void SetSaveTime(size_t SetId, double SaveTime) {
    SaveTime_[SetId] = SaveTime;
  }
  void SetQueryPerSec(size_t SetId, double QueryPerSec) {
    QueryPerSec_[SetId] = QueryPerSec;
  }
  void SetImprEfficiency(size_t SetId, double ImprEfficiency) {
    ImprEfficiency_[SetId] = ImprEfficiency;
  }
  void SetImprDistComp(size_t SetId, double ImprDistComp) {
    ImprDistComp_[SetId] = ImprDistComp;
  }

  void ComputeAll() {
    ComputeOneSimple("Recall", Recall_, RecallAvg, RecallConfMin, RecallConfMax);
    ComputeOneSimple("PrecisionOfApprox", PrecisionOfApprox_, PrecisionOfApproxAvg, PrecisionOfApproxConfMin, PrecisionOfApproxConfMax);
    ComputeOneSimple("ClassAccuracy", ClassAccuracy_, ClassAccuracyAvg, ClassAccuracyConfMin, ClassAccuracyConfMax);
    ComputeOneSimple("LogRelPosError", LogRelPosError_, LogRelPosErrorAvg, LogRelPosErrorConfMin, LogRelPosErrorConfMax);
    ComputeOneSimple("NumCloser", NumCloser_, NumCloserAvg, NumCloserConfMin, NumCloserConfMax);
    ComputeOneSimple("RecallAt1", RecallAt1_, RecallAt1Avg, RecallAt1ConfMin, RecallAt1ConfMax);
    ComputeOneMeta("QueryTime", QueryTime_, QueryTimeAvg, QueryTimeConfMin, QueryTimeConfMax);
    ComputeOneMeta("DistComp", DistComp_, DistCompAvg, DistCompConfMin, DistCompConfMax);

    ComputeOneSimple("ImprEfficiency", ImprEfficiency_, ImprEfficiencyAvg, ImprEfficiencyConfMin, ImprEfficiencyConfMax);
    ComputeOneSimple("ImprDistComp", ImprDistComp_, ImprDistCompAvg, ImprDistCompConfMin, ImprDistCompConfMax);
    ComputeOneSimple("MemUsage", Mem_, MemAvg, MemConfMin, MemConfMax);
    ComputeOneSimple("IndexTime", IndexTime_, IndexTimeAvg, IndexTimeConfMin, IndexTimeConfMax);
    ComputeOneSimple("LoadTime", LoadTime_, LoadTimeAvg, LoadTimeConfMin, LoadTimeConfMax);
    ComputeOneSimple("SaveTime", SaveTime_, SaveTimeAvg, SaveTimeConfMin, SaveTimeConfMax);
    ComputeOneSimple("QueryPerSec", QueryPerSec_, QueryPerSecAvg, QueryPerSecConfMin, QueryPerSecConfMax);
  }

  double GetRecallAvg() const { return RecallAvg;} 
  double GetRecallConfMin() const{return RecallConfMin;}; 
  double GetRecallConfMax() const { return RecallConfMax;}

  double GetPrecisionOfApproxAvg() const { return PrecisionOfApproxAvg;} 
  double GetPrecisionOfApproxConfMin() const{return PrecisionOfApproxConfMin;}; 
  double GetPrecisionOfApproxConfMax() const { return PrecisionOfApproxConfMax;}

  double GetClassAccuracyAvg() const { return ClassAccuracyAvg;} 
  double GetClassAccuracyConfMin() const{return ClassAccuracyConfMin;}; 
  double GetClassAccuracyConfMax() const { return ClassAccuracyConfMax;}

  double GetRelPosErrorAvg() const { return exp(LogRelPosErrorAvg);} 
  double GetRelPosErrorConfMin() const{return exp(LogRelPosErrorConfMin);}; 
  double GetRelPosErrorConfMax() const { return exp(LogRelPosErrorConfMax);}

  double GetNumCloserAvg() const { return NumCloserAvg;} 
  double GetNumCloserConfMin() const{return NumCloserConfMin;}; 
  double GetNumCloserConfMax() const { return NumCloserConfMax;}

  double GetRecallAt1Avg() const { return RecallAt1Avg;} 
  double GetRecallAt1ConfMin() const{return RecallAt1ConfMin;}; 
  double GetRecallAt1ConfMax() const { return RecallAt1ConfMax;}

  double GetImprEfficiencyAvg() const { return ImprEfficiencyAvg;} 
  double GetImprEfficiencyConfMin() const{return ImprEfficiencyConfMin;}; 
  double GetImprEfficiencyConfMax() const { return ImprEfficiencyConfMax;}

  double GetImprDistCompAvg() const { return ImprDistCompAvg;} 
  double GetImprDistCompConfMin() const{return ImprDistCompConfMin;}; 
  double GetImprDistCompConfMax() const { return ImprDistCompConfMax;}

  double GetMemAvg() const { return MemAvg;} 
  double GetMemConfMin() const{return MemConfMin;}; 
  double GetMemConfMax() const { return MemConfMax;}

  double GetIndexTimeAvg() const { return IndexTimeAvg;} 
  double GetIndexTimeConfMin() const{return IndexTimeConfMin;}; 
  double GetIndexTimeConfMax() const { return IndexTimeConfMax;}

  double GetLoadTimeAvg() const { return LoadTimeAvg;} 
  double GetLoadTimeConfMin() const{return LoadTimeConfMin;}; 
  double GetLoadTimeConfMax() const { return LoadTimeConfMax;}

  double GetSaveTimeAvg() const { return SaveTimeAvg;} 
  double GetSaveTimeConfMin() const{return SaveTimeConfMin;}; 
  double GetSaveTimeConfMax() const { return SaveTimeConfMax;}

  double GetQueryPerSecAvg() const { return QueryPerSecAvg;} 
  double GetQueryPerSecConfMin() const{return QueryPerSecConfMin;}; 
  double GetQueryPerSecConfMax() const { return QueryPerSecConfMax;}

  double GetQueryTimeAvg() const { return QueryTimeAvg;} 
  double GetQueryTimeConfMin() const{return QueryTimeConfMin;}; 
  double GetQueryTimeConfMax() const { return QueryTimeConfMax;}

  double GetDistCompAvg() const { return DistCompAvg;} 
  double GetDistCompConfMin() const{return DistCompConfMin;}; 
  double GetDistCompConfMax() const { return DistCompConfMax;}
private:
double RecallAvg, RecallConfMin, RecallConfMax;
double PrecisionOfApproxAvg, PrecisionOfApproxConfMin, PrecisionOfApproxConfMax;
double ClassAccuracyAvg, ClassAccuracyConfMin, ClassAccuracyConfMax;
double LogRelPosErrorAvg, LogRelPosErrorConfMin, LogRelPosErrorConfMax;
double NumCloserAvg, NumCloserConfMin, NumCloserConfMax;
double RecallAt1Avg, RecallAt1ConfMin, RecallAt1ConfMax;
double QueryTimeAvg, QueryTimeConfMin, QueryTimeConfMax;
double DistCompAvg, DistCompConfMin, DistCompConfMax;
double ImprEfficiencyAvg, ImprEfficiencyConfMin, ImprEfficiencyConfMax;
double ImprDistCompAvg, ImprDistCompConfMin, ImprDistCompConfMax;
double MemAvg, MemConfMin, MemConfMax;
double IndexTimeAvg, IndexTimeConfMin, IndexTimeConfMax;
double LoadTimeAvg, LoadTimeConfMin, LoadTimeConfMax;
double SaveTimeAvg, SaveTimeConfMin, SaveTimeConfMax;
double QueryPerSecAvg, QueryPerSecConfMin, QueryPerSecConfMax;
double zVal_;

vector<vector<double>>   Recall_; 
vector<vector<double>>   PrecisionOfApprox_; 
vector<vector<double>>   ClassAccuracy_; 
vector<vector<double>>   LogRelPosError_; 
vector<vector<double>>   NumCloser_; 
vector<vector<double>>   RecallAt1_; 
vector<vector<double>>   QueryTime_; 
vector<vector<double>>   DistComp_; 
vector<double>           ImprEfficiency_; 
vector<double>           ImprDistComp_; 
vector<double>           Mem_; 
vector<double>           IndexTime_; 
vector<double>           LoadTime_; 
vector<double>           SaveTime_; 
vector<double>           QueryPerSec_; 

MetaAnalysis(){} // be private!

void ComputeOneSimple(const string &Name, 
                    const vector<vector<double>>& vals, double& avg, double& ConfMin, double& ConfMax) {
  vector<double> avgVals;
  for (size_t i = 0; i < vals.size(); ++i) {
    avgVals.push_back(Mean(&vals[i][0], vals[i].size()));
  }
  ComputeOneSimple(Name, avgVals, avg, ConfMin, ConfMax);
}

void ComputeOneSimple(const string& Name, 
                      const vector<double>& vals, double& avg, double& ConfMin, double& ConfMax) {
  avg = Mean(&vals[0], vals.size());
#if 0
    LOG(INFO) << ".... " << Name  << " " << avg << " qty: " << vals.size();
#endif
  double var = vals.size()> 1 ? Variance(&vals[0], vals.size(), avg) : 0;
  double sigma = sqrt(var/vals.size());
  // 5% confidence interval (assuming normal distrbution).
  ConfMin = avg - zVal_ * sigma;
  ConfMax = avg + zVal_ * sigma;
}

void ComputeOneMeta(const string &Name, 
                    const vector<vector<double>>& vals, double& avg, double& ConfMin, double& ConfMax) {
  avg = 0;

  /*
   * Fixed-effect meta-analysis.
   * See L. Hedges,  and J. Vevea, 1998, Fixed and Random-Effects Models in Meta-Analysis 
   *
   */

  double SumW = 0;

  for (size_t i = 0; i < vals.size(); ++i) {
    double mean = Mean(&vals[i][0], vals[i].size());

#if 0
    LOG(INFO) << ".... " << Name  << " " << mean << " qty: " << vals[i].size();
#endif
    /* 
     * Sometimes variance is very small or zero,
     * this causes some kind of a numerical *flow.
     * Let's pretend the variance is never too small.
     */
    double var = std::max(1e-5, Variance(&vals[i][0], vals[i].size(), mean));
    var /= vals[i].size();
    double w = 1/var;
    SumW += w;
    avg += w * mean;
  }

  double sigma = sqrt(1/SumW);
  avg /= SumW;

#if 0
    LOG(INFO) << ".... " << Name  << " mean: " << avg << " SumW: " << SumW;
#endif

  // 5% confidence interval (assuming normal distrbution).
  ConfMin = avg - zVal_ * sigma;
  ConfMax = avg + zVal_ * sigma;
}
};



}   // namespace similarity

#endif
