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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <cmath>
#include <memory>
#include <limits>
#include <string>
#include <sstream>
#include <vector>
#include <fstream>
#include <map>

#include "init.h"
#include "global.h"
#include "utils.h"
#include "memory.h"
#include "ztimer.h"
#include "experiments.h"
#include "experimentconf.h"
#include "space.h"
#include "index.h"
#include "tune.h"
#include "method/vptree.h"
#include "logging.h"
#include "spacefactory.h"
#include "methodfactory.h"
#include "params_def.h"

#include "meta_analysis.h"
#include "params.h"
#include "cmd_options.h"

//#define DETAILED_LOG_INFO 1

using namespace similarity;

using std::vector;
using std::map;
using std::make_pair;
using std::string;
using std::stringstream;

template <typename dist_t>
void RunExper(unsigned AddRestartQty,
             const string&                  MethodName,
             const AnyParams&               IndexParams,
             const AnyParams&               QueryTimeParams,
             const string&                  SpaceType,
             const AnyParams&               SpaceParams,
             unsigned                       TestSetQty,
             const string&                  DataFile,
             const string&                  QueryFile,
             unsigned                       MaxNumData,
             unsigned                       MaxNumQuery,
             vector<unsigned>               knnAll,
             float                          eps,
             const string&                  RangeArg,
             const string&                  ResFile,
             unsigned                       MinExp,
             unsigned                       MaxExp,
             unsigned                       MaxIter,
             unsigned                       MaxRecDepth,
             unsigned                       StepN,
             float                          FullFactor,
             float                          maxCacheGSRelativeQty
)
{
  vector<dist_t> rangeAll;

  if (!RangeArg.empty()) {
    if (!SplitStr(RangeArg, rangeAll, ',')) {
      LOG(LIB_FATAL) << "Wrong format of the range argument: '" << RangeArg << "' Should be a list of coma-separated values.";
    }
  }

  vector<string>  vAllowedMeth = {METH_VPTREE};
  string          allowedMethList;

  for (string s: vAllowedMeth) allowedMethList += s + " ";

  bool ok = false;
  for (string s: vAllowedMeth) {
    if (MethodName  == s) {
      ok = true;
      break;
    }
  }

  if (!ok) {
    LOG(LIB_FATAL) << "Wrong method name, " <<
                      "you should specify only a single method from the list: " << allowedMethList;
  }

  LOG(LIB_INFO) << "We are going to tune parameters for " << MethodName;

  static  thread_local auto&          engine(getThreadLocalRandomGenerator());
  static  std::normal_distribution<>  normGen(0.0f, log(FullFactor));

  AnyParamManager pmgr(IndexParams);

  float         desiredRecall = 0;

  pmgr.GetParamRequired(DESIRED_RECALL_PARAM, desiredRecall);

  string metricName;

  pmgr.GetParamOptional(OPTIM_METRIC_PARAMETER, metricName, OPTIM_METRIC_DEFAULT);

  OptimMetric metric = getOptimMetric(metricName);

  if (IMPR_INVALID == metric) {
    stringstream err;

    err << "Invalid metric name: " << metricName;
    LOG(LIB_FATAL) << err.str();
  }

  try {

    if (!MaxExp) throw runtime_error(string(MIN_EXP_PARAM) + " can't be zero!");
    if (MaxExp < MinExp) throw runtime_error(string(MAX_EXP_PARAM) + " can't be < " + string(MIN_EXP_PARAM));

    if (rangeAll.size() + knnAll.size() != 1) {
      LOG(LIB_FATAL) << "You need to specify exactly one range or one knn search!";
    }

    unique_ptr<ExperimentConfig<dist_t>>  config;
    unique_ptr<Space<dist_t>>             space(SpaceFactoryRegistry<dist_t>::Instance().CreateSpace(SpaceType, SpaceParams));

    if (NULL == space.get()) {
      LOG(LIB_FATAL) << "Cannot create space: '" << SpaceType;
    }

    for (unsigned i = 0; i < rangeAll.size(); ++i) {
      vector<dist_t>      range;
      vector<unsigned>    knn;

      range.push_back(rangeAll[i]);

      config.reset(new ExperimentConfig<dist_t>(*space,
                                      DataFile, QueryFile, TestSetQty,
                                      MaxNumData, MaxNumQuery,
                                      knn, eps, range));
    }

    for (unsigned i = 0; i < knnAll.size(); ++i) {
      vector<dist_t>      range;
      vector<unsigned>    knn;

      knn.push_back(knnAll[i]);

      config.reset(new ExperimentConfig<dist_t>(*space,
                                      DataFile, QueryFile, TestSetQty,
                                      MaxNumData, MaxNumQuery,
                                      knn, eps, range));
    }

    CHECK(config.get());

    config->ReadDataset();

    float recall = 0, time_best = 0, impr_best = -1, alpha_left = 0, alpha_right = 0;
    unsigned exp_left = 0, exp_right = 0;

    for (unsigned ce = MinExp; ce <= MaxExp; ++ce)
    for (unsigned k = 0; k < 1 + AddRestartQty; ++k) {
      unsigned expLeft = ce, expRight = ce;
      float recall_loc, time_best_loc, impr_best_loc,
            alpha_left_loc = 1.0, alpha_right_loc = 1.0; // These are initial values

      if (k > 0) {
        // Let's do some random normal fun
        alpha_left_loc = exp(normGen(engine));
        alpha_right_loc = exp(normGen(engine));
        LOG(LIB_INFO) << " RANDOM STARTING POINTS: " << alpha_left_loc << " " << alpha_right_loc;
      }

      GetOptimalAlphas(true,
                     *config,
                     metric, desiredRecall,
                     SpaceType,
                     MethodName,
                     pmgr.ExtractParametersExcept({DESIRED_RECALL_PARAM, OPTIM_METRIC_PARAMETER}),
                     QueryTimeParams,
                     recall_loc,
                     time_best_loc, impr_best_loc,
                     alpha_left_loc, expLeft, alpha_right_loc, expRight,
                     MaxIter, MaxRecDepth, StepN, FullFactor, maxCacheGSRelativeQty);

      if (impr_best_loc > impr_best) {
        recall = recall_loc;
        time_best = time_best_loc;
        impr_best = impr_best_loc;
        alpha_left = alpha_left_loc;
        alpha_right = alpha_right_loc;
        exp_left = expLeft;
        exp_right = expRight;
      }
    }

    stringstream  bestParams;
    bestParams << ALPHA_LEFT_PARAM << "=" << alpha_left << "," << ALPHA_RIGHT_PARAM << "=" << alpha_right << ","
                 << EXP_LEFT_PARAM   << "=" << exp_left   << "," << EXP_RIGHT_PARAM   << "=" << exp_right;

    LOG(LIB_INFO) << "Optimization results";
    if (!knnAll.empty()) {
      LOG(LIB_INFO) << "K: "  << knnAll[0];
    } else {
      LOG(LIB_INFO) << "Range: "  << rangeAll[0];
    }
    LOG(LIB_INFO) << "Recall: " << recall;
    LOG(LIB_INFO) << "Best time: " << time_best;
    LOG(LIB_INFO) << "Best impr. " << impr_best << " (" << getOptimMetricName(metric) << ")";
    LOG(LIB_INFO) << "alpha_left: " << alpha_left;
    LOG(LIB_INFO) << "exp_left: " << exp_left;
    LOG(LIB_INFO) << "alpha_right: " << alpha_right;
    LOG(LIB_INFO) << "exp_right: " << exp_right;
    LOG(LIB_INFO) << "optimal parameters: " << bestParams.str();

    if (recall < desiredRecall) {
      LOG(LIB_FATAL) << "Failed to get the desired recall!";
    }

    if (!ResFile.empty()) {
      ofstream out(ResFile, ios::trunc);

      if (!out) {
        LOG(LIB_FATAL) << "Can't open file: '" << ResFile << "' for writing";
      }

      out << bestParams.str() << endl;
      out.close();
    }

  } catch (const std::exception& e) {
    LOG(LIB_FATAL) << "Exception: " << e.what();
  } catch (...) {
    LOG(LIB_FATAL) << "Unknown exception";
  }
}

void ParseCommandLineForTuning(int argc, char*argv[],
                      string&                 LogFile,
                      string&                 DistType,
                      string&                 SpaceType,
                      shared_ptr<AnyParams>&  SpaceParams,
                      string&                 ResFile,
                      unsigned&               TestSetQty,
                      string&                 DataFile,
                      string&                 QueryFile,
                      float&                  MaxCacheGSRelativeQty,
                      unsigned&               MaxNumData,
                      unsigned&               MaxNumQuery,
                      vector<unsigned>&       knn,
                      float&                  eps,
                      string&                 RangeArg,
                      unsigned&               MinExp,
                      unsigned&               MaxExp,
                      unsigned&               MaxIter,
                      unsigned&               MaxRecDepth,
                      unsigned&               StepN,
                      float&                  FullFactor,
                      unsigned&               addRestartQty,
                      string&                 MethodName,
                      shared_ptr<AnyParams>&  IndexParams,
                      shared_ptr<AnyParams>&  QueryTimeParams) {
  knn.clear();
  RangeArg.clear();

  string          methParams;
  string          knnArg;
  // Using double due to an Intel's bug with __builtin_signbit being undefined for float
  double          epsTmp;
  double          fullFactorTmp;

  string          indexTimeParamStr;
  string          queryTimeParamStr;

  CmdOptions cmd_options;

  cmd_options.Add(new CmdParam(SPACE_TYPE_PARAM_OPT, SPACE_TYPE_PARAM_MSG,
                               &SpaceType, true));
  cmd_options.Add(new CmdParam(DIST_TYPE_PARAM_OPT, DIST_TYPE_PARAM_MSG,
                               &DistType, false, DIST_TYPE_FLOAT));
  cmd_options.Add(new CmdParam(DATA_FILE_PARAM_OPT, DATA_FILE_PARAM_MSG,
                               &DataFile, true));
  cmd_options.Add(new CmdParam(MAX_NUM_DATA_PARAM_OPT, MAX_NUM_QUERY_PARAM_MSG,
                               &MaxNumData, false, MAX_NUM_DATA_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(QUERY_FILE_PARAM_OPT, QUERY_FILE_PARAM_MSG,
                               &QueryFile, false, QUERY_FILE_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(MAX_CACHE_GS_QTY_PARAM_OPT, MAX_CACHE_GS_QTY_PARAM_MSG,
                               &MaxCacheGSRelativeQty, false, MAX_CACHE_GS_QTY_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(LOG_FILE_PARAM_OPT, LOG_FILE_PARAM_MSG,
                               &LogFile, false, LOG_FILE_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(MAX_NUM_QUERY_PARAM_OPT, MAX_NUM_QUERY_PARAM_MSG,
                               &MaxNumQuery, false, MAX_NUM_QUERY_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(TEST_SET_QTY_PARAM_OPT, TEST_SET_QTY_PARAM_MSG,
                               &TestSetQty, false, TEST_SET_QTY_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(KNN_PARAM_OPT, KNN_PARAM_MSG,
                               &knnArg, false));
  cmd_options.Add(new CmdParam(RANGE_PARAM_OPT, RANGE_PARAM_MSG,
                               &RangeArg, false));
  cmd_options.Add(new CmdParam(EPS_PARAM_OPT, EPS_PARAM_MSG,
                               &epsTmp, false, EPS_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(METHOD_PARAM_OPT, METHOD_PARAM_MSG,
                               &MethodName, true));
  cmd_options.Add(new CmdParam("outFile,o", "output file",
                               &ResFile, false));

  cmd_options.Add(new CmdParam(QUERY_TIME_PARAMS_PARAM_OPT, QUERY_TIME_PARAMS_PARAM_MSG,
                               &queryTimeParamStr, false));
  cmd_options.Add(new CmdParam(INDEX_TIME_PARAMS_PARAM_OPT, INDEX_TIME_PARAMS_PARAM_MSG,
                               &indexTimeParamStr, false));

  cmd_options.Add(new CmdParam(MIN_EXP_PARAM, "the minimum exponent in the pruning oracle.",
                               &MinExp, false, MIN_EXP_DEFAULT));
  cmd_options.Add(new CmdParam(MAX_EXP_PARAM, "the maximum exponent in the pruning oracle.",
                               &MaxExp, false, MAX_EXP_DEFAULT));
  cmd_options.Add(new CmdParam(MAX_ITER_PARAM, "the maximum number of iteration while we are looking for a point where a desired recall can be achieved.",
                               &MaxIter, false, MAX_ITER_DEFAULT));
  cmd_options.Add(new CmdParam(MAX_REC_DEPTH_PARAM, "the maximum recursion in the maximization algorithm (each recursion leads to decrease in the grid search step).",
                               &MaxRecDepth, false, MAX_REC_DEPTH_DEFAULT));
  cmd_options.Add(new CmdParam(STEP_N_PARAM, "each local step of the grid search involves (2StepN+1)^2 mini-iterations.",
                               &StepN, false, STEP_N_DEFAULT));
  cmd_options.Add(new CmdParam(ADD_RESTART_QTY_PARAM, "number of *ADDITIONAL* restarts, initial values are selected randomly",
                               &addRestartQty, false, ADD_RESTART_QTY_DEFAULT));
  cmd_options.Add(new CmdParam(FULL_FACTOR_PARAM, "the maximum factor used in the local grid search (i.e., if (A, B) is a starting point for the grid search, the first element will be in the range: [A/Factor,A*Factor], while the second element will be in the range [B/Factor,B*Factor]. In the beginning, Factor==FullFactor, but it gradually decreases as the algorithm converges.",
                               &fullFactorTmp, false, FULL_FACTOR_DEFAULT));

  try {
    cmd_options.Parse(argc, argv);
  } catch (const CmdParserException& e) {
    cmd_options.ToString();
    std::cout.flush();
    LOG(LIB_FATAL) << e.what();
  } catch (const std::exception& e) {
    cmd_options.ToString();
    std::cout.flush();
    LOG(LIB_FATAL) << e.what();
  } catch (...) {
    cmd_options.ToString();
    std::cout.flush();
    LOG(LIB_FATAL) << "Failed to parse cmd arguments";
  }

  eps = epsTmp;
  FullFactor = fullFactorTmp;

  ToLower(DistType);
  ToLower(SpaceType);

  try {
    {
      vector<string> SpaceDesc;
      string str = SpaceType;
      ParseSpaceArg(str, SpaceType, SpaceDesc);
      SpaceParams = shared_ptr<AnyParams>(new AnyParams(SpaceDesc));
    }

    if (!knnArg.empty() && !SplitStr(knnArg, knn, ',')) {
      LOG(LIB_FATAL) << "Wrong format of the KNN argument: '" << knnArg;
    }

    if (DataFile.empty()) {
      LOG(LIB_FATAL) << "data file is not specified!";
    }

    if (!DoesFileExist(DataFile)) {
      LOG(LIB_FATAL) << "data file " << DataFile << " doesn't exist";
    }

    if (!QueryFile.empty() && !DoesFileExist(QueryFile)) {
      LOG(LIB_FATAL) << "query file " << QueryFile << " doesn't exist";
    }

    if (!MaxNumQuery && QueryFile.empty()) {
      LOG(LIB_FATAL) << "Set a positive # of queries or specify a query file!";
    }

    CHECK_MSG(MaxNumData < MAX_DATASET_QTY, "The maximum number of points should not exceed" + ConvertToString(MAX_DATASET_QTY));
    CHECK_MSG(MaxNumQuery < MAX_DATASET_QTY, "The maximum number of queries should not exceed" + ConvertToString(MAX_DATASET_QTY));

    {
      vector<string>     desc;
      ParseArg(indexTimeParamStr, desc);
      IndexParams = shared_ptr<AnyParams>(new AnyParams(desc));
    }

    {
      vector<string>     desc;
      ParseArg(queryTimeParamStr, desc);
      QueryTimeParams = shared_ptr<AnyParams>(new AnyParams(desc));
    }

  } catch (const exception& e) {
    LOG(LIB_FATAL) << "Exception: " << e.what();
  }
}

int main(int ac, char* av[]) {
  WallClockTimer timer;
  timer.reset();

  string                  LogFile;
  string                  DistType;
  string                  SpaceType;
  shared_ptr<AnyParams>   SpaceParams;
  string                  ResFile;
  unsigned                TestSetQty;
  string                  DataFile;
  string                  QueryFile;
  float                   MaxCacheGSRelativeQty;
  unsigned                MaxNumData;
  unsigned                MaxNumQuery;
  vector<unsigned>        knn;
  string                  RangeArg;
  float                   eps;
  string                  MethodName;
  shared_ptr<AnyParams>   IndexParams;
  shared_ptr<AnyParams>   QueryTimeParams;

  unsigned MinExp;
  unsigned MaxExp;
  unsigned MaxIter;
  unsigned MaxRecDepth;
  unsigned StepN;
  float    FullFactor;
  unsigned AddRestartQty;

  ParseCommandLineForTuning(ac, av,
                       LogFile,
                       DistType,
                       SpaceType,
                       SpaceParams,
                       ResFile,
                       TestSetQty,
                       DataFile,
                       QueryFile,
                       MaxCacheGSRelativeQty,
                       MaxNumData,
                       MaxNumQuery,
                       knn,
                       eps,
                       RangeArg,
                       MinExp,
                       MaxExp,
                       MaxIter,
                       MaxRecDepth,
                       StepN,
                       FullFactor,
                       AddRestartQty,
                       MethodName,
                       IndexParams,
                       QueryTimeParams);

  initLibrary(0, LogFile.empty() ? LIB_LOGSTDERR:LIB_LOGFILE, LogFile.c_str());

  ToLower(DistType);

  if (!SpaceParams) {
    LOG(LIB_FATAL) << "Failed to initialized space parameters!";
  }
  if (!IndexParams) {
    LOG(LIB_FATAL) << "Failed to initialized index-time method parameters!";
  }
  if (!QueryTimeParams) {
    LOG(LIB_FATAL) << "Failed to initialized query-time method parameters!";
  }

  if (DIST_TYPE_INT == DistType) {
    RunExper<int>(AddRestartQty,
                  MethodName, *IndexParams, *QueryTimeParams,
                  SpaceType,
                  *SpaceParams,
                  TestSetQty,
                  DataFile,
                  QueryFile,
                  MaxNumData,
                  MaxNumQuery,
                  knn,
                  eps,
                  RangeArg,
                  ResFile,
                  MinExp,
                  MaxExp,
                  MaxIter,
                  MaxRecDepth,
                  StepN,
                  FullFactor,
                  MaxCacheGSRelativeQty
                 );
  } else if (DIST_TYPE_FLOAT == DistType) {
    RunExper<float>(AddRestartQty,
                  MethodName, *IndexParams, *QueryTimeParams,
                  SpaceType,
                  *SpaceParams,
                  TestSetQty,
                  DataFile,
                  QueryFile,
                  MaxNumData,
                  MaxNumQuery,
                  knn,
                  eps,
                  RangeArg,
                  ResFile,
                  MinExp,
                  MaxExp,
                  MaxIter,
                  MaxRecDepth,
                  StepN,
                  FullFactor,
                  MaxCacheGSRelativeQty
                 );
  } else {
    LOG(LIB_FATAL) << "Unknown distance value type: " << DistType;
  }

  timer.split();
  LOG(LIB_INFO) << "Time elapsed = " << timer.elapsed() / 1e6;
  LOG(LIB_INFO) << "Finished at " << LibGetCurrentTime();

  return 0;
}
