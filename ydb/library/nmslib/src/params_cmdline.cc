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
#include <cstring>
#include <map>
#include <limits>
#include <iostream>
#include <stdexcept>

#include "utils.h"
#include "params_cmdline.h"
#include "params_def.h"
#include "logging.h"
#include "space.h"
#include "cmd_options.h"

#include <cmath>

namespace similarity {

using std::multimap;
using std::string;

void ParseCommandLine(int argc, char* argv[], bool& bPrintProgress,
                      string&                 LogFile,
                      string&                 LoadIndexLoc,
                      string&                 SaveIndexLoc,
                      string&                 DistType,
                      string&                 SpaceType,
                      shared_ptr<AnyParams>&  SpaceParams,
                      unsigned&               ThreadTestQty,
                      bool&                   AppendToResFile,
                      string&                 ResFilePrefix,
                      unsigned&               TestSetQty,
                      string&                 DataFile,
                      string&                 QueryFile,
                      string&                 CacheGSFilePrefix,
                      float&                  maxCacheGSRelativeQty,
                      bool&                   recallOnly,
                      unsigned&               MaxNumData,
                      unsigned&               MaxNumQuery,
                      vector<unsigned>&       knn,
                      float&                  eps,
                      string&                 RangeArg,
                      string&                 MethodName,
                      shared_ptr<AnyParams>&          IndexTimeParams,
                      vector<shared_ptr<AnyParams>>&  QueryTimeParams) {
  knn.clear();
  RangeArg.clear();
  QueryTimeParams.clear();

  string          indexTimeParamStr;
  vector<string>  vQueryTimeParamStr;
  string          spaceParamStr;
  string          knnArg;
  // Conversion to double is due to an Intel's bug with __builtin_signbit being undefined for float
  double          epsTmp;

  bPrintProgress  = true;

  bool bSuppressPrintProgress;

  CmdOptions cmd_options;

  cmd_options.Add(new CmdParam(SPACE_TYPE_PARAM_OPT, SPACE_TYPE_PARAM_MSG,
                               &spaceParamStr, true));
  cmd_options.Add(new CmdParam(DIST_TYPE_PARAM_OPT, DIST_TYPE_PARAM_MSG,
                               &DistType, false, DIST_TYPE_FLOAT));
  cmd_options.Add(new CmdParam(DATA_FILE_PARAM_OPT, DATA_FILE_PARAM_MSG,
                               &DataFile, true));
  cmd_options.Add(new CmdParam(MAX_NUM_DATA_PARAM_OPT, MAX_NUM_DATA_PARAM_MSG,
                               &MaxNumData, false, MAX_NUM_DATA_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(QUERY_FILE_PARAM_OPT, QUERY_FILE_PARAM_MSG,
                               &QueryFile, false, QUERY_FILE_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(LOAD_INDEX_PARAM_OPT, LOAD_INDEX_PARAM_MSG,
                               &LoadIndexLoc, false, LOAD_INDEX_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(SAVE_INDEX_PARAM_OPT, SAVE_INDEX_PARAM_MSG,
                               &SaveIndexLoc, false, SAVE_INDEX_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(CACHE_PREFIX_GS_PARAM_OPT, CACHE_PREFIX_GS_PARAM_MSG,
                               &CacheGSFilePrefix, false, CACHE_PREFIX_GS_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(MAX_CACHE_GS_QTY_PARAM_OPT, MAX_CACHE_GS_QTY_PARAM_MSG,
                               &maxCacheGSRelativeQty, false, MAX_CACHE_GS_QTY_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(RECALL_ONLY_PARAM_OPT, RECALL_ONLY_PARAM_MSG,
                               &recallOnly, false, RECALL_ONLY_PARAM_DEFAULT));
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
  cmd_options.Add(new CmdParam(QUERY_TIME_PARAMS_PARAM_OPT, QUERY_TIME_PARAMS_PARAM_MSG,
                               &vQueryTimeParamStr, false));
  cmd_options.Add(new CmdParam(INDEX_TIME_PARAMS_PARAM_OPT, INDEX_TIME_PARAMS_PARAM_MSG,
                               &indexTimeParamStr, false));
  cmd_options.Add(new CmdParam(METHOD_PARAM_OPT, METHOD_PARAM_MSG,
                               &MethodName, false));
  cmd_options.Add(new CmdParam(THREAD_TEST_QTY_PARAM_OPT, THREAD_TEST_QTY_PARAM_MSG,
                               &ThreadTestQty, false, THREAD_TEST_QTY_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(OUT_FILE_PREFIX_PARAM_OPT, OUT_FILE_PREFIX_PARAM_MSG,
                               &ResFilePrefix, false, OUT_FILE_PREFIX_PARAM_DEFAULT));
  cmd_options.Add(new CmdParam(APPEND_TO_RES_FILE_PARAM_OPT, APPEND_TO_RES_FILE_PARAM_MSG,
                               &AppendToResFile, false));
  cmd_options.Add(new CmdParam(NO_PROGRESS_PARAM_OPT, NO_PROGRESS_PARAM_MSG,
                               &bSuppressPrintProgress, false));

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

  bPrintProgress = !bSuppressPrintProgress;

  eps = epsTmp;

  ToLower(DistType);
  ToLower(spaceParamStr);
  ToLower(MethodName);

  try {
    {
      vector<string>     desc;
      ParseSpaceArg(spaceParamStr, SpaceType, desc);
      SpaceParams = shared_ptr<AnyParams>(new AnyParams(desc));
    }

    {
      vector<string>     desc;
      ParseArg(indexTimeParamStr, desc);
      IndexTimeParams = shared_ptr<AnyParams>(new AnyParams(desc));
    }

    if (vQueryTimeParamStr.empty())
      vQueryTimeParamStr.push_back("");

    for(const auto s: vQueryTimeParamStr) {
      vector<string>  desc;

      ParseArg(s, desc);
      QueryTimeParams.push_back(shared_ptr<AnyParams>(new AnyParams(desc)));
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
  } catch (const exception& e) {
    LOG(LIB_FATAL) << "Exception: " << e.what();
  }
}

}


