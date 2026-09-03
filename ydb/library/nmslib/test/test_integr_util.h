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
#ifndef TEST_INTEGR_UTILS_H
#define TEST_INTEGR_UTILS_H

#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <map>
#include <cstdio>

#include "utils.h"
#include "bunit.h"
#include "report.h"
#include "params_def.h"
#include "params_cmdline.h"
#include "report_intr_dim.h"

using std::set;
using std::multimap;
using std::vector;
using std::copy;
using std::string;
using std::stringstream;

/* 
 * Test ranges are not bullet proof, so random violations of test conditions
 * are possible. However, several failures in a row is basically impossible
 */
#define ATTEMPT_QTY 10

using namespace similarity;

/*
 * A data structure that defines 
 * 1) Search parameters
 * 2) Method parameters
 * 3) Search outcome (recall range, range for the improvement in distance computation)
 */
struct MethodTestCase {
  string  distType_;
  string  spaceType_;
  string  dataSet_;
  string  methodName_;
  string  indexParams_;
  bool    testReload_; // Test save/load index
  string  queryTypeParams_;
  float   recallMin_;
  float   recallMax_;
  bool    recallOnly_;
  float   numCloserMin_;
  float   numCloserMax_;
  float   imprDistCompMin_;
  float   imprDistCompMax_;

  unsigned   mKNN;
  float      mRange;

  MethodTestCase(
               string distType,
               string spaceType,
               string dataSet,
               string methodName,
               bool   testReload,
               string indexParams,
               string queryTimeParams,
               unsigned  knn,
               float range, 
               float recallMin,
               float recallMax,
               float numCloserMin,
               float numCloserMax,
               float imprDistCompMin,
               float imprDistCompMax,
               bool recallOnly = false) :
                   distType_(distType),
                   spaceType_(spaceType),
                   dataSet_(dataSet),
                   methodName_(methodName),
                   indexParams_(indexParams),
                   testReload_(testReload),
                   queryTypeParams_(queryTimeParams),
                   recallMin_(recallMin),
                   recallMax_(recallMax),
                   recallOnly_(recallOnly),
                   numCloserMin_(numCloserMin),
                   numCloserMax_(numCloserMax),
                   imprDistCompMin_(imprDistCompMin),
                   imprDistCompMax_(imprDistCompMax),
                   mKNN(knn), mRange(range)
                {
                  ToLower(distType_);
                  ToLower(spaceType);
                }
};

template <typename dist_t>
bool ProcessAndCheckResults(
                    const string& cmdStr,
                    const string& distType,
                    const string& spaceType,
                    const MethodTestCase& testCase,
                    const ExperimentConfig<dist_t>& config,
                    MetaAnalysis& ExpRes,
                    string& PrintStr // For display
                    ) {
  std::stringstream Print, Data, Header;

  ExpRes.ComputeAll();

  PrintStr  = produceHumanReadableReport(config, ExpRes, testCase.methodName_, testCase.indexParams_, testCase.queryTypeParams_);

  bool bFail = false;

  if (ExpRes.GetRecallAvg() < testCase.recallMin_) {
    cerr << "Failed to meet min recall requirement, expect >= " << testCase.recallMin_ 
         << " got " << ExpRes.GetRecallAvg() << endl 
         << " method: " << testCase.methodName_ << " ; "
         << " index-time params: " << testCase.indexParams_ << " ; "
         << " query-time params: " << testCase.queryTypeParams_ << " ; "
         << " data set: " << testCase.dataSet_ << " ; "
         << " dist value  type: " << distType << " ; "
         << " space type: " << spaceType << endl << cmdStr << endl;
    bFail = true;
  }

  if (ExpRes.GetRecallAvg() > testCase.recallMax_) {
    cerr << "Failed to meet max recall requirement, expect <= " << testCase.recallMax_ 
         << " got " << ExpRes.GetRecallAvg() << endl 
         << " method: " << testCase.methodName_ << " ; "
         << " index-time params: " << testCase.indexParams_ << " ; "
         << " query-time params: " << testCase.queryTypeParams_ << " ; "
         << " data set: " << testCase.dataSet_ << " ; "
         << " dist value  type: " << distType << " ; "
         << " space type: " << spaceType << endl << cmdStr << endl;
    bFail = true;
  }

  if (ExpRes.GetNumCloserAvg() < testCase.numCloserMin_) {
    cerr << "Failed to meet min # of points closer requirement, expect >= " << testCase.numCloserMin_ 
         << " got " << ExpRes.GetNumCloserAvg() << endl 
         << " method: " << testCase.methodName_ << " ; "
         << " index-time params: " << testCase.indexParams_ << " ; "
         << " query-time params: " << testCase.queryTypeParams_ << " ; "
         << " data set: " << testCase.dataSet_ << " ; "
         << " dist value  type: " << distType << " ; "
         << " space type: " << spaceType << endl << cmdStr << endl;
    bFail = true;
  }

  if (ExpRes.GetNumCloserAvg() > testCase.numCloserMax_) {
    cerr << "Failed to meet max # of points closer requirement, expect <= " << testCase.numCloserMax_ 
         << " got " << ExpRes.GetNumCloserAvg() << endl 
         << " method: " << testCase.methodName_ << " ; "
         << " index-time params: " << testCase.indexParams_ << " ; "
         << " query-time params: " << testCase.queryTypeParams_ << " ; "
         << " data set: " << testCase.dataSet_ << " ; "
         << " dist value  type: " << distType << " ; "
         << " space type: " << spaceType << endl << cmdStr << endl;
    bFail = true;
  }

  if (testCase.imprDistCompMin_ > 0 && ExpRes.GetImprDistCompAvg() < testCase.imprDistCompMin_) {
    cerr << "Failed to meet min improvement requirement in the # of distance computations, expect >= "
         << testCase.imprDistCompMin_ << " got " << ExpRes.GetImprDistCompAvg() << endl 
         << " method: " << testCase.methodName_ << " ; "
         << " index-time params: " << testCase.indexParams_ << " ; "
         << " query-time params: " << testCase.queryTypeParams_ << " ; "
         << " data set: " << testCase.dataSet_ << " ; "
         << " dist value  type: " << distType << " ; "
         << " space type: " << spaceType << endl << cmdStr << endl;
    bFail = true;
  }

  if (testCase.imprDistCompMax_ > 0 && ExpRes.GetImprDistCompAvg() > testCase.imprDistCompMax_) {
    cerr << "Failed to meet max improvement requirement in the # of distance computations, expect <= "
         << testCase.imprDistCompMax_ << " got " << ExpRes.GetImprDistCompAvg() << endl 
         << " method: " << testCase.methodName_ << " ; "
         << " index-time params: " << testCase.indexParams_ << " ; "
         << " query-time params: " << testCase.queryTypeParams_ << " ; "
         << " data set: " << testCase.dataSet_ << " ; "
         << " dist value  type: " << distType << " ; "
         << " space type: " << spaceType << endl << cmdStr << endl;
    bFail = true;
  }

  return !bFail;
};

inline string getFirstParam(const string& paramDef) {
  vector<string>  vParDefs;
  
  if (!SplitStr(paramDef, vParDefs, ',')) {
    PREPARE_RUNTIME_ERR(err) << "Cannot comma-split the parameter definition: '" << paramDef << "'";
    THROW_RUNTIME_ERR(err);
  }
  if (vParDefs.empty()) {
    PREPARE_RUNTIME_ERR(err) << "Empty definition list in the parameter definition: '" << paramDef << "'";
    THROW_RUNTIME_ERR(err);
  }
  return "--" + vParDefs[0];  
}

inline string quoteEmpty(const string& str) {
  return str.empty() ? string("\"\"") : str;
};

string CreateCmdStr(
             const MethodTestCase&        testCase,
             bool                         isRange,
             const string&                rangeOrKnnArg,
             const string&                DistType, 
             string                       SpaceTypeStr,
             unsigned                     ThreadTestQty,
             unsigned                     TestSetQty,
             const string&                DataFile,
             const string&                QueryFile,
             unsigned                     MaxNumData,
             unsigned                     MaxNumQuery,
             const                        float eps) {
  stringstream res;
  // our test data files shouldn't have spaces, so we won't escape them
  // anyways, this is only a debug output 
  res << getFirstParam(DATA_FILE_PARAM_OPT)    << " " << DataFile << " " 
      << getFirstParam(MAX_NUM_DATA_PARAM_OPT) << " " << MaxNumData << " " 
      << getFirstParam(DIST_TYPE_PARAM_OPT)    << " " << DistType << " " 
      << getFirstParam(SPACE_TYPE_PARAM_OPT)   << " " << SpaceTypeStr << " " 
      << getFirstParam(THREAD_TEST_QTY_PARAM_OPT)<< " " << ThreadTestQty << " " 
      << getFirstParam(EPS_PARAM_OPT)          << " " << eps << " " ;
   if (QueryFile.empty())
     res << getFirstParam(TEST_SET_QTY_PARAM_OPT) << " " << TestSetQty << " " ;
   else
     res << getFirstParam(QUERY_FILE_PARAM_OPT) << " " << QueryFile << " " ;

  res << getFirstParam(MAX_NUM_QUERY_PARAM_OPT) << " " << MaxNumQuery << " " 
      << getFirstParam(isRange ? RANGE_PARAM_OPT : KNN_PARAM_OPT)    << " " << rangeOrKnnArg << " " 
      << getFirstParam(METHOD_PARAM_OPT)        << " " << testCase.methodName_ << " " 
      << getFirstParam(INDEX_TIME_PARAMS_PARAM_OPT) << " " << quoteEmpty(testCase.indexParams_) << " " 
      << getFirstParam(QUERY_TIME_PARAMS_PARAM_OPT) << " " << quoteEmpty(testCase.queryTypeParams_);

  return res.str();
};
                    

/*
 * returns the # of failed tests
 */
template <typename dist_t>
size_t RunTestExper(const vector<MethodTestCase>& vTestCases,
             bool                         bTestReload,
             const string&                IndexFileNamePrefix,
             const string&                DistType, 
             string                       SpaceTypeStr,
             unsigned                     ThreadTestQty,
             unsigned                     TestSetQty,
             const string&                DataFile,
             const string&                QueryFile,
             unsigned                     MaxNumData,
             unsigned                     MaxNumQuery,
             const string&                KnnArg,
             const                        float eps,
             const string&                RangeArg
)
{
  // For better reproducibility, let's reset
  // random number generators. 
  defaultRandomSeed = 0; // Will affect any new threads
  getThreadLocalRandomGenerator().seed(defaultRandomSeed); // Affects only the current thread

  vector<unsigned> knn;
  vector<dist_t>   range;

  size_t nFail = 0;

  if (!KnnArg.empty() && !RangeArg.empty()) {
    PREPARE_RUNTIME_ERR(err) << "Function: " << __func__ << " cannot test range and k-NN search jointly!";
    THROW_RUNTIME_ERR(err);
  }

  if (!KnnArg.empty()) {
    if (!SplitStr(KnnArg, knn, ',')) {
      PREPARE_RUNTIME_ERR(err) << "Wrong format of the knn argument: '" << KnnArg << "' Should be a list of coma-separated int > 0 values.";
      THROW_RUNTIME_ERR(err);
    }
  }

  if (!RangeArg.empty()) {
    if (!SplitStr(RangeArg, range, ',')) {
      PREPARE_RUNTIME_ERR(err) << "Wrong format of the range argument: '" << RangeArg << "' Should be a list of coma-separated distance-type values.";
      THROW_RUNTIME_ERR(err);
    }
  }

  ToLower(SpaceTypeStr);
  string SpaceType;

  shared_ptr<AnyParams> SpaceParams;

  {
    vector<string>     desc;
    ParseSpaceArg(SpaceTypeStr, SpaceType, desc);
    SpaceParams = shared_ptr<AnyParams>(new AnyParams(desc));
  }


  unique_ptr<Space<dist_t>> space(SpaceFactoryRegistry<dist_t>::
                                  Instance().CreateSpace(SpaceType, *SpaceParams));

  if (NULL == space.get()) {
    PREPARE_RUNTIME_ERR(err) << "Cannot create space: '" << SpaceType;
    THROW_RUNTIME_ERR(err);
  }
 
  ExperimentConfig<dist_t> config(*space,
                                  DataFile, QueryFile, TestSetQty,
                                  MaxNumData, MaxNumQuery,
                                  knn, eps, range);

  config.ReadDataset();
  MemUsage  mem_usage_measure;


  std::vector<std::string>          MethDescStr;
  std::vector<std::string>          MethParams;
  vector<double>                    MemUsage;

  vector<unique_ptr<GoldStandardManager<dist_t>>> vManagerGS(config.GetTestSetToRunQty());

  for (int testSetId = 0; testSetId < config.GetTestSetToRunQty(); ++testSetId) {
    config.SelectTestSet(testSetId);

    LOG(LIB_INFO) << ">>>> Computing GS for test set id: " << testSetId << " (set qty: " << config.GetTestSetToRunQty() << ")";

    vManagerGS[testSetId].reset(new GoldStandardManager<dist_t>(config));
    vManagerGS[testSetId]->Compute(ThreadTestQty, 0); // Keeping all GS entries, should be Ok here because our data sets are smallish
  }
   
  for (size_t methNum = 0; methNum < vTestCases.size(); ++methNum) {
    const string&   methodName  = vTestCases[methNum].methodName_;
    bool            recallOnly = vTestCases[methNum].recallOnly_;

    cout << "Testing: " << yellow << methodName << no_color << endl;
    LOG(LIB_INFO) << ">>>> Index type : " << methodName;

    // Exactly one range or k-NN search is executed
    CHECK_MSG(config.GetRange().size() + config.GetKNN().size() == 1,
             "Exactly one k-NN or range search should be executed!");

    vector<vector<MetaAnalysis*>> expResRange(config.GetRange().size(), 
                                              vector<MetaAnalysis*>(1));
    vector<vector<MetaAnalysis*>> expResKNN(config.GetKNN().size(),
                                              vector<MetaAnalysis*>(1));

    vector<string>         vCmdStrRange(expResRange.size());
    vector<string>         vCmdStrKNN(expResKNN.size());

    for (size_t i = 0; i < config.GetRange().size(); ++i) {
      expResRange[i][0] = new MetaAnalysis(config.GetTestSetToRunQty());

      vCmdStrRange[i] = CreateCmdStr(vTestCases[methNum],
                                             true,
                                             ConvertToString(config.GetRange()[i]),
                                             DistType, 
                                             SpaceTypeStr,
                                             ThreadTestQty,
                                             TestSetQty,
                                             DataFile,
                                             QueryFile,
                                             MaxNumData,
                                             MaxNumQuery,
                                             eps);
      cout << vCmdStrRange[i] << endl;
      LOG(LIB_INFO) << "Command line params: " << vCmdStrRange[i];
    }

    for (size_t i = 0; i < config.GetKNN().size(); ++i) {
      expResKNN[i][0] = new MetaAnalysis(config.GetTestSetToRunQty());

      vCmdStrKNN[i] = CreateCmdStr(vTestCases[methNum],
                                             false,
                                             ConvertToString(config.GetKNN()[i]),
                                             DistType, 
                                             SpaceTypeStr,
                                             ThreadTestQty,
                                             TestSetQty,
                                             DataFile,
                                             QueryFile,
                                             MaxNumData,
                                             MaxNumQuery,
                                             eps);
      cout << vCmdStrKNN[i] << endl;
    }

    shared_ptr<AnyParams>           indexParams; 
    vector<shared_ptr<AnyParams>>   vQueryTimeParams;

    {
      vector<string>     desc;
      ParseArg(vTestCases[methNum].indexParams_, desc);
      indexParams = shared_ptr<AnyParams>(new AnyParams(desc));
    }

    {
      vector<string>     desc;
      ParseArg(vTestCases[methNum].queryTypeParams_, desc);
      vQueryTimeParams.push_back(shared_ptr<AnyParams>(new AnyParams(desc)));
    }

    LOG(LIB_INFO) << ">>>> Index-time parameters: " << indexParams->ToString();

    // Reset random number generator before each method's run.
    // Above we reset it before random data split
    defaultRandomSeed = 0; // Will affect any new threads
    getThreadLocalRandomGenerator().seed(defaultRandomSeed); // Affects only the current thread

    for (int testSetId = 0; testSetId < config.GetTestSetToRunQty(); ++testSetId) {
      config.SelectTestSet(testSetId);

      LOG(LIB_INFO) << ">>>> Test set id: " << testSetId << " (set qty: " << config.GetTestSetToRunQty() << ")";

      const GoldStandardManager<dist_t>& managerGS = *vManagerGS[testSetId];

      const double vmsize_before = mem_usage_measure.get_vmsize();

      WallClockTimer wtm;

      wtm.reset();
      
      LOG(LIB_INFO) << "Creating a new index" ;

      shared_ptr<Index<dist_t>> IndexPtr(
                         MethodFactoryRegistry<dist_t>::Instance().
                         CreateMethod(false /* don't print progress */,
                                      methodName,
                                      SpaceType, config.GetSpace(), 
                                      config.GetDataObjects())
                         );

      IndexPtr->CreateIndex(*indexParams);

      if (bTestReload) {
        LOG(LIB_INFO) << "Saving the index" ;

        string indexLocAdd = "_" + ConvertToString(testSetId);
        string fullIndexName = IndexFileNamePrefix + indexLocAdd;

        if (DoesFileExist(fullIndexName)) {
          CHECK_MSG(std::remove(fullIndexName.c_str()) == 0, 
                  "Failed to delete file '" + fullIndexName + "'")
        }

        IndexPtr->SaveIndex(fullIndexName);

        IndexPtr.reset(
                         MethodFactoryRegistry<dist_t>::Instance().
                         CreateMethod(false /* don't print progress */,
                                      methodName,
                                      SpaceType, config.GetSpace(), 
                                      config.GetDataObjects())
                      );

        LOG(LIB_INFO) << "Loading the index" ;

        IndexPtr->LoadIndex(fullIndexName);
      }

      LOG(LIB_INFO) << "==============================================";

      const double vmsize_after = mem_usage_measure.get_vmsize();

      const double data_size = DataSpaceUsed(config.GetDataObjects()) / 1024.0 / 1024.0;

      const double TotalMemByMethod = vmsize_after - vmsize_before + data_size;

      wtm.split();

      LOG(LIB_INFO) << ">>>> Process memory usage: " << vmsize_after << " MBs";
      LOG(LIB_INFO) << ">>>> Virtual memory usage: " << TotalMemByMethod << " MBs";
      LOG(LIB_INFO) << ">>>> Data size:            " << data_size << " MBs";
      LOG(LIB_INFO) << ">>>> Time elapsed:         " << (wtm.elapsed()/double(1e6)) << " sec";

      CHECK_MSG(vQueryTimeParams.size() == 1, 
                "Test integration code is currently can execute only one set of query-time parameters!");

      Experiments<dist_t>::RunAll(true /* print progress */, 
                                  ThreadTestQty,
                                  testSetId,
                                  managerGS,
                                  recallOnly,
                                  expResRange, expResKNN,
                                  config, 
                                  *IndexPtr, 
                                  vQueryTimeParams);

    }

    string Print, Data, Header;

    for (size_t i = 0; i < config.GetRange().size(); ++i) {
      MetaAnalysis* res = expResRange[i][0];

      string cmdStr = vCmdStrRange[i];
      if (!ProcessAndCheckResults(cmdStr,
                                  DistType, SpaceType, 
                                  vTestCases[methNum], config, *res, Print)) {
        nFail++;
        cout << red << "failed" << no_color << " (see logs for more details) " << endl;
      } else {
        cout << green << "passed" << no_color << endl;
      }
      LOG(LIB_INFO) << "Range: " << config.GetRange()[i];
      LOG(LIB_INFO) << Print;
      LOG(LIB_INFO) << "Data: " << Header << Data;

      delete res;
    }

    for (size_t i = 0; i < config.GetKNN().size(); ++i) {
      MetaAnalysis* res = expResKNN[i][0];

      string cmdStr = vCmdStrKNN[i];

      if (!ProcessAndCheckResults(cmdStr,
                                  DistType, SpaceType, 
                                  vTestCases[methNum], config, *res, Print)) {
        cout << red << "failed" << no_color << " (see logs for more details) " << endl;
        nFail++;
      } else {
        cout << green << "passed" << no_color << endl;
      }
      LOG(LIB_INFO) << "KNN: " << config.GetKNN()[i];
      LOG(LIB_INFO) << Print;
      LOG(LIB_INFO) << "Data: " << Header << Data;

      delete res;
    }
  }

  return nFail;
}

// Returns the number of failures of the last attempt
inline unsigned RunOneTest(const vector<MethodTestCase>& vTestCases,
               bool                         bTestReload,
               string                       IndexFileNamePrefix,
               string                       DistType, 
               string                       SpaceTypeStr,
               unsigned                     ThreadTestQty,
               unsigned                     TestSetQty,
               const string&                DataFile,
               const string&                QueryFile,
               unsigned                     MaxNumData,
               unsigned                     MaxNumQuery,
               const string&                KnnArg,
               const                        float eps,
               const string&                RangeArg) {
  size_t failQty = 0;
  ToLower(DistType);
  for (unsigned attId = 0; attId < ATTEMPT_QTY; ++attId) {
    failQty = 0;
    if (DIST_TYPE_INT == DistType) {
      failQty = RunTestExper<int>(vTestCases,
                  bTestReload,
                  IndexFileNamePrefix,
                  DistType,
                  SpaceTypeStr,
                  ThreadTestQty,
                  TestSetQty,
                  DataFile,
                  QueryFile,
                  MaxNumData,
                  MaxNumQuery,
                  KnnArg,
                  eps,
                  RangeArg
                 );
    } else if (DIST_TYPE_FLOAT == DistType) {
      failQty = RunTestExper<float>(vTestCases,
                  bTestReload,
                  IndexFileNamePrefix,
                  DistType,
                  SpaceTypeStr,
                  ThreadTestQty,
                  TestSetQty,
                  DataFile,
                  QueryFile,
                  MaxNumData,
                  MaxNumQuery,
                  KnnArg,
                  eps,
                  RangeArg
                 );
    } else {
      PREPARE_RUNTIME_ERR(err) << "Unknown distance value type: " << DistType;
      THROW_RUNTIME_ERR(err);
    }

    if (!failQty) {
      return 0;
    }
    LOG(LIB_INFO) << "Failed " << failQty << " tests, attempt id: " << attId;
  }
  LOG(LIB_ERROR) << "Failed after making " << ATTEMPT_QTY << " attempts" << " with " << failQty << "failures";

  return failQty;
};

#endif
