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
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>

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
#include "spacefactory.h"
#include "logging.h"
#include "report.h"
#include "methodfactory.h"

#include "meta_analysis.h"
#include "params.h"
#include "params_cmdline.h"

using namespace similarity;

using std::multimap;
using std::vector;
using std::string;
using std::stringstream;

void OutData(bool DoAppend, const string& FilePrefix,
             const string& Print, const string& Header, const string& Data) {
  string FileNameData = FilePrefix + ".dat";
  string FileNameRep  = FilePrefix + ".rep";

  LOG(LIB_INFO) << "DoAppend? " << DoAppend;

  std::ofstream   OutFileData(FileNameData.c_str(),
                              (DoAppend ? std::ios::app : (std::ios::trunc | std::ios::out)));

  if (!OutFileData) {
    LOG(LIB_FATAL) << "Cannot create output file: '" << FileNameData << "'";
  }
  OutFileData.exceptions(std::ios::badbit);

  std::ofstream   OutFileRep(FileNameRep.c_str(),
                              (DoAppend ? std::ios::app : (std::ios::trunc | std::ios::out)));

  if (!OutFileRep) {
    LOG(LIB_FATAL) << "Cannot create output file: '" << FileNameRep << "'";
  }
  OutFileRep.exceptions(std::ios::badbit);

  if (!DoAppend) {
      OutFileData << Header;
  }
  OutFileData<< Data;
  OutFileRep<< Print;

  OutFileRep.close();
  OutFileData.close();
}

template <typename dist_t>
void ProcessResults(const ExperimentConfig<dist_t>& config,
                    MetaAnalysis& ExpRes,
                    const string& MethodName,
                    const string& IndexParamStr,
                    const string& QueryTimeParamStr,
                    string& PrintStr, // For display
                    string& HeaderStr,
                    string& DataStr   /* to be processed by a script */) {
  std::stringstream Print, Data, Header;

  ExpRes.ComputeAll();

  Header << "MethodName\tRecall\tRecall@1\tPrecisionOfApprox\tRelPosError\tNumCloser\tClassAccuracy\tQueryTime\tDistComp\tImprEfficiency\tImprDistComp\tMem\tIndexTime\tIndexLoadTime\tIndexSaveTime\tQueryPerSec\tIndexParams\tQueryTimeParams\tNumData" << std::endl;

  Data << "\"" << MethodName << "\"\t";
  Data << ExpRes.GetRecallAvg() << "\t";
  Data << ExpRes.GetRecallAt1Avg() << "\t";
  Data << ExpRes.GetPrecisionOfApproxAvg() << "\t";
  Data << ExpRes.GetRelPosErrorAvg() << "\t";
  Data << ExpRes.GetNumCloserAvg() << "\t";
  Data << ExpRes.GetClassAccuracyAvg() << "\t";
  Data << ExpRes.GetQueryTimeAvg() << "\t";
  Data << ExpRes.GetDistCompAvg() << "\t";
  Data << ExpRes.GetImprEfficiencyAvg() << "\t";
  Data << ExpRes.GetImprDistCompAvg() << "\t";
  Data << size_t(ExpRes.GetMemAvg()) << "\t";
  Data << ExpRes.GetIndexTimeAvg() << "\t";
  Data << ExpRes.GetLoadTimeAvg() << "\t";
  Data << ExpRes.GetSaveTimeAvg() << "\t";
  Data << ExpRes.GetQueryPerSecAvg() << "\t";
  Data << "\"" << IndexParamStr << "\"" << "\t";
  Data << "\"" << QueryTimeParamStr << "\"" << "\t";
  Data << config.GetDataObjects().size();
  Data << std::endl;

  PrintStr  = produceHumanReadableReport(config, ExpRes, MethodName, IndexParamStr, QueryTimeParamStr);

  DataStr   = Data.str();
  HeaderStr = Header.str();
};

template <typename dist_t>
void RunExper(bool                                bPrintProgress,
             const string&                        LoadIndexLoc,
             const string&                        SaveIndexLoc, 
             const string&                        MethodName,
             const shared_ptr<AnyParams>&         IndexTimeParams,
             const vector<shared_ptr<AnyParams>>& QueryTimeParams,
             const string                         SpaceType,
             const shared_ptr<AnyParams>&         SpaceParams,
             unsigned                             ThreadTestQty,
             bool                                 DoAppend, 
             const string&                        ResFilePrefix,
             unsigned                             TestSetQty,
             const string&                        DataFile,
             const string&                        QueryFile,
             const string&                        CacheGSFilePrefix,
             float                                maxCacheGSRelativeQty,
             bool                                 recallOnly,
             unsigned                             MaxNumData,
             unsigned                             MaxNumQuery,
             const                                vector<unsigned>& knn,
             const                                float eps,
             const string&                        RangeArg
)
{
  LOG(LIB_INFO) << "### Append? : "       << DoAppend;
  LOG(LIB_INFO) << "### OutFilePrefix : " << ResFilePrefix;
  vector<dist_t> range;

  bool bWriteGSCache = false;
  bool bReadGSCache = false;
  bool bFail = false;
  bool bCacheGS = !CacheGSFilePrefix.empty();

  unique_ptr<fstream>      cacheGSControl;
  unique_ptr<fstream>      cacheGSBinary;

  if (!RangeArg.empty()) {
    if (!SplitStr(RangeArg, range, ',')) {
      LOG(LIB_FATAL) << "Wrong format of the range argument: '" << RangeArg << "' Should be a list of coma-separated values.";
    }
  }

  
  unique_ptr<Space<dist_t>> space (SpaceFactoryRegistry<dist_t>::
                                  Instance().CreateSpace(SpaceType, *SpaceParams));

  ExperimentConfig<dist_t> config(*space,
                                  DataFile, QueryFile, TestSetQty,
                                  MaxNumData, MaxNumQuery,
                                  knn, eps, range);

  size_t cacheDataSetQty = 0;

  const string& cacheGSIncompleteFlagName = CacheGSFilePrefix + "_incomplete.flag";

  if (bCacheGS) {
    const string& cacheGSControlName = CacheGSFilePrefix + "_ctrl.txt";
    const string& cacheGSBinaryName  = CacheGSFilePrefix + "_data.bin";

    if (DoesFileExist(cacheGSIncompleteFlagName) ||
        (DoesFileExist(cacheGSControlName) != DoesFileExist(cacheGSBinaryName))
        ) {
      LOG(LIB_INFO) << "Incomplete cache file detected! Removing incomplete entries...";
      if (DoesFileExist(cacheGSBinaryName))
        CHECK_MSG(std::remove(cacheGSBinaryName.c_str())==0, "Error removing the file: " + cacheGSBinaryName);
      if (DoesFileExist(cacheGSControlName))
        CHECK_MSG(std::remove(cacheGSControlName.c_str())==0, "Error removing the file: " + cacheGSControlName);
    }

    if (DoesFileExist(cacheGSControlName)) {
    // Cache exists => reuse it
      if (!DoesFileExist(cacheGSBinaryName)) {
        throw runtime_error("Inconsistent cache state, there is a text control file: '" +
                            cacheGSControlName + "' but no binary data file: '" +
                            cacheGSBinaryName + "'");
      }
      cacheGSControl.reset(new fstream(cacheGSControlName.c_str(),
                                        std::ios::in));
      cacheGSBinary.reset(new fstream(cacheGSBinaryName.c_str(),
                                        std::ios::in 
                                        // On Windows you don't get a proper binary stream without ios::binary!
                                        | ios::binary));

      bReadGSCache = true;
    } else {
      if (DoesFileExist(cacheGSBinaryName)) {
        throw runtime_error("Inconsistent cache state, there is no text control file: '" +
                            cacheGSControlName + "' but there is binary data file: '" +
                            cacheGSBinaryName + "'");
      }
    // No cache => create new file, but first mark cache as incomplete!
      ofstream flag_file(cacheGSIncompleteFlagName.c_str());
      CHECK_MSG(flag_file, "Error creating file: " + cacheGSIncompleteFlagName);
      flag_file.close();

      cacheGSControl.reset(new fstream(cacheGSControlName.c_str(),
                                        std::ios::trunc | std::ios::out));
      cacheGSBinary.reset(new fstream(cacheGSBinaryName.c_str(),
                                        std::ios::trunc | std::ios::out |
                                        // On Windows you don't get a proper binary stream without ios::binary!
                                        ios::binary));

      bWriteGSCache = true;
    }

    cacheGSControl->exceptions(std::ios::badbit);
    cacheGSBinary->exceptions(std::ios::badbit);

    /*
     * If the cache exists, it should be read before ReadData() is called.
     */
    if (!bWriteGSCache) {
      config.Read(*cacheGSControl, *cacheGSBinary, cacheDataSetQty);
    }
  }

  config.ReadDataset();

  if (bReadGSCache) {
    // Let's check the number of data entries, must exactly coincide with
    // what was used to create the cache!
    if (config.GetOrigDataQty() != cacheDataSetQty) {
      stringstream err;
      err << "The number of entries in the file, or the maximum number "
          << "of data elements don't match the value in the cache file: "
          << cacheDataSetQty;
      throw runtime_error(err.str());
    }
  }

  /*
   * Yet, if we need to create a new cache file, we must write the cache
   * after reading the data set.
   */
  if (bWriteGSCache) {

    config.Write(*cacheGSControl, *cacheGSBinary);
  }


  MemUsage  mem_usage_measure;


  vector<double>                    MemUsage;

  CHECK_MSG(!QueryTimeParams.empty(), "The array of query-time parameters shouldn't be empty!");

  vector<vector<MetaAnalysis*>> ExpResRange(config.GetRange().size(),
                                                vector<MetaAnalysis*>(QueryTimeParams.size()));
  vector<vector<MetaAnalysis*>> ExpResKNN(config.GetKNN().size(),
                                              vector<MetaAnalysis*>(QueryTimeParams.size()));

  GoldStandardManager<dist_t> managerGS(config);


  string MethodDescStr;

  // qtmParamId is the ID of the query time parameter set
  for (size_t qtmParamId = 0; qtmParamId < QueryTimeParams.size(); ++qtmParamId) {
    for (size_t i = 0; i < config.GetRange().size(); ++i) {
      ExpResRange[i][qtmParamId] = new MetaAnalysis(config.GetTestSetToRunQty());
    }
    for (size_t i = 0; i < config.GetKNN().size(); ++i) {
      ExpResKNN[i][qtmParamId] = new MetaAnalysis(config.GetTestSetToRunQty());
    }
  }

  for (int TestSetId = 0; TestSetId < config.GetTestSetToRunQty(); ++TestSetId) {
    config.SelectTestSet(TestSetId);

    string indexLocAdd = "";

    if (QueryFile.empty() && config.GetTestSetToRunQty() > 0) {
      indexLocAdd = "_" + ConvertToString(TestSetId);
    }

    // SelectTestSet must go before managerGS.Compute()!!!

    if (bReadGSCache) {
      size_t cacheTestId = 0, savedThreadQty = 0;
      managerGS.Read(*cacheGSControl, *cacheGSBinary,
                     config.GetTotalQueryQty(),
                     cacheTestId, savedThreadQty);
      if (cacheTestId != TestSetId) {
        stringstream err;
        err << "Perhaps, the input file is corrput (or is incompatible with "
            << "program parameters), expect test set id=" << TestSetId
            << "but obtained " << cacheTestId;
        throw runtime_error(err.str());
      }
      CHECK_MSG(savedThreadQty == ThreadTestQty,
                "Error: the gold standard was computed using " +ConvertToString(savedThreadQty) + " threads, but the current test will use "  +
                ConvertToString(ThreadTestQty) + " threads. You have to use the same number of threads while computing gold standard data and testing!");
    } else {
      managerGS.Compute(ThreadTestQty, maxCacheGSRelativeQty);
      if (bWriteGSCache) {
        LOG(LIB_INFO) << "Saving the cache";
        managerGS.Write(*cacheGSControl, *cacheGSBinary, TestSetId, ThreadTestQty);

        // Remove the incomplete-file flag, but only if this is the last test set 
        if (TestSetId + 1 == config.GetTestSetToRunQty())
          CHECK_MSG(std::remove(cacheGSIncompleteFlagName.c_str())==0, 
                                "Error removing the file: " + cacheGSIncompleteFlagName);
      }
    }

    LOG(LIB_INFO) << ">>>> Test set id: " << TestSetId << " (set qty: " << config.GetTestSetToRunQty() << ")";

    //ReportIntrinsicDimensionality("Main data set" , *config.GetSpace(), config.GetDataObjects());

    if (MethodName.empty()) {
      LOG(LIB_INFO) << "No method is specified, so we will not run any tests...";
    } else {
      unique_ptr<Index<dist_t>>   IndexPtr;

      try {
        LOG(LIB_INFO) << ">>>> Index type : "           << MethodName;
        LOG(LIB_INFO) << ">>>> Index Time Parameters: " << IndexTimeParams->ToString();

        const double vmsize_before = mem_usage_measure.get_vmsize();

        WallClockTimer wtm;

        wtm.reset();
        
          IndexPtr.reset(MethodFactoryRegistry<dist_t>::Instance().
                       CreateMethod(bPrintProgress,
                                    MethodName, 
                                    SpaceType, config.GetSpace(), 
                                    config.GetDataObjects()));

        string adjLoadLoc = LoadIndexLoc + indexLocAdd;

        bool bCreate = LoadIndexLoc.empty() || !DoesFileExist(adjLoadLoc);

        if (bCreate) {
          LOG(LIB_INFO) << "Creating an index from scratch";

          IndexPtr->CreateIndex(*IndexTimeParams);
        } else {
          LOG(LIB_INFO) << "Loading an index for test set id " << TestSetId << " using location: " << adjLoadLoc;
          IndexPtr->LoadIndex(adjLoadLoc);
        }

        if (!TestSetId) MethodDescStr = IndexPtr->StrDesc();

        LOG(LIB_INFO) << "==============================================";

        const double vmsize_after = mem_usage_measure.get_vmsize();

        wtm.split();

        const double IndexTime = bCreate ? double(wtm.elapsed())/1e6 : 0;
        const double LoadTime  = !bCreate ? double(wtm.elapsed())/1e6 : 0;

        const double data_size = DataSpaceUsed(config.GetDataObjects()) / 1024.0 / 1024.0;
        const double TotalMemByMethod =  vmsize_after - vmsize_before + data_size;
        double AdjustedMemByMethod = TotalMemByMethod;

        if (IndexPtr->DuplicateData()) AdjustedMemByMethod -= data_size;

        wtm.reset();
        // We won't save the index if it is already created
        string adjSaveLoc = SaveIndexLoc + indexLocAdd;
        if (!SaveIndexLoc.empty() && !DoesFileExist(adjSaveLoc)) {
          LOG(LIB_INFO) << "Saving an index for test set id " << TestSetId << " using location: " << adjSaveLoc;
          IndexPtr->SaveIndex(adjSaveLoc);
        }
        wtm.split();
        const double SaveTime  = double(wtm.elapsed())/1e6;
    

        LOG(LIB_INFO) << ">>>> Process memory usage:  " << vmsize_after         << " MBs";
        LOG(LIB_INFO) << ">>>> Virtual memory usage:  " << TotalMemByMethod     << " MBs";
        LOG(LIB_INFO) << ">>>> Adjusted memory usage: " << AdjustedMemByMethod  << " MBs";
        LOG(LIB_INFO) << ">>>> Data size:             " << data_size            << " MBs";
        LOG(LIB_INFO) << ">>>> Indexing time:         " << IndexTime            << " sec";
        LOG(LIB_INFO) << ">>>> Index loading time:    " << LoadTime             << " sec";
        LOG(LIB_INFO) << ">>>> Index saving  time:    " << SaveTime             << " sec";

        for (size_t qtmParamId = 0; qtmParamId < QueryTimeParams.size(); ++qtmParamId) {
          for (size_t i = 0; i < config.GetRange().size(); ++i) {
            MetaAnalysis* res = ExpResRange[i][qtmParamId];
            res->SetMem(TestSetId, AdjustedMemByMethod);
            res->SetIndexTime(TestSetId, IndexTime);
            res->SetLoadTime(TestSetId, LoadTime);
            res->SetSaveTime(TestSetId, SaveTime);
          }
          for (size_t i = 0; i < config.GetKNN().size(); ++i) {
            MetaAnalysis* res = ExpResKNN[i][qtmParamId];
            res->SetMem(TestSetId, AdjustedMemByMethod);
            res->SetIndexTime(TestSetId, IndexTime);
            res->SetLoadTime(TestSetId, LoadTime);
            res->SetSaveTime(TestSetId, SaveTime);
          }
        }

        Experiments<dist_t>::RunAll(bPrintProgress,
                                    ThreadTestQty, 
                                    TestSetId,
                                    managerGS,
                                    recallOnly,
                                    ExpResRange, ExpResKNN,
                                    config, 
                                    *IndexPtr, 
                                    QueryTimeParams);


      } catch (const std::exception& e) {
        LOG(LIB_ERROR) << "Exception: " << e.what();
        bFail = true;
      } catch (...) {
        LOG(LIB_ERROR) << "Unknown exception";
        bFail = true;
      }

      if (bFail) {
        LOG(LIB_FATAL) << "Failure due to an exception!";
      }
    }
  }

  // Don't save results, if the method wasn't specified
  if (!MethodName.empty()) {
    for (size_t MethNum = 0; MethNum < QueryTimeParams.size(); ++MethNum) {
      // Don't overwrite file after we output data at least for one method!
      bool DoAppendHere = DoAppend || MethNum;

      string Print, Data, Header;

      for (size_t i = 0; i < config.GetRange().size(); ++i) {
        MetaAnalysis* res = ExpResRange[i][MethNum];

        ProcessResults(config, *res, MethodDescStr,
                      IndexTimeParams->ToString(), QueryTimeParams[MethNum]->ToString(), 
                      Print, Header, Data);
        LOG(LIB_INFO) << "Range: " << config.GetRange()[i];
        LOG(LIB_INFO) << Print;
        LOG(LIB_INFO) << "Data: " << Header << Data;

        if (!ResFilePrefix.empty()) {
          stringstream str;
          str << ResFilePrefix << "_r=" << config.GetRange()[i];
          OutData(DoAppendHere, str.str(), Print, Header, Data);
        }

        delete res;
      }

      for (size_t i = 0; i < config.GetKNN().size(); ++i) {
        MetaAnalysis* res = ExpResKNN[i][MethNum];

        ProcessResults(config, *res, MethodDescStr,
                      IndexTimeParams->ToString(), QueryTimeParams[MethNum]->ToString(), 
                      Print, Header, Data);
        LOG(LIB_INFO) << "KNN: " << config.GetKNN()[i];
        LOG(LIB_INFO) << Print;
        LOG(LIB_INFO) << "Data: " << Header << Data;

        if (!ResFilePrefix.empty()) {
          stringstream str;
          str << ResFilePrefix << "_K=" << config.GetKNN()[i];
          OutData(DoAppendHere, str.str(), Print, Header, Data);
        }

        delete res;
      }
    }
  }
}

int main(int ac, char* av[]) {
  // This should be the first function called before

  WallClockTimer timer;
  timer.reset();


  bool                  bPrintProgress;
  string                LogFile;
  string                DistType;
  string                LoadIndexLoc;
  string                SaveIndexLoc;
  string                SpaceType;
  shared_ptr<AnyParams> SpaceParams;
  bool                  DoAppend;
  string                ResFilePrefix;
  unsigned              TestSetQty;
  string                DataFile;
  string                QueryFile;
  string                CacheGSFilePrefix;
  float                 maxCacheGSRelativeQty;
  bool                  recallOnly;
  unsigned              MaxNumData;
  unsigned              MaxNumQuery;
  vector<unsigned>      knn;
  string                RangeArg;
  float                 eps = 0.0;
  unsigned              ThreadTestQty;

  shared_ptr<AnyParams>           IndexTimeParams;
  vector<shared_ptr<AnyParams>>   QueryTimeParams;

  try {
    string                                  MethodName;

    ParseCommandLine(ac, av, bPrintProgress,
                         LogFile,
                         LoadIndexLoc,
                         SaveIndexLoc, 
                         DistType,
                         SpaceType,
                         SpaceParams,
                         ThreadTestQty,
                         DoAppend, 
                         ResFilePrefix,
                         TestSetQty,
                         DataFile,
                         QueryFile,
                         CacheGSFilePrefix,
                         maxCacheGSRelativeQty,
                         recallOnly,
                         MaxNumData,
                         MaxNumQuery,
                         knn,
                         eps,
                         RangeArg,
                         MethodName,
                         IndexTimeParams,
                         QueryTimeParams);

    if ((!LoadIndexLoc.empty() || !SaveIndexLoc.empty()) &&
         CacheGSFilePrefix.empty() &&
         MaxNumQuery != 0 &&
         QueryFile.empty()) {
      throw
          runtime_error(string("If there is i) no query file ii) # of queries > 0 iii) you ask to save/load the index,")+
                        "then you have to specify the gold-standard cache file!");
    }

    initLibrary(0, LogFile.empty() ? LIB_LOGSTDERR:LIB_LOGFILE, LogFile.c_str());

    LOG(LIB_INFO) << "Program arguments are processed";

    ToLower(DistType);

    if (DIST_TYPE_INT == DistType) {
      RunExper<int>(bPrintProgress,
                    LoadIndexLoc,
                    SaveIndexLoc,
                    MethodName,
                    IndexTimeParams,
                    QueryTimeParams,
                    SpaceType,
                    SpaceParams,
                    ThreadTestQty,
                    DoAppend, 
                    ResFilePrefix,
                    TestSetQty,
                    DataFile,
                    QueryFile,
                    CacheGSFilePrefix,
                    maxCacheGSRelativeQty,
                    recallOnly,
                    MaxNumData,
                    MaxNumQuery,
                    knn,
                    eps,
                    RangeArg
                   );
    } else if (DIST_TYPE_FLOAT == DistType) {
      RunExper<float>(bPrintProgress,
                    LoadIndexLoc,
                    SaveIndexLoc,
                    MethodName,
                    IndexTimeParams,
                    QueryTimeParams,
                    SpaceType,
                    SpaceParams,
                    ThreadTestQty,
                    DoAppend, 
                    ResFilePrefix,
                    TestSetQty,
                    DataFile,
                    QueryFile,
                    CacheGSFilePrefix,
                    maxCacheGSRelativeQty,
                    recallOnly,
                    MaxNumData,
                    MaxNumQuery,
                    knn,
                    eps,
                    RangeArg
                   );
    } else {
      LOG(LIB_FATAL) << "Unknown distance value type: " << DistType;
    }

    timer.split();
    LOG(LIB_INFO) << "Time elapsed = " << timer.elapsed() / 1e6;
    LOG(LIB_INFO) << "Finished at " << LibGetCurrentTime();
  } catch (const std::exception& e) {
    LOG(LIB_FATAL) << "Exception: " << e.what();
  } catch (...) {
    LOG(LIB_FATAL) << "Unknown exception";
  }

  return 0;
}
