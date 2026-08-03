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
#ifndef _EXPERIMENTS_H
#define _EXPERIMENTS_H

#include <errno.h>
#include <string.h>

#include <iostream>
#include <vector>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <thread>
#include <mutex>
#include <memory>

#include "global.h"
#include "object.h"
#include "memory.h"
#include "ztimer.h"
#include "utils.h"
#include "experimentconf.h"
#include "space.h"
#include "index.h"
#include "logging.h"
#include "methodfactory.h"
#include "eval_results.h"
#include "meta_analysis.h"
#include "query_creator.h"
#include "thread_pool.h"

namespace similarity {

using std::vector;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::mutex;
using std::thread;
using std::ref;
using std::lock_guard;
using std::unique_ptr;

template <typename dist_t>
class Experiments {
public:
  typedef Index<dist_t> IndexType;

  static void RunAll(bool                                 LogInfo, 
                     unsigned                             ThreadTestQty,
                     size_t                               TestSetId,
                     const GoldStandardManager<dist_t>&   managerGS,
                     bool                                 recallOnly,
                     vector<vector<MetaAnalysis*>>&       ExpResRange,
                     vector<vector<MetaAnalysis*>>&       ExpResKNN,
                     const ExperimentConfig<dist_t>&      config,
                     IndexType&                           Method,
                     const vector<shared_ptr<AnyParams>>& QueryTimeParams) {

    if (LogInfo) LOG(LIB_INFO) << ">>>> TestSetId: " << TestSetId;
    if (LogInfo) LOG(LIB_INFO) << ">>>> Will use: "  << ThreadTestQty << " threads in efficiency testing";
    if (LogInfo) config.PrintInfo();

    if (!config.GetRange().empty()) {
      for (size_t i = 0; i < config.GetRange().size(); ++i) {
        const dist_t radius = config.GetRange()[i];
        RangeCreator<dist_t>  cr(radius);
        Execute<RangeQuery<dist_t>, RangeCreator<dist_t>>(LogInfo,
                                                  ThreadTestQty, TestSetId,
                                                  managerGS.GetRangeGS(i),
                                                  recallOnly,
                                                  ExpResRange[i], config, cr, 
                                                  Method, QueryTimeParams);
      }
    }

    if (!config.GetKNN().empty()) {
      for (size_t i = 0; i < config.GetKNN().size(); ++i) {
        const size_t K = config.GetKNN()[i];
        KNNCreator<dist_t>  cr(K, config.GetEPS());
        Execute<KNNQuery<dist_t>, KNNCreator<dist_t>>(LogInfo,
                                              ThreadTestQty, TestSetId,
                                              managerGS.GetKNNGS(i),
                                              recallOnly,
                                              ExpResKNN[i], config, cr, 
                                              Method, QueryTimeParams);
      }
    }
    if (LogInfo) LOG(LIB_INFO) << "experiment done at " << LibGetCurrentTime();
  }


  template <typename QueryType, typename QueryCreatorType>
  static void Execute(bool LogInfo, unsigned ThreadTestQty, size_t TestSetId,
                     const vector<unique_ptr<GoldStandard<dist_t>>> &goldStand,
                     bool                                           recallOnly,
                     std::vector<MetaAnalysis*>&                    ExpRes,
                     const ExperimentConfig<dist_t>&                config,
                     const QueryCreatorType&                        QueryCreator,
                     IndexType&                                     Method,
                     const vector<shared_ptr<AnyParams>>&           QueryTimeParams) {
    size_t numquery = config.GetQueryObjects().size();
    unsigned MethQty = QueryTimeParams.size();

    if (LogInfo) LOG(LIB_INFO) << "##### Query type: " << QueryType::Type();
    if (LogInfo) LOG(LIB_INFO) << ">>>> query params = "  << QueryCreator.ParamsForPrint();
    if (LogInfo) LOG(LIB_INFO) << ">>>> Computing efficiency metrics ";
    if (LogInfo) LOG(LIB_INFO) << ">>>> # of query time parameters: " << MethQty;

    vector<uint64_t>  SearchTime(MethQty); 

    vector<double>    ClassAccuracy(MethQty);
    vector<double>    Recall(MethQty);
    vector<double>    NumCloser(MethQty);
    vector<double>    RecallAt1(MethQty);
    vector<double>    LogPosErr(MethQty);
    vector<double>    PrecisionOfApprox(MethQty);
    vector<double>    SystemTimeElapsed(MethQty);

    uint64_t  SeqSearchTime     = 0;

    vector<double>    AvgNumDistComp(MethQty);
    vector<double>    ImprDistComp(MethQty);
    vector<unsigned>  max_result_size(MethQty);
    vector<double>    avg_result_size(MethQty);
    vector<uint64_t>  DistCompQty(MethQty);

    mutex             UpdateStat;

    config.GetSpace().SetQueryPhase();

    for (size_t MethNum = 0; MethNum < QueryTimeParams.size(); ++MethNum) {
     /* 
      * Setting query time parameters must be done 
      * before running any tests, in particular, because
      * the function SetQueryTimeParams is NOT supposed to be THREAD-SAFE. 
      */
      const AnyParams& qtp = *QueryTimeParams[MethNum];
      Method.SetQueryTimeParams(qtp);

      LOG(LIB_INFO) << ">>>> Query-Time Parameters: " << qtp.ToString();

      if (LogInfo) LOG(LIB_INFO) << ">>>> Efficiency test for: "<< Method.StrDesc();

      WallClockTimer wtm;

      wtm.reset();

      if (!ThreadTestQty) ThreadTestQty = 1;

      vector<vector<size_t>>                  QueryIds;
      vector<vector<unique_ptr<QueryType>>>   Queries; // queries with results

      QueryIds.resize(ThreadTestQty);
      Queries.resize(ThreadTestQty);

      /*
       * Because each thread uses its own parameter set, we must use
       * exactly ThreadTestQty sets.
       */
      ParallelFor(0, ThreadTestQty, ThreadTestQty, [&](unsigned QueryPart, unsigned ThreadId) {
        size_t numquery = config.GetQueryObjects().size();

        WallClockTimer wtm;

        wtm.reset();

        for (size_t q = 0; q < numquery; ++q) {
          if ((q % ThreadTestQty) == QueryPart) {
            unique_ptr<QueryType> query(QueryCreator(config.GetSpace(), 
                                        config.GetQueryObjects()[q]));
            uint64_t  t1 = wtm.split();
            Method.Search(query.get());
            uint64_t  t2 = wtm.split();

            {
              lock_guard<mutex> g(UpdateStat);

              ExpRes[MethNum]->AddDistComp(TestSetId, query->DistanceComputations());
              ExpRes[MethNum]->AddQueryTime(TestSetId, (1.0*t2 - t1)/1e3);


              DistCompQty[MethNum] += query->DistanceComputations();
              avg_result_size[MethNum] += query->ResultSize();

              if (query->ResultSize() > max_result_size[MethNum]) {
                max_result_size[MethNum] = query->ResultSize();
              }

              QueryIds[QueryPart].push_back(q);
              Queries[QueryPart].push_back(std::move(query));
            }
          }
        }
      });

      wtm.split();

      SearchTime[MethNum] = wtm.elapsed();

      AvgNumDistComp[MethNum] = static_cast<double>(DistCompQty[MethNum])/numquery;
      ImprDistComp[MethNum]   = config.GetDataObjects().size() / AvgNumDistComp[MethNum];

      ExpRes[MethNum]->SetImprDistComp(TestSetId, ImprDistComp[MethNum]);

      avg_result_size[MethNum] /= static_cast<double>(numquery);

      if (LogInfo) LOG(LIB_INFO) << ">>>> Computing effectiveness metrics for " << Method.StrDesc();

      for (unsigned QueryPart = 0; QueryPart < ThreadTestQty; ++QueryPart) {
        for (size_t qi = 0; qi < Queries[QueryPart].size(); ++qi) {
          size_t            q = QueryIds[QueryPart][qi] ;
          const QueryType*  pQuery = Queries[QueryPart][qi].get();

          unique_ptr<QueryType> queryGS(QueryCreator(config.GetSpace(), config.GetQueryObjects()[q]));

          const GoldStandard<dist_t>&  QueryGS = *goldStand[q];

          EvalResults<dist_t>     Eval(config.GetSpace(), pQuery, QueryGS, recallOnly);

          NumCloser[MethNum]    += Eval.GetNumCloser();
          RecallAt1[MethNum]    += Eval.GetRecallAt1();
          LogPosErr[MethNum]    += Eval.GetLogRelPos();
          Recall[MethNum]       += Eval.GetRecall();
          double addAccuracy = (Eval.GetClassCorrect() == kClassCorrect ? 1:0);
          ClassAccuracy[MethNum]+= addAccuracy;
          PrecisionOfApprox[MethNum] += Eval.GetPrecisionOfApprox();

          ExpRes[MethNum]->AddPrecisionOfApprox(TestSetId, Eval.GetPrecisionOfApprox());
          ExpRes[MethNum]->AddRecall(TestSetId, Eval.GetRecall());
          ExpRes[MethNum]->AddClassAccuracy(TestSetId, addAccuracy);
          ExpRes[MethNum]->AddLogRelPosError(TestSetId, Eval.GetLogRelPos());
          ExpRes[MethNum]->AddNumCloser(TestSetId, Eval.GetNumCloser());
          ExpRes[MethNum]->AddRecallAt1(TestSetId, Eval.GetRecallAt1());

        }
      }
    }

    config.GetSpace().SetIndexPhase();

    /* 
     * Sequential search times should be computed only once.
     */
    for (size_t q = 0; q < numquery; ++q) {
      const GoldStandard<dist_t>&  QueryGS = *goldStand[q];
      SeqSearchTime     += QueryGS.GetSeqSearchTime();
    }

    for (size_t MethNum = 0; MethNum < QueryTimeParams.size(); ++MethNum) {
      double timeSec = SearchTime[MethNum]/double(1e6);
      double queryPerSec = numquery / timeSec;

      if (LogInfo) {
        LOG(LIB_INFO) << "=========================================";
        LOG(LIB_INFO) << ">>>> Index type is "<< Method.StrDesc();
        LOG(LIB_INFO) << "=========================================";
        LOG(LIB_INFO) << ">>>> max # results = " << max_result_size[MethNum];
        LOG(LIB_INFO) << ">>>> avg # results = " << avg_result_size[MethNum];
        LOG(LIB_INFO) << ">>>> # of distance computations = " << AvgNumDistComp[MethNum];
        LOG(LIB_INFO) << ">>>> Impr in # of dist comp: " << ImprDistComp[MethNum];
        LOG(LIB_INFO) << "=========================================";
        LOG(LIB_INFO) << ">>>> Time elapsed:           " << timeSec << " sec";
        LOG(LIB_INFO) << ">>>> # of queries per sec: : " << queryPerSec;
        LOG(LIB_INFO) << ">>>> Avg time per query:     " << (timeSec/1e3/numquery) << " msec";
        LOG(LIB_INFO) << ">>>> System time elapsed:    " << (SystemTimeElapsed[MethNum]/double(1e6)) << " sec";
        LOG(LIB_INFO) << "=========================================";
      }

      // This number is adjusted for the number of threads!
      double  ImprEfficiency = static_cast<double>(SeqSearchTime)/(SearchTime[MethNum]*ThreadTestQty);

      ExpRes[MethNum]->SetImprEfficiency(TestSetId, ImprEfficiency);
      ExpRes[MethNum]->SetQueryPerSec(TestSetId, queryPerSec);

      Recall[MethNum]            /= static_cast<double>(numquery);
      ClassAccuracy[MethNum]     /= static_cast<double>(numquery);
      NumCloser[MethNum]         /= static_cast<double>(numquery);
      RecallAt1[MethNum]         /= static_cast<double>(numquery);
      LogPosErr[MethNum]         /= static_cast<double>(numquery);
      PrecisionOfApprox[MethNum] /= static_cast<double>(numquery);
    
      if (LogInfo) {
        LOG(LIB_INFO) << "=========================================";
        LOG(LIB_INFO) << ">>>> # of test threads:              " << ThreadTestQty;
        LOG(LIB_INFO) << ">>>> Seq. search time elapsed:       " << (SeqSearchTime/double(1e6)) << " sec";
        LOG(LIB_INFO) << ">>>> Avg Seq. search time per query: " << (SeqSearchTime/double(1e3)/numquery) << " msec";
        LOG(LIB_INFO) << ">>>> Impr. in Efficiency = "           << ImprEfficiency;
        LOG(LIB_INFO) << ">>>> Recall              = "           << Recall[MethNum];
        LOG(LIB_INFO) << ">>>> PrecisionOfApprox   = "           << PrecisionOfApprox[MethNum];
        LOG(LIB_INFO) << ">>>> RelPosError         = "           << exp(LogPosErr[MethNum]);
        LOG(LIB_INFO) << ">>>> NumCloser           = "           << NumCloser[MethNum];
        LOG(LIB_INFO) << ">>>> RecallAt1           = "           << RecallAt1[MethNum];
        LOG(LIB_INFO) << ">>>> Class. accuracy     = "           << ClassAccuracy[MethNum];
      }
    }

    if (LogInfo) LOG(LIB_INFO) << "#### Finished " << QueryType::Type() << " " << LibGetCurrentTime();
  }
};

}   // namespace similarity

#endif

