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
#ifndef TUNE_H
#define TUNE_H

#include <vector>
#include <map>
#include <utility>
#include <memory>
#include <string>
#include <sstream>
#include <thread>

#include "experimentconf.h"
#include "searchoracle.h"
#include "logging.h"
#include "ztimer.h"
#include "experiments.h"
#include "experimentconf.h"
#include "space.h"
#include "index.h"

//#define DETAILED_PRINT_INFO

namespace similarity {

using std::vector;
using std::map;
using std::make_pair;
using std::string;
using std::cout;
using std::end;
using std::stringstream;

template <typename dist_t>
void GetOptimalAlphas(bool bPrintProgres,
                      ExperimentConfig<dist_t>&     config, 
                      OptimMetric                   metric, 
                      float desiredRecall,
                      const string&                 SpaceType,
                      const string&                 methodName,
                      const AnyParams&              IndexParams, 
                      AnyParams                     QueryTimeParams, 
                      float                         StepFactor, 
                      float                         alpha_left_base,
                      float                         alpha_right_base,
                      vector<shared_ptr<GoldStandardManager<dist_t>>>&  
                                                    vManagerGS,
                      vector<shared_ptr<Index<dist_t>>>&
                                                    vIndexForAllSetsPtrs,
                      float& recall, float& time_best, float& impr_best,
                      float& alpha_left_best, unsigned exp_left,
                      float& alpha_right_best, unsigned exp_right,
                      unsigned MaxIter,
                      unsigned MaxRecDepth,
                      int StepN,
                      float    maxCacheGSRelativeQty,
                      unsigned recLevel) {
  if (recLevel >= MaxRecDepth) {
    if (bPrintProgres) {
      cout << "Reached the maximum recursion level: " << recLevel << endl;
    }
    return;
  }

  size_t ThreadTestQty = thread::hardware_concurrency();

  if (bPrintProgres) {
    cout << "================================================================" << endl;
    cout << ALPHA_LEFT_PARAM << ": " << alpha_left_base << " " << ALPHA_RIGHT_PARAM << ": " << alpha_right_base << endl;
    cout << EXP_LEFT_PARAM << ": " << exp_left << " " << EXP_RIGHT_PARAM ": " << exp_right << endl;
    cout << "================================================================" << endl;
  }

  QueryTimeParams.AddChangeParam(EXP_LEFT_PARAM, exp_left);
  QueryTimeParams.AddChangeParam(EXP_RIGHT_PARAM, exp_right);

  for (unsigned iter = 0; iter < MaxIter; ++iter) {
    if (bPrintProgres) {
      cout << "Iteration: " << iter << " Level: " << recLevel << " StepFactor: " << StepFactor << endl;
    }
    double MinRecall = 1.0;
    double MaxRecall = 0;
      
    for (int left = -StepN; left < StepN; ++left) {
      for (int right = -StepN; right < StepN; ++right) {
        float alphaLeftCurr = alpha_left_base * pow(StepFactor, left);
        float alphaRightCurr = alpha_right_base * pow(StepFactor, right);
        QueryTimeParams.AddChangeParam(ALPHA_LEFT_PARAM, alphaLeftCurr);
        QueryTimeParams.AddChangeParam(ALPHA_RIGHT_PARAM, alphaRightCurr);

#ifdef DETAILED_PRINT_INFO
        if (bPrintProgres) {
          cout << "left: " << left << " right: " << right << endl;
        }
#endif

        vector<vector<MetaAnalysis*>> ExpResRange(config.GetRange().size(),
                                                vector<MetaAnalysis*>(1));
        vector<vector<MetaAnalysis*>> ExpResKNN(config.GetKNN().size(),
                                              vector<MetaAnalysis*>(1));

        MetaAnalysis    Stat(config.GetTestSetToRunQty());

        // Stat is used exactly only once: for one GetRange() or one GetKNN() (but not both)
        for (size_t i = 0; i < config.GetRange().size(); ++i) {
          ExpResRange[i][0] = &Stat;
        }
        for (size_t i = 0; i < config.GetKNN().size(); ++i) {
          ExpResKNN[i][0] = &Stat;
        }
        for (int TestSetId = 0; TestSetId < config.GetTestSetToRunQty(); ++TestSetId) {
          config.SelectTestSet(TestSetId);
#ifdef DETAILED_PRINT_INFO
          if (bPrintProgres) {
            cout << "****************************************************" << endl;
            cout << "*** Test set id: " << TestSetId << 
                            " (set qty: " << config.GetTestSetToRunQty() << ")" << " iteration: " << iter << "\t***" << endl;
            cout << "****************************************************" << endl;
          }
#endif
          if (!vManagerGS[TestSetId].get()) {
            vManagerGS[TestSetId].reset(new GoldStandardManager<dist_t>(config));
            vManagerGS[TestSetId]->Compute(ThreadTestQty, maxCacheGSRelativeQty);
          } else {
#ifdef DETAILED_PRINT_INFO
            if (bPrintProgres) {
              cout << "Using existing GS for test set id: " << TestSetId << endl;
            }
#endif
          }

          std::shared_ptr<Index<dist_t>> MethodPtr = vIndexForAllSetsPtrs[TestSetId];
          
          if (!MethodPtr.get()) {
            if (bPrintProgres) {
              cout << "Creating a new index, params: " << IndexParams.ToString() << endl;
            }
            MethodPtr.reset(MethodFactoryRegistry<dist_t>::Instance().
                                           CreateMethod(false, /* don't print the progress info, it's gonna be quick */
                                                        methodName,
                                                        SpaceType, config.GetSpace(), 
                                                        config.GetDataObjects()));
            MethodPtr->CreateIndex(IndexParams);
            vIndexForAllSetsPtrs[TestSetId] = MethodPtr;
          } else {
#ifdef DETAILED_PRINT_INFO
            if (bPrintProgres) {
              cout << "Reusing an existing index, only reseting params" << endl;
            }
#endif
          }

          vector<shared_ptr<AnyParams>> vQueryTimeParams;
          vQueryTimeParams.push_back(shared_ptr<AnyParams>(new AnyParams(QueryTimeParams)));

          const bool recallOnly = true; // don't need anything except recall here

          Experiments<dist_t>::RunAll(false /* don't print info */, 
                                      ThreadTestQty,
                                      TestSetId,
                                      *vManagerGS[TestSetId],
                                      recallOnly,
                                      ExpResRange, ExpResKNN,
                                      config, 
                                      *MethodPtr,
                                      vQueryTimeParams);
        }
        Stat.ComputeAll();
        double impr_val = (metric == IMPR_DIST_COMP) ? Stat.GetImprDistCompAvg() : Stat.GetImprEfficiencyAvg();
        if (Stat.GetRecallAvg() >= desiredRecall &&
          impr_val > impr_best) {
          recall =    Stat.GetRecallAvg();
          time_best = Stat.GetQueryTimeAvg();
          impr_best = impr_val;
          alpha_left_best  = alphaLeftCurr; 
          alpha_right_best = alphaRightCurr; 

          if (bPrintProgres) {
            cout << " ************* BETTER EFFICIENCY POINT ******************* " << endl;
            cout << "iteration: " << iter << " out of " << MaxIter << endl;
            cout << ALPHA_LEFT_PARAM  << "=" << alpha_left_best  << " " << EXP_LEFT_PARAM  << "=" << exp_left << " "
                            ALPHA_RIGHT_PARAM << "=" << alpha_right_best << " " << EXP_RIGHT_PARAM << "=" << exp_right << endl;
            cout << "Recall: " << Stat.GetRecallAvg() << endl;
            cout << "Query time: " << Stat.GetQueryTimeAvg() << endl;
            cout << "Improvement metric value:  " << impr_val << " (" << getOptimMetricName(metric) << ")" << endl;
            cout << "Impr. in efficiency     : " << Stat.GetImprEfficiencyAvg() << endl;
            cout << "Impr. in dist comp      :  " << Stat.GetImprDistCompAvg() << endl;
            cout << " ********************************************************** " << endl;
          }
        }
        MinRecall = std::min(MinRecall, Stat.GetRecallAvg());
        MaxRecall = std::max(MaxRecall, Stat.GetRecallAvg());
      }
    }

    if (bPrintProgres) {
      cout << " ********** After iteration statistics ******************** " << endl;
      cout << " Local: MinRecall: " << MinRecall << " MaxRecall: " << MaxRecall << " Desired: " << desiredRecall << endl;

      cout << " Global: best improvement. " << impr_best << " Corresp. time: "<< time_best << " Corresp. Recall: " << recall << endl;
      cout << " Using: " << ALPHA_LEFT_PARAM  << "=" << alpha_left_best  << " " << EXP_LEFT_PARAM  << "=" << exp_left << " "
                                  << ALPHA_RIGHT_PARAM << "=" << alpha_right_best << " " << EXP_RIGHT_PARAM << "=" << exp_right << endl;
    }

    // Now let's see, if we need to increase/decrease base alpha levels
    if (MaxRecall < desiredRecall) {
      // Two situations are possible
      if (recall < desiredRecall) { 
        // we never got a required recall (including previous iterations), let's decrease existing alphas
        // so that recall will go up.
        alpha_left_base  = alpha_left_base / StepFactor;
        alpha_right_base = alpha_right_base / StepFactor;

        if (bPrintProgres) {
          cout << "[CHANGE] max recall < desired recall, setting alpha_left_base=" << alpha_left_base << " alpha_right_base=" << alpha_right_base << endl;
        }
      } else {
        // we encountered the required recall before, but we somehow managed to select
        // large alphas, so that in THIS ITERATION even the maximum recall is below desiredRecall.
        // If this is the case, let's return to the previously know good point,
        // with a decreased step factor.
        if (bPrintProgres) {
          cout << "[CHANGE] max recall < desired recall, setting alpha_left_base=" << alpha_left_base << " alpha_right_base=" << alpha_right_base << endl;
        }

        GetOptimalAlphas(bPrintProgres,
                   config, 
                   metric, desiredRecall,
                   SpaceType, methodName,
                   IndexParams, QueryTimeParams,
                   sqrt(StepFactor), alpha_left_best, alpha_right_best,
                   vManagerGS, vIndexForAllSetsPtrs,
                   recall, time_best, impr_best,
                   alpha_left_best, exp_left,
                   alpha_right_best, exp_right,
                   MaxIter, MaxRecDepth, StepN, maxCacheGSRelativeQty,
                   recLevel + 1);
        return;
      }
    } else if (MinRecall > desiredRecall) {
    /* 
     * If we are above the minimum recall, this means we chose alphas two pessimistically.
     * Let's multiply our best values so far by StepFactor.
     */
      alpha_left_base  = alpha_left_best * StepFactor;
      alpha_right_base = alpha_right_best * StepFactor;
      if (bPrintProgres) {
        cout << "[CHANGE] max recall > desired recall, setting alpha_left_base=" << alpha_left_base << " alpha_right_base=" << alpha_right_base << endl;
      }
    } else {
      CHECK(MaxRecall >= desiredRecall && MinRecall <= desiredRecall); 
        // Let's return to the previously know good point,
        // with a decreased step factor.

      if (bPrintProgres) {
        cout << "[CHANGE] desired recall is between min and max recall, decreasing factor, setting alpha_left_base=" << alpha_left_best << " alpha_right_base=" << alpha_right_best << endl;
      }

      GetOptimalAlphas(bPrintProgres,
                   config, 
                   metric, desiredRecall,
                   SpaceType, methodName,
                   IndexParams, QueryTimeParams,
                   sqrt(StepFactor), alpha_left_best, alpha_right_best,
                   vManagerGS, vIndexForAllSetsPtrs,
                   recall, time_best, impr_best,
                   alpha_left_best, exp_right,
                   alpha_right_best, exp_right,
                   MaxIter, MaxRecDepth, StepN, maxCacheGSRelativeQty,
                   recLevel + 1);
      return;
    }
  }

  if (bPrintProgres) {
    cout << "Exhausted " << MaxIter << " iterations" << endl;
  }
}

template <typename dist_t>
void GetOptimalAlphas(bool bPrintProgres,
                      ExperimentConfig<dist_t>& config, 
                      OptimMetric               metric, 
                      float                     desiredRecall,
                      const string&             SpaceType,
                      const string&             methodName,
                      const AnyParams           IndexParams, 
                      AnyParams                 QueryTimeParams, 
                      float& recall, float& time_best, float& impr_best,
                      float& alpha_left_best,  unsigned exp_left,
                      float& alpha_right_best, unsigned exp_right,
                      unsigned               MaxIter,
                      unsigned               MaxRecDepth,
                      unsigned               StepN,
                      float                  FullFactor,
                      float                  maxCacheGSRelativeQty) {
  time_best = std::numeric_limits<float>::max();
  impr_best = 0;
  recall = 0;

  if (bPrintProgres) {
    cout << EXP_LEFT_PARAM << ": " << exp_left << " " << EXP_RIGHT_PARAM ": " << exp_right << endl;
  }

  if (bPrintProgres) {
    cout << "Method index parameters:      " << IndexParams.ToString() << endl;
    cout << "Method query-time parameters: " << QueryTimeParams.ToString() << endl;
  }

  vector<shared_ptr<GoldStandardManager<dist_t>>>  vManagerGS(config.GetTestSetToRunQty());
  vector<shared_ptr<Index<dist_t>>>                vIndexForAllSetsPtrs(config.GetTestSetToRunQty());

  GetOptimalAlphas(bPrintProgres, 
                   config, 
                   metric, desiredRecall,
                   SpaceType, methodName,
                   IndexParams, QueryTimeParams,
                   pow(FullFactor, 1.0/StepN), alpha_left_best, alpha_right_best,
                   vManagerGS, vIndexForAllSetsPtrs,
                   recall, time_best, impr_best,
                   alpha_left_best, exp_right,
                   alpha_right_best, exp_right,
                   MaxIter, MaxRecDepth, StepN, maxCacheGSRelativeQty,
                   0 /* recLevel */);
}

}


#endif

