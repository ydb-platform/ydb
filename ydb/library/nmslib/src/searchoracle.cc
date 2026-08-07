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
#include "searchoracle.h"
#include "method/vptree_utils.h"
#include "space.h"
#include "method/vptree.h"
#include "tune.h"

#include <vector>
#include <iostream>
#include <queue>
#include <cmath>

namespace similarity {

const size_t TUNE_QTY_DEFAULT = 20000;
const size_t TUNE_QUERY_QTY   = 200;
const size_t TUNE_SPLIT_QTY   = 5;
const size_t TOTAL_QUERY_QTY  = TUNE_QUERY_QTY * TUNE_SPLIT_QTY;
const size_t MIN_TUNE_QTY     = TOTAL_QUERY_QTY; 

using std::vector;
using std::round;
using std::endl;
using std::cout;
using std::pow;
using std::log;

template <typename dist_t>
void PolynomialPruner<dist_t>::SetQueryTimeParams(AnyParamManager& pmgr) {
  /* 
   * If no tunning was carried out previously, 
   * default values are for the classic triangle inequality.
   * However, after tunning is done, we will use default values
   * obtained after tunning.
   */
  pmgr.GetParamOptional(ALPHA_LEFT_PARAM,   alpha_left_,    alpha_left_default_);
  pmgr.GetParamOptional(ALPHA_RIGHT_PARAM,  alpha_right_,   alpha_right_default_);
  pmgr.GetParamOptional(EXP_LEFT_PARAM,     exp_left_,      exp_left_default_);
  pmgr.GetParamOptional(EXP_RIGHT_PARAM,    exp_right_,     exp_right_default_);

  LOG(LIB_INFO) << "Set polynomial pruner query-time parameters:";
  LOG(LIB_INFO) << Dump();
}

template <typename dist_t>
void PolynomialPruner<dist_t>::SetIndexTimeParams(AnyParamManager& pmgr) {
  if (pmgr.hasParam(TUNE_R_PARAM) && pmgr.hasParam(TUNE_K_PARAM)) {
    stringstream err;

    err << "Specify only one parameter: " << TUNE_R_PARAM << " or " << TUNE_K_PARAM;
    LOG(LIB_INFO) << err.str();
    throw runtime_error(err.str());
  }

  /*
   * The tuning part is hardcoded to make only with the VP-tree.
   */
  if (pmgr.hasParam(TUNE_R_PARAM) || pmgr.hasParam(TUNE_K_PARAM)) {
  // Obtain optimal parameters automatically
    size_t fullBucketSize;
    pmgr.GetParamRequired("bucketSize", fullBucketSize);

    if (data_.size() < TOTAL_QUERY_QTY) {
      stringstream err;
      err << "The data size is too small: it should have " << (TOTAL_QUERY_QTY - data_.size()) << " MORE entries!";
      LOG(LIB_INFO) << err.str();
      throw runtime_error(err.str());
    }

    size_t tuneQty;
    pmgr.GetParamOptional(TUNE_QTY_PARAM, tuneQty, TUNE_QTY_DEFAULT);

    if (tuneQty < MIN_TUNE_QTY) {
      stringstream err;
      err << "The value of parameter " <<  TUNE_QTY_PARAM << " should be >= " << MIN_TUNE_QTY;
      LOG(LIB_INFO) << err.str();
      throw runtime_error(err.str());
    }

    float desiredRecallRequested = 0;
    pmgr.GetParamRequired(DESIRED_RECALL_PARAM, desiredRecallRequested);

    float  desiredRecallAdjusted = desiredRecallRequested;
    size_t bucketSizeAdjusted = fullBucketSize;

    if (tuneQty >= data_.size()) {
      tuneQty = data_.size(); // Perfect, nothing to do at this point
    } else {
      float treeHeightQty = float(data_.size())/ fullBucketSize; 

      if (treeHeightQty > tuneQty) {
        bucketSizeAdjusted = 1;
        // Because treeHeightQty > tuneQty > 0, logs are well defined.
        // Furthermore, the ratio of logs should be < 1, so we increase the desired recall
        desiredRecallAdjusted = pow(desiredRecallRequested, log(tuneQty)/log(treeHeightQty));
        LOG(LIB_INFO) << "Adjusting recall value from " << desiredRecallRequested << " to " << desiredRecallAdjusted;
      } else {
        // in this conditional branch we expect treeHeightQty to be <= tuneQty so the adjusted bucket size should be > 0
        bucketSizeAdjusted = tuneQty / treeHeightQty;
      }
    }

    LOG(LIB_INFO) << "Adjusted values for tuneQty: " << tuneQty << 
                     " bucket size: " << bucketSizeAdjusted << 
                     " recall: " << desiredRecallAdjusted;

    vector<string>                paramDesc;
    stringstream                  methParamDesc;
    string                        methName;

    methParamDesc << "bucketSize=" << bucketSizeAdjusted;

    ParseArg(methParamDesc.str(), paramDesc);

    size_t minExp = 0, maxExp = 0;

    pmgr.GetParamOptional(MIN_EXP_PARAM, minExp, MIN_EXP_DEFAULT);
    pmgr.GetParamOptional(MAX_EXP_PARAM, maxExp, MAX_EXP_DEFAULT);

    if (!maxExp) throw runtime_error(string(MIN_EXP_PARAM) + " can't be zero!");
    if (maxExp < minExp) throw runtime_error(string(MAX_EXP_PARAM) + " can't be < " + string(MIN_EXP_PARAM));


    string metricName;

    pmgr.GetParamOptional(OPTIM_METRIC_PARAMETER, metricName, OPTIM_METRIC_DEFAULT);

    OptimMetric metric = getOptimMetric(metricName);

    if (IMPR_INVALID == metric) {
      stringstream err;
  
      err << "Invalid metric name: " << metricName;
      LOG(LIB_INFO) << err.str();
      throw runtime_error(err.str());
    }


    unique_ptr<ExperimentConfig<dist_t>>  config;

    dist_t eps = 0;

    ObjectVector emptyQueries;

    vector<unsigned>                  knn;
    vector<dist_t>                    range;

    if (pmgr.hasParam(TUNE_R_PARAM)) {
      dist_t r;
      pmgr.GetParamRequired(TUNE_R_PARAM, r);
      range.push_back(r);
      
      config.reset(new ExperimentConfig<dist_t>(space_, 
                                      data_, emptyQueries, 
                                      TUNE_SPLIT_QTY,
                                      tuneQty, TUNE_QUERY_QTY,
                                      knn, eps, range));
  
    }

    if (pmgr.hasParam(TUNE_K_PARAM)) {
      size_t k;
      pmgr.GetParamRequired(TUNE_K_PARAM, k);
      knn.push_back(k);
      
      config.reset(new ExperimentConfig<dist_t>(space_, 
                                      data_, emptyQueries, 
                                      TUNE_SPLIT_QTY,
                                      tuneQty, TUNE_QUERY_QTY,
                                      knn, eps, range));
  
    }

    CHECK(config.get());

    config->ReadDataset();

    size_t maxCacheGSQty;
    pmgr.GetParamOptional(MAX_CACHE_GS_QTY_PARAM, maxCacheGSQty, MAX_CACHE_GS_QTY_DEFAULT);
    size_t maxIter = 0;
    pmgr.GetParamOptional(MAX_ITER_PARAM, maxIter, MAX_ITER_DEFAULT);
    size_t maxRecDepth = 0;
    pmgr.GetParamOptional(MAX_REC_DEPTH_PARAM, maxRecDepth, MAX_REC_DEPTH_DEFAULT);
    size_t stepN = 0;
    pmgr.GetParamOptional(STEP_N_PARAM, stepN, STEP_N_DEFAULT);
    size_t addRestartQty = 0;
    pmgr.GetParamOptional(ADD_RESTART_QTY_PARAM, addRestartQty, ADD_RESTART_QTY_DEFAULT);
    float fullFactor = 0;
    pmgr.GetParamOptional(FULL_FACTOR_PARAM, fullFactor, (float)FULL_FACTOR_DEFAULT);

    float recall = 0, time_best = 0, impr_best = -1, alpha_left = 0, alpha_right = 0; 
    unsigned exp_left = 0, exp_right = 0;


    static thread_local auto&           randGen(getThreadLocalRandomGenerator());
    static  std::normal_distribution<>  normGen(0.0f, log(fullFactor));


    for (unsigned ce = minExp; ce <= maxExp; ++ce)
    for (unsigned k = 0; k < 1 + addRestartQty; ++k) {
      unsigned expLeft = ce, expRight = ce;
      float recall_loc, time_best_loc, impr_best_loc, 
            alpha_left_loc = 1.0, alpha_right_loc = 1.0; // These are initial values

      if (k > 0) {
        // Let's do some random normal fun
        alpha_left_loc = exp(normGen(randGen));
        alpha_right_loc = exp(normGen(randGen));
        LOG(LIB_INFO) << " RANDOM STARTING POINTS: " << alpha_left_loc << " " << alpha_right_loc;
      } 

      string SpaceType; // VP-tree doesn't use the space type during creation.

      GetOptimalAlphas(printProgress_,
                     *config, 
                     metric, desiredRecallAdjusted,
                     SpaceType, 
                     METH_VPTREE, 
                     AnyParams(paramDesc), getEmptyParams(),
                     recall_loc, 
                     time_best_loc, impr_best_loc,
                     alpha_left_loc, expLeft, alpha_right_loc, expRight,
                     maxIter, maxRecDepth, stepN, fullFactor, maxCacheGSQty);

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

    /*
     * Note that the tunning procedure overrides the default values,
     * to prevent the procedure SetQueryTimeParams from overriding them.
     * If one calls SetQueryTimeParams with the set of empty parameters,
     * all parameters are reset to default values. So, we want to this default
     * values to be equal to the values obtained during tuning.
     */
    alpha_left_default_ = alpha_left_ = alpha_left;
    alpha_right_default_ = alpha_right_ = alpha_right;
    exp_left_default_ = exp_left_ = exp_left;
    exp_right_default_ = exp_right_ = exp_right;

    stringstream  bestParams;
    bestParams << ALPHA_LEFT_PARAM << "=" << alpha_left_ << "," << ALPHA_RIGHT_PARAM << "=" << alpha_right_ << ","
                 << EXP_LEFT_PARAM   << "=" << exp_left_   << "," << EXP_RIGHT_PARAM   << "=" << exp_right_;

    if (printProgress_) {
      cout << "===============================================================" << endl;
      cout << "|                      OPTIMIZATION RESULTS                   |" << endl;
      cout << "===============================================================" << endl;
      if (!knn.empty()) {
        cout << "K: "  << knn[0] << endl;
      } else {
        cout << "Range: "  << range[0] << endl;
      }
      cout << "Recall: " << recall << endl;
      cout << "Best time: " << time_best << endl;
      cout << "Best impr. " << impr_best << " (" << getOptimMetricName(metric) << ")" << endl; 
      cout << "alpha_left: " << alpha_left << endl;
      cout << "exp_left: " << exp_left << endl;
      cout << "alpha_right: " << alpha_right << endl;
      cout << "exp_right: " << exp_right << endl;
      cout << "optimal parameters: " << bestParams.str() << endl;
      cout << "===============================================================" << endl;
    }

    if (recall < desiredRecallAdjusted) {
      LOG(LIB_INFO) << "Failed to get the desired recall!";
      throw runtime_error("Failed to get the desired recall!");
    }
 
  }
}

template class PolynomialPruner<int>;
template class PolynomialPruner<float>;


}
