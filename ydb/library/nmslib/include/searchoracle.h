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
#ifndef SEARCH_ORACLE_HPP
#define SEARCH_ORACLE_HPP

#include <string>
#include <cmath>
#include <vector>
#include <sstream>

#include "object.h"
#include "space.h"
#include "pow.h"
#include "params.h"
#include "experimentconf.h"
#include "logging.h"

#define EXP_LEFT_PARAM              "expLeft"
#define EXP_RIGHT_PARAM             "expRight"
#define ALPHA_LEFT_PARAM            "alphaLeft"
#define ALPHA_RIGHT_PARAM           "alphaRight"
#define MIN_EXP_PARAM               "minExp"
#define MAX_EXP_PARAM               "maxExp"
#define DESIRED_RECALL_PARAM        "desiredRecall"
#define TUNE_K_PARAM                "tuneK"
#define TUNE_R_PARAM                "tuneR"
#define TUNE_QTY_PARAM              "tuneQty"

#define MAX_CACHE_GS_QTY_PARAM      "maxCacheGSQty"
#define MAX_ITER_PARAM              "maxIter"
#define MAX_REC_DEPTH_PARAM         "maxRecDepth"
#define STEP_N_PARAM                "stepN"
#define ADD_RESTART_QTY_PARAM       "addRestartQty"
#define FULL_FACTOR_PARAM           "fullFactor"

namespace similarity {

const size_t MIN_EXP_DEFAULT          =  1;
const size_t MAX_EXP_DEFAULT          =  1;

const size_t MAX_CACHE_GS_QTY_DEFAULT =  1000;
const size_t MAX_ITER_DEFAULT         =  10;
const size_t MAX_REC_DEPTH_DEFAULT    =  5;
const size_t STEP_N_DEFAULT           =  2;
const size_t ADD_RESTART_QTY_DEFAULT  =  2;
const double FULL_FACTOR_DEFAULT      = 8.0;

using std::string; 
using std::vector; 
using std::stringstream;

enum OptimMetric  {IMPR_DIST_COMP,
                   IMPR_EFFICIENCY,
                   IMPR_INVALID};

#define OPTIM_METRIC_PARAMETER  "metric"

#define OPTIM_IMPR_DIST_COMP  "dist"
#define OPTIM_IMPR_EFFICIENCY "time"
#define OPTIM_METRIC_DEFAULT   OPTIM_IMPR_DIST_COMP 

inline OptimMetric getOptimMetric(const string& s) {
  string s1 = s;
  ToLower(s1);
  if (s1 == OPTIM_IMPR_DIST_COMP) return IMPR_DIST_COMP;
  if (s1 == OPTIM_IMPR_EFFICIENCY) return IMPR_EFFICIENCY;
  return IMPR_INVALID;
}

inline string getOptimMetricName(OptimMetric metr) {
  if (IMPR_DIST_COMP == metr) return "improvement in dist. comp";
  if (IMPR_EFFICIENCY == metr) return "improvement in efficiency";
  throw runtime_error("Bug: Invalid optimization metric name");
}

/*
 * Basic pruning oracles are built on the idea that you can relax the pruning criterion
 * in a kd-tree or a vp-tree.
 *
 * First, this idea was proposed by P.N. Yianilos in 1999:
 * Peter N. Yianilos, Locally lifting the curse of dimensionality for nearest neighbor search.
 *
 * It was later generalized to metric spaces. The introduced technique was called
 * "stretching of the triangle inequality". Stretching was governed by a single coefficient alpha,
 * so that the classic metric-space VP-tree pruning rule:
 *
 * MaxDist <= | M  - d(q, pivot) |
 *
 * was replaced by:
 *
 * MaxDist <= alpha | M  - d(q, pivot) |
 *
 * Here, M is a median distance from data points to the pivot and MaxDist
 * is the minimum distance from an object to query encountered during the search
 * (prior to encountering the current pivot/node), which plays a role of the 
 * query radius.
 *
 * Stretching of the triangle inequality was described in:
 *
 * Probabilistic proximity search: 
 * Fighting the curse of dimensionality in metric spaces
 * E Chavez, G Navarro 
 *
 * Boytsov and Bilegsaikhan showed that a more generic pruning is needed if we want to
 * search in generic metric spaces, where the distance is not symmetric. This more generic
 * pruning method can also be more efficient in metric spaces than the originally proposed 
 * stretching rule.
 *
 * More specifically, two potentially different stretching coefficients alphaLeft and alphaRight
 * are used for the left and the right partition, respectively. 
 *
 * The results were published in:
 * Boytsov, Leonid, and Bilegsaikhan Naidan. 
 * "Learning to prune in metric and non-metric spaces." Advances in Neural Information Processing Systems. 2013.
 * However, the tunning procedure itself was later slightly improved and, most importantly, modified to
 * to tune to a specific recall using only a sample of the data.
 *
 * Another small extension is to support a polynomial approximation of the pruning rule.
 * This works best for low-dimensional spaces. For high dimensional spaces, the linear rule is not worse.
 * In the left subtree we prune if:
 * MaxDist <= alphaLeft | M  - d(q, pivot) |^expLeft
 * In the right subtree we prune if:
 * MaxDist <= alphaRight | M  - d(q, pivot) |^expRight
 */

enum VPTreeVisitDecision { kVisitLeft = 1, kVisitRight = 2, kVisitBoth = 3 };

template <typename dist_t>
class PolynomialPruner {
public:
  static std::string GetName() { return "polynomial pruner"; }
  PolynomialPruner(Space<dist_t>& space, const ObjectVector& data, bool bPrintProgres) : 
      space_(space), data_(data), printProgress_(bPrintProgres), 
       alpha_left_(1), exp_left_(1), alpha_right_(1), exp_right_(1),
       alpha_left_default_(1), exp_left_default_(1), alpha_right_default_(1), exp_right_default_(1) {}
  // It's important to pass parameters only by reference here!
  void SetQueryTimeParams(AnyParamManager& pmgr);
  void SetIndexTimeParams(AnyParamManager& pmgr);

  vector<string> GetQueryTimeParamNames() const {
    vector<string> res = {ALPHA_LEFT_PARAM, EXP_LEFT_PARAM, ALPHA_RIGHT_PARAM, EXP_RIGHT_PARAM};

    return res;
  }
  
  void LogParams() {
    LOG(LIB_INFO) << ALPHA_LEFT_PARAM << " = "   << alpha_left_ << " " << EXP_LEFT_PARAM << " = " << exp_left_;
    LOG(LIB_INFO) << ALPHA_RIGHT_PARAM << " = " << alpha_right_ << " " << EXP_RIGHT_PARAM << " = " << exp_right_;
  }

  inline VPTreeVisitDecision Classify(dist_t distQueryPivot, dist_t MaxDist, dist_t MedianDist) const {

    /*
     * If the median is in both subtrees (e.g., this is often the case of a discrete metric)
     * and the distance to the pivot is MedianDist, we need to visit both subtrees.
     * Hence, we check for the strict inequality!!! Even if MaxDist == 0, 
     * for the case of dist == MedianDist, 0 < 0 may be false. 
     * Thus, we visit both subtrees. 
     */
    if (distQueryPivot <= MedianDist) {
      double diff = double(MedianDist - distQueryPivot);
      double expDiff = EfficientPow(diff, exp_left_);
      //LOG(LIB_INFO) << " ### " << diff << " -> " << expDiff;
      if (double(MaxDist) < alpha_left_ * expDiff) return (kVisitLeft);
    }
    if (distQueryPivot >= MedianDist) {
      double diff = double(distQueryPivot - MedianDist);  
      double expDiff = EfficientPow(diff, exp_right_);
      //LOG(LIB_INFO) << " ### " << diff << " -> " << expDiff;
      if (double(MaxDist) < alpha_right_* expDiff) return (kVisitRight);
    }

    return (kVisitBoth);
  }
  string Dump() { 
    stringstream str;

    str << ALPHA_LEFT_PARAM << ": " << alpha_left_ << " ExponentLeft: " << exp_left_ << " " <<
           ALPHA_RIGHT_PARAM << ": " << alpha_right_ << " ExponentRight: " << exp_right_ ;
    return str.str();
  }
private:
  Space<dist_t>&        space_;
  const ObjectVector    data_;
  bool                  printProgress_;

  double    alpha_left_;
  unsigned  exp_left_;
  double    alpha_right_;
  unsigned  exp_right_;

  double    alpha_left_default_;
  unsigned  exp_left_default_;
  double    alpha_right_default_;
  unsigned  exp_right_default_;
};


}

#endif
