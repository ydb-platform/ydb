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
#ifndef REPORT_INTR_DIM_H
#define REPORT_INTR_DIM_H

#include <string>
#include "object.h"
#include "my_isnan_isinf.h"

namespace similarity {

/*
 * This version of intrinsic dimensionality is defined in 
 * E. Chavez, G. Navarro, R. Baeza-Yates, and J. L. Marroquin, 2001, Searching in metric spaces.
 *
 * Note that this measure may be irrelevant in non-metric spaces.
 */
template <typename dist_t>
void ComputeIntrinsicDimensionality(const Space<dist_t>& space, 
                               const ObjectVector& dataset,
                               double& IntrDim,
                               double& DistMean,
                               double& DistSigma,
                               std::vector<double>& dist,
                               size_t SampleQty = 1000000) {
  DistMean = 0;
  dist.clear();
  for (size_t n = 0; n < SampleQty; ++n) {
    size_t r1 = RandomInt() % dataset.size();
    size_t r2 = RandomInt() % dataset.size();
    CHECK(r1 < dataset.size());
    CHECK(r2 < dataset.size());
    const Object* obj1 = dataset[r1];
    const Object* obj2 = dataset[r2];
    dist_t d = space.IndexTimeDistance(obj1, obj2);
    dist.push_back(d);
    if (my_isnan(d)) {
      /* 
       * TODO: @leo Dump object contents here. To this end,
       *            we need to subclass objects, so that sparse
       *            vectors, dense vectors and other objects
       *            can implement their own dump function.
       */
      throw runtime_error("!!! Bug: a distance returned NAN!");
    }
    DistMean += d;
  }
  DistMean /= double(SampleQty);
  DistSigma = 0;
  for (size_t i = 0; i < SampleQty; ++i) {
    DistSigma += (dist[i] - DistMean) * (dist[i] - DistMean);
  }
  DistSigma /= double(SampleQty);
  IntrDim = DistMean * DistMean / (2 * DistSigma);
  DistSigma = sqrt(DistSigma);
}

template <typename dist_t>
void ReportIntrinsicDimensionality(const string& reportName,
                                   const Space<dist_t>& space, 
                                   const ObjectVector& dataset,
                                   std::vector<double>& dist,
                                   size_t SampleQty = 1000000) {
    double DistMean, DistSigma, IntrDim;

    ComputeIntrinsicDimensionality(space, dataset,
                                  IntrDim,
                                  DistMean,
                                  DistSigma,
                                  dist,
                                  SampleQty);

    LOG(LIB_INFO) << "### " << reportName;
    LOG(LIB_INFO) << "### intrinsic dim: " << IntrDim;
    LOG(LIB_INFO) << "### distance mean: " << DistMean;
    LOG(LIB_INFO) << "### distance sigma: " << DistSigma;
}

}

#endif
