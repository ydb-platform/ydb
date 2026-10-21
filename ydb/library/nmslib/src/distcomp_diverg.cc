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
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <cmath>

#include "distcomp.h"
#include "logging.h"
#include "utils.h"
#include "pow.h"

namespace similarity {

using namespace std;

template <typename T> T alphaBetaDivergenceSlow(const T *x, const T *y,
                                                const int length, float alpha,
                                                float beta) {
  T res = 0;
  float alphaPlus1 = alpha + 1;
  for (int i = 0; i < length; ++i) {
    res += pow(x[i], alphaPlus1)*pow(y[i], beta);  
  } 
  return res;
}

template  float alphaBetaDivergenceSlow(const float* x, const float* y, const int length, float alpha, float beta);

template <typename T> T alphaBetaDivergenceFast(const T *x, const T *y, const int length, float alpha, float beta) {
  T res = 0;
  PowerProxyObject<T> powalphaPlus1(alpha + 1), powBeta(beta);

  for (int i = 0; i < length; ++i) {
    res += powalphaPlus1.pow(x[i])*powBeta.pow(y[i]);
  } 
  return res;
}

template  float alphaBetaDivergenceFast(const float* x, const float* y, const int length, float alpha, float beta);

template <typename T> T alphaBetaDivergenceSlowProxy(const T *x, const T *y,
                                                     const int length,
                                                     float alpha, float beta) {
  T res = 0;
  float alphaPlus1 = alpha + 1;
  for (int i = 0; i < length; ++i) {
    res += (pow(x[i], alphaPlus1)*pow(y[i], beta) + 
            pow(y[i], alphaPlus1)*pow(x[i], beta)) * 0.5;
  } 
  return res;
}

template  float alphaBetaDivergenceSlowProxy(const float* x, const float* y, const int length, float alpha, float beta);

template <typename T> T alphaBetaDivergenceFastProxy(const T *x, const T *y, const int length, float alpha, float beta) {
  PowerProxyObject<T> powalphaPlus1(alpha + 1), powBeta(beta);
  T res = 0;

  for (int i = 0; i < length; ++i) {
    res += powalphaPlus1.pow(x[i])* powBeta.pow(y[i]) + 
           powalphaPlus1.pow(y[i])* powBeta.pow(x[i]) * 0.5;
  } 
  return res;
}

template  float alphaBetaDivergenceFastProxy(const float* x, const float* y, const int length, float alpha, float beta);

template <typename T> T renyiDivergenceSlow(const T *x, const T *y,
                                            const int length, float alpha) {
  T sum = 0;
  T eps = -1e-6;
  float t = alpha-1; 
  for (int i = 0; i < length; ++i) {
    sum += x[i]*pow(x[i]/y[i], t);
  } 
  float res = 1/t * log(sum);
  CHECK_MSG(res >= eps, "Expected a non-negative result, but got " + ConvertToString(res) + " for alpha="  + ConvertToString(alpha));
  // Might be slightly negative due to rounding errors
  return max<T>(0, res);
}

template  float renyiDivergenceSlow(const float* x, const float* y, const int length, float alpha);

template <typename T> T renyiDivergenceFast(const T *x, const T *y, const int length, float alpha) {
  float t = alpha-1; 
  PowerProxyObject<T> powAlphaMinusOne(t);
  T sum = 0;
  T eps = -1e-6;
  for (int i = 0; i < length; ++i) {
    sum += x[i]*powAlphaMinusOne.pow(x[i]/y[i]);
  } 
  float res = 1/t * log(sum);
  CHECK_MSG(res >= eps, "Expected a non-negative result, but got " + ConvertToString(res) + " for alpha="  + ConvertToString(alpha));
  // Might be slightly negative due to rounding errors
  return max<T>(0, res);
}

template  float renyiDivergenceFast(const float* x, const float* y, const int length, float alpha);

}
