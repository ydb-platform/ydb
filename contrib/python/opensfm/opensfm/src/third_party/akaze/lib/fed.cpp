//=============================================================================
//
// fed.cpp
// Authors: Pablo F. Alcantarilla (1), Jesus Nuevo (2)
// Institutions: Toshiba Research Europe Ltd (1)
//               TrueVision Solutions (2)
// Date: 07/10/2014
// Email: pablofdezalc@gmail.com
//
// AKAZE Features Copyright 2014, Pablo F. Alcantarilla, Jesus Nuevo
// All Rights Reserved
// See LICENSE for the license information
//=============================================================================

/**
 * @file fed.cpp
 * @brief Functions for performing Fast Explicit Diffusion and building the
 * nonlinear scale space
 * @date Oct 07, 2014
 * @author Pablo F. Alcantarilla, Jesus Nuevo
 * @note This code is derived from FED/FJ library from Grewenig et al.,
 * The FED/FJ library allows solving more advanced problems
 * Please look at the following papers for more information about FED:
 * [1] S. Grewenig, J. Weickert, C. Schroers, A. Bruhn. Cyclic Schemes for
 * PDE-Based Image Analysis. Technical Report No. 327, Department of Mathematics,
 * Saarland University, Saarbrücken, Germany, March 2013
 * [2] S. Grewenig, J. Weickert, A. Bruhn. From box filtering to fast explicit diffusion.
 * DAGM, 2010
 *
*/

#define _USE_MATH_DEFINES
#include "fed.h"

// System
#include <cmath>

using namespace std;

/* ************************************************************************* */
int fed_tau_by_process_time(const float T, const int M, const float tau_max,
                            const bool reordering, std::vector<float>& tau) {
  // All cycles have the same fraction of the stopping time
  return fed_tau_by_cycle_time(T/(float)M,tau_max,reordering,tau);
}

/* ************************************************************************* */
int fed_tau_by_cycle_time(const float t, const float tau_max,
                          const bool reordering, std::vector<float>& tau) {

  int n = 0;          // Number of time steps
  float scale = 0.0;  // Ratio of t we search to maximal t

  // Compute necessary number of time steps
  n = (int)(ceil(sqrt(3.0*t/tau_max+0.25f)-0.5f-1.0e-8f)+ 0.5f);
  scale = 3.0*t/(tau_max*(float)(n*(n+1)));

  // Call internal FED time step creation routine
  return fed_tau_internal(n,scale,tau_max,reordering,tau);
}

/* ************************************************************************* */
int fed_tau_internal(const int n, const float scale, const float tau_max,
                     const bool reordering, std::vector<float>& tau) {

  float c = 0.0, d = 0.0;     // Time savers
  vector<float> tauh;    // Helper vector for unsorted taus

  if (n <= 0)
    return 0;

  // Allocate memory for the time step size
  tau = vector<float>(n);

  if (reordering)
    tauh = vector<float>(n);

  // Compute time saver
  c = 1.0f / (4.0f * (float)n + 2.0f);
  d = scale * tau_max / 2.0f;

  // Set up originally ordered tau vector
  for (int k = 0; k < n; ++k) {
    float h = cos(M_PI * (2.0f * (float)k + 1.0f) * c);

    if (reordering)
      tauh[k] = d / (h * h);
    else
      tau[k] = d / (h * h);
  }

  // Permute list of time steps according to chosen reordering function
  int kappa = 0, prime = 0;

  if (reordering == true) {
    // Choose kappa cycle with k = n/2
    // This is a heuristic. We can use Leja ordering instead!!
    kappa = n / 2;

    // Get modulus for permutation
    prime = n + 1;

    while (!fed_is_prime_internal(prime)) {
      prime++;
    }

    // Perform permutation
    for (int k = 0, l = 0; l < n; ++k, ++l) {
      int index = 0;
      while ((index = ((k+1)*kappa) % prime - 1) >= n) {
        k++;
      }

      tau[l] = tauh[index];
    }
  }

  return n;
}

/* ************************************************************************* */
bool fed_is_prime_internal(const int number) {

  if (number <= 1) {
    return false;
  }
  else if (number == 2 || number == 3 || number == 5 || number == 7) {
    return true;
  }
  else if ((number % 2) == 0 || (number % 3) == 0 || (number % 5) == 0 || (number % 7) == 0) {
    return false;
  }
  else {
    int upperLimit = sqrt(number+1.0);
    int divisor = 11;

    while (divisor <= upperLimit ) {
      if (number % divisor == 0)
      {
        return false;
      }

      divisor +=2;
    }

    return true;
  }
}
