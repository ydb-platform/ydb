/**
 * @file fed.h
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

#pragma once

/* ************************************************************************* */
// System
#include <vector>

/* ************************************************************************* */
/// This function allocates an array of the least number of time steps such
/// that a certain stopping time for the whole process can be obtained and fills
/// it with the respective FED time step sizes for one cycle
/// The function returns the number of time steps per cycle or 0 on failure
/// @param T Desired process stopping time
/// @param M Desired number of cycles
/// @param tau_max Stability limit for the explicit scheme
/// @param reordering Reordering flag
/// @param tau The vector with the dynamic step sizes
int fed_tau_by_process_time(const float T, const int M, const float tau_max,
                            const bool reordering, std::vector<float>& tau);

/// This function allocates an array of the least number of time steps such
/// that a certain stopping time for the whole process can be obtained and fills it
/// it with the respective FED time step sizes for one cycle
/// The function returns the number of time steps per cycle or 0 on failure
/// @param t Desired cycle stopping time
/// @param tau_max Stability limit for the explicit scheme
/// @param reordering Reordering flag
/// @param tau The vector with the dynamic step sizes
int fed_tau_by_cycle_time(const float t, const float tau_max,
                          const bool reordering, std::vector<float>& tau);

/// This function allocates an array of time steps and fills it with FED
/// time step sizes
/// The function returns the number of time steps per cycle or 0 on failure
/// @param n Number of internal steps
/// @param scale Ratio of t we search to maximal t
/// @param tau_max Stability limit for the explicit scheme
/// @param reordering Reordering flag
/// @param tau The vector with the dynamic step sizes
int fed_tau_internal(const int n, const float scale, const float tau_max,
                     const bool reordering, std::vector<float>& tau);

/// This function checks if a number is prime or not
bool fed_is_prime_internal(const int number);
