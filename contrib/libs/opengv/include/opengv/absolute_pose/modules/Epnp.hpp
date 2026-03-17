/******************************************************************************
 * Author:   Laurent Kneip                                                    *
 * Contact:  kneip.laurent@gmail.com                                          *
 * License:  Copyright (c) 2013 Laurent Kneip, ANU. All rights reserved.      *
 *                                                                            *
 * Redistribution and use in source and binary forms, with or without         *
 * modification, are permitted provided that the following conditions         *
 * are met:                                                                   *
 * * Redistributions of source code must retain the above copyright           *
 *   notice, this list of conditions and the following disclaimer.            *
 * * Redistributions in binary form must reproduce the above copyright        *
 *   notice, this list of conditions and the following disclaimer in the      *
 *   documentation and/or other materials provided with the distribution.     *
 * * Neither the name of ANU nor the names of its contributors may be         *
 *   used to endorse or promote products derived from this software without   *
 *   specific prior written permission.                                       *
 *                                                                            *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"*
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  *
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE *
 * ARE DISCLAIMED. IN NO EVENT SHALL ANU OR THE CONTRIBUTORS BE LIABLE        *
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL *
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR *
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER *
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT         *
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY  *
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF     *
 * SUCH DAMAGE.                                                               *
 ******************************************************************************/

// Note: this code has been downloaded from the homepage of the "Computer 
// Vision Laboratory" at EPFL Lausanne, and was originally developped by the 
// authors of [4]. I only adapted it to Eigen.


#ifndef OPENGV_ABSOLUTE_POSE_MODULES_EPNP_HPP_
#define OPENGV_ABSOLUTE_POSE_MODULES_EPNP_HPP_

#include <stdlib.h>
#include <Eigen/Eigen>
#include <Eigen/src/Core/util/DisableStupidWarnings.h>

namespace opengv
{
namespace absolute_pose
{
namespace modules
{

class Epnp
{
public:
  Epnp(void);
  ~Epnp();

  void set_maximum_number_of_correspondences(const int n);
  void reset_correspondences(void);
  void add_correspondence(
      const double X,
      const double Y,
      const double Z,
      const double x,
      const double y,
      const double z);

  double compute_pose(double R[3][3], double T[3]);

  void relative_error(
      double & rot_err,
      double & transl_err,
      const double Rtrue[3][3],
      const double ttrue[3],
      const double Rest[3][3],
      const double test[3]);

  void print_pose(const double R[3][3], const double t[3]);
  double reprojection_error(const double R[3][3], const double t[3]);

private:
  void choose_control_points(void);
  void compute_barycentric_coordinates(void);
  void fill_M(
      Eigen::MatrixXd & M,
      const int row,
      const double * alphas,
      const double u,
      const double v);
  void compute_ccs(const double * betas, const Eigen::MatrixXd & ut);
  void compute_pcs(void);

  void solve_for_sign(void);

  void find_betas_approx_1(
      const Eigen::Matrix<double,6,10> & L_6x10,
      const Eigen::Matrix<double,6,1> & Rho,
      double * betas);
  void find_betas_approx_2(
      const Eigen::Matrix<double,6,10> & L_6x10,
      const Eigen::Matrix<double,6,1> & Rho,
      double * betas);
  void find_betas_approx_3(
      const Eigen::Matrix<double,6,10> & L_6x10,
      const Eigen::Matrix<double,6,1> & Rho,
      double * betas);
  void qr_solve(
      Eigen::Matrix<double,6,4> & A,
      Eigen::Matrix<double,6,1> & b,
      Eigen::Matrix<double,4,1> & X);

  double dot(const double * v1, const double * v2);
  double dist2(const double * p1, const double * p2);

  void compute_rho(Eigen::Matrix<double,6,1> & Rho);
  void compute_L_6x10(
      const Eigen::MatrixXd & Ut,
      Eigen::Matrix<double,6,10> & L_6x10 );

  void gauss_newton(
      const Eigen::Matrix<double,6,10> & L_6x10,
      const Eigen::Matrix<double,6,1> & Rho,
      double current_betas[4]);
  void compute_A_and_b_gauss_newton(
      const Eigen::Matrix<double,6,10> & L_6x10,
      const Eigen::Matrix<double,6,1> & Rho,
      double cb[4],
      Eigen::Matrix<double,6,4> & A,
      Eigen::Matrix<double,6,1> & b);

  double compute_R_and_t(
      const Eigen::MatrixXd & Ut,
      const double * betas,
      double R[3][3],
      double t[3]);

  void estimate_R_and_t(double R[3][3], double t[3]);

  void copy_R_and_t(
      const double R_dst[3][3],
      const double t_dst[3],
      double R_src[3][3],
      double t_src[3]);

  void mat_to_quat(const double R[3][3], double q[4]);


  double uc, vc, fu, fv;

  double * pws, * us, * alphas, * pcs;
  int * signs; //added!
  int maximum_number_of_correspondences;
  int number_of_correspondences;

  double cws[4][3], ccs[4][3];
  double cws_determinant;
};

}
}
}

#endif /* OPENGV_ABSOLUTE_POSE_MODULES_EPNP_HPP_ */
