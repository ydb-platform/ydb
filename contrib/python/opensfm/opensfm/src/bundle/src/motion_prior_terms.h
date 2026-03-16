#pragma once

#include "error_utils.h"

#include <Eigen/Eigen>

struct BALinearMotionError {
  BALinearMotionError(double alpha,
                      double position_std_deviation,
                      double orientation_std_deviation)
      : alpha_(alpha)
      , position_scale_(1.0 / position_std_deviation)
      , orientation_scale_(1.0 / orientation_std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot0,
                  const T* const shot1,
                  const T* const shot2,
                  T* r) const {
    Eigen::Map< const Eigen::Matrix<T,3,1> > R0(shot0 + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > t0(shot0 + BA_SHOT_TX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > R1(shot1 + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > t1(shot1 + BA_SHOT_TX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > R2(shot2 + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > t2(shot2 + BA_SHOT_TX);

    // Residual have the general form :
    //  op( alpha . op(2, -0), op(0, -1))
    // with - being the opposite
    Eigen::Map< Eigen::Matrix<T,6,1> > residual(r);

    // Position residual : op is translation
    const auto c0 = OriginFromRT(R0, t0);
    const auto c1 = OriginFromRT(R1, t1);
    const auto c2 = OriginFromRT(R2, t2);
    residual.segment(0, 3) = T(position_scale_) * (T(alpha_) * (c2 - c0) + (c0 - c1));

    // Rotation residual : op is rotation
    const Eigen::Matrix<T,3,1> R2_R0t = T(alpha_) * MultRotations(R2, (-R0).eval());
    const Eigen::Matrix<T,3,1> R0_R1t = MultRotations(R0, (-R1).eval());
    residual.segment(3, 3) = T(position_scale_) * MultRotations(R2_R0t, R0_R1t);
    return true;
  }

  Eigen::Vector3d acceleration_;
  double position_scale_;
  double orientation_scale_;
  double alpha_;
};