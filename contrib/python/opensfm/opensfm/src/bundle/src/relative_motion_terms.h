#pragma once

#include "error_utils.h"

#include <Eigen/Eigen>

struct BARelativeMotionError {
  BARelativeMotionError(const Eigen::VectorXd& Rtij,
                        const Eigen::MatrixXd& scale_matrix)
      : Rtij_(Rtij)
      , scale_matrix_(scale_matrix)
  {}

  template <typename T>
  Eigen::Matrix<T, 6, 1> Error(const T* const shot_i, const T* const scale,
                               const T* const shot_j) const {
    // Get rotation and translation values.
    Eigen::Map< const Eigen::Matrix<T,3,1> > Ri(shot_i + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > Rj(shot_j + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > ti(shot_i + BA_SHOT_TX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > tj(shot_j + BA_SHOT_TX);
    Eigen::Matrix<T,6,1> residual;

    // Compute rotation residual: log( Rij Ri Rj^t )
    const Eigen::Matrix<T,3,1> Rij = Rtij_.segment<3>(BA_SHOT_RX).cast<T>();
    residual.segment(0, 3) = RelativeRotationError(Ri, Rj, Rij);

    // Compute translation residual: tij - scale * ( tj - Rj Ri^t ti )
    const auto tij = Rtij_.segment<3>(BA_SHOT_TX).cast<T>();
    residual.segment(3, 3) = tij - scale[0] * (tj - RotatePoint(Rj, RotatePoint((-Ri).eval(), ti)));
    return residual;
  }

  template <typename T>
  bool operator()(const T* const shot_i, 
                  const T* const scale,
                  const T* const shot_j, 
                  T* r) const {
    Eigen::Map< Eigen::Matrix<T,6,1> > residual(r);
    residual = scale_matrix_.cast<T>()*Error(shot_i, scale, shot_j);
    return true;
  }

  Eigen::VectorXd Rtij_;
  Eigen::MatrixXd scale_matrix_;
};

struct BARelativeSimilarityError : public BARelativeMotionError {
  BARelativeSimilarityError(const Eigen::VectorXd& Rtij, double Sij, const Eigen::MatrixXd& scale_matrix)
      : BARelativeMotionError(Rtij, scale_matrix), Sij_(Sij)
  {}

  template <typename T>
  bool operator()(const T* const shot_i, const T* const scale_i,
                  const T* const shot_j, const T* const scale_j,
                  T* r) const {
    Eigen::Map< Eigen::Matrix<T,7,1> > residual(r);
    residual.segment(0, 6) = BARelativeMotionError::Error(shot_i, scale_j, shot_j);
    residual(6) = (T(Sij_) - scale_j[0] / scale_i[0]);
    residual = scale_matrix_.cast<T>()*residual;
    return true;
  }

  double Sij_;
};

struct BARelativeRotationError {
  BARelativeRotationError(const Eigen::Vector3d& Rij,
                          const Eigen::Matrix3d& scale_matrix)
      : Rij_(Rij)
      , scale_matrix_(scale_matrix)
  {}

  template <typename T>
  bool operator()(const T* const shot_i,
                  const T* const shot_j,
                  T* r) const {
    // Get rotation and translation values.
    Eigen::Map< const Eigen::Matrix<T,3,1> > Ri(shot_i + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > Rj(shot_j + BA_SHOT_RX);
    Eigen::Map< Eigen::Matrix<T,3,1> > residual(r);

    // Compute rotation residual: log( Rij Ri Rj^t )
    const Eigen::Matrix<T,3,1> Rij = Rij_.cast<T>();
    residual = scale_matrix_.cast<T>()*RelativeRotationError(Ri, Rj, Rij);
    return true;
  }

  Eigen::Vector3d Rij_;
  Eigen::Matrix3d scale_matrix_;
};

struct BACommonPositionError {
  BACommonPositionError(double margin, double std_deviation)
      : margin_(margin), scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot_1,
                  const T* const shot_2,
                  T* r) const {
    Eigen::Map< const Eigen::Matrix<T,3,1> > R1(shot_1 + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > t1(shot_1 + BA_SHOT_TX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > R2(shot_2 + BA_SHOT_RX);
    Eigen::Map< const Eigen::Matrix<T,3,1> > t2(shot_2 + BA_SHOT_TX);
    Eigen::Map< Eigen::Matrix<T,3,1> > residual(r);

    // error is : shot_origin_1 - shot_origin_2
    Eigen::Matrix<T,3,1> error = OriginFromRT(R1, t1) - OriginFromRT(R2, t2);

    // restrict XYerror to some positive margin (?)
    for( int i = 0; i < 2; ++i){
      error(i) = std::max(T(0.0), ceres::abs(error(i)) - T(margin_));
    }
    residual = T(scale_) * error;
    return true;
  }

  double margin_;
  double scale_;
};