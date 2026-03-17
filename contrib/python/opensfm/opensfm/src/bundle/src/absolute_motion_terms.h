#pragma once

#include "error_utils.h"

#include <Eigen/Eigen>


template< class PosFunc >
struct BAAbsolutePositionError {
  BAAbsolutePositionError(const PosFunc& pos_func, 
                          const Eigen::Vector3d& pos_prior,
                          double std_deviation,
                          bool has_std_deviation_param,
                          const PositionConstraintType& type)
      : pos_func_(pos_func)
      , pos_prior_(pos_prior)
      , scale_(1.0 / std_deviation)
      , has_std_deviation_param_(has_std_deviation_param)
      , type_(type)
  {}

  template <typename T>
  bool operator()(T const* const* p, T* r) const {
    Eigen::Map< Eigen::Matrix<T,3,1> > residual(r);

    // error is : position_prior - adjusted_position
    residual = pos_prior_.cast<T>() - pos_func_(p);
    if(has_std_deviation_param_){
      residual /= p[1][0];
    }
    else{
      residual *= T(scale_);
    }

    // filter axises to use
    const std::vector<PositionConstraintType> axises = {
        PositionConstraintType::X, 
        PositionConstraintType::Y,
        PositionConstraintType::Z};
    for (int i = 0; i < axises.size(); ++i) {
      if(!HasFlag(axises[i])){
        residual(i) *= T(0.);
      }
    }
    return true;
  }

  inline bool HasFlag(const PositionConstraintType& flag)const{
    return (int(type_) & int(flag)) == int(flag);
  }

  PosFunc pos_func_;
  Eigen::Vector3d pos_prior_;
  double scale_;
  bool has_std_deviation_param_;
  PositionConstraintType type_;
};

template <typename T>
T diff_between_angles(T a, T b) {
  T d = a - b;
  if (d > T(M_PI)) {
    return d - T(2 * M_PI);
  } else if (d < -T(M_PI)) {
    return d + T(2 * M_PI);
  } else {
    return d;
  }
}

struct BAUpVectorError {
  BAUpVectorError(const Eigen::Vector3d& acceleration, double std_deviation)
      : scale_(1.0 / std_deviation)
  {
    acceleration_ = acceleration.normalized();
  }

  template <typename T>
  bool operator()(const T* const shot, T* r) const {
    Eigen::Map< const Eigen::Matrix<T,3,1> > R(shot + BA_SHOT_RX);
    Eigen::Map< Eigen::Matrix<T,3,1> > residual(r);

    const Eigen::Matrix<T,3,1> acceleration = acceleration_.cast<T>();
    const Eigen::Matrix<T,3,1> z_world = RotatePoint((-R).eval(), acceleration);
    const Eigen::Matrix<T,3,1> z_axis =  Eigen::Vector3d(0, 0, 1).cast<T>();
    residual = T(scale_) * (z_world - z_axis);
    return true;
  }

  Eigen::Vector3d acceleration_;
  double scale_;
};

struct BAPanAngleError {
  BAPanAngleError(double angle, double std_deviation)
      : angle_(angle)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot,
                  T* residuals) const {
    Eigen::Map< const Eigen::Matrix<T,3,1> > R(shot + BA_SHOT_RX);

    const Eigen::Matrix<T,3,1> z_axis = Eigen::Vector3d(0, 0, 1).cast<T>();
    const auto z_world = RotatePoint((-R).eval(), z_axis);

    if (ceres::abs(z_world(0)) < T(1e-8) && ceres::abs(z_world(1)) < T(1e-8)) {
      residuals[0] = T(0.0);
    } else {
      const T predicted_angle = atan2(z_world(0), z_world(1));
      residuals[0] = T(scale_) * diff_between_angles(predicted_angle, T(angle_));
    }
    return true;
  }

  double angle_;
  double scale_;
};

struct BATiltAngleError {
  BATiltAngleError(double angle, double std_deviation)
      : angle_(angle)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot,
                  T* residuals) const {
    const T* const R = shot + BA_SHOT_RX;
    T Rt[3] = { -R[0], -R[1], -R[2] }; // transposed R
    T ez[3] = { T(0), T(0), T(1) }; // ez: A point in front of the camera (z=1)
    T Rt_ez[3];
    ceres::AngleAxisRotatePoint(Rt, ez, Rt_ez);

    T l = sqrt(Rt_ez[0] * Rt_ez[0] + Rt_ez[1] * Rt_ez[1]);
    T predicted_angle = -atan2(Rt_ez[2], l);

    residuals[0] = T(scale_) * diff_between_angles(predicted_angle, T(angle_));
    return true;
  }

  double angle_;
  double scale_;
};

struct BARollAngleError {
  BARollAngleError(double angle, double std_deviation)
      : angle_(angle)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot,
                  T* residuals) const {
    const T* const R = shot + BA_SHOT_RX;
    T Rt[3] = { -R[0], -R[1], -R[2] }; // transposed R
    T ex[3] = { T(1), T(0), T(0) }; // A point to the right of the camera (x=1)
    T ez[3] = { T(0), T(0), T(1) }; // A point in front of the camera (z=1)
    T Rt_ex[3], Rt_ez[3];
    T tangle_ = T(angle_);
    ceres::AngleAxisRotatePoint(Rt, ex, Rt_ex);
    ceres::AngleAxisRotatePoint(Rt, ez, Rt_ez);

    T a[3] = {Rt_ez[1], -Rt_ez[0], T(0)};
    T la = sqrt(a[0] * a[0] + a[1] * a[1]);

    if (la < 1e-5) {
      residuals[0] = T(0.0);
      return true;
    }

    a[0] /= la;
    a[1] /= la;
    T b[3];
    ceres::CrossProduct(Rt_ex, a, b);
    T sin_roll = Rt_ez[0] * b[0] + Rt_ez[1] * b[1] + Rt_ez[2] * b[2];
    T predicted_angle = asin(sin_roll);
    residuals[0] = T(scale_) * diff_between_angles(predicted_angle, T(angle_));

    return true;
  }

  double angle_;
  double scale_;
};

struct RotationPriorError {
  RotationPriorError(double *R_prior, double std_deviation)
      : R_prior_(R_prior)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot, T* residuals) const {
    // Get rotation and translation values.
    const T* const R = shot + BA_SHOT_RX;
    T Rpt[3] = { -T(R_prior_[0]),
                 -T(R_prior_[1]),
                 -T(R_prior_[2]) };

    // Compute rotation residual: log( R Rp^t )
    T qR[4], qRpt[4], qR_Rpt[4];
    ceres::AngleAxisToQuaternion(R, qR);
    ceres::AngleAxisToQuaternion(Rpt, qRpt);
    ceres::QuaternionProduct(qR, qRpt, qR_Rpt);
    T R_Rpt[3];
    ceres::QuaternionToAngleAxis(qR_Rpt, R_Rpt);

    residuals[0] = T(scale_) * R_Rpt[0];
    residuals[1] = T(scale_) * R_Rpt[1];
    residuals[2] = T(scale_) * R_Rpt[2];

    return true;
  }

  double *R_prior_;
  double scale_;
};

struct TranslationPriorError {
  TranslationPriorError(double *translation_prior, double std_deviation)
      : translation_prior_(translation_prior)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot, T* residuals) const {
    residuals[0] = T(scale_) * (T(translation_prior_[0]) - shot[BA_SHOT_TX]);
    residuals[1] = T(scale_) * (T(translation_prior_[1]) - shot[BA_SHOT_TY]);
    residuals[2] = T(scale_) * (T(translation_prior_[2]) - shot[BA_SHOT_TZ]);
    return true;
  }

  double *translation_prior_;
  double scale_;
};

struct PositionPriorError {
  PositionPriorError(double *position_prior, double std_deviation)
      : position_prior_(position_prior)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const shot, T* residuals) const {
    T Rt[3] = { -shot[BA_SHOT_RX], -shot[BA_SHOT_RY], -shot[BA_SHOT_RZ] };
    T p[3];
    ceres::AngleAxisRotatePoint(Rt, shot + BA_SHOT_TX, p);

    residuals[0] = T(scale_) * (p[0] + T(position_prior_[0]));
    residuals[1] = T(scale_) * (p[1] + T(position_prior_[1]));
    residuals[2] = T(scale_) * (p[2] + T(position_prior_[2]));
    return true;
  }

  double *position_prior_;
  double scale_;
};

struct UnitTranslationPriorError {
  UnitTranslationPriorError() {}

  template <typename T>
  bool operator()(const T* const shot, T* residuals) const {
    const T* const t = shot + 3;
    residuals[0] = log(t[0] * t[0] + t[1] * t[1] + t[2] * t[2]);
    return true;
  }
};

struct PointPositionPriorError {
  PointPositionPriorError(double *position, double std_deviation)
      : position_(position)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const p, T* residuals) const {
    residuals[0] = T(scale_) * (p[0] - T(position_[0]));
    residuals[1] = T(scale_) * (p[1] - T(position_[1]));
    residuals[2] = T(scale_) * (p[2] - T(position_[2]));
    return true;
  }

  double *position_;
  double scale_;
};
