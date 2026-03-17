#pragma once

#include <Eigen/Eigen>

template <class PointFunc> 
struct BABearingError {
  BABearingError(const Eigen::Vector3d& bearing,
                 double bearing_std_deviation,
                 const PointFunc& pos_func)
      : bearing_(bearing)
      , bearing_scale_(1.0 / bearing_std_deviation)
      , pos_func_(pos_func)
  {}

  template <typename T>
  bool operator()(T const* const* p, T* r) const {
    Eigen::Map< Eigen::Matrix<T,3,1> > residual(r);
    const auto shot_point = pos_func_(p);
    residual = (bearing_.normalized().template cast<T>() - shot_point.normalized()) * T(bearing_scale_);
    return true;
  }

  Eigen::Vector3d bearing_;
  double bearing_scale_;
  PointFunc pos_func_;
};

template <typename T>
void WorldToCameraCoordinates(const T* const shot,
                              const T world_point[3],
                              T camera_point[3]) {
  ceres::AngleAxisRotatePoint(shot + BA_SHOT_RX, world_point, camera_point);
  camera_point[0] += shot[BA_SHOT_TX];
  camera_point[1] += shot[BA_SHOT_TY];
  camera_point[2] += shot[BA_SHOT_TZ];
}

template <typename T>
void PerspectiveProject(const T* const camera,
                        const T point[3],
                        T projection[2]) {
  T xp = point[0] / point[2];
  T yp = point[1] / point[2];

  // Apply second and fourth order radial distortion.
  const T& l1 = camera[BA_CAMERA_K1];
  const T& l2 = camera[BA_CAMERA_K2];
  T r2 = xp * xp + yp * yp;
  T distortion = T(1.0) + r2  * (l1 + r2 * l2);

  // Compute final projected point position.
  const T& focal = camera[BA_CAMERA_FOCAL];
  projection[0] = focal * distortion * xp;
  projection[1] = focal * distortion * yp;
}

struct PerspectiveReprojectionError {
  PerspectiveReprojectionError(double observed_x, double observed_y, double std_deviation)
      : observed_x_(observed_x)
      , observed_y_(observed_y)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const camera,
                  const T* const shot,
                  const T* const point,
                  T* residuals) const {
    T camera_point[3];
    WorldToCameraCoordinates(shot, point, camera_point);

    T predicted[2];
    PerspectiveProject(camera, camera_point, predicted);

    // The error is the difference between the predicted and observed position.
    residuals[0] = T(scale_) * (predicted[0] - T(observed_x_));
    residuals[1] = T(scale_) * (predicted[1] - T(observed_y_));

    return true;
  }

  double observed_x_;
  double observed_y_;
  double scale_;
};

template <typename T>
void BrownPerspectiveProject(const T* const camera,
                             const T point[3],
                             T projection[2]) {
  T xp = point[0] / point[2];
  T yp = point[1] / point[2];

  // Apply Brown--Conrady radial and tangential distortion
  const T& k1 = camera[BA_BROWN_CAMERA_K1];
  const T& k2 = camera[BA_BROWN_CAMERA_K2];
  const T& p1 = camera[BA_BROWN_CAMERA_P1];
  const T& p2 = camera[BA_BROWN_CAMERA_P2];
  const T& k3 = camera[BA_BROWN_CAMERA_K3];
  T r2 = xp * xp + yp * yp;
  T radial_distortion = T(1.0) + r2  * (k1 + r2 * (k2 + r2 * k3));

  T x_tangential_distortion = T(2.0) * p1 * xp * yp + p2 * (r2 + T(2.0) * xp * xp);
  T x_distorted = xp * radial_distortion + x_tangential_distortion;

  T y_tangential_distortion = p1 * (r2 + T(2.0) * yp * yp) + T(2.0) * p2 * xp * yp;
  T y_distorted = yp * radial_distortion + y_tangential_distortion;

  // Compute final projected point position.
  const T& focal_x = camera[BA_BROWN_CAMERA_FOCAL_X];
  const T& focal_y = camera[BA_BROWN_CAMERA_FOCAL_Y];
  const T& c_x = camera[BA_BROWN_CAMERA_C_X];
  const T& c_y = camera[BA_BROWN_CAMERA_C_Y];
  projection[0] = focal_x * x_distorted + c_x;
  projection[1] = focal_y * y_distorted + c_y;
}

struct BrownPerspectiveReprojectionError {
  BrownPerspectiveReprojectionError(double observed_x, double observed_y, double std_deviation)
      : observed_x_(observed_x)
      , observed_y_(observed_y)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const camera,
                  const T* const shot,
                  const T* const point,
                  T* residuals) const {
    T camera_point[3];
    WorldToCameraCoordinates(shot, point, camera_point);

    T predicted[2];
    BrownPerspectiveProject(camera, camera_point, predicted);

    // The error is the difference between the predicted and observed position.
    residuals[0] = T(scale_) * (predicted[0] - T(observed_x_));
    residuals[1] = T(scale_) * (predicted[1] - T(observed_y_));

    return true;
  }

  double observed_x_;
  double observed_y_;
  double scale_;
};

template <typename T>
void FisheyeProject(const T* const camera,
                    const T point[3],
                    T projection[2]) {
  const T& focal = camera[BA_CAMERA_FOCAL];
  const T& k1 = camera[BA_CAMERA_K1];
  const T& k2 = camera[BA_CAMERA_K2];
  const T &x = point[0];
  const T &y = point[1];
  const T &z = point[2];

  T l = sqrt(x * x + y * y);
  T theta = atan2(l, z);
  T theta2 = theta * theta;
  T theta_d = theta * (T(1.0) + theta2 * (k1 + theta2 * k2));
  T s = focal * theta_d / l;

  projection[0] = s * x;
  projection[1] = s * y; 
}

template <typename T>
void DualProject(const T* const camera, const T point[3], T projection[2]) {
  const T& focal = camera[BA_DUAL_CAMERA_FOCAL];
  const T& transition = camera[BA_DUAL_CAMERA_TRANSITION];
  const T& k1 = camera[BA_DUAL_CAMERA_K1];
  const T& k2 = camera[BA_DUAL_CAMERA_K2];
  const T& x = point[0];
  const T& y = point[1];
  const T& z = point[2];

  T l = sqrt(x * x + y * y);
  T theta = atan2(l, z);
  T x_fish = theta / l * x;
  T y_fish = theta / l * y;

  T x_persp = point[0] / point[2];
  T y_persp = point[1] / point[2];

  T x_dual = transition*x_persp + (1.0-transition)*x_fish;
  T y_dual = transition*y_persp + (1.0-transition)*y_fish;

  T r2 = x_dual * x_dual + y_dual * y_dual;
  T distortion = T(1.0) + r2  * (k1 + r2 * k2);

  projection[0] = focal * distortion * x_dual;
  projection[1] = focal * distortion * y_dual;
}

struct FisheyeReprojectionError {
  FisheyeReprojectionError(double observed_x, double observed_y, double std_deviation)
      : observed_x_(observed_x)
      , observed_y_(observed_y)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const camera,
                  const T* const shot,
                  const T* const point,
                  T* residuals) const {
    T camera_point[3];
    WorldToCameraCoordinates(shot, point, camera_point);

    T predicted[2];
    FisheyeProject(camera, camera_point, predicted);

    // The error is the difference between the predicted and observed position.
    residuals[0] = T(scale_) * (predicted[0] - T(observed_x_));
    residuals[1] = T(scale_) * (predicted[1] - T(observed_y_));

    return true;
  }

  double observed_x_;
  double observed_y_;
  double scale_;
};

struct DualReprojectionError {
  DualReprojectionError(double observed_x, double observed_y, double std_deviation)
      : observed_x_(observed_x)
      , observed_y_(observed_y)
      , scale_(1.0 / std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const camera,
                  const T* const shot,
                  const T* const point,
                  T* residuals) const {
    T camera_point[3];
    WorldToCameraCoordinates(shot, point, camera_point);

    T predicted[2];
    DualProject(camera, camera_point, predicted);

    // The error is the difference between the predicted and observed position.
    residuals[0] = T(scale_) * (predicted[0] - T(observed_x_));
    residuals[1] = T(scale_) * (predicted[1] - T(observed_y_));

    return true;
  }

  double observed_x_;
  double observed_y_;
  double scale_;
};

struct EquirectangularReprojectionError {
  EquirectangularReprojectionError(double observed_x, double observed_y, double std_deviation)
      : scale_(1.0 / std_deviation)
  {
    double lon = observed_x * 2 * M_PI;
    double lat = -observed_y * 2 * M_PI;
    bearing_vector_[0] = cos(lat) * sin(lon);
    bearing_vector_[1] = -sin(lat);
    bearing_vector_[2] = cos(lat) * cos(lon);
  }

  template <typename T>
  bool operator()(const T* const shot,
                  const T* const point,
                  T* residuals) const {
    // Position vector in camera coordinates.
    T p[3];
    WorldToCameraCoordinates(shot, point, p);

    // Project to unit sphere.
    const T l = sqrt(p[0] * p[0] + p[1] * p[1] + p[2] * p[2]);
    p[0] /= l;
    p[1] /= l;
    p[2] /= l;

    // Difference between projected vector and observed bearing vector
    // We use the difference between unit vectors as an approximation
    // to the angle for small angles.
    residuals[0] = T(scale_) * (p[0] - T(bearing_vector_[0]));
    residuals[1] = T(scale_) * (p[1] - T(bearing_vector_[1]));
    residuals[2] = T(scale_) * (p[2] - T(bearing_vector_[2]));

    return true;
  }

  double bearing_vector_[3];
  double scale_;
};

struct BasicRadialInternalParametersPriorError {
  BasicRadialInternalParametersPriorError(double focal_estimate,
                                          double focal_std_deviation,
                                          double k1_estimate,
                                          double k1_std_deviation,
                                          double k2_estimate,
                                          double k2_std_deviation)
      : log_focal_estimate_(log(focal_estimate))
      , focal_scale_(1.0 / focal_std_deviation)
      , k1_estimate_(k1_estimate)
      , k1_scale_(1.0 / k1_std_deviation)
      , k2_estimate_(k2_estimate)
      , k2_scale_(1.0 / k2_std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const parameters, T* residuals) const {
    residuals[0] = T(focal_scale_) * (log(parameters[BA_CAMERA_FOCAL]) - T(log_focal_estimate_));
    residuals[1] = T(k1_scale_) * (parameters[BA_CAMERA_K1] - T(k1_estimate_));
    residuals[2] = T(k2_scale_) * (parameters[BA_CAMERA_K2] - T(k2_estimate_));
    return true;
  }

  double log_focal_estimate_;
  double focal_scale_;
  double k1_estimate_;
  double k1_scale_;
  double k2_estimate_;
  double k2_scale_;
};

struct BrownInternalParametersPriorError {
  BrownInternalParametersPriorError(double focal_x_estimate,
                                    double focal_x_std_deviation,
                                    double focal_y_estimate,
                                    double focal_y_std_deviation,
                                    double c_x_estimate,
                                    double c_x_std_deviation,
                                    double c_y_estimate,
                                    double c_y_std_deviation,
                                    double k1_estimate,
                                    double k1_std_deviation,
                                    double k2_estimate,
                                    double k2_std_deviation,
                                    double p1_estimate,
                                    double p1_std_deviation,
                                    double p2_estimate,
                                    double p2_std_deviation,
                                    double k3_estimate,
                                    double k3_std_deviation)
      : log_focal_x_estimate_(log(focal_x_estimate))
      , focal_x_scale_(1.0 / focal_x_std_deviation)
      , log_focal_y_estimate_(log(focal_y_estimate))
      , focal_y_scale_(1.0 / focal_y_std_deviation)
      , c_x_estimate_(c_x_estimate)
      , c_x_scale_(1.0 / c_x_std_deviation)
      , c_y_estimate_(c_y_estimate)
      , c_y_scale_(1.0 / c_y_std_deviation)
      , k1_estimate_(k1_estimate)
      , k1_scale_(1.0 / k1_std_deviation)
      , k2_estimate_(k2_estimate)
      , k2_scale_(1.0 / k2_std_deviation)
      , p1_estimate_(p1_estimate)
      , p1_scale_(1.0 / p1_std_deviation)
      , p2_estimate_(p2_estimate)
      , p2_scale_(1.0 / p2_std_deviation)
      , k3_estimate_(k3_estimate)
      , k3_scale_(1.0 / k3_std_deviation)
  {}

  template <typename T>
  bool operator()(const T* const parameters, T* residuals) const {
    residuals[0] = T(focal_x_scale_) * (log(parameters[BA_BROWN_CAMERA_FOCAL_X]) - T(log_focal_x_estimate_));
    residuals[1] = T(focal_y_scale_) * (log(parameters[BA_BROWN_CAMERA_FOCAL_Y]) - T(log_focal_y_estimate_));
    residuals[2] = T(c_x_scale_) * (parameters[BA_BROWN_CAMERA_C_X] - T(c_x_estimate_));
    residuals[3] = T(c_y_scale_) * (parameters[BA_BROWN_CAMERA_C_Y] - T(c_y_estimate_));
    residuals[4] = T(k1_scale_) * (parameters[BA_BROWN_CAMERA_K1] - T(k1_estimate_));
    residuals[5] = T(k2_scale_) * (parameters[BA_BROWN_CAMERA_K2] - T(k2_estimate_));
    residuals[6] = T(p1_scale_) * (parameters[BA_BROWN_CAMERA_P1] - T(p1_estimate_));
    residuals[7] = T(p2_scale_) * (parameters[BA_BROWN_CAMERA_P2] - T(p2_estimate_));
    residuals[8] = T(k3_scale_) * (parameters[BA_BROWN_CAMERA_K3] - T(k3_estimate_));
    return true;
  }

  double log_focal_x_estimate_;
  double focal_x_scale_;
  double log_focal_y_estimate_;
  double focal_y_scale_;
  double c_x_estimate_;
  double c_x_scale_;
  double c_y_estimate_;
  double c_y_scale_;
  double k1_estimate_;
  double k1_scale_;
  double k2_estimate_;
  double k2_scale_;
  double p1_estimate_;
  double p1_scale_;
  double p2_estimate_;
  double p2_scale_;
  double k3_estimate_;
  double k3_scale_;
};
