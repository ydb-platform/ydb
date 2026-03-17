#include <cmath>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <string>

#include "ceres/ceres.h"

extern "C" {
#include <string.h>
}

enum {
  BA_SHOT_RX,
  BA_SHOT_RY,
  BA_SHOT_RZ,
  BA_SHOT_TX,
  BA_SHOT_TY,
  BA_SHOT_TZ,
  BA_SHOT_NUM_PARAMS
};

enum PositionConstraintType {
  X = 0x1,
  Y = 0x2,
  Z = 0x4,
  XY = X | Y,
  XYZ = XY | Z
};

enum BACameraType {
  BA_PERSPECTIVE_CAMERA,
  BA_BROWN_PERSPECTIVE_CAMERA,
  BA_FISHEYE_CAMERA,
  BA_DUAL_CAMERA,
  BA_EQUIRECTANGULAR_CAMERA
};

struct BACamera {
  std::string id;
  bool constant;

  virtual ~BACamera() {}
  virtual BACameraType type() = 0;
};

enum {
  BA_CAMERA_FOCAL,
  BA_CAMERA_K1,
  BA_CAMERA_K2,
  BA_CAMERA_NUM_PARAMS
};

enum {
  BA_DUAL_CAMERA_FOCAL,
  BA_DUAL_CAMERA_K1,
  BA_DUAL_CAMERA_K2,
  BA_DUAL_CAMERA_TRANSITION,
  BA_DUAL_CAMERA_NUM_PARAMS
};

enum {
  BA_BROWN_CAMERA_FOCAL_X,
  BA_BROWN_CAMERA_FOCAL_Y,
  BA_BROWN_CAMERA_C_X,
  BA_BROWN_CAMERA_C_Y,
  BA_BROWN_CAMERA_K1,
  BA_BROWN_CAMERA_K2,
  BA_BROWN_CAMERA_P1,
  BA_BROWN_CAMERA_P2,
  BA_BROWN_CAMERA_K3,
  BA_BROWN_CAMERA_NUM_PARAMS
};

struct BAPerspectiveCamera : public BACamera {
  double parameters[BA_CAMERA_NUM_PARAMS];
  double focal_prior;
  double k1_prior;
  double k2_prior;

  BACameraType type() { return BA_PERSPECTIVE_CAMERA; }
  double GetFocal() { return parameters[BA_CAMERA_FOCAL]; }
  double GetK1() { return parameters[BA_CAMERA_K1]; }
  double GetK2() { return parameters[BA_CAMERA_K2]; }
  void SetFocal(double v) { parameters[BA_CAMERA_FOCAL] = v; }
  void SetK1(double v) { parameters[BA_CAMERA_K1] = v; }
  void SetK2(double v) { parameters[BA_CAMERA_K2] = v; }
};

struct BABrownPerspectiveCamera : public BACamera {
  double parameters[BA_BROWN_CAMERA_NUM_PARAMS];
  double focal_x_prior;
  double focal_y_prior;
  double c_x_prior;
  double c_y_prior;
  double k1_prior;
  double k2_prior;
  double p1_prior;
  double p2_prior;
  double k3_prior;

  BACameraType type() { return BA_BROWN_PERSPECTIVE_CAMERA; }
  double GetFocalX() { return parameters[BA_BROWN_CAMERA_FOCAL_X]; }
  double GetFocalY() { return parameters[BA_BROWN_CAMERA_FOCAL_Y]; }
  double GetCX() { return parameters[BA_BROWN_CAMERA_C_X]; }
  double GetCY() { return parameters[BA_BROWN_CAMERA_C_Y]; }
  double GetK1() { return parameters[BA_BROWN_CAMERA_K1]; }
  double GetK2() { return parameters[BA_BROWN_CAMERA_K2]; }
  double GetP1() { return parameters[BA_BROWN_CAMERA_P1]; }
  double GetP2() { return parameters[BA_BROWN_CAMERA_P2]; }
  double GetK3() { return parameters[BA_BROWN_CAMERA_K3]; }
  void SetFocalX(double v) { parameters[BA_BROWN_CAMERA_FOCAL_X] = v; }
  void SetFocalY(double v) { parameters[BA_BROWN_CAMERA_FOCAL_Y] = v; }
  void SetCX(double v) { parameters[BA_BROWN_CAMERA_C_X] = v; }
  void SetCY(double v) { parameters[BA_BROWN_CAMERA_C_Y] = v; }
  void SetK1(double v) { parameters[BA_BROWN_CAMERA_K1] = v; }
  void SetK2(double v) { parameters[BA_BROWN_CAMERA_K2] = v; }
  void SetP1(double v) { parameters[BA_BROWN_CAMERA_P1] = v; }
  void SetP2(double v) { parameters[BA_BROWN_CAMERA_P2] = v; }
  void SetK3(double v) { parameters[BA_BROWN_CAMERA_K3] = v; }
};

struct BAFisheyeCamera : public BACamera{
  double parameters[BA_CAMERA_NUM_PARAMS];
  double focal_prior;
  double k1_prior;
  double k2_prior;

  BACameraType type() { return BA_FISHEYE_CAMERA; }
  double GetFocal() { return parameters[BA_CAMERA_FOCAL]; }
  double GetK1() { return parameters[BA_CAMERA_K1]; }
  double GetK2() { return parameters[BA_CAMERA_K2]; }
  void SetFocal(double v) { parameters[BA_CAMERA_FOCAL] = v; }
  void SetK1(double v) { parameters[BA_CAMERA_K1] = v; }
  void SetK2(double v) { parameters[BA_CAMERA_K2] = v; }
};

struct BADualCamera : public BACamera{
  double parameters[BA_DUAL_CAMERA_NUM_PARAMS];
  double focal_prior;
  double k1_prior;
  double k2_prior;

  BACameraType type() { return BA_DUAL_CAMERA; }
  double GetFocal() { return parameters[BA_DUAL_CAMERA_FOCAL]; }
  double GetK1() { return parameters[BA_DUAL_CAMERA_K1]; }
  double GetK2() { return parameters[BA_DUAL_CAMERA_K2]; }
  double GetTransition() { return parameters[BA_DUAL_CAMERA_TRANSITION]; }
  void SetFocal(double v) { parameters[BA_DUAL_CAMERA_FOCAL] = v; }
  void SetK1(double v) { parameters[BA_DUAL_CAMERA_K1] = v; }
  void SetK2(double v) { parameters[BA_DUAL_CAMERA_K2] = v; }
  void SetTransition(double t) { parameters[BA_DUAL_CAMERA_TRANSITION] = t; }
};

struct BAEquirectangularCamera : public BACamera {
  BACameraType type() { return BA_EQUIRECTANGULAR_CAMERA; }
};

struct BAShot {
  std::string id;
  std::string camera;
  Eigen::Matrix<double, BA_SHOT_NUM_PARAMS, 1> parameters;
  double covariance[BA_SHOT_NUM_PARAMS * BA_SHOT_NUM_PARAMS];
  bool constant;

  Eigen::Vector3d GetRotation() const {return parameters.segment<3>(BA_SHOT_RX);}
  Eigen::Vector3d GetTranslation() const {return parameters.segment<3>(BA_SHOT_TX);}
  double GetCovariance(int i, int j) { return covariance[i * BA_SHOT_NUM_PARAMS + j]; }

  void SetRotation(const Eigen::Vector3d &r) {parameters.segment<3>(BA_SHOT_RX) = r;}
  void SetTranslation(const Eigen::Vector3d &t) {parameters.segment<3>(BA_SHOT_TX) = t;}
};

struct BAPoint {
  std::string id;
  Eigen::Matrix<double, 3, 1> parameters;
  bool constant;
  std::map<std::string, Eigen::VectorXd> reprojection_errors;

  Eigen::Vector3d GetPoint() const {return parameters;}
  void SetPoint(const Eigen::Vector3d &p) {parameters = p;}
};

struct BAReconstruction {
  std::string id;
  std::map<std::string, double > scales;
  std::map<std::string, BAShot *> shots;
  bool constant;
  bool shared;

  double* GetScalePtr(const std::string& shot) {
    if (shared) {
      return &(scales.begin()->second);
    }
    return &(scales[shot]);
  }

  double GetScale(const std::string& shot) {
    if (shared) {
      return scales.begin()->second;
    }
    return scales[shot];
  }
  void SetScale(const std::string& shot, double v) 
  { 
    if (shared) {
      scales.begin()->second = v;
    }
    scales[shot] = v; 
  }
};

struct BAPointProjectionObservation {
  double coordinates[2];
  BACamera *camera;
  BAShot *shot;
  BAPoint *point;
  double std_deviation;
};

struct BARotationPrior {
  BAShot *shot;
  double rotation[3];
  double std_deviation;
};

struct BATranslationPrior {
  BAShot *shot;
  double translation[3];
  double std_deviation;
};

struct BAPositionPrior {
  BAShot *shot;
  double position[3];
  double std_deviation;
};

struct BAPointPositionPrior {
  BAPoint *point;
  double position[3];
  double std_deviation;
};

struct BARelativeMotion {
  BARelativeMotion(const std::string &reconstruction_i,
                   const std::string &shot_i,
                   const std::string &reconstruction_j,
                   const std::string &shot_j,
                   const Eigen::Vector3d &rotation,
                   const Eigen::Vector3d &translation) {
    reconstruction_id_i = reconstruction_i;
    shot_id_i = shot_i;
    reconstruction_id_j = reconstruction_j;
    shot_id_j = shot_j;
    parameters.resize(BA_SHOT_NUM_PARAMS);
    parameters.segment(BA_SHOT_RX, 3) = rotation;
    parameters.segment(BA_SHOT_TX, 3) = translation;
    scale_matrix.resize(BA_SHOT_NUM_PARAMS, BA_SHOT_NUM_PARAMS);
    scale_matrix.setIdentity();
  }

  Eigen::Vector3d GetRotation() const {return parameters.segment(BA_SHOT_RX, 3);}
  Eigen::Vector3d GetTranslation() const {return parameters.segment(BA_SHOT_TX, 3);}
  void SetRotation(const Eigen::Vector3d &r) {parameters.segment(BA_SHOT_RX, 3) = r;}
  void SetTranslation(const Eigen::Vector3d &t) {parameters.segment(BA_SHOT_TX, 3) = t;}
  void SetScaleMatrix(const Eigen::MatrixXd& s) {scale_matrix = s;}

  std::string reconstruction_id_i;
  std::string shot_id_i;
  std::string reconstruction_id_j;
  std::string shot_id_j;
  Eigen::VectorXd parameters;
  Eigen::MatrixXd scale_matrix;
};

struct BARelativeSimilarity : public BARelativeMotion {
  BARelativeSimilarity(const std::string &reconstruction_i,
                       const std::string &shot_i,
                       const std::string &reconstruction_j,
                       const std::string &shot_j,
                       const Eigen::Vector3d &rotation,
                       const Eigen::Vector3d &translation,
                       double s)
      : BARelativeMotion(reconstruction_i, shot_i, 
                         reconstruction_j, shot_j,
                         rotation, translation),
        scale(s) {
    scale_matrix.resize(BA_SHOT_NUM_PARAMS + 1, BA_SHOT_NUM_PARAMS + 1);
    scale_matrix.setIdentity();
  }
  double scale;
};

struct BARelativeSimilarityCovariance
{
  static const int Size = BA_SHOT_NUM_PARAMS+1;
  std::vector<Eigen::Vector3d> points;
  Eigen::Matrix<double, Size, Size> covariance;

  void AddPoint(const Eigen::Vector3d& v){points.push_back(v);}

  void Compute(){
    covariance.setZero();
    for(const auto& p : points){
      const auto& x = p[0];
      const auto& y = p[1];
      const auto& z = p[2];
      Eigen::Matrix<double, 3, BA_SHOT_NUM_PARAMS+1> local_jacobian;
      local_jacobian.block(0, BA_SHOT_TX, 3, 3) = Eigen::Matrix<double,3,3>::Identity();
      local_jacobian.block(0, BA_SHOT_RX, 3, 3) <<  0, z, -y, 
                                                    -z, 0, x,
                                                     y, -x, 0;
      local_jacobian.block(0, BA_SHOT_NUM_PARAMS, 3, 1) << x, y, z;
      covariance += local_jacobian.transpose()*local_jacobian;
    }
    if(covariance.determinant() < 1e-20){
      covariance.setIdentity();
    }
    else{
      covariance = covariance.inverse();
    }    
  }

  Eigen::Matrix<double, Size, Size> GetCovariance()const{
    return covariance;
  }
};

struct BARelativeRotation {
  BARelativeRotation(const std::string &shot_i,
                     const std::string &shot_j,
                     const Eigen::Vector3d &r) {
    shot_id_i = shot_i;
    shot_id_j = shot_j;
    rotation = r;
    scale_matrix.setIdentity();
  }
  Eigen::Vector3d GetRotation() const {return rotation;}
  void SetRotation(const Eigen::Vector3d& r) {rotation = r;}
  void SetScaleMatrix(const Eigen::Matrix3d& s) {scale_matrix = s;}

  std::string shot_id_i;
  std::string shot_id_j;
  Eigen::Vector3d rotation;
  Eigen::Matrix3d scale_matrix;
};

struct BACommonPosition {
  BAShot *shot1;
  BAShot *shot2;
  double margin;
  double std_deviation;
};

struct BAAbsolutePosition {
  BAShot *shot;
  Eigen::Vector3d position;
  double std_deviation;
  std::string std_deviation_group;
};

struct BAAbsoluteUpVector {
  BAShot *shot;
  Eigen::Vector3d up_vector;
  double std_deviation;
};

struct BAAbsoluteAngle{
  BAShot *shot;
  double angle;
  double std_deviation;
};

struct BALinearMotion {
  BAShot *shot0;
  BAShot *shot1;
  BAShot *shot2;
  double alpha;
  double position_std_deviation;
  double orientation_std_deviation;
};

struct BAPointPositionShot {
  std::string shot_id;
  std::string reconstruction_id;
  std::string point_id;
  Eigen::Vector3d position;
  double std_deviation;
  PositionConstraintType type;
};

struct BAPointBearingShot {
  std::string shot_id;
  std::string reconstruction_id;
  std::string point_id;
  Eigen::Vector3d bearing;
  double std_deviation;
};

struct BAPointPositionWorld {
  std::string point_id;
  Eigen::Vector3d position;
  double std_deviation;
  PositionConstraintType type;
};

class BundleAdjuster {
 public:
  BundleAdjuster();
  virtual ~BundleAdjuster() = default;

  // Bundle variables

  void AddPerspectiveCamera(
      const std::string &id,
      double focal,
      double k1,
      double k2,
      double focal_prior,
      double k1_prior,
      double k2_prior,
      bool constant);

  void AddBrownPerspectiveCamera(const BABrownPerspectiveCamera &c);

  void AddFisheyeCamera(
      const std::string &id,
      double focal,
      double k1,
      double k2,
      double focal_prior,
      double k1_prior,
      double k2_prior,
      bool constant);

  void AddDualCamera(
      const std::string &id,
      double focal,
      double k1,
      double k2,
      double focal_prior,
      double k1_prior,
      double k2_prior,
      double transition,
      bool constant);

  void AddEquirectangularCamera(const std::string &id);

  void AddShot(
      const std::string &id,
      const std::string &camera,
      const Eigen::Vector3d& rotation,
      const Eigen::Vector3d& translation,
      bool constant);
  void AddReconstruction(
      const std::string &id,
      bool constant);
  void AddReconstructionShot(const std::string& reconstruction_id, double scale,
                             const std::string& shot_id);
  void SetScaleSharing(const std::string &id, bool share);
  void AddPoint(const std::string &id, 
                const Eigen::Vector3d& position,
                bool constant);

  // averaging constraints

  // point projection
  void AddPointProjectionObservation(
      const std::string &shot,
      const std::string &point,
      double x,
      double y,
      double std_deviation);
  void AddRotationPrior(
      const std::string &shot_id,
      double rx,
      double ry,
      double rz,
      double std_deviation);
  void AddTranslationPrior(
      const std::string &shot_id,
      double tx,
      double ty,
      double tz,
      double std_deviation);
  void AddPositionPrior(
      const std::string &shot_id,
      double x,
      double y,
      double z,
      double std_deviation);
  void AddPointPositionPrior(
      const std::string &point_id,
      double x,
      double y,
      double z,
      double std_deviation);

  void SetOriginShot(const std::string &shot_id);
  void SetUnitTranslationShot(const std::string &shot_id);

  // relative motion ones
  void AddRelativeMotion(const BARelativeMotion& rm);
  void AddRelativeSimilarity(const BARelativeSimilarity& rm);
  void AddRelativeRotation(const BARelativeRotation &rr);

  // absolute motion ones
  void AddCommonPosition(
      const std::string &shot_id1,
      const std::string &shot_id2,
      double margin,
      double std_deviation);
  void AddAbsolutePosition(
      const std::string &shot_id,
      const Eigen::Vector3d& position,
      double std_deviation,
      const std::string& std_deviation_group);
  void AddAbsoluteUpVector(
      const std::string &shot_id,
      const Eigen::Vector3d& up_vector,
      double std_deviation);
  void AddAbsolutePan(
      const std::string &shot_id,
      double angle,
      double std_deviation);
  void AddAbsoluteTilt(
      const std::string &shot_id,
      double angle,
      double std_deviation);
  void AddAbsoluteRoll(
      const std::string &shot_id,
      double angle,
      double std_deviation);

  // motion priors
  void AddLinearMotion(
      const std::string &shot0_id,
      const std::string &shot1_id,
      const std::string &shot2_id,
      double alpha,
      double position_std_deviation,
      double orientation_std_deviation);

  // point positions
  void AddPointPositionShot(const std::string &point_id,
                            const std::string &shot_id,
                            const std::string &reconstruction_id,
                            const Eigen::Vector3d& position,
                            double std_deviation,
                            const PositionConstraintType& type);
  void AddPointPositionWorld(const std::string &point_id,
                             const Eigen::Vector3d& position,
                             double std_deviation,
                             const PositionConstraintType& type);

  // point bearing
  void AddPointBearingShot(const std::string &point_id,
                           const std::string &shot_id,
                           const std::string &reconstruction_id,
                           const Eigen::Vector3d& bearing,
                           double std_deviation);

  // minimization setup
  void SetPointProjectionLossFunction(std::string name, double threshold);
  void SetRelativeMotionLossFunction(std::string name, double threshold);
  void SetAdjustAbsolutePositionStd(bool adjust);

  void SetMaxNumIterations(int miter);
  void SetNumThreads(int n);
  void SetLinearSolverType(std::string t);

  void SetInternalParametersPriorSD(
      double focal_sd,
      double c_sd,
      double k1_sd,
      double k2_sd,
      double p1_sd,
      double p2_sd,
      double k3_sd);

  void SetComputeCovariances(bool v);
  bool GetCovarianceEstimationValid();
  void SetComputeReprojectionErrors(bool v);

  // minimization
  void Run();
  void AddObservationResidualBlock(
      const BAPointProjectionObservation &observation,
      ceres::LossFunction *loss,
      ceres::Problem *problem);
  void ComputeCovariances(ceres::Problem *problem);
  void ComputeReprojectionErrors();

  // getters
  BAPerspectiveCamera GetPerspectiveCamera(const std::string &id);
  BABrownPerspectiveCamera GetBrownPerspectiveCamera(const std::string &id);
  BAFisheyeCamera GetFisheyeCamera(const std::string &id);
  BADualCamera GetDualCamera(const std::string &id);
  BAEquirectangularCamera GetEquirectangularCamera(const std::string &id);
  BAShot GetShot(const std::string &id);
  BAReconstruction GetReconstruction(const std::string &id);
  BAPoint GetPoint(const std::string &id);

  // minimization details
  std::string BriefReport();
  std::string FullReport();

 private:
  // minimized data
  std::map<std::string, std::unique_ptr<BACamera> > cameras_;
  std::map<std::string, BAShot> shots_;
  std::map<std::string, BAReconstruction> reconstructions_;
  std::map<std::string, BAPoint> points_;
  
  // minimization constraints

  // reprojection observation
  std::vector<BAPointProjectionObservation> point_projection_observations_;

  // relative motion between shots
  std::vector<BARelativeMotion> relative_motions_;
  std::vector<BARelativeSimilarity> relative_similarity_;
  std::vector<BARelativeRotation> relative_rotations_;
  std::vector<BACommonPosition> common_positions_;

  // shots absolute positions
  std::vector<BAAbsolutePosition> absolute_positions_;
  std::vector<BAAbsoluteUpVector> absolute_up_vectors_;
  std::vector<BAAbsoluteAngle> absolute_pans_;
  std::vector<BAAbsoluteAngle> absolute_tilts_;
  std::vector<BAAbsoluteAngle> absolute_rolls_;

  std::vector<BARotationPrior> rotation_priors_;
  std::vector<BATranslationPrior> translation_priors_;
  std::vector<BAPositionPrior> position_priors_;
  std::vector<BAPointPositionPrior> point_position_priors_;

  BAShot *unit_translation_shot_;

  // motion priors
  std::vector<BALinearMotion> linear_motion_prior_;

  // points absolute constraints
  std::vector<BAPointPositionShot> point_positions_shot_;
  std::vector<BAPointBearingShot> point_bearing_shot_;
  std::vector<BAPointPositionWorld> point_positions_world_;

  // Camera parameters prior
  double focal_prior_sd_;
  double c_prior_sd_;
  double k1_sd_;
  double k2_sd_;
  double p1_sd_;
  double p2_sd_;
  double k3_sd_;

  // minimization setup
  std::string point_projection_loss_name_;
  double point_projection_loss_threshold_;
  std::string relative_motion_loss_name_;
  double relative_motion_loss_threshold_;
  bool adjust_absolute_position_std_;

  bool compute_covariances_;
  bool covariance_estimation_valid_;
  bool compute_reprojection_errors_;

  int max_num_iterations_;
  int num_threads_;
  std::string linear_solver_type_;

  // internal
  ceres::Solver::Summary last_run_summary_;
};


