#pragma once

#include <opencv2/opencv.hpp>
#include <random>

namespace csfm {

float Variance(float *x, int n);

class NCCEstimator {
 public:
  NCCEstimator();
  void Push(float x, float y, float w);
  float Get();

 private:
  float sumx_, sumy_;
  float sumxx_, sumyy_, sumxy_;
  float sumw_;
};

void ApplyHomography(const cv::Matx33f &H,
                     float x1, float y1,
                     float *x2, float *y2);

cv::Matx33d PlaneInducedHomography(const cv::Matx33d &K1,
                                   const cv::Matx33d &R1,
                                   const cv::Vec3d &t1,
                                   const cv::Matx33d &K2,
                                   const cv::Matx33d &R2,
                                   const cv::Vec3d &t2,
                                   const cv::Vec3d &v);

cv::Matx33f PlaneInducedHomographyBaked(const cv::Matx33d &K1inv,
                                        const cv::Matx33d &Q2,
                                        const cv::Vec3d &a2,
                                        const cv::Matx33d &K2,
                                        const cv::Vec3d &v);

cv::Vec3d Project(const cv::Vec3d &x,
                  const cv::Matx33d &K,
                  const cv::Matx33d &R,
                  const cv::Vec3d &t);

cv::Vec3d Backproject(double x, double y, double depth,
                      const cv::Matx33d &K,
                      const cv::Matx33d &R,
                      const cv::Vec3d &t);

float DepthOfPlaneBackprojection(double x, double y,
                                 const cv::Matx33d &K,
                                 const cv::Vec3d &plane);

cv::Vec3f PlaneFromDepthAndNormal(float x, float y,
                                  const cv::Matx33d &K,
                                  float depth,
                                  const cv::Vec3f &normal);

float UniformRand(float a, float b);

struct DepthmapEstimatorResult {
  cv::Mat depth;
  cv::Mat plane;
  cv::Mat score;
  cv::Mat nghbr;
};

class DepthmapEstimator {
 public:
  DepthmapEstimator();
  void AddView(const double *pK, const double *pR, const double *pt,
               const unsigned char *pimage, const unsigned char *pmask,
               int width, int height);
  void SetDepthRange(double min_depth, double max_depth, int num_depth_planes);
  void SetPatchMatchIterations(int n);
  void SetPatchSize(int size);
  void SetMinPatchSD(float sd);
  void ComputeBruteForce(DepthmapEstimatorResult *result);
  void ComputePatchMatch(DepthmapEstimatorResult *result);
  void ComputePatchMatchSample(DepthmapEstimatorResult *result);
  void AssignMatrices(DepthmapEstimatorResult *result);
  void RandomInitialization(DepthmapEstimatorResult *result, bool sample);
  void ComputeIgnoreMask(DepthmapEstimatorResult *result);
  float PatchVariance(int i, int j);
  void PatchMatchForwardPass(DepthmapEstimatorResult *result, bool sample);
  void PatchMatchBackwardPass(DepthmapEstimatorResult *result, bool sample);
  void PatchMatchUpdatePixel(DepthmapEstimatorResult *result, int i, int j,
                             int adjacent[2][2], bool sample);
  void CheckPlaneCandidate(DepthmapEstimatorResult *result, int i, int j,
                           const cv::Vec3f &plane);
  void CheckPlaneImageCandidate(DepthmapEstimatorResult *result, int i, int j,
                                const cv::Vec3f &plane, int nghbr);
  void AssignPixel(DepthmapEstimatorResult *result, int i, int j,
                   const float depth, const cv::Vec3f &plane, const float score,
                   const int nghbr);
  void ComputePlaneScore(int i, int j, const cv::Vec3f &plane, float *score,
                         int *nghbr);
  float ComputePlaneImageScoreUnoptimized(int i, int j, const cv::Vec3f &plane,
                                          int other);
  float ComputePlaneImageScore(int i, int j, const cv::Vec3f &plane, int other);
  float BilateralWeight(float dcolor, float dx, float dy);
  void PostProcess(DepthmapEstimatorResult *result);

 private:
  std::vector<cv::Mat> images_;
  std::vector<cv::Mat> masks_;
  std::vector<cv::Matx33d> Ks_;
  std::vector<cv::Matx33d> Rs_;
  std::vector<cv::Vec3d> ts_;
  std::vector<cv::Matx33d> Kinvs_;
  std::vector<cv::Matx33d> Qs_;
  std::vector<cv::Vec3d> as_;
  int patch_size_;
  double min_depth_, max_depth_;
  int num_depth_planes_;
  int patchmatch_iterations_;
  float min_patch_variance_;
  std::mt19937 rng_;
  std::uniform_int_distribution<int> uni_;
  std::normal_distribution<float> unit_normal_;
};

class DepthmapCleaner {
 public:
  DepthmapCleaner();
  void SetSameDepthThreshold(float t);
  void SetMinConsistentViews(int n);
  void AddView(const double *pK, const double *pR, const double *pt,
               const float *pdepth, int width, int height);
  void Clean(cv::Mat *clean_depth);

 private:
  std::vector<cv::Mat> depths_;
  std::vector<cv::Matx33d> Ks_;
  std::vector<cv::Matx33d> Rs_;
  std::vector<cv::Vec3d> ts_;
  float same_depth_threshold_;
  int min_consistent_views_;
};

class DepthmapPruner {
 public:
  DepthmapPruner();
  void SetSameDepthThreshold(float t);
  void AddView(const double *pK, const double *pR, const double *pt,
               const float *pdepth, const float *pplane,
               const unsigned char *pcolor, const unsigned char *plabel,
               const unsigned char *pdetection,
               int width, int height);
  void Prune(std::vector<float> *merged_points,
             std::vector<float> *merged_normals,
             std::vector<unsigned char> *merged_colors,
             std::vector<unsigned char> *merged_labels,
             std::vector<unsigned char> *merged_detections);

 private:
  std::vector<cv::Mat> depths_;
  std::vector<cv::Mat> planes_;
  std::vector<cv::Mat> colors_;
  std::vector<cv::Mat> labels_;
  std::vector<cv::Mat> detections_;
  std::vector<cv::Matx33d> Ks_;
  std::vector<cv::Matx33d> Rs_;
  std::vector<cv::Vec3d> ts_;
  float same_depth_threshold_;
};

}  // namespace csfm
