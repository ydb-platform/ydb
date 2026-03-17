#include <map>
#include <vector>
#include <opencv2/core/core.hpp>
#include <pybind11/pybind11.h>

#include "types.h"


namespace csfm {

float DistanceL1(const float *pa, const float *pb, int n) {
  float distance = 0;
  for (int i = 0; i < n; ++i) {
    distance += fabs(pa[i] - pb[i]);
  }
  return distance;
}

float DistanceL2(const float *pa, const float *pb, int n) {
  float distance = 0;
  for (int i = 0; i < n; ++i) {
    distance += (pa[i] - pb[i]) * (pa[i] - pb[i]);
  }
  return sqrt(distance);
}

void MatchUsingWords(const cv::Mat &f1,
                     const cv::Mat &w1,
                     const cv::Mat &f2,
                     const cv::Mat &w2,
                     float lowes_ratio,
                     int max_checks,
                     cv::Mat *matches) {
  // Index features on the second image.
  std::multimap<int, int> index2;
  const int *pw2 = &w2.at<int>(0, 0);
  for (unsigned int i = 0; i < w2.rows * w2.cols; ++i) {
    index2.insert(std::pair<int, int>(pw2[i], i));
  }

  std::vector<int> best_match(f1.rows, -1),
                   second_best_match(f1.rows, -1);
  std::vector<float> best_distance(f1.rows, 99999999),
                     second_best_distance(f1.rows, 99999999);
  *matches = cv::Mat(0, 2, CV_32S);
  cv::Mat tmp_match(1, 2, CV_32S);
  for (unsigned int i = 0; i < w1.rows; ++i) {
    int checks = 0;
    for (unsigned int j = 0; j < w1.cols; ++j) {
      int word = w1.at<int>(i, j);
      auto range = index2.equal_range(word);
      for (auto it = range.first; it != range.second; ++it) {
        int match = it->second;
        const float *pa = f1.ptr<float>(i);
        const float *pb = f2.ptr<float>(match);
        float distance = DistanceL2(pa, pb, f1.cols);
        if (distance < best_distance[i]) {
          second_best_distance[i] = best_distance[i];
          second_best_match[i] = best_match[i];
          best_distance[i] = distance;
          best_match[i] = match;
        } else if (distance < second_best_distance[i]) {
          second_best_distance[i] = distance;
          second_best_match[i] = match;
        }
        checks++;
      }
      if (checks >= max_checks) break;
    }
    if (best_distance[i] < lowes_ratio * second_best_distance[i]) {
      tmp_match.at<int>(0, 0) = i;
      tmp_match.at<int>(0, 1) = best_match[i];
      matches->push_back(tmp_match);
    }
  }
}

py::object match_using_words(pyarray_f features1,
                             pyarray_int words1,
                             pyarray_f features2,
                             pyarray_int words2,
                             float lowes_ratio,
                             int max_checks) {
  cv::Mat cv_f1 = pyarray_cv_mat_view(features1);
  cv::Mat cv_w1 = pyarray_cv_mat_view(words1);
  cv::Mat cv_f2 = pyarray_cv_mat_view(features2);
  cv::Mat cv_w2 = pyarray_cv_mat_view(words2);
  cv::Mat matches;

  MatchUsingWords(cv_f1, cv_w1,
                  cv_f2, cv_w2,
                  lowes_ratio,
                  max_checks,
                  &matches);

  return py_array_from_cvmat<int>(matches);
}

}
