#include "types.h"
#include "AKAZE.h"
#include "akaze_bind.h"
#include <opencv2/imgproc.hpp>

namespace csfm {


py::object akaze(pyarray_uint8 image, AKAZEOptions options) {
  py::gil_scoped_release release;

  const cv::Mat img(image.shape(0), image.shape(1), CV_8U, (void *)image.data());

  cv::Mat img_32;
  img.convertTo(img_32, CV_32F, 1.0 / 255.0, 0);

  // Don't forget to specify image dimensions in AKAZE's options
  options.img_width = img_32.cols;
  options.img_height = img_32.rows;

  // Extract features
  libAKAZE::AKAZE evolution(options);
  std::vector<cv::KeyPoint> kpts;

  evolution.Create_Nonlinear_Scale_Space(img_32);
  evolution.Feature_Detection(kpts);

  // Compute descriptors.
  cv::Mat desc;
  evolution.Compute_Descriptors(kpts, desc);

  evolution.Show_Computation_Times();

  // Convert to numpy.
  cv::Mat keys(kpts.size(), 4, CV_32F);
  for (int i = 0; i < (int) kpts.size(); ++i) {
    keys.at<float>(i, 0) = kpts[i].pt.x;
    keys.at<float>(i, 1) = kpts[i].pt.y;
    keys.at<float>(i, 2) = kpts[i].size;
    keys.at<float>(i, 3) = kpts[i].angle;
  }

  py::list retn;
  retn.append(py_array_from_data(keys.ptr<float>(0), keys.rows, keys.cols));

  if (options.descriptor == MLDB_UPRIGHT || options.descriptor == MLDB) {
    retn.append(py_array_from_data(desc.ptr<unsigned char>(0), desc.rows, desc.cols));
  } else {
    retn.append(py_array_from_data(desc.ptr<float>(0), desc.rows, desc.cols));
  }
  return retn;
}


}
