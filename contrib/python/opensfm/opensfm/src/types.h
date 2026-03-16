#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <vector>
#include <iostream>
#include "opencv2/core/core.hpp"


namespace csfm {

namespace py = pybind11;


typedef py::array_t<float, py::array::c_style | py::array::forcecast> pyarray_f;
typedef py::array_t<double, py::array::c_style | py::array::forcecast> pyarray_d;
typedef py::array_t<int, py::array::c_style | py::array::forcecast> pyarray_int;
typedef py::array_t<unsigned char, py::array::c_style | py::array::forcecast> pyarray_uint8;

template <typename T>
py::array_t<T> py_array_from_data(const T *data, py::ssize_t shape0) {
  py::array_t<T> res({shape0});
  std::copy(data, data + shape0, res.mutable_data());
  return res;
}

template <typename T>
py::array_t<T> py_array_from_data(const T *data, py::ssize_t shape0, py::ssize_t shape1) {
  py::array_t<T> res({shape0, shape1});
  std::copy(data, data + shape0 * shape1, res.mutable_data());
  return res;
}

template <typename T>
py::array_t<T> py_array_from_data(const T *data, py::ssize_t shape0, py::ssize_t shape1, py::ssize_t shape2) {
  py::array_t<T> res({shape0, shape1, shape2});
  std::copy(data, data + shape0 * shape1 * shape2, res.mutable_data());
  return res;
}

template <typename T>
py::array_t<T> py_array_from_vector(const std::vector<T> &v) {
  const T *data = v.size() ? &v[0] : NULL;
  return py_array_from_data(data, v.ssize());
}

template <typename T>
py::array_t<T> py_array_from_cvmat(const cv::Mat &m) {
  const T *data = m.rows ? m.ptr<T>(0) : NULL;
  return py_array_from_data(data, m.rows, m.cols);
}

template<typename T>
cv::Mat pyarray_cv_mat_view_typed(T &array, int type) {
  int height = 1;
  int width = 1;

  if (array.ndim() == 1) {
    width = array.shape(0);
  } else if (array.ndim() == 2) {
    height = array.shape(0);
    width = array.shape(1);
  }

  return cv::Mat(height, width, type, array.mutable_data());
}

template<typename T>
cv::Mat pyarray_cv_mat_view(T &array) {}

template<>
cv::Mat pyarray_cv_mat_view(pyarray_f &array);

template<>
cv::Mat pyarray_cv_mat_view(pyarray_d &array);

template<>
cv::Mat pyarray_cv_mat_view(pyarray_int &array);

template<>
cv::Mat pyarray_cv_mat_view(pyarray_uint8 &array);

}
