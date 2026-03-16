#ifndef __TYPES_H__
#define __TYPES_H__

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#include <vector>
#include <iostream>


namespace pyopengv {

namespace py = pybind11;

typedef py::array_t<double, py::array::c_style | py::array::forcecast> pyarray_d;

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
py::array_t<T> py_array_from_vector(const std::vector<T> &v) {
  const T *data = v.size() ? &v[0] : NULL;
  return py_array_from_data(data, v.ssize());
}


}

#endif // __TYPES_H__
