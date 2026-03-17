#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

namespace pybind11 {

void gil_assert();

}

namespace CHDB {

namespace py {

using namespace pybind11;

} // namespace py

struct PythonGILWrapper
{
    py::gil_scoped_acquire acquire;
};

} // namespace CHDB
