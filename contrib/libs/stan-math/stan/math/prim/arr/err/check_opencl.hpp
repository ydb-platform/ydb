#ifndef STAN_MATH_PRIM_ARR_ERR_CHECK_OPENCL_HPP
#define STAN_MATH_PRIM_ARR_ERR_CHECK_OPENCL_HPP
#ifdef STAN_OPENCL
#define __CL_ENABLE_EXCEPTIONS

#include <stan/math/prim/scal/err/system_error.hpp>
#error #include <CL/cl.hpp>
#include <iostream>
#include <stdexcept>
#include <string>

/** @file stan/math/prim/arr/err/check_opencl.hpp
 *    @brief checking OpenCL error numbers
 */

namespace stan {
namespace math {

/**
 * Throws the domain error with specifying the OpenCL error that
 * occured. It outputs the OpenCL errors that are specified
 * in OpenCL 2.0. If no matching error number is found,
 * it throws the error with the number.
 * @param function the name of the function where the error occurred
 * @param e The error number
 */
inline void check_opencl_error(const char *function, const cl::Error &e) {
  switch (e.err()) {
    case 0:
      // CL_SUCCESS - no need to throw
      return;
    case -1:
      system_error(function, e.what(), e.err(), "CL_DEVICE_NOT_FOUND");
    case -2:
      system_error(function, e.what(), e.err(), "CL_DEVICE_NOT_AVAILABLE");
    case -3:
      system_error(function, e.what(), e.err(), "CL_COMPILER_NOT_AVAILABLE");
    case -4:
      system_error(function, e.what(), e.err(),
                   "CL_MEM_OBJECT_ALLOCATION_FAILURE");
    case -5:
      system_error(function, e.what(), e.err(), "CL_OUT_OF_RESOURCES");
    case -6:
      system_error(function, e.what(), e.err(), "CL_OUT_OF_HOST_MEMORY");
    case -7:
      system_error(function, e.what(), e.err(),
                   "CL_PROFILING_INFO_NOT_AVAILABLE");
    case -8:
      system_error(function, e.what(), e.err(), "CL_MEM_COPY_OVERLAP");
    case -9:
      system_error(function, e.what(), e.err(), "CL_IMAGE_FORMAT_MISMATCH");
    case -10:
      system_error(function, e.what(), e.err(),
                   "CL_IMAGE_FORMAT_NOT_SUPPORTED");
    case -11:
      system_error(function, e.what(), e.err(), "CL_BUILD_PROGRAM_FAILURE");
    case -12:
      system_error(function, e.what(), e.err(), "CL_MAP_FAILURE");
    case -13:
      system_error(function, e.what(), e.err(),
                   "CL_MISALIGNED_SUB_BUFFER_OFFSET");
    case -14:
      system_error(function, e.what(), e.err(),
                   "CL_EXEC_STATUS_ERROR_FOR_EVENTS_IN_WAIT_LIST");
    case -15:
      system_error(function, e.what(), e.err(), "CL_COMPILE_PROGRAM_FAILURE");
    case -16:
      system_error(function, e.what(), e.err(), "CL_LINKER_NOT_AVAILABLE");
    case -17:
      system_error(function, e.what(), e.err(), "CL_LINK_PROGRAM_FAILURE");
    case -18:
      system_error(function, e.what(), e.err(), "CL_DEVICE_PARTITION_FAILED");
    case -19:
      system_error(function, e.what(), e.err(),
                   "CL_KERNEL_ARG_INFO_NOT_AVAILABLE");
    case -30:
      system_error(function, e.what(), e.err(), "CL_INVALID_VALUE");
    case -31:
      system_error(function, e.what(), e.err(), "CL_INVALID_DEVICE_TYPE");
    case -32:
      system_error(function, e.what(), e.err(), "CL_INVALID_PLATFORM");
    case -33:
      system_error(function, e.what(), e.err(), "CL_INVALID_DEVICE");
    case -34:
      system_error(function, e.what(), e.err(), "CL_INVALID_CONTEXT");
    case -35:
      system_error(function, e.what(), e.err(), "CL_INVALID_QUEUE_PROPERTIES");
    case -36:
      system_error(function, e.what(), e.err(), "CL_INVALID_COMMAND_QUEUE");
    case -37:
      system_error(function, e.what(), e.err(), "CL_INVALID_HOST_PTR");
    case -38:
      system_error(function, e.what(), e.err(), "CL_INVALID_MEM_OBJECT");
    case -39:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_IMAGE_FORMAT_DESCRIPTOR");
    case -40:
      system_error(function, e.what(), e.err(), "CL_INVALID_IMAGE_SIZE");
    case -41:
      system_error(function, e.what(), e.err(), "CL_INVALID_SAMPLER");
    case -42:
      system_error(function, e.what(), e.err(), "CL_INVALID_BINARY");
    case -43:
      system_error(function, e.what(), e.err(), "CL_INVALID_BUILD_OPTIONS");
    case -44:
      system_error(function, e.what(), e.err(), "CL_INVALID_PROGRAM");
    case -45:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_PROGRAM_EXECUTABLE");
    case -46:
      system_error(function, e.what(), e.err(), "CL_INVALID_KERNEL_NAME");
    case -47:
      system_error(function, e.what(), e.err(), "CL_INVALID_KERNEL_DEFINITION");
    case -48:
      system_error(function, e.what(), e.err(), "CL_INVALID_KERNEL");
    case -49:
      system_error(function, e.what(), e.err(), "CL_INVALID_ARG_INDEX");
    case -50:
      system_error(function, e.what(), e.err(), "CL_INVALID_ARG_VALUE");
    case -51:
      system_error(function, e.what(), e.err(), "CL_INVALID_ARG_SIZE");
    case -52:
      system_error(function, e.what(), e.err(), "CL_INVALID_KERNEL_ARGS");
    case -53:
      system_error(function, e.what(), e.err(), "CL_INVALID_WORK_DIMENSION");
    case -54:
      system_error(function, e.what(), e.err(), "CL_INVALID_WORK_GROUP_SIZE");
    case -55:
      system_error(function, e.what(), e.err(), "CL_INVALID_WORK_ITEM_SIZE");
    case -56:
      system_error(function, e.what(), e.err(), "CL_INVALID_GLOBAL_OFFSET");
    case -57:
      system_error(function, e.what(), e.err(), "CL_INVALID_EVENT_WAIT_LIST");
    case -58:
      system_error(function, e.what(), e.err(), "CL_INVALID_EVENT");
    case -59:
      system_error(function, e.what(), e.err(), "CL_INVALID_OPERATION");
    case -60:
      system_error(function, e.what(), e.err(), "CL_INVALID_GL_OBJECT");
    case -61:
      system_error(function, e.what(), e.err(), "CL_INVALID_BUFFER_SIZE");
    case -62:
      system_error(function, e.what(), e.err(), "CL_INVALID_MIP_LEVEL");
    case -63:
      system_error(function, e.what(), e.err(), "CL_INVALID_GLOBAL_WORK_SIZE");
    case -64:
      system_error(function, e.what(), e.err(), "CL_INVALID_PROPERTY");
    case -65:
      system_error(function, e.what(), e.err(), "CL_INVALID_IMAGE_DESCRIPTOR");
    case -66:
      system_error(function, e.what(), e.err(), "CL_INVALID_COMPILER_OPTIONS");
    case -67:
      system_error(function, e.what(), e.err(), "CL_INVALID_LINKER_OPTIONS");
    case -68:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_DEVICE_PARTITION_COUNT");
    case -69:
      system_error(function, e.what(), e.err(), "CL_INVALID_PIPE_SIZE");
    case -70:
      system_error(function, e.what(), e.err(), "CL_INVALID_DEVICE_QUEUE");
    case -1000:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_GL_SHAREGROUP_REFERENCE_KHR");
    case -1001:
      system_error(function, e.what(), e.err(), "CL_PLATFORM_NOT_FOUND_KHR");
    case -1002:
      system_error(function, e.what(), e.err(), "CL_INVALID_D3D10_DEVICE_KHR");
    case -1003:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_D3D10_RESOURCE_KHR");
    case -1004:
      system_error(function, e.what(), e.err(),
                   "CL_D3D10_RESOURCE_ALREADY_ACQUIRED_KHR");
    case -1005:
      system_error(function, e.what(), e.err(),
                   "CL_D3D10_RESOURCE_NOT_ACQUIRED_KHR");
    case -1006:
      system_error(function, e.what(), e.err(), "CL_INVALID_D3D11_DEVICE_KHR");
    case -1007:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_D3D11_RESOURCE_KHR");
    case -1008:
      system_error(function, e.what(), e.err(),
                   "CL_D3D11_RESOURCE_ALREADY_ACQUIRED_KHR");
    case -1009:
      system_error(function, e.what(), e.err(),
                   "CL_D3D11_RESOURCE_NOT_ACQUIRED_KHR");
    case -101:
      system_error(function, e.what(), e.err(), "CL_INVALID_D3D9_DEVICE_NV ");
    case -1011:
      system_error(function, e.what(), e.err(), "CL_INVALID_D3D9_RESOURCE_NV ");
    case -1012:
      system_error(function, e.what(), e.err(),
                   "CL_D3D9_RESOURCE_ALREADY_ACQUIRED_NV "
                   "CL_DX9_RESOURCE_ALREADY_ACQUIRED_INTEL");
    case -1013:
      system_error(function, e.what(), e.err(),
                   "CL_D3D9_RESOURCE_NOT_ACQUIRED_NV "
                   "CL_DX9_RESOURCE_NOT_ACQUIRED_INTEL");
    case -1092:
      system_error(function, e.what(), e.err(),
                   "CL_EGL_RESOURCE_NOT_ACQUIRED_KHR");
    case -1093:
      system_error(function, e.what(), e.err(), "CL_INVALID_EGL_OBJECT_KHR");
    case -1094:
      system_error(function, e.what(), e.err(), "CL_INVALID_ACCELERATOR_INTEL");
    case -1095:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_ACCELERATOR_TYPE_INTEL");
    case -1096:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_ACCELERATOR_DESCRIPTOR_INTEL");
    case -1097:
      system_error(function, e.what(), e.err(),
                   "CL_ACCELERATOR_TYPE_NOT_SUPPORTED_INTEL");
    case -1098:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_VA_API_MEDIA_ADAPTER_INTEL");
    case -1099:
      system_error(function, e.what(), e.err(),
                   "CL_INVALID_VA_API_MEDIA_SURFACE_INTEL");
    case -1100:
      system_error(function, e.what(), e.err(),
                   "CL_VA_API_MEDIA_SURFACE_ALREADY_ACQUIRED_INTEL");
    case -1101:
      system_error(function, e.what(), e.err(),
                   "CL_VA_API_MEDIA_SURFACE_NOT_ACQUIRED_INTEL");
    case -9999:
      system_error(function, e.what(), e.err(), "ILLEGAL_READ_OR_WRITE_NVIDIA");
    default:
      system_error(function, e.what(), e.err(),
                   std::to_string(e.err()).c_str());
  }
}
}  // namespace math
}  // namespace stan
#endif
#endif
