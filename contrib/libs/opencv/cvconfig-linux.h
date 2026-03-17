#ifndef OPENCV_CVCONFIG_H_INCLUDED
#define OPENCV_CVCONFIG_H_INCLUDED

/* OpenCV compiled as static or dynamic libs */
#define BUILD_SHARED_LIBS

/* OpenCV intrinsics optimized code */
#define CV_ENABLE_INTRINSICS

/* OpenCV additional optimized code */
/* #undef CV_DISABLE_OPTIMIZATION */

/* Compile for 'real' NVIDIA GPU architectures */
// #define CUDA_ARCH_BIN " 35 37 50 52 60 61 70 75 80 86"

/* NVIDIA GPU features are used */
// #define CUDA_ARCH_FEATURES " 35 37 50 52 60 61 70 75 80 86 86"

/* Compile for 'virtual' NVIDIA PTX architectures */
// #define CUDA_ARCH_PTX " 86"

/* AMD's Basic Linear Algebra Subprograms Library*/
/* #undef HAVE_CLAMDBLAS */

/* AMD's OpenCL Fast Fourier Transform Library*/
/* #undef HAVE_CLAMDFFT */

/* Clp support */
/* #undef HAVE_CLP */

/* NVIDIA CUDA Runtime API*/
// #define HAVE_CUDA

/* NVIDIA CUDA Basic Linear Algebra Subprograms (BLAS) API*/
// #define HAVE_CUBLAS

/* NVIDIA CUDA Deep Neural Network (cuDNN) API*/
/* #undef HAVE_CUDNN */

/* NVIDIA CUDA Fast Fourier Transform (FFT) API*/
// #define HAVE_CUFFT

/* DirectX */
/* #undef HAVE_DIRECTX */
/* #undef HAVE_DIRECTX_NV12 */
/* #undef HAVE_D3D11 */
/* #undef HAVE_D3D10 */
/* #undef HAVE_D3D9 */

/* Eigen Matrix & Linear Algebra Library */
#define HAVE_EIGEN

/* Geospatial Data Abstraction Library */
/* #undef HAVE_GDAL */

/* Halide support */
/* #undef HAVE_HALIDE */

/* Vulkan support */
/* #undef HAVE_VULKAN */

/* Define to 1 if you have the <inttypes.h> header file. */
/* #undef HAVE_INTTYPES_H */

/* Intel Integrated Performance Primitives */
/* #undef HAVE_IPP */
/* #undef HAVE_IPP_ICV */
/* #undef HAVE_IPP_IW */
/* #undef HAVE_IPP_IW_LL */

/* JPEG-2000 codec */
#define HAVE_OPENJPEG
/* #undef HAVE_JASPER */

/* AVIF codec */
/* #undef HAVE_AVIF */

/* IJG JPEG codec */
#define HAVE_JPEG

/* JPEG XL codec */
/* #undef HAVE_JPEGXL */

/* GDCM DICOM codec */
/* #undef HAVE_GDCM */

/* NVIDIA Video Decoding API*/
/* #undef HAVE_NVCUVID */
/* #undef HAVE_NVCUVID_HEADER */
/* #undef HAVE_DYNLINK_NVCUVID_HEADER */

/* NVIDIA Video Encoding API*/
/* #undef HAVE_NVCUVENC */

/* OpenCL Support */
#define HAVE_OPENCL
/* #undef HAVE_OPENCL_STATIC */
/* #undef HAVE_OPENCL_SVM */

/* NVIDIA OpenCL D3D Extensions support */
/* #undef HAVE_OPENCL_D3D11_NV */

/* OpenEXR codec */
/* #undef HAVE_OPENEXR */

/* OpenGL support*/
/* #undef HAVE_OPENGL */

/* PNG codec */
#define HAVE_PNG

/* PNG codec */
/* #undef HAVE_SPNG */

/* Posix threads (pthreads) */
#define HAVE_PTHREAD

/* parallel_for with pthreads */
/* #define HAVE_PTHREADS_PF */ // https://a.yandex-team.ru/arc/commit/2957190 OpenCV must be compiled without muli-threading support

/* Intel Threading Building Blocks */
/* #undef HAVE_TBB */

/* Ste||ar Group High Performance ParallelX */
/* #undef HAVE_HPX */

/* TIFF codec */
#define HAVE_TIFF

/* Define if your processor stores words with the most significant byte
   first (like Motorola and SPARC, unlike Intel and VAX). */
/* #undef WORDS_BIGENDIAN */

/* VA library (libva) */
/* #undef HAVE_VA */

/* Intel VA-API/OpenCL */
/* #undef HAVE_VA_INTEL */

/* Lapack */
#define HAVE_LAPACK

/* Library was compiled with functions instrumentation */
/* #undef ENABLE_INSTRUMENTATION */

/* OpenVX */
/* #undef HAVE_OPENVX */

/* OpenCV trace utilities */
#define OPENCV_TRACE

/* Library QR-code decoding */
#define HAVE_QUIRC

#endif // OPENCV_CVCONFIG_H_INCLUDED
