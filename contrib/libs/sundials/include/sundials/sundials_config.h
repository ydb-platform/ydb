/* ----------------------------------------------------------------- 
 * Programmer(s): Aaron Collier and Radu Serban @ LLNL
 * -----------------------------------------------------------------
 * LLNS/SMU Copyright Start
 * Copyright (c) 2002-2018, Southern Methodist University and
 * Lawrence Livermore National Security
 *
 * This work was performed under the auspices of the U.S. Department
 * of Energy by Southern Methodist University and Lawrence Livermore
 * National Laboratory under Contract DE-AC52-07NA27344.
 * Produced at Southern Methodist University and the Lawrence
 * Livermore National Laboratory.
 *
 * All rights reserved.
 * For details, see the LICENSE file.
 * LLNS/SMU Copyright End
 * -----------------------------------------------------------------
 * SUNDIALS configuration header file
 * -----------------------------------------------------------------*/

/* Define SUNDIALS version numbers */
#define SUNDIALS_VERSION "4.1.0"
#define SUNDIALS_VERSION_MAJOR 4
#define SUNDIALS_VERSION_MINOR 1
#define SUNDIALS_VERSION_PATCH 0
#define SUNDIALS_VERSION_LABEL ""

/* FCMIX: Define Fortran name-mangling macro for C identifiers.
 * Depending on the inferred scheme, one of the following six
 * macros will be defined:
 *     #define SUNDIALS_F77_FUNC(name,NAME) name
 *     #define SUNDIALS_F77_FUNC(name,NAME) name ## _
 *     #define SUNDIALS_F77_FUNC(name,NAME) name ## __
 *     #define SUNDIALS_F77_FUNC(name,NAME) NAME
 *     #define SUNDIALS_F77_FUNC(name,NAME) NAME ## _
 *     #define SUNDIALS_F77_FUNC(name,NAME) NAME ## __
 */


/* FCMIX: Define Fortran name-mangling macro for C identifiers
 *        which contain underscores.
 */


/* Define precision of SUNDIALS data type 'realtype' 
 * Depending on the precision level, one of the following 
 * three macros will be defined:
 *     #define SUNDIALS_SINGLE_PRECISION 1
 *     #define SUNDIALS_DOUBLE_PRECISION 1
 *     #define SUNDIALS_EXTENDED_PRECISION 1
 */
#define SUNDIALS_DOUBLE_PRECISION 1

/* Define type of vector indices in SUNDIALS 'sunindextype'. 
 * Depending on user choice of index type, one of the following 
 * two macros will be defined:
 *     #define SUNDIALS_INT64_T 1
 *     #define SUNDIALS_INT32_T 1
 */
#define SUNDIALS_INT32_T 1

/* Define the type of vector indices in SUNDIALS 'sunindextype'.
 * The macro will be defined with a type of the appropriate size.
 */
#define SUNDIALS_INDEX_TYPE int32_t

/* Use generic math functions 
 * If it was decided that generic math functions can be used, then
 *     #define SUNDIALS_USE_GENERIC_MATH
 */
#define SUNDIALS_USE_GENERIC_MATH

/* Use POSIX timers if available.
 *     #define SUNDIALS_HAVE_POSIX_TIMERS
 */
#define SUNDIALS_HAVE_POSIX_TIMERS

/* Blas/Lapack available
 * If working libraries for Blas/lapack support were found, then
 *     #define SUNDIALS_BLAS_LAPACK
 */
/* #undef SUNDIALS_BLAS_LAPACK */

/* SUPERLUMT available
 * If working libraries for SUPERLUMT support were found, then
 *     #define SUNDIALS_SUPERLUMT 
 */
/* #undef SUNDIALS_SUPERLUMT */
/* #undef SUNDIALS_SUPERLUMT_THREAD_TYPE */

/* KLU available
 * If working libraries for KLU support were found, then
 *     #define SUNDIALS_KLU 
 */
/* #undef SUNDIALS_KLU */

/* Set if SUNDIALS is built with MPI support.
 * 
 */


/* FNVECTOR: Allow user to specify different MPI communicator
 * If it was found that the MPI implementation supports MPI_Comm_f2c, then
 *      #define SUNDIALS_MPI_COMM_F2C 1
 * otherwise
 *      #define SUNDIALS_MPI_COMM_F2C 0
 */
#define SUNDIALS_MPI_COMM_F2C 0

/* Mark SUNDIALS API functions for export/import
 * When building shared SUNDIALS libraries under Windows, use
 *      #define SUNDIALS_EXPORT __declspec(dllexport)
 * When linking to shared SUNDIALS libraries under Windows, use
 *      #define SUNDIALS_EXPORT __declspec(dllimport)
 * In all other cases (other platforms or static libraries under
 * Windows), the SUNDIALS_EXPORT macro is empty
 */
#define SUNDIALS_EXPORT
