/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 *  Header file for error values, etc.
 */
#ifndef H5Eprivate_H
#define H5Eprivate_H

#include "H5Epublic.h"

/* Private headers needed by this file */
#include "H5private.h"

/* Typedef for error stack (defined in H5Epkg.h) */
typedef struct H5E_t H5E_t;

/*
 * HERROR macro, used to facilitate error reporting between a FUNC_ENTER()
 * and a FUNC_LEAVE() within a function body.  The arguments are the major
 * error number, the minor error number, and a description of the error.
 */
#define HERROR(maj_id, min_id, ...)                                                                          \
    do {                                                                                                     \
        H5E_printf_stack(NULL, __FILE__, __func__, __LINE__, H5E_ERR_CLS_g, maj_id, min_id, __VA_ARGS__);    \
    } while (0)

/*
 * HCOMMON_ERROR macro, used by HDONE_ERROR and HGOTO_ERROR
 * (Shouldn't need to be used outside this header file)
 */
#define HCOMMON_ERROR(maj, min, ...)                                                                         \
    do {                                                                                                     \
        HERROR(maj, min, __VA_ARGS__);                                                                       \
        err_occurred = true;                                                                                 \
        err_occurred = err_occurred; /* Shut GCC warnings up! */                                             \
    } while (0)

/*
 * HDONE_ERROR macro, used to facilitate error reporting between a
 * FUNC_ENTER() and a FUNC_LEAVE() within a function body, but _AFTER_ the
 * "done:" label.  The arguments are
 * the major error number, the minor error number, a return value, and a
 * description of the error.
 * (This macro can also be used to push an error and set the return value
 *      without jumping to any labels)
 */
#define HDONE_ERROR(maj, min, ret_val, ...)                                                                  \
    do {                                                                                                     \
        HCOMMON_ERROR(maj, min, __VA_ARGS__);                                                                \
        ret_value = ret_val;                                                                                 \
    } while (0)

/*
 * HGOTO_ERROR macro, used to facilitate error reporting between a
 * FUNC_ENTER() and a FUNC_LEAVE() within a function body.  The arguments are
 * the major error number, the minor error number, the return value, and an
 * error string.  The return value is assigned to a variable `ret_value' and
 * control branches to the `done' label.
 */
#define HGOTO_ERROR(maj, min, ret_val, ...)                                                                  \
    do {                                                                                                     \
        HCOMMON_ERROR(maj, min, __VA_ARGS__);                                                                \
        HGOTO_DONE(ret_val);                                                                                 \
    } while (0)

/*
 * HGOTO_ERROR_TAG macro, used like HGOTO_ERROR between H5_BEGIN_TAG and
 * H5_END_TAG statements.  Resets the metadata tag before leaving the function.
 */
#define HGOTO_ERROR_TAG(maj, min, ret_val, ...)                                                              \
    do {                                                                                                     \
        H5AC_tag(prv_tag, NULL);                                                                             \
        HCOMMON_ERROR(maj, min, __VA_ARGS__);                                                                \
        HGOTO_DONE(ret_val);                                                                                 \
    } while (0)

/*
 * HGOTO_DONE macro, used to facilitate normal return between a FUNC_ENTER()
 * and a FUNC_LEAVE() within a function body. The argument is the return
 * value which is assigned to the `ret_value' variable.	 Control branches to
 * the `done' label.
 */
#define HGOTO_DONE(ret_val)                                                                                  \
    do {                                                                                                     \
        ret_value = ret_val;                                                                                 \
        goto done;                                                                                           \
    } while (0)

/*
 * HGOTO_DONE_TAG macro, used like HGOTO_DONE between H5_BEGIN_TAG and
 * H5_END_TAG statements.  Resets the metadata tag before leaving the function.
 */
#define HGOTO_DONE_TAG(ret_val)                                                                              \
    do {                                                                                                     \
        H5AC_tag(prv_tag, NULL);                                                                             \
        HGOTO_DONE(ret_val);                                                                                 \
    } while (0)

/*
 * Macros handling system error messages as described in C standard.
 * These macros assume errnum is a valid system error code.
 */

/* Retrieve the error code description string and push it onto the error
 * stack.
 */
#ifndef H5_HAVE_WIN32_API
#define HSYS_DONE_ERROR(majorcode, minorcode, retcode, str)                                                  \
    {                                                                                                        \
        int myerrno = errno;                                                                                 \
        /* Other projects may rely on the description format to get the errno and any changes should be      \
         * considered as an API change                                                                       \
         */                                                                                                  \
        HDONE_ERROR(majorcode, minorcode, retcode, "%s, errno = %d, error message = '%s'", str, myerrno,     \
                    strerror(myerrno));                                                                      \
    }
#define HSYS_GOTO_ERROR(majorcode, minorcode, retcode, str)                                                  \
    {                                                                                                        \
        int myerrno = errno;                                                                                 \
        /* Other projects may rely on the description format to get the errno and any changes should be      \
         * considered as an API change                                                                       \
         */                                                                                                  \
        HGOTO_ERROR(majorcode, minorcode, retcode, "%s, errno = %d, error message = '%s'", str, myerrno,     \
                    strerror(myerrno));                                                                      \
    }
#else /* H5_HAVE_WIN32_API */
/* On Windows we also emit the result of GetLastError(). This call returns a DWORD, which is always a
 * 32-bit unsigned type. Note that on Windows, either errno or GetLastError() (but probably not both) will
 * be useful depending on whether a C/POSIX or Win32 call failed. The other value will likely be zero,
 * though I wouldn't count on that.
 */
#define HSYS_DONE_ERROR(majorcode, minorcode, retcode, str)                                                  \
    {                                                                                                        \
        int   myerrno   = errno;                                                                             \
        DWORD win_error = GetLastError();                                                                    \
        /* Other projects may rely on the description format to get the errno and any changes should be      \
         * considered as an API change                                                                       \
         */                                                                                                  \
        HDONE_ERROR(majorcode, minorcode, retcode,                                                           \
                    "%s, errno = %d, error message = '%s', Win32 GetLastError() = %" PRIu32 "", str,         \
                    myerrno, strerror(myerrno), win_error);                                                  \
    }
#define HSYS_GOTO_ERROR(majorcode, minorcode, retcode, str)                                                  \
    {                                                                                                        \
        int   myerrno   = errno;                                                                             \
        DWORD win_error = GetLastError();                                                                    \
        /* Other projects may rely on the description format to get the errno and any changes should be      \
         * considered as an API change                                                                       \
         */                                                                                                  \
        HGOTO_ERROR(majorcode, minorcode, retcode,                                                           \
                    "%s, errno = %d, error message = '%s', Win32 GetLastError() = %" PRIu32 "", str,         \
                    myerrno, strerror(myerrno), win_error);                                                  \
    }
#endif /* H5_HAVE_WIN32_API */

#ifdef H5_HAVE_PARALLEL
/*
 * MPI error handling macros.
 */

extern char H5E_mpi_error_str[MPI_MAX_ERROR_STRING];
extern int  H5E_mpi_error_str_len;

#define HMPI_DONE_ERROR(retcode, str, mpierr)                                                                \
    {                                                                                                        \
        MPI_Error_string(mpierr, H5E_mpi_error_str, &H5E_mpi_error_str_len);                                 \
        HDONE_ERROR(H5E_INTERNAL, H5E_MPI, retcode, "%s: MPI error string is '%s'", str, H5E_mpi_error_str); \
    }
#define HMPI_GOTO_ERROR(retcode, str, mpierr)                                                                \
    {                                                                                                        \
        MPI_Error_string(mpierr, H5E_mpi_error_str, &H5E_mpi_error_str_len);                                 \
        HGOTO_ERROR(H5E_INTERNAL, H5E_MPI, retcode, "%s: MPI error string is '%s'", str, H5E_mpi_error_str); \
    }
#endif /* H5_HAVE_PARALLEL */

/* Library-private functions defined in H5E package */
H5_DLL herr_t H5E_init(void);
H5_DLL herr_t H5E_printf_stack(H5E_t *estack, const char *file, const char *func, unsigned line, hid_t cls_id,
                               hid_t maj_id, hid_t min_id, const char *fmt, ...) H5_ATTR_FORMAT(printf, 8, 9);
H5_DLL herr_t H5E_clear_stack(H5E_t *estack);
H5_DLL herr_t H5E_dump_api_stack(bool is_api);

#endif /* H5Eprivate_H */
