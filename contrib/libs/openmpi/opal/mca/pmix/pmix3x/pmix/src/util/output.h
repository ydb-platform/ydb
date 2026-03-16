/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2011 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2015-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 * PMIX output stream facility.
 *
 * The PMIX output stream facility is used to send output from the PMIX
 * libraries to output devices.  It is meant to fully replace all
 * forms of printf() (and friends).  Output streams are opened via the
 * pmix_output_open() function call, and then sent output via
 * pmix_output_verbose(), PMIX_OUTPUT(), and pmix_output().  Streams are
 * closed with pmix_output_close().
 *
 * Streams can multiplex output to several kinds of outputs (one of
 * each):
 *
 * - the syslog (if available)
 * - standard output
 * - standard error
 * - file
 *
 * Which outputs to use are specified during pmix_output_open().
 *
 * WARNING: When using "file" as an output destination, be aware that
 * the file may not exist until the session directory for the process
 * exists.  This is at least part of the way through MPI_INIT (for
 * example).  Most MCA components and internals of PMIx won't be
 * affected by this, but some RTE / startup aspects of PMIx will
 * not be able to write to a file for output.  See pmix_output() for
 * details on what happens in these cases.
 *
 * pmix_output_open() returns an integer handle that is used in
 * successive calls to PMIX_OUTPUT() and pmix_output() to send output to
 * the stream.
 *
 * The default "verbose" stream is opened after invoking
 * pmix_output_init() (and closed after invoking
 * pmix_output_finalize()).  This stream outputs to stderr only, and
 * has a stream handle ID of 0.
 *
 * It is erroneous to have one thread close a stream and have another
 * try to write to it.  Multiple threads writing to a single stream
 * will be serialized in an unspecified order.
 */

#ifndef PMIX_OUTPUT_H_
#define PMIX_OUTPUT_H_

#include <src/include/pmix_config.h>


#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif

#include "pmix_rename.h"
#include "src/class/pmix_object.h"

BEGIN_C_DECLS

/* There are systems where all output needs to be redirected to syslog
 * and away from stdout/stderr or files - e.g., embedded systems whose
 * sole file system is in flash. To support such systems, we provide
 * the following environmental variables that support redirecting -all-
 * output (both from pmix_output and stdout/stderr of processes) to
 * syslog:
 *
 * PMIX_OUTPUT_REDIRECT - set to "syslog" to redirect to syslog. Other
 *                        options may someday be supported
 * PMIX_OUTPUT_SYSLOG_PRI - set to "info", "error", or "warn" to have
 *                        output sent to syslog at that priority
 * PMIX_OUTPUT_SYSLOG_IDENT - a string identifier for the log
 *
 * We also define two global variables that notify all other
 * layers that output is being redirected to syslog at the given
 * priority. These are used, for example, by the IO forwarding
 * subsystem to tell it to dump any collected output directly to
 * syslog instead of forwarding it to another location.
 */
extern bool pmix_output_redirected_to_syslog;
extern int pmix_output_redirected_syslog_pri;

/**
 * \class pmix_output_stream_t
 *
 * Structure used to request the opening of a PMIX output stream.  A
 * pointer to this structure is passed to pmix_output_open() to tell
 * the pmix_output subsystem where to send output for a given stream.
 * It is valid to specify multiple destinations of output for a stream
 * -- output streams can be multiplexed to multiple different
 * destinations through the pmix_output facility.
 *
 * Note that all strings in this struct are cached on the stream by
 * value; there is no need to keep them allocated after the return
 * from pmix_output_open().
 */
struct pmix_output_stream_t {
    /** Class parent */
    pmix_object_t super;

    /**
     * Indicate the starting verbosity level of the stream.
     *
     * Verbose levels are a convenience mechanisms, and are only
     * consulted when output is sent to a stream through the
     * pmix_output_verbose() function.  Verbose levels are ignored in
     * PMIX_OUTPUT() and pmix_output().
     *
     * Valid verbose levels typically start at 0 (meaning "minimal
     * information").  Higher verbosity levels generally indicate that
     * more output and diagnostics should be displayed.
     */
    int lds_verbose_level;

    /**
     * When pmix_output_stream_t::lds_want_syslog is true, this field is
     * examined to see what priority output from the stream should be
     * sent to the syslog.
     *
     * This value should be set as per the syslog(3) man page.  It is
     * typically the OR value of "facilty" and "level" values described
     * in the man page.
     */
    int lds_syslog_priority;
    /**
     * When pmix_output_stream_t::lds_want_syslog is true, this field is
     * examined to see what ident value should be passed to openlog(3).
     *
     * If a NULL value is given, the string "pmix" is used.
     */
#if !defined(__WINDOWS__)
    char *lds_syslog_ident;
#elif !defined(_MSC_VER)
    char *lds_syslog_ident;
#else
    HANDLE lds_syslog_ident;
#endif  /* !defined(__WINDOWS__) */

    /**
     * String prefix added to all output on the stream.
     *
     * When this field is non-NULL, it is prefixed to all lines of
     * output on the stream.  When this field is NULL, no prefix is
     * added to each line of output in the stream. The prefix is copied
     * to an internal structure in the call to pmix_output_open()!
     */
    char *lds_prefix;

    /**
     * String suffix added to all output on the stream.
     *
     * When this field is non-NULL, it is appended to all lines of
     * output on the stream.  When this field is NULL, no suffix is
     * added to each line of output in the stream. The suffix is copied
     * to an internal structure in the call to pmix_output_open()!
     */
    char *lds_suffix;

    /**
     * Indicates whether the output of the stream is
     * debugging/developer-only output or not.
     *
     * This field should be "true" if the output is for debugging
     * purposes only.  In that case, the output will never be sent to
     * the stream unless PMIX was configured with --enable-debug.
     */
    bool lds_is_debugging;

    /**
     * Indicates whether output of the stream should be sent to the
     * syslog or not.
     *
     * If this field is true, output from this stream is sent to the
     * syslog, and the following fields are also examined:
     *
     * - lds_syslog_priority
     * - lds_syslog_ident
     * - lds_prefix
     *
     * If this field is false, the above three fields are ignored.
     */
    bool lds_want_syslog;

    /**
     * Whether to send stream output to stdout or not.
     *
     * If this field is true, stream output is sent to stdout.
     */
    bool lds_want_stdout;
    /**
     * Whether to send stream output to stderr or not.
     *
     * If this field is true, stream output is sent to stderr.
     */
    bool lds_want_stderr;

    /**
     * Whether to send stream output to a file or not.
     *
     * When this field is true, stream output is sent to a file, and the
     * following fields are also examined:
     *
     * - lds_want_file_append
     * - lda_file_suffix
     */
    bool lds_want_file;
    /**
     * When pmix_output_stream_t::lds_want_file is true, this field
     * indicates whether to append the file (if it exists) or overwrite
     * it.
     *
     * If false, the file is opened with the O_TRUNC flag.
     */
    bool lds_want_file_append;
    /**
     * When pmix_output_stream_t::lds_want_file is true, this field
     * indicates the string suffix to add to the filename.
     *
     * The output file will be in the directory and begin with the
     * prefix set by pmix_output_set_output_file_info() (e.g.,
     * "$dir/$prefix$suffix").  If this field is NULL and
     * lds_want_file is true, then the suffix "output.txt" is used.
     *
     * Note that it is possible that the output directory may not
     * exist when pmix_output_open() is invoked.  See pmix_output()
     * for details on what happens in this situation.
     */
    char *lds_file_suffix;

};

/**
 * Convenience typedef
 */
typedef struct pmix_output_stream_t pmix_output_stream_t;

/**
 * Initializes the output stream system and opens a default
 * "verbose" stream.
 *
 * @retval true Upon success.
 * @retval false Upon failure.
 *
 * This should be the first function invoked in the output
 * subsystem.  After this call, the default "verbose" stream is open
 * and can be written to via calls to pmix_output_verbose() and
 * pmix_output_error().
 *
 * By definition, the default verbose stream has a handle ID of 0,
 * and has a verbose level of 0.
 */
PMIX_EXPORT bool pmix_output_init(void);

/**
 * Shut down the output stream system.
 *
 * Shut down the output stream system, including the default verbose
 * stream.
 */
PMIX_EXPORT void pmix_output_finalize(void);

/**
 * Opens an output stream.
 *
 * @param lds A pointer to pmix_output_stream_t describing what the
 * characteristics of the output stream should be.
 *
 * This function opens an output stream and returns an integer
 * handle.  The caller is responsible for maintaining the handle and
 * using it in successive calls to PMIX_OUTPUT(), pmix_output(),
 * pmix_output_switch(), and pmix_output_close().
 *
 * If lds is NULL, the default descriptions will be used, meaning
 * that output will only be sent to stderr.
 *
 * It is safe to have multiple threads invoke this function
 * simultaneously; their execution will be serialized in an
 * unspecified manner.
 *
 * Be sure to see pmix_output() for a description of what happens
 * when open_open() / pmix_output() is directed to send output to a
 * file but the process session directory does not yet exist.
 */
PMIX_EXPORT int pmix_output_open(pmix_output_stream_t *lds);

/**
 * Re-opens / redirects an output stream.
 *
 * @param output_id Stream handle to reopen
 * @param lds A pointer to pmix_output_stream_t describing what the
 * characteristics of the reopened output stream should be.
 *
 * This function redirects an existing stream into a new [set of]
 * location[s], as specified by the lds parameter.  If the output_id
 * passed is invalid, this call is effectively the same as opening a
 * new stream with a specific stream handle.
 */
PMIX_EXPORT int pmix_output_reopen(int output_id, pmix_output_stream_t *lds);

/**
 * Enables and disables output streams.
 *
 * @param output_id Stream handle to switch
 * @param enable Boolean indicating whether to enable the stream
 * output or not.
 *
 * @returns The previous enable state of the stream (true == enabled,
 * false == disabled).
 *
 * The output of a stream can be temporarily disabled by passing an
 * enable value to false, and later resumed by passing an enable
 * value of true.  This does not close the stream -- it simply tells
 * the pmix_output subsystem to intercept and discard any output sent
 * to the stream via PMIX_OUTPUT() or pmix_output() until the output
 * is re-enabled.
 */
PMIX_EXPORT bool pmix_output_switch(int output_id, bool enable);

/**
 * \internal
 *
 * Reopens all existing output streams.
 *
 * This function should never be called by user applications; it is
 * typically only invoked after a restart (i.e., in a new process)
 * where output streams need to be re-initialized.
 */
PMIX_EXPORT void pmix_output_reopen_all(void);

/**
 * Close an output stream.
 *
 * @param output_id Handle of the stream to close.
 *
 * Close an output stream.  No output will be sent to the stream
 * after it is closed.  Be aware that output handles tend to be
 * re-used; it is possible that after a stream is closed, if another
 * stream is opened, it will get the same handle value.
 */
PMIX_EXPORT void pmix_output_close(int output_id);

/**
 * Main function to send output to a stream.
 *
 * @param output_id Stream id returned from pmix_output_open().
 * @param format printf-style format string.
 * @param varargs printf-style varargs list to fill the string
 * specified by the format parameter.
 *
 * This is the main function to send output to custom streams (note
 * that output to the default "verbose" stream is handled through
 * pmix_output_verbose() and pmix_output_error()).
 *
 * It is never necessary to send a trailing "\n" in the strings to
 * this function; some streams requires newlines, others do not --
 * this function will append newlines as necessary.
 *
 * Verbosity levels are ignored in this function.
 *
 * Note that for output streams that are directed to files, the
 * files are stored under the process' session directory.  If the
 * session directory does not exist when pmix_output() is invoked,
 * the output will be discarded!  Once the session directory is
 * created, pmix_output() will automatically create the file and
 * writing to it.
 */
PMIX_EXPORT void pmix_output(int output_id, const char *format, ...) __pmix_attribute_format__(__printf__, 2, 3);

/**
 * Send output to a stream only if the passed verbosity level is
 * high enough.
 *
 * @param output_id Stream id returned from pmix_output_open().
 * @param level Target verbosity level.
 * @param format printf-style format string.
 * @param varargs printf-style varargs list to fill the string
 * specified by the format parameter.
 *
 * Output is only sent to the stream if the current verbosity level
 * is greater than or equal to the level parameter.  This mechanism
 * can be used to send "information" kinds of output to user
 * applications, but only when the user has asked for a high enough
 * verbosity level.
 *
 * It is never necessary to send a trailing "\n" in the strings to
 * this function; some streams requires newlines, others do not --
 * this function will append newlines as necessary.
 *
 * This function is really a convenience wrapper around checking the
 * current verbosity level set on the stream, and if the passed
 * level is less than or equal to the stream's verbosity level, this
 * function will effectively invoke pmix_output to send the output to
 * the stream.
 *
 * @see pmix_output_set_verbosity()
 */
PMIX_EXPORT void pmix_output_verbose(int verbose_level, int output_id,
                                     const char *format, ...) __pmix_attribute_format__(__printf__, 3, 4);

/**
* Same as pmix_output_verbose(), but takes a va_list form of varargs.
*/
PMIX_EXPORT void pmix_output_vverbose(int verbose_level, int output_id,
                                      const char *format, va_list ap) __pmix_attribute_format__(__printf__, 3, 0);

/**
 * Set the verbosity level for a stream.
 *
 * @param output_id Stream id returned from pmix_output_open().
 * @param level New verbosity level
 *
 * This function sets the verbosity level on a given stream.  It
 * will be used for all future invocations of pmix_output_verbose().
 */
PMIX_EXPORT void pmix_output_set_verbosity(int output_id, int level);

/**
 * Get the verbosity level for a stream
 *
 * @param output_id Stream id returned from pmix_output_open()
 * @returns Verbosity of stream
 */
PMIX_EXPORT int pmix_output_get_verbosity(int output_id);

/**
 * Set characteristics for output files.
 *
 * @param dir Directory where output files will go
 * @param olddir If non-NULL, the directory where output files
 * were previously opened
 * @param prefix Prefix of files in the output directory
 * @param oldprefix If non-NULL, the old prefix
 *
 * This function controls the final filename used for all new
 * output streams that request output files.  Specifically, when
 * pmix_output_stream_t::lds_want_file is true, the output
 * filename will be of the form $dir/$prefix$suffix.
 *
 * The default value for the output directory is whatever is
 * specified in the TMPDIR environment variable if it exists, or
 * $HOME if it does not.  The default value for the prefix is
 * "output-pid<pid>-" (where "<pid>" is replaced by the PID of the
 * current process).
 *
 * If dir or prefix are NULL, new values are not set.  The strings
 * represented by dir and prefix are copied into internal storage;
 * it is safe to pass string constants or free() these values
 * after pmix_output_set_output_file_info() returns.
 *
 * If olddir or oldprefix are not NULL, copies of the old
 * directory and prefix (respectively) are returned in these
 * parameters.  The caller is responsible for calling (free) on
 * these values.  This allows one to get the old values, output an
 * output file in a specific directory and/or with a specific
 * prefix, and then restore the old values.
 *
 * Note that this function only affects the creation of \em new
 * streams -- streams that have already started writing to output
 * files are not affected (i.e., their output files are not moved
 * to the new directory).  More specifically, the pmix_output
 * system only opens/creates output files lazily -- so calling
 * this function affects both new streams \em and any stream that
 * was previously opened but had not yet output anything.
 */
PMIX_EXPORT void pmix_output_set_output_file_info(const char *dir,
                                                  const char *prefix,
                                                  char **olddir,
                                                  char **oldprefix);

/**
 * Same as pmix_output_verbose(), but pointer to buffer and size.
 */
PMIX_EXPORT void pmix_output_hexdump(int verbose_level, int output_id,
                                     void *ptr, int buflen);

#if PMIX_ENABLE_DEBUG
    /**
     * Main macro for use in sending debugging output to output streams;
     * will be "compiled out" when PMIX is configured without
     * --enable-debug.
     *
     * @see pmix_output()
     */
#define PMIX_OUTPUT(a) pmix_output a

    /**
     * Macro for use in sending debugging output to the output
     * streams.  Will be "compiled out" when PMIX is configured
     * without --enable-debug.
     *
     * @see pmix_output_verbose()
     */
#define PMIX_OUTPUT_VERBOSE(a) pmix_output_verbose a
#else
    /**
     * Main macro for use in sending debugging output to output streams;
     * will be "compiled out" when PMIX is configured without
     * --enable-debug.
     *
     * @see pmix_output()
     */
#define PMIX_OUTPUT(a)

    /**
     * Macro for use in sending debugging output to the output
     * streams.  Will be "compiled out" when PMIX is configured
     * without --enable-debug.
     *
     * @see pmix_output_verbose()
     */
#define PMIX_OUTPUT_VERBOSE(a)
#endif

/**
 * Declare the class of this type.  Note that the constructor for
 * this class is for convenience only -- it is \em not necessary
 * to be invoked.  If the constructor it used, it sets all values
 * in the struct to be false / 0 (i.e., turning off all output).
 * The intended usage is to invoke the constructor and then enable
 * the output fields that you want.
 */
PMIX_CLASS_DECLARATION(pmix_output_stream_t);

END_C_DECLS

#endif /* PMIX_OUTPUT_H_ */
