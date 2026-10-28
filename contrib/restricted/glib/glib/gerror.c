/* GLIB - Library of useful routines for C programming
 * Copyright (C) 1995-1997  Peter Mattis, Spencer Kimball and Josh MacDonald
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

/*
 * Modified by the GLib Team and others 1997-2000.  See the AUTHORS
 * file for a list of people on the GLib Team.  See the ChangeLog
 * files for a list of changes.  These files are distributed with
 * GLib at ftp://ftp.gtk.org/pub/gtk/.
 */

/**
 * SECTION:error_reporting
 * @Title: Error Reporting
 * @Short_description: a system for reporting errors
 *
 * GLib provides a standard method of reporting errors from a called
 * function to the calling code. (This is the same problem solved by
 * exceptions in other languages.) It's important to understand that
 * this method is both a data type (the #GError struct) and a [set of
 * rules][gerror-rules]. If you use #GError incorrectly, then your code will not
 * properly interoperate with other code that uses #GError, and users
 * of your API will probably get confused. In most cases, [using #GError is
 * preferred over numeric error codes][gerror-comparison], but there are
 * situations where numeric error codes are useful for performance.
 *
 * First and foremost: #GError should only be used to report recoverable
 * runtime errors, never to report programming errors. If the programmer
 * has screwed up, then you should use g_warning(), g_return_if_fail(),
 * g_assert(), g_error(), or some similar facility. (Incidentally,
 * remember that the g_error() function should only be used for
 * programming errors, it should not be used to print any error
 * reportable via #GError.)
 *
 * Examples of recoverable runtime errors are "file not found" or
 * "failed to parse input." Examples of programming errors are "NULL
 * passed to strcmp()" or "attempted to free the same pointer twice."
 * These two kinds of errors are fundamentally different: runtime errors
 * should be handled or reported to the user, programming errors should
 * be eliminated by fixing the bug in the program. This is why most
 * functions in GLib and GTK+ do not use the #GError facility.
 *
 * Functions that can fail take a return location for a #GError as their
 * last argument. On error, a new #GError instance will be allocated and
 * returned to the caller via this argument. For example:
 * |[<!-- language="C" -->
 * gboolean g_file_get_contents (const gchar  *filename,
 *                               gchar       **contents,
 *                               gsize        *length,
 *                               GError      **error);
 * ]|
 * If you pass a non-%NULL value for the `error` argument, it should
 * point to a location where an error can be placed. For example:
 * |[<!-- language="C" -->
 * gchar *contents;
 * GError *err = NULL;
 *
 * g_file_get_contents ("foo.txt", &contents, NULL, &err);
 * g_assert ((contents == NULL && err != NULL) || (contents != NULL && err == NULL));
 * if (err != NULL)
 *   {
 *     // Report error to user, and free error
 *     g_assert (contents == NULL);
 *     fprintf (stderr, "Unable to read file: %s\n", err->message);
 *     g_error_free (err);
 *   }
 * else
 *   {
 *     // Use file contents
 *     g_assert (contents != NULL);
 *   }
 * ]|
 * Note that `err != NULL` in this example is a reliable indicator
 * of whether g_file_get_contents() failed. Additionally,
 * g_file_get_contents() returns a boolean which
 * indicates whether it was successful.
 *
 * Because g_file_get_contents() returns %FALSE on failure, if you
 * are only interested in whether it failed and don't need to display
 * an error message, you can pass %NULL for the @error argument:
 * |[<!-- language="C" -->
 * if (g_file_get_contents ("foo.txt", &contents, NULL, NULL)) // ignore errors
 *   // no error occurred 
 *   ;
 * else
 *   // error
 *   ;
 * ]|
 *
 * The #GError object contains three fields: @domain indicates the module
 * the error-reporting function is located in, @code indicates the specific
 * error that occurred, and @message is a user-readable error message with
 * as many details as possible. Several functions are provided to deal
 * with an error received from a called function: g_error_matches()
 * returns %TRUE if the error matches a given domain and code,
 * g_propagate_error() copies an error into an error location (so the
 * calling function will receive it), and g_clear_error() clears an
 * error location by freeing the error and resetting the location to
 * %NULL. To display an error to the user, simply display the @message,
 * perhaps along with additional context known only to the calling
 * function (the file being opened, or whatever - though in the
 * g_file_get_contents() case, the @message already contains a filename).
 *
 * Since error messages may be displayed to the user, they need to be valid
 * UTF-8 (all GTK widgets expect text to be UTF-8). Keep this in mind in
 * particular when formatting error messages with filenames, which are in
 * the 'filename encoding', and need to be turned into UTF-8 using
 * g_filename_to_utf8(), g_filename_display_name() or g_utf8_make_valid().
 *
 * Note, however, that many error messages are too technical to display to the
 * user in an application, so prefer to use g_error_matches() to categorize errors
 * from called functions, and build an appropriate error message for the context
 * within your application. Error messages from a #GError are more appropriate
 * to be printed in system logs or on the command line. They are typically
 * translated.
 *
 * When implementing a function that can report errors, the basic
 * tool is g_set_error(). Typically, if a fatal error occurs you
 * want to g_set_error(), then return immediately. g_set_error()
 * does nothing if the error location passed to it is %NULL.
 * Here's an example:
 * |[<!-- language="C" -->
 * gint
 * foo_open_file (GError **error)
 * {
 *   gint fd;
 *   int saved_errno;
 *
 *   g_return_val_if_fail (error == NULL || *error == NULL, -1);
 *
 *   fd = open ("file.txt", O_RDONLY);
 *   saved_errno = errno;
 *
 *   if (fd < 0)
 *     {
 *       g_set_error (error,
 *                    FOO_ERROR,                 // error domain
 *                    FOO_ERROR_BLAH,            // error code
 *                    "Failed to open file: %s", // error message format string
 *                    g_strerror (saved_errno));
 *       return -1;
 *     }
 *   else
 *     return fd;
 * }
 * ]|
 *
 * Things are somewhat more complicated if you yourself call another
 * function that can report a #GError. If the sub-function indicates
 * fatal errors in some way other than reporting a #GError, such as
 * by returning %TRUE on success, you can simply do the following:
 * |[<!-- language="C" -->
 * gboolean
 * my_function_that_can_fail (GError **err)
 * {
 *   g_return_val_if_fail (err == NULL || *err == NULL, FALSE);
 *
 *   if (!sub_function_that_can_fail (err))
 *     {
 *       // assert that error was set by the sub-function
 *       g_assert (err == NULL || *err != NULL);
 *       return FALSE;
 *     }
 *
 *   // otherwise continue, no error occurred
 *   g_assert (err == NULL || *err == NULL);
 * }
 * ]|
 *
 * If the sub-function does not indicate errors other than by
 * reporting a #GError (or if its return value does not reliably indicate
 * errors) you need to create a temporary #GError
 * since the passed-in one may be %NULL. g_propagate_error() is
 * intended for use in this case.
 * |[<!-- language="C" -->
 * gboolean
 * my_function_that_can_fail (GError **err)
 * {
 *   GError *tmp_error;
 *
 *   g_return_val_if_fail (err == NULL || *err == NULL, FALSE);
 *
 *   tmp_error = NULL;
 *   sub_function_that_can_fail (&tmp_error);
 *
 *   if (tmp_error != NULL)
 *     {
 *       // store tmp_error in err, if err != NULL,
 *       // otherwise call g_error_free() on tmp_error
 *       g_propagate_error (err, tmp_error);
 *       return FALSE;
 *     }
 *
 *   // otherwise continue, no error occurred
 * }
 * ]|
 *
 * Error pileups are always a bug. For example, this code is incorrect:
 * |[<!-- language="C" -->
 * gboolean
 * my_function_that_can_fail (GError **err)
 * {
 *   GError *tmp_error;
 *
 *   g_return_val_if_fail (err == NULL || *err == NULL, FALSE);
 *
 *   tmp_error = NULL;
 *   sub_function_that_can_fail (&tmp_error);
 *   other_function_that_can_fail (&tmp_error);
 *
 *   if (tmp_error != NULL)
 *     {
 *       g_propagate_error (err, tmp_error);
 *       return FALSE;
 *     }
 * }
 * ]|
 * @tmp_error should be checked immediately after sub_function_that_can_fail(),
 * and either cleared or propagated upward. The rule is: after each error,
 * you must either handle the error, or return it to the calling function.
 *
 * Note that passing %NULL for the error location is the equivalent
 * of handling an error by always doing nothing about it. So the
 * following code is fine, assuming errors in sub_function_that_can_fail()
 * are not fatal to my_function_that_can_fail():
 * |[<!-- language="C" -->
 * gboolean
 * my_function_that_can_fail (GError **err)
 * {
 *   GError *tmp_error;
 *
 *   g_return_val_if_fail (err == NULL || *err == NULL, FALSE);
 *
 *   sub_function_that_can_fail (NULL); // ignore errors
 *
 *   tmp_error = NULL;
 *   other_function_that_can_fail (&tmp_error);
 *
 *   if (tmp_error != NULL)
 *     {
 *       g_propagate_error (err, tmp_error);
 *       return FALSE;
 *     }
 * }
 * ]|
 *
 * Note that passing %NULL for the error location ignores errors;
 * it's equivalent to
 * `try { sub_function_that_can_fail (); } catch (...) {}`
 * in C++. It does not mean to leave errors unhandled; it means
 * to handle them by doing nothing.
 *
 * Error domains and codes are conventionally named as follows:
 *
 * - The error domain is called <NAMESPACE>_<MODULE>_ERROR,
 *   for example %G_SPAWN_ERROR or %G_THREAD_ERROR:
 *   |[<!-- language="C" -->
 *   #define G_SPAWN_ERROR g_spawn_error_quark ()
 *
 *   G_DEFINE_QUARK (g-spawn-error-quark, g_spawn_error)
 *   ]|
 *
 * - The quark function for the error domain is called
 *   <namespace>_<module>_error_quark,
 *   for example g_spawn_error_quark() or g_thread_error_quark().
 *
 * - The error codes are in an enumeration called
 *   <Namespace><Module>Error;
 *   for example, #GThreadError or #GSpawnError.
 *
 * - Members of the error code enumeration are called
 *   <NAMESPACE>_<MODULE>_ERROR_<CODE>,
 *   for example %G_SPAWN_ERROR_FORK or %G_THREAD_ERROR_AGAIN.
 *
 * - If there's a "generic" or "unknown" error code for unrecoverable
 *   errors it doesn't make sense to distinguish with specific codes,
 *   it should be called <NAMESPACE>_<MODULE>_ERROR_FAILED,
 *   for example %G_SPAWN_ERROR_FAILED. In the case of error code
 *   enumerations that may be extended in future releases, you should
 *   generally not handle this error code explicitly, but should
 *   instead treat any unrecognized error code as equivalent to
 *   FAILED.
 *
 * ## Comparison of #GError and traditional error handling # {#gerror-comparison}
 *
 * #GError has several advantages over traditional numeric error codes:
 * importantly, tools like
 * [gobject-introspection](https://developer.gnome.org/gi/stable/) understand
 * #GErrors and convert them to exceptions in bindings; the message includes
 * more information than just a code; and use of a domain helps prevent
 * misinterpretation of error codes.
 *
 * #GError has disadvantages though: it requires a memory allocation, and
 * formatting the error message string has a performance overhead. This makes it
 * unsuitable for use in retry loops where errors are a common case, rather than
 * being unusual. For example, using %G_IO_ERROR_WOULD_BLOCK means hitting these
 * overheads in the normal control flow. String formatting overhead can be
 * eliminated by using g_set_error_literal() in some cases.
 *
 * These performance issues can be compounded if a function wraps the #GErrors
 * returned by the functions it calls: this multiplies the number of allocations
 * and string formatting operations. This can be partially mitigated by using
 * g_prefix_error().
 *
 * ## Rules for use of #GError # {#gerror-rules}
 *
 * Summary of rules for use of #GError:
 *
 * - Do not report programming errors via #GError.
 * 
 * - The last argument of a function that returns an error should
 *   be a location where a #GError can be placed (i.e. `GError **error`).
 *   If #GError is used with varargs, the `GError**` should be the last
 *   argument before the `...`.
 *
 * - The caller may pass %NULL for the `GError**` if they are not interested
 *   in details of the exact error that occurred.
 *
 * - If %NULL is passed for the `GError**` argument, then errors should
 *   not be returned to the caller, but your function should still
 *   abort and return if an error occurs. That is, control flow should
 *   not be affected by whether the caller wants to get a #GError.
 *
 * - If a #GError is reported, then your function by definition had a
 *   fatal failure and did not complete whatever it was supposed to do.
 *   If the failure was not fatal, then you handled it and you should not
 *   report it. If it was fatal, then you must report it and discontinue
 *   whatever you were doing immediately.
 *
 * - If a #GError is reported, out parameters are not guaranteed to
 *   be set to any defined value.
 *
 * - A `GError*` must be initialized to %NULL before passing its address
 *   to a function that can report errors.
 *
 * - #GError structs must not be stack-allocated.
 *
 * - "Piling up" errors is always a bug. That is, if you assign a
 *   new #GError to a `GError*` that is non-%NULL, thus overwriting
 *   the previous error, it indicates that you should have aborted
 *   the operation instead of continuing. If you were able to continue,
 *   you should have cleared the previous error with g_clear_error().
 *   g_set_error() will complain if you pile up errors.
 *
 * - By convention, if you return a boolean value indicating success
 *   then %TRUE means success and %FALSE means failure. Avoid creating
 *   functions which have a boolean return value and a #GError parameter,
 *   but where the boolean does something other than signal whether the
 *   #GError is set.  Among other problems, it requires C callers to allocate
 *   a temporary error.  Instead, provide a `gboolean *` out parameter.
 *   There are functions in GLib itself such as g_key_file_has_key() that
 *   are hard to use because of this. If %FALSE is returned, the error must
 *   be set to a non-%NULL value.  One exception to this is that in situations
 *   that are already considered to be undefined behaviour (such as when a
 *   g_return_val_if_fail() check fails), the error need not be set.
 *   Instead of checking separately whether the error is set, callers
 *   should ensure that they do not provoke undefined behaviour, then
 *   assume that the error will be set on failure.
 *
 * - A %NULL return value is also frequently used to mean that an error
 *   occurred. You should make clear in your documentation whether %NULL
 *   is a valid return value in non-error cases; if %NULL is a valid value,
 *   then users must check whether an error was returned to see if the
 *   function succeeded.
 *
 * - When implementing a function that can report errors, you may want
 *   to add a check at the top of your function that the error return
 *   location is either %NULL or contains a %NULL error (e.g.
 *   `g_return_if_fail (error == NULL || *error == NULL);`).
 *
 * ## Extended #GError Domains # {#gerror-extended-domains}
 *
 * Since GLib 2.68 it is possible to extend the #GError type. This is
 * done with the G_DEFINE_EXTENDED_ERROR() macro. To create an
 * extended #GError type do something like this in the header file:
 * |[<!-- language="C" -->
 * typedef enum
 * {
 *   MY_ERROR_BAD_REQUEST,
 * } MyError;
 * #define MY_ERROR (my_error_quark ())
 * GQuark my_error_quark (void);
 * int
 * my_error_get_parse_error_id (GError *error);
 * const char *
 * my_error_get_bad_request_details (GError *error);
 * ]|
 * and in implementation:
 * |[<!-- language="C" -->
 * typedef struct
 * {
 *   int parse_error_id;
 *   char *bad_request_details;
 * } MyErrorPrivate;
 *
 * static void
 * my_error_private_init (MyErrorPrivate *priv)
 * {
 *   priv->parse_error_id = -1;
 *   // No need to set priv->bad_request_details to NULL,
 *   // the struct is initialized with zeros.
 * }
 *
 * static void
 * my_error_private_copy (const MyErrorPrivate *src_priv, MyErrorPrivate *dest_priv)
 * {
 *   dest_priv->parse_error_id = src_priv->parse_error_id;
 *   dest_priv->bad_request_details = g_strdup (src_priv->bad_request_details);
 * }
 *
 * static void
 * my_error_private_clear (MyErrorPrivate *priv)
 * {
 *   g_free (priv->bad_request_details);
 * }
 *
 * // This defines the my_error_get_private and my_error_quark functions.
 * G_DEFINE_EXTENDED_ERROR (MyError, my_error)
 *
 * int
 * my_error_get_parse_error_id (GError *error)
 * {
 *   MyErrorPrivate *priv = my_error_get_private (error);
 *   g_return_val_if_fail (priv != NULL, -1);
 *   return priv->parse_error_id;
 * }
 *
 * const char *
 * my_error_get_bad_request_details (GError *error)
 * {
 *   MyErrorPrivate *priv = my_error_get_private (error);
 *   g_return_val_if_fail (priv != NULL, NULL);
 *   g_return_val_if_fail (error->code != MY_ERROR_BAD_REQUEST, NULL);
 *   return priv->bad_request_details;
 * }
 *
 * static void
 * my_error_set_bad_request (GError     **error,
 *                           const char  *reason,
 *                           int          error_id,
 *                           const char  *details)
 * {
 *   MyErrorPrivate *priv;
 *   g_set_error (error, MY_ERROR, MY_ERROR_BAD_REQUEST, "Invalid request: %s", reason);
 *   if (error != NULL && *error != NULL)
 *     {
 *       priv = my_error_get_private (error);
 *       g_return_val_if_fail (priv != NULL, NULL);
 *       priv->parse_error_id = error_id;
 *       priv->bad_request_details = g_strdup (details);
 *     }
 * }
 * ]|
 * An example of use of the error could be:
 * |[<!-- language="C" -->
 * gboolean
 * send_request (GBytes *request, GError **error)
 * {
 *   ParseFailedStatus *failure = validate_request (request);
 *   if (failure != NULL)
 *     {
 *       my_error_set_bad_request (error, failure->reason, failure->error_id, failure->details);
 *       parse_failed_status_free (failure);
 *       return FALSE;
 *     }
 *
 *   return send_one (request, error);
 * }
 * ]|
 *
 * Please note that if you are a library author and your library
 * exposes an existing error domain, then you can't make this error
 * domain an extended one without breaking ABI. This is because
 * earlier it was possible to create an error with this error domain
 * on the stack and then copy it with g_error_copy(). If the new
 * version of your library makes the error domain an extended one,
 * then g_error_copy() called by code that allocated the error on the
 * stack will try to copy more data than it used to, which will lead
 * to undefined behavior. You must not stack-allocate errors with an
 * extended error domain, and it is bad practice to stack-allocate any
 * other #GErrors.
 *
 * Extended error domains in unloadable plugins/modules are not
 * supported.
 */

#include <contrib/restricted/glib/config.h>

#include "gvalgrind.h"
#include <string.h>

#include "gerror.h"

#include "ghash.h"
#include "glib-init.h"
#include "gslice.h"
#include "gstrfuncs.h"
#include "gtestutils.h"
#include "gthread.h"

static GRWLock error_domain_global;
/* error_domain_ht must be accessed with error_domain_global
 * locked.
 */
static GHashTable *error_domain_ht = NULL;

void
g_error_init (void)
{
  error_domain_ht = g_hash_table_new (NULL, NULL);
}

typedef struct
{
  /* private_size is already aligned. */
  gsize private_size;
  GErrorInitFunc init;
  GErrorCopyFunc copy;
  GErrorClearFunc clear;
} ErrorDomainInfo;

/* Must be called with error_domain_global locked.
 */
static inline ErrorDomainInfo *
error_domain_lookup (GQuark domain)
{
  return g_hash_table_lookup (error_domain_ht,
                              GUINT_TO_POINTER (domain));
}

/* Copied from gtype.c. */
#define STRUCT_ALIGNMENT (2 * sizeof (gsize))
#define ALIGN_STRUCT(offset) \
      ((offset + (STRUCT_ALIGNMENT - 1)) & -STRUCT_ALIGNMENT)

static void
error_domain_register (GQuark            error_quark,
                       gsize             error_type_private_size,
                       GErrorInitFunc    error_type_init,
                       GErrorCopyFunc    error_type_copy,
                       GErrorClearFunc   error_type_clear)
{
  g_rw_lock_writer_lock (&error_domain_global);
  if (error_domain_lookup (error_quark) == NULL)
    {
      ErrorDomainInfo *info = g_new (ErrorDomainInfo, 1);
      info->private_size = ALIGN_STRUCT (error_type_private_size);
      info->init = error_type_init;
      info->copy = error_type_copy;
      info->clear = error_type_clear;

      g_hash_table_insert (error_domain_ht,
                           GUINT_TO_POINTER (error_quark),
                           info);
    }
  else
    {
      const char *name = g_quark_to_string (error_quark);

      g_critical ("Attempted to register an extended error domain for %s more than once", name);
    }
  g_rw_lock_writer_unlock (&error_domain_global);
}

/**
 * g_error_domain_register_static:
 * @error_type_name: static string to create a #GQuark from
 * @error_type_private_size: size of the private error data in bytes
 * @error_type_init: function initializing fields of the private error data
 * @error_type_copy: function copying fields of the private error data
 * @error_type_clear: function freeing fields of the private error data
 *
 * This function registers an extended #GError domain.
 *
 * @error_type_name should not be freed. @error_type_private_size must
 * be greater than 0.
 *
 * @error_type_init receives an initialized #GError and should then initialize
 * the private data.
 *
 * @error_type_copy is a function that receives both original and a copy
 * #GError and should copy the fields of the private error data. The standard
 * #GError fields are already handled.
 *
 * @error_type_clear receives the pointer to the error, and it should free the
 * fields of the private error data. It should not free the struct itself though.
 *
 * Normally, it is better to use G_DEFINE_EXTENDED_ERROR(), as it
 * already takes care of passing valid information to this function.
 *
 * Returns: #GQuark representing the error domain
 * Since: 2.68
 */
GQuark
g_error_domain_register_static (const char        *error_type_name,
                                gsize              error_type_private_size,
                                GErrorInitFunc     error_type_init,
                                GErrorCopyFunc     error_type_copy,
                                GErrorClearFunc    error_type_clear)
{
  GQuark error_quark;

  g_return_val_if_fail (error_type_name != NULL, 0);
  g_return_val_if_fail (error_type_private_size > 0, 0);
  g_return_val_if_fail (error_type_init != NULL, 0);
  g_return_val_if_fail (error_type_copy != NULL, 0);
  g_return_val_if_fail (error_type_clear != NULL, 0);

  error_quark = g_quark_from_static_string (error_type_name);
  error_domain_register (error_quark,
                         error_type_private_size,
                         error_type_init,
                         error_type_copy,
                         error_type_clear);
  return error_quark;
}

/**
 * g_error_domain_register:
 * @error_type_name: string to create a #GQuark from
 * @error_type_private_size: size of the private error data in bytes
 * @error_type_init: function initializing fields of the private error data
 * @error_type_copy: function copying fields of the private error data
 * @error_type_clear: function freeing fields of the private error data
 *
 * This function registers an extended #GError domain.
 * @error_type_name will be duplicated. Otherwise does the same as
 * g_error_domain_register_static().
 *
 * Returns: #GQuark representing the error domain
 * Since: 2.68
 */
GQuark
g_error_domain_register (const char        *error_type_name,
                         gsize              error_type_private_size,
                         GErrorInitFunc     error_type_init,
                         GErrorCopyFunc     error_type_copy,
                         GErrorClearFunc    error_type_clear)
{
  GQuark error_quark;

  g_return_val_if_fail (error_type_name != NULL, 0);
  g_return_val_if_fail (error_type_private_size > 0, 0);
  g_return_val_if_fail (error_type_init != NULL, 0);
  g_return_val_if_fail (error_type_copy != NULL, 0);
  g_return_val_if_fail (error_type_clear != NULL, 0);

  error_quark = g_quark_from_string (error_type_name);
  error_domain_register (error_quark,
                         error_type_private_size,
                         error_type_init,
                         error_type_copy,
                         error_type_clear);
  return error_quark;
}

static GError *
g_error_allocate (GQuark domain, ErrorDomainInfo *out_info)
{
  guint8 *allocated;
  GError *error;
  ErrorDomainInfo *info;
  gsize private_size;

  g_rw_lock_reader_lock (&error_domain_global);
  info = error_domain_lookup (domain);
  if (info != NULL)
    {
      if (out_info != NULL)
        *out_info = *info;
      private_size = info->private_size;
      g_rw_lock_reader_unlock (&error_domain_global);
    }
  else
    {
      g_rw_lock_reader_unlock (&error_domain_global);
      if (out_info != NULL)
        memset (out_info, 0, sizeof (*out_info));
      private_size = 0;
    }
  /* See comments in g_type_create_instance in gtype.c to see what
   * this magic is about.
   */
#ifdef ENABLE_VALGRIND
  if (private_size > 0 && RUNNING_ON_VALGRIND)
    {
      private_size += ALIGN_STRUCT (1);
      allocated = g_slice_alloc0 (private_size + sizeof (GError) + sizeof (gpointer));
      *(gpointer *) (allocated + private_size + sizeof (GError)) = allocated + ALIGN_STRUCT (1);
      VALGRIND_MALLOCLIKE_BLOCK (allocated + private_size, sizeof (GError) + sizeof (gpointer), 0, TRUE);
      VALGRIND_MALLOCLIKE_BLOCK (allocated + ALIGN_STRUCT (1), private_size - ALIGN_STRUCT (1), 0, TRUE);
    }
  else
#endif
    allocated = g_slice_alloc0 (private_size + sizeof (GError));

  error = (GError *) (allocated + private_size);
  return error;
}

/* This function takes ownership of @message. */
static GError *
g_error_new_steal (GQuark           domain,
                   gint             code,
                   gchar           *message,
                   ErrorDomainInfo *out_info)
{
  ErrorDomainInfo info;
  GError *error = g_error_allocate (domain, &info);

  error->domain = domain;
  error->code = code;
  error->message = message;

  if (info.init != NULL)
    info.init (error);
  if (out_info != NULL)
    *out_info = info;

  return error;
}

/**
 * g_error_new_valist:
 * @domain: error domain
 * @code: error code
 * @format: printf()-style format for error message
 * @args: #va_list of parameters for the message format
 *
 * Creates a new #GError with the given @domain and @code,
 * and a message formatted with @format.
 *
 * Returns: a new #GError
 *
 * Since: 2.22
 */
GError*
g_error_new_valist (GQuark       domain,
                    gint         code,
                    const gchar *format,
                    va_list      args)
{
  /* Historically, GError allowed this (although it was never meant to work),
   * and it has significant use in the wild, which g_return_val_if_fail
   * would break. It should maybe g_return_val_if_fail in GLib 4.
   * (GNOME#660371, GNOME#560482)
   */
  g_warn_if_fail (domain != 0);
  g_warn_if_fail (format != NULL);

  return g_error_new_steal (domain, code, g_strdup_vprintf (format, args), NULL);
}

/**
 * g_error_new:
 * @domain: error domain
 * @code: error code
 * @format: printf()-style format for error message
 * @...: parameters for message format
 *
 * Creates a new #GError with the given @domain and @code,
 * and a message formatted with @format.
 *
 * Returns: a new #GError
 */
GError*
g_error_new (GQuark       domain,
             gint         code,
             const gchar *format,
             ...)
{
  GError* error;
  va_list args;

  g_return_val_if_fail (format != NULL, NULL);
  g_return_val_if_fail (domain != 0, NULL);

  va_start (args, format);
  error = g_error_new_valist (domain, code, format, args);
  va_end (args);

  return error;
}

/**
 * g_error_new_literal:
 * @domain: error domain
 * @code: error code
 * @message: error message
 *
 * Creates a new #GError; unlike g_error_new(), @message is
 * not a printf()-style format string. Use this function if
 * @message contains text you don't have control over,
 * that could include printf() escape sequences.
 *
 * Returns: a new #GError
 **/
GError*
g_error_new_literal (GQuark         domain,
                     gint           code,
                     const gchar   *message)
{
  g_return_val_if_fail (message != NULL, NULL);
  g_return_val_if_fail (domain != 0, NULL);

  return g_error_new_steal (domain, code, g_strdup (message), NULL);
}

/**
 * g_error_free:
 * @error: a #GError
 *
 * Frees a #GError and associated resources.
 */
void
g_error_free (GError *error)
{
  gsize private_size;
  ErrorDomainInfo *info;
  guint8 *allocated;

  g_return_if_fail (error != NULL);

  g_rw_lock_reader_lock (&error_domain_global);
  info = error_domain_lookup (error->domain);
  if (info != NULL)
    {
      GErrorClearFunc clear = info->clear;

      private_size = info->private_size;
      g_rw_lock_reader_unlock (&error_domain_global);
      clear (error);
    }
  else
    {
      g_rw_lock_reader_unlock (&error_domain_global);
      private_size = 0;
    }

  g_free (error->message);
  allocated = ((guint8 *) error) - private_size;
  /* See comments in g_type_free_instance in gtype.c to see what this
   * magic is about.
   */
#ifdef ENABLE_VALGRIND
  if (private_size > 0 && RUNNING_ON_VALGRIND)
    {
      private_size += ALIGN_STRUCT (1);
      allocated -= ALIGN_STRUCT (1);
      *(gpointer *) (allocated + private_size + sizeof (GError)) = NULL;
      g_slice_free1 (private_size + sizeof (GError) + sizeof (gpointer), allocated);
      VALGRIND_FREELIKE_BLOCK (allocated + ALIGN_STRUCT (1), 0);
      VALGRIND_FREELIKE_BLOCK (error, 0);
    }
  else
#endif
  g_slice_free1 (private_size + sizeof (GError), allocated);
}

/**
 * g_error_copy:
 * @error: a #GError
 *
 * Makes a copy of @error.
 *
 * Returns: a new #GError
 */
GError*
g_error_copy (const GError *error)
{
  GError *copy;
  ErrorDomainInfo info;

  g_return_val_if_fail (error != NULL, NULL);
  /* See g_error_new_valist for why these don't return */
  g_warn_if_fail (error->domain != 0);
  g_warn_if_fail (error->message != NULL);

  copy = g_error_new_steal (error->domain,
                            error->code,
                            g_strdup (error->message),
                            &info);
  if (info.copy != NULL)
    info.copy (error, copy);

  return copy;
}

/**
 * g_error_matches:
 * @error: (nullable): a #GError
 * @domain: an error domain
 * @code: an error code
 *
 * Returns %TRUE if @error matches @domain and @code, %FALSE
 * otherwise. In particular, when @error is %NULL, %FALSE will
 * be returned.
 *
 * If @domain contains a `FAILED` (or otherwise generic) error code,
 * you should generally not check for it explicitly, but should
 * instead treat any not-explicitly-recognized error code as being
 * equivalent to the `FAILED` code. This way, if the domain is
 * extended in the future to provide a more specific error code for
 * a certain case, your code will still work.
 *
 * Returns: whether @error has @domain and @code
 */
gboolean
g_error_matches (const GError *error,
                 GQuark        domain,
                 gint          code)
{
  return error &&
    error->domain == domain &&
    error->code == code;
}

#define ERROR_OVERWRITTEN_WARNING "GError set over the top of a previous GError or uninitialized memory.\n" \
               "This indicates a bug in someone's code. You must ensure an error is NULL before it's set.\n" \
               "The overwriting error message was: %s"

/**
 * g_set_error:
 * @err: (out callee-allocates) (optional): a return location for a #GError
 * @domain: error domain
 * @code: error code
 * @format: printf()-style format
 * @...: args for @format
 *
 * Does nothing if @err is %NULL; if @err is non-%NULL, then *@err
 * must be %NULL. A new #GError is created and assigned to *@err.
 */
void
g_set_error (GError      **err,
             GQuark        domain,
             gint          code,
             const gchar  *format,
             ...)
{
  GError *new;

  va_list args;

  if (err == NULL)
    return;

  va_start (args, format);
  new = g_error_new_valist (domain, code, format, args);
  va_end (args);

  if (*err == NULL)
    *err = new;
  else
    {
      g_warning (ERROR_OVERWRITTEN_WARNING, new->message);
      g_error_free (new);
    }
}

/**
 * g_set_error_literal:
 * @err: (out callee-allocates) (optional): a return location for a #GError
 * @domain: error domain
 * @code: error code
 * @message: error message
 *
 * Does nothing if @err is %NULL; if @err is non-%NULL, then *@err
 * must be %NULL. A new #GError is created and assigned to *@err.
 * Unlike g_set_error(), @message is not a printf()-style format string.
 * Use this function if @message contains text you don't have control over,
 * that could include printf() escape sequences.
 *
 * Since: 2.18
 */
void
g_set_error_literal (GError      **err,
                     GQuark        domain,
                     gint          code,
                     const gchar  *message)
{
  if (err == NULL)
    return;

  if (*err == NULL)
    *err = g_error_new_literal (domain, code, message);
  else
    g_warning (ERROR_OVERWRITTEN_WARNING, message);
}

/**
 * g_propagate_error:
 * @dest: (out callee-allocates) (optional) (nullable): error return location
 * @src: (transfer full): error to move into the return location
 *
 * If @dest is %NULL, free @src; otherwise, moves @src into *@dest.
 * The error variable @dest points to must be %NULL.
 *
 * @src must be non-%NULL.
 *
 * Note that @src is no longer valid after this call. If you want
 * to keep using the same GError*, you need to set it to %NULL
 * after calling this function on it.
 */
void
g_propagate_error (GError **dest,
		   GError  *src)
{
  g_return_if_fail (src != NULL);
 
  if (dest == NULL)
    {
      g_error_free (src);
      return;
    }
  else
    {
      if (*dest != NULL)
        {
          g_warning (ERROR_OVERWRITTEN_WARNING, src->message);
          g_error_free (src);
        }
      else
        *dest = src;
    }
}

/**
 * g_clear_error:
 * @err: a #GError return location
 *
 * If @err or *@err is %NULL, does nothing. Otherwise,
 * calls g_error_free() on *@err and sets *@err to %NULL.
 */
void
g_clear_error (GError **err)
{
  if (err && *err)
    {
      g_error_free (*err);
      *err = NULL;
    }
}

G_GNUC_PRINTF(2, 0)
static void
g_error_add_prefix (gchar       **string,
                    const gchar  *format,
                    va_list       ap)
{
  gchar *oldstring;
  gchar *prefix;

  prefix = g_strdup_vprintf (format, ap);
  oldstring = *string;
  *string = g_strconcat (prefix, oldstring, NULL);
  g_free (oldstring);
  g_free (prefix);
}

/**
 * g_prefix_error:
 * @err: (inout) (optional) (nullable): a return location for a #GError
 * @format: printf()-style format string
 * @...: arguments to @format
 *
 * Formats a string according to @format and prefix it to an existing
 * error message. If @err is %NULL (ie: no error variable) then do
 * nothing.
 *
 * If *@err is %NULL (ie: an error variable is present but there is no
 * error condition) then also do nothing.
 *
 * Since: 2.16
 */
void
g_prefix_error (GError      **err,
                const gchar  *format,
                ...)
{
  if (err && *err)
    {
      va_list ap;

      va_start (ap, format);
      g_error_add_prefix (&(*err)->message, format, ap);
      va_end (ap);
    }
}

/**
 * g_prefix_error_literal:
 * @err: (allow-none): a return location for a #GError, or %NULL
 * @prefix: string to prefix @err with
 *
 * Prefixes @prefix to an existing error message. If @err or *@err is
 * %NULL (i.e.: no error variable) then do nothing.
 *
 * Since: 2.70
 */
void
g_prefix_error_literal (GError      **err,
                        const gchar  *prefix)
{
  if (err && *err)
    {
      gchar *oldstring;

      oldstring = (*err)->message;
      (*err)->message = g_strconcat (prefix, oldstring, NULL);
      g_free (oldstring);
    }
}

/**
 * g_propagate_prefixed_error:
 * @dest: error return location
 * @src: error to move into the return location
 * @format: printf()-style format string
 * @...: arguments to @format
 *
 * If @dest is %NULL, free @src; otherwise, moves @src into *@dest.
 * *@dest must be %NULL. After the move, add a prefix as with
 * g_prefix_error().
 *
 * Since: 2.16
 **/
void
g_propagate_prefixed_error (GError      **dest,
                            GError       *src,
                            const gchar  *format,
                            ...)
{
  g_propagate_error (dest, src);

  if (dest)
    {
      va_list ap;

      g_assert (*dest != NULL);
      va_start (ap, format);
      g_error_add_prefix (&(*dest)->message, format, ap);
      va_end (ap);
    }
}
