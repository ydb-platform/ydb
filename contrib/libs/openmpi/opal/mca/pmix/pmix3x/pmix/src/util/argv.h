/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire. All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC. All rights reserved.
 * Copyright (c) 2015-2016 Intel, Inc.  All rights reserved.
 *
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Generic routines for "argv"-like handling.  Helpful for creating
 * arrays of strings, especially when creating command lines.
 */

#ifndef PMIX_ARGV_H
#define PMIX_ARGV_H

#include <src/include/pmix_config.h>


#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include <pmix_common.h>

BEGIN_C_DECLS

  /**
   * Append a string (by value) to an new or existing NULL-terminated
   * argv array.
   *
   * @param argc Pointer to the length of the argv array.  Must not be
   * NULL.
   * @param argv Pointer to an argv array.
   * @param str Pointer to the string to append.
   *
   * @retval PMIX_SUCCESS On success
   * @retval PMIX_ERROR On failure
   *
   * This function adds a string to an argv array of strings by value;
   * it is permissable to pass a string on the stack as the str
   * argument to this function.
   *
   * To add the first entry to an argv array, call this function with
   * (*argv == NULL).  This function will allocate an array of length
   * 2; the first entry will point to a copy of the string passed in
   * arg, the second entry will be set to NULL.
   *
   * If (*argv != NULL), it will be realloc'ed to be 1 (char*) larger,
   * and the next-to-last entry will point to a copy of the string
   * passed in arg.  The last entry will be set to NULL.
   *
   * Just to reinforce what was stated above: the string is copied by
   * value into the argv array; there is no need to keep the original
   * string (i.e., the arg parameter) after invoking this function.
   */
PMIX_EXPORT pmix_status_t pmix_argv_append(int *argc, char ***argv, const char *arg) __pmix_attribute_nonnull__(1) __pmix_attribute_nonnull__(3);

  /**
   * Append to an argv-style array, but ignore the size of the array.
   *
   * @param argv Pointer to an argv array.
   * @param str Pointer to the string to append.
   *
   * @retval PMIX_SUCCESS On success
   * @retval PMIX_ERROR On failure
   *
   * This function is identical to the pmix_argv_append() function
   * except that it does not take a pointer to an argc (integer
   * representing the size of the array).  This is handy for
   * argv-style arrays that do not have integers that are actively
   * maintaing their sizes.
   */
PMIX_EXPORT pmix_status_t pmix_argv_append_nosize(char ***argv, const char *arg);

/**
 * Insert the provided arg at the beginning of the array
 *
 * @param argv Pointer to an argv array
 * @param str Pointer to the string to prepend
 *
 * @retval PMIX_SUCCESS On success
 * @retval PMIX_ERROR On failure
 */
PMIX_EXPORT pmix_status_t pmix_argv_prepend_nosize(char ***argv, const char *arg);

/**
 * Append to an argv-style array, but only if the provided argument
 * doesn't already exist somewhere in the array. Ignore the size of the array.
 *
 * @param argv Pointer to an argv array.
 * @param str Pointer to the string to append.
 * @param bool Whether or not to overwrite a matching value if found
 *
 * @retval PMIX_SUCCESS On success
 * @retval PMIX_ERROR On failure
 *
 * This function is identical to the pmix_argv_append_nosize() function
 * except that it only appends the provided argument if it does not already
 * exist in the provided array, or overwrites it if it is.
 */
PMIX_EXPORT pmix_status_t pmix_argv_append_unique_nosize(char ***argv, const char *arg, bool overwrite);

/**
   * Free a NULL-terminated argv array.
   *
   * @param argv Argv array to free.
   *
   * This function frees an argv array and all of the strings that it
   * contains.  Since the argv parameter is passed by value, it is not
   * set to NULL in the caller's scope upon return.
   *
   * It is safe to invoke this function with a NULL pointer.  It is
   * not safe to invoke this function with a non-NULL-terminated argv
   * array.
   */
PMIX_EXPORT void pmix_argv_free(char **argv);

  /**
   * Split a string into a NULL-terminated argv array. Do not include empty
   * strings in result array.
   *
   * @param src_string Input string.
   * @param delimiter Delimiter character.
   *
   * @retval argv pointer to new argv array on success
   * @retval NULL on error
   *
   * All strings are insertted into the argv array by value; the
   * newly-allocated array makes no references to the src_string
   * argument (i.e., it can be freed after calling this function
   * without invalidating the output argv).
   */
PMIX_EXPORT char **pmix_argv_split(const char *src_string, int delimiter) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;

  /**
   * Split a string into a NULL-terminated argv array. Include empty
   * strings in result array.
   *
   * @param src_string Input string.
   * @param delimiter Delimiter character.
   *
   * @retval argv pointer to new argv array on success
   * @retval NULL on error
   *
   * All strings are insertted into the argv array by value; the
   * newly-allocated array makes no references to the src_string
   * argument (i.e., it can be freed after calling this function
   * without invalidating the output argv).
   */
PMIX_EXPORT char **pmix_argv_split_with_empty(const char *src_string, int delimiter) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;

  /**
   * Return the length of a NULL-terminated argv array.
   *
   * @param argv The input argv array.
   *
   * @retval 0 If NULL is passed as argv.
   * @retval count Number of entries in the argv array.
   *
   * The argv array must be NULL-terminated.
   */
PMIX_EXPORT int pmix_argv_count(char **argv);

  /**
   * Join all the elements of an argv array into a single
   * newly-allocated string.
   *
   * @param argv The input argv array.
   * @param delimiter Delimiter character placed between each argv string.
   *
   * @retval new_string Output string on success.
   * @retval NULL On failure.
   *
   * Similar to the Perl join function, this function takes an input
   * argv and joins them into into a single string separated by the
   * delimiter character.
   *
   * It is the callers responsibility to free the returned string.
   */
PMIX_EXPORT char *pmix_argv_join(char **argv, int delimiter) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;

PMIX_EXPORT char *pmix_argv_join_range(char **argv, size_t start, size_t end, int delimiter) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;

  /**
   * Return the number of bytes consumed by an argv array.
   *
   * @param argv The input argv array.
   *
   * Count the number of bytes consumed by a NULL-terminated argv
   * array.  This includes the number of bytes used by each of the
   * strings as well as the pointers used in the argv array.
   */
PMIX_EXPORT size_t pmix_argv_len(char **argv);

  /**
   * Copy a NULL-terminated argv array.
   *
   * @param argv The input argv array.
   *
   * @retval argv Copied argv array on success.
   * @retval NULL On failure.
   *
   * Copy an argv array, including copying all off its strings.
   * Specifically, the output argv will be an array of the same length
   * as the input argv, and strcmp(argv_in[i], argv_out[i]) will be 0.
   */
PMIX_EXPORT char **pmix_argv_copy(char **argv) __pmix_attribute_malloc__ __pmix_attribute_warn_unused_result__;

    /**
     * Delete one or more tokens from the middle of an argv.
     *
     * @param argv The argv to delete from
     * @param start The index of the first token to delete
     * @param num_to_delete How many tokens to delete
     *
     * @retval PMIX_SUCCESS Always
     *
     * Delete some tokens from within an existing argv.  The start
     * parameter specifies the first token to delete, and will delete
     * (num_to_delete-1) tokens following it.  argv will be realloc()ed
     * to *argc - num_deleted size.
     *
     * If start is beyond the end of the argv array, this function is
     * a no-op.
     *
     * If num_to_delete runs beyond the end of the argv array, this
     * function will delete all tokens starting with start to the end
     * of the array.
     *
     * All deleted items in the argv array will have their contents
     * free()ed (it is assumed that the argv "owns" the memory that
     * the pointer points to).
     */
PMIX_EXPORT pmix_status_t pmix_argv_delete(int *argc, char ***argv,
                                              int start, int num_to_delete);

    /**
     * Insert one argv array into the middle of another
     *
     * @param target The argv to insert tokens into
     * @param start Index where the first token will be placed in target
     * @param source The argv to copy tokens from
     *
     * @retval PMIX_SUCCESS upon success
     * @retval PMIX_BAD_PARAM if any parameters are non-sensical
     *
     * This function takes one arg and inserts it in the middle of
     * another.  The first token in source will be insertted at index
     * start in the target argv; all other tokens will follow it.
     * Similar to pmix_argv_append(), the target may be realloc()'ed
     * to accomodate the new storage requirements.
     *
     * The source array is left unaffected -- its contents are copied
     * by value over to the target array (i.e., the strings that
     * source points to are strdup'ed into the new locations in
     * target).
     */
PMIX_EXPORT pmix_status_t pmix_argv_insert(char ***target, int start, char **source);

/**
 * Insert one argv element in front of a specific position in an array
 *
 * @param target The argv to insert tokens into
 * @param location Index where the token will be placed in target
 * @param source The token to be inserted
 *
 * @retval PMIX_SUCCESS upon success
 * @retval PMIX_BAD_PARAM if any parameters are non-sensical
 *
 * This function takes one arg and inserts it in the middle of
 * another.  The token will be inserted at the specified index
 * in the target argv; all other tokens will be shifted down.
 * Similar to pmix_argv_append(), the target may be realloc()'ed
 * to accomodate the new storage requirements.
 *
 * The source token is left unaffected -- its contents are copied
 * by value over to the target array (i.e., the string that
 * source points to is strdup'ed into the new location in
 * target).
 */
PMIX_EXPORT pmix_status_t pmix_argv_insert_element(char ***target, int location, char *source);

END_C_DECLS

#endif /* PMIX_ARGV_H */
