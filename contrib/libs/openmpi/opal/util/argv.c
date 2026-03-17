/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire. All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC. All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include <stdlib.h>
#include <string.h>

#include "opal/util/argv.h"
#include "opal/constants.h"

#define ARGSIZE 128


/*
 * Append a string to the end of a new or existing argv array.
 */
int opal_argv_append(int *argc, char ***argv, const char *arg)
{
    int rc;

    /* add the new element */
    if (OPAL_SUCCESS != (rc = opal_argv_append_nosize(argv, arg))) {
        return rc;
    }

    *argc = opal_argv_count(*argv);

    return OPAL_SUCCESS;
}

int opal_argv_append_nosize(char ***argv, const char *arg)
{
    int argc;

  /* Create new argv. */

  if (NULL == *argv) {
    *argv = (char**) malloc(2 * sizeof(char *));
    if (NULL == *argv) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    argc = 0;
    (*argv)[0] = NULL;
    (*argv)[1] = NULL;
  }

  /* Extend existing argv. */
  else {
        /* count how many entries currently exist */
        argc = opal_argv_count(*argv);

        *argv = (char**) realloc(*argv, (argc + 2) * sizeof(char *));
        if (NULL == *argv) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
    }

    /* Set the newest element to point to a copy of the arg string */

    (*argv)[argc] = strdup(arg);
    if (NULL == (*argv)[argc]) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    argc = argc + 1;
    (*argv)[argc] = NULL;

    return OPAL_SUCCESS;
}

int opal_argv_prepend_nosize(char ***argv, const char *arg)
{
    int argc;
    int i;

    /* Create new argv. */

    if (NULL == *argv) {
        *argv = (char**) malloc(2 * sizeof(char *));
        if (NULL == *argv) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        (*argv)[0] = strdup(arg);
        (*argv)[1] = NULL;
    } else {
        /* count how many entries currently exist */
        argc = opal_argv_count(*argv);

        *argv = (char**) realloc(*argv, (argc + 2) * sizeof(char *));
        if (NULL == *argv) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        (*argv)[argc+1] = NULL;

        /* shift all existing elements down 1 */
        for (i=argc; 0 < i; i--) {
            (*argv)[i] = (*argv)[i-1];
        }
        (*argv)[0] = strdup(arg);
    }

    return OPAL_SUCCESS;
}

int opal_argv_append_unique_nosize(char ***argv, const char *arg, bool overwrite)
{
    int i;

    /* if the provided array is NULL, then the arg cannot be present,
     * so just go ahead and append
     */
    if (NULL == *argv) {
        return opal_argv_append_nosize(argv, arg);
    }

    /* see if this arg is already present in the array */
    for (i=0; NULL != (*argv)[i]; i++) {
        if (0 == strcmp(arg, (*argv)[i])) {
            /* already exists - are we authorized to overwrite? */
            if (overwrite) {
                free((*argv)[i]);
                (*argv)[i] = strdup(arg);
            }
            return OPAL_SUCCESS;
        }
    }

    /* we get here if the arg is not in the array - so add it */
    return opal_argv_append_nosize(argv, arg);
}

/*
 * Free a NULL-terminated argv array.
 */
void opal_argv_free(char **argv)
{
  char **p;

  if (NULL == argv)
    return;

  for (p = argv; NULL != *p; ++p) {
    free(*p);
  }

  free(argv);
}


/*
 * Split a string into a NULL-terminated argv array.
 */
static char **opal_argv_split_inter(const char *src_string, int delimiter,
        int include_empty)
{
  char arg[ARGSIZE];
  char **argv = NULL;
  const char *p;
  char *argtemp;
  int argc = 0;
  size_t arglen;

  while (src_string && *src_string) {
    p = src_string;
    arglen = 0;

    while (('\0' != *p) && (*p != delimiter)) {
      ++p;
      ++arglen;
    }

    /* zero length argument, skip */

    if (src_string == p) {
      if (include_empty) {
        arg[0] = '\0';
        if (OPAL_SUCCESS != opal_argv_append(&argc, &argv, arg))
          return NULL;
      }
    }

    /* tail argument, add straight from the original string */

    else if ('\0' == *p) {
      if (OPAL_SUCCESS != opal_argv_append(&argc, &argv, src_string))
	return NULL;
      src_string = p;
      continue;
    }

    /* long argument, malloc buffer, copy and add */

    else if (arglen > (ARGSIZE - 1)) {
        argtemp = (char*) malloc(arglen + 1);
      if (NULL == argtemp)
	return NULL;

      strncpy(argtemp, src_string, arglen);
      argtemp[arglen] = '\0';

      if (OPAL_SUCCESS != opal_argv_append(&argc, &argv, argtemp)) {
	free(argtemp);
	return NULL;
      }

      free(argtemp);
    }

    /* short argument, copy to buffer and add */

    else {
      strncpy(arg, src_string, arglen);
      arg[arglen] = '\0';

      if (OPAL_SUCCESS != opal_argv_append(&argc, &argv, arg))
	return NULL;
    }

    src_string = p + 1;
  }

  /* All done */

  return argv;
}

char **opal_argv_split(const char *src_string, int delimiter)
{
    return opal_argv_split_inter(src_string, delimiter, 0);
}

char **opal_argv_split_with_empty(const char *src_string, int delimiter)
{
    return opal_argv_split_inter(src_string, delimiter, 1);
}

/*
 * Return the length of a NULL-terminated argv array.
 */
int opal_argv_count(char **argv)
{
  char **p;
  int i;

  if (NULL == argv)
    return 0;

  for (i = 0, p = argv; *p; i++, p++)
    continue;

  return i;
}


/*
 * Join all the elements of an argv array into a single
 * newly-allocated string.
 */
char *opal_argv_join(char **argv, int delimiter)
{
  char **p;
  char *pp;
  char *str;
  size_t str_len = 0;
  size_t i;

  /* Bozo case */

  if (NULL == argv || NULL == argv[0]) {
      return strdup("");
  }

  /* Find the total string length in argv including delimiters.  The
     last delimiter is replaced by the NULL character. */

  for (p = argv; *p; ++p) {
    str_len += strlen(*p) + 1;
  }

  /* Allocate the string. */

  if (NULL == (str = (char*) malloc(str_len)))
    return NULL;

  /* Loop filling in the string. */

  str[--str_len] = '\0';
  p = argv;
  pp = *p;

  for (i = 0; i < str_len; ++i) {
    if ('\0' == *pp) {

      /* End of a string, fill in a delimiter and go to the next
         string. */

      str[i] = (char) delimiter;
      ++p;
      pp = *p;
    } else {
      str[i] = *pp++;
    }
  }

  /* All done */

  return str;
}


/*
 * Join all the elements of an argv array from within a
 * specified range into a single newly-allocated string.
 */
char *opal_argv_join_range(char **argv, size_t start, size_t end, int delimiter)
{
    char **p;
    char *pp;
    char *str;
    size_t str_len = 0;
    size_t i;

    /* Bozo case */

    if (NULL == argv || NULL == argv[0] || (int)start > opal_argv_count(argv)) {
        return strdup("");
    }

    /* Find the total string length in argv including delimiters.  The
     last delimiter is replaced by the NULL character. */

    for (p = &argv[start], i=start; *p && i < end; ++p, ++i) {
        str_len += strlen(*p) + 1;
    }

    /* Allocate the string. */

    if (NULL == (str = (char*) malloc(str_len)))
        return NULL;

    /* Loop filling in the string. */

    str[--str_len] = '\0';
    p = &argv[start];
    pp = *p;

    for (i = 0; i < str_len; ++i) {
        if ('\0' == *pp) {

            /* End of a string, fill in a delimiter and go to the next
             string. */

            str[i] = (char) delimiter;
            ++p;
            pp = *p;
        } else {
            str[i] = *pp++;
        }
    }

    /* All done */

    return str;
}


/*
 * Return the number of bytes consumed by an argv array.
 */
size_t opal_argv_len(char **argv)
{
  char **p;
  size_t length;

  if (NULL == argv)
    return (size_t) 0;

  length = sizeof(char *);

  for (p = argv; *p; ++p) {
    length += strlen(*p) + 1 + sizeof(char *);
  }

  return length;
}


/*
 * Copy a NULL-terminated argv array.
 */
char **opal_argv_copy(char **argv)
{
  char **dupv = NULL;
  int dupc = 0;

  if (NULL == argv)
    return NULL;

  /* create an "empty" list, so that we return something valid if we
     were passed a valid list with no contained elements */
  dupv = (char**) malloc(sizeof(char*));
  dupv[0] = NULL;

  while (NULL != *argv) {
    if (OPAL_SUCCESS != opal_argv_append(&dupc, &dupv, *argv)) {
      opal_argv_free(dupv);
      return NULL;
    }

    ++argv;
  }

  /* All done */

  return dupv;
}


int opal_argv_delete(int *argc, char ***argv, int start, int num_to_delete)
{
    int i;
    int count;
    int suffix_count;
    char **tmp;

    /* Check for the bozo cases */
    if (NULL == argv || NULL == *argv || 0 == num_to_delete) {
        return OPAL_SUCCESS;
    }
    count = opal_argv_count(*argv);
    if (start > count) {
        return OPAL_SUCCESS;
    } else if (start < 0 || num_to_delete < 0) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* Ok, we have some tokens to delete.  Calculate the new length of
       the argv array. */

    suffix_count = count - (start + num_to_delete);
    if (suffix_count < 0) {
        suffix_count = 0;
    }

    /* Free all items that are being deleted */

    for (i = start; i < count && i < start + num_to_delete; ++i) {
        free((*argv)[i]);
    }

    /* Copy the suffix over the deleted items */

    for (i = start; i < start + suffix_count; ++i) {
        (*argv)[i] = (*argv)[i + num_to_delete];
    }

    /* Add the trailing NULL */

    (*argv)[i] = NULL;

    /* adjust the argv array */
    tmp = (char**)realloc(*argv, sizeof(char*) * (i + 1));
    if (NULL != tmp) *argv = tmp;

    /* adjust the argc */
    (*argc) -= num_to_delete;

    return OPAL_SUCCESS;
}


int opal_argv_insert(char ***target, int start, char **source)
{
    int i, source_count, target_count;
    int suffix_count;

    /* Check for the bozo cases */

    if (NULL == target || NULL == *target || start < 0) {
        return OPAL_ERR_BAD_PARAM;
    } else if (NULL == source) {
        return OPAL_SUCCESS;
    }

    /* Easy case: appending to the end */

    target_count = opal_argv_count(*target);
    source_count = opal_argv_count(source);
    if (start > target_count) {
        for (i = 0; i < source_count; ++i) {
            opal_argv_append(&target_count, target, source[i]);
        }
    }

    /* Harder: insertting into the middle */

    else {

        /* Alloc out new space */

        *target = (char**) realloc(*target,
                                   sizeof(char *) * (target_count + source_count + 1));

        /* Move suffix items down to the end */

        suffix_count = target_count - start;
        for (i = suffix_count - 1; i >= 0; --i) {
            (*target)[start + source_count + i] =
                (*target)[start + i];
        }
        (*target)[start + suffix_count + source_count] = NULL;

        /* Strdup in the source argv */

        for (i = start; i < start + source_count; ++i) {
            (*target)[i] = strdup(source[i - start]);
        }
    }

    /* All done */

    return OPAL_SUCCESS;
}

int opal_argv_insert_element(char ***target, int location, char *source)
{
    int i, target_count;
    int suffix_count;

    /* Check for the bozo cases */

    if (NULL == target || NULL == *target || location < 0) {
        return OPAL_ERR_BAD_PARAM;
    } else if (NULL == source) {
        return OPAL_SUCCESS;
    }

    /* Easy case: appending to the end */
    target_count = opal_argv_count(*target);
    if (location > target_count) {
        opal_argv_append(&target_count, target, source);
        return OPAL_SUCCESS;
    }

    /* Alloc out new space */
    *target = (char**) realloc(*target,
                               sizeof(char*) * (target_count + 2));

    /* Move suffix items down to the end */
    suffix_count = target_count - location;
    for (i = suffix_count - 1; i >= 0; --i) {
        (*target)[location + 1 + i] =
        (*target)[location + i];
    }
    (*target)[location + suffix_count + 1] = NULL;

    /* Strdup in the source */
    (*target)[location] = strdup(source);

    /* All done */
    return OPAL_SUCCESS;
}
