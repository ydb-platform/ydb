/* Open and close files for Bison.

   Copyright (C) 1984, 1986, 1989, 1992, 2000-2015, 2018-2021 Free
   Software Foundation, Inc.

   This file is part of Bison, the GNU Compiler Compiler.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

#include <config.h>
#include "system.h"

#include <configmake.h> /* PKGDATADIR */
#include <dirname.h>
#include <error.h>
#include <get-errno.h>
#include <gl_array_list.h>
#include <gl_xlist.h>
#include <quote.h>
#include <quotearg.h>
#include <relocatable.h> /* relocate2 */
#include <stdio-safer.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <xstrndup.h>

#include "complain.h"
#include "files.h"
#include "getargs.h"
#include "gram.h"

/* Initializing some values below (such SPEC_NAME_PREFIX to 'yy') is
   tempting, but don't do that: for the time being our handling of the
   %directive vs --option leaves precedence to the options by deciding
   that if a %directive sets a variable which is really set (i.e., not
   NULL), then the %directive is ignored.  As a result, %name-prefix,
   for instance, will not be honored.  */

char const *spec_outfile = NULL;       /* for -o. */
char const *spec_file_prefix = NULL;   /* for -b. */
location spec_file_prefix_loc = EMPTY_LOCATION_INIT;
char const *spec_name_prefix = NULL;   /* for -p. */
location spec_name_prefix_loc = EMPTY_LOCATION_INIT;
char *spec_verbose_file = NULL;  /* for --verbose. */
char *spec_graph_file = NULL;    /* for -g. */
char *spec_xml_file = NULL;      /* for -x. */
char *spec_header_file = NULL;  /* for --defines. */
char *spec_mapped_header_file = NULL;
char *parser_file_name;

/* All computed output file names.  */
typedef struct generated_file
{
  /** File name.  */
  char *name;
  /** Whether is a generated source file (e.g., *.c, *.java...), as
      opposed to the report file (e.g., *.output).  When late errors
      are detected, generated source files are removed.  */
  bool is_source;
} generated_file;
static generated_file *generated_files = NULL;
static int generated_files_size = 0;

uniqstr grammar_file = NULL;

/* If --output=dir/foo.c was specified,
   DIR_PREFIX gis 'dir/' and ALL_BUT_EXT and ALL_BUT_TAB_EXT are 'dir/foo'.

   If --output=dir/foo.tab.c was specified, DIR_PREFIX is 'dir/',
   ALL_BUT_EXT is 'dir/foo.tab', and ALL_BUT_TAB_EXT is 'dir/foo'.

   If --output was not specified but --file-prefix=dir/foo was specified,
   ALL_BUT_EXT = 'foo.tab' and ALL_BUT_TAB_EXT = 'foo'.

   If neither --output nor --file was specified but the input grammar
   is name dir/foo.y, ALL_BUT_EXT and ALL_BUT_TAB_EXT are 'foo'.

   If neither --output nor --file was specified, DIR_PREFIX is the
   empty string (meaning the current directory); otherwise it is
   'dir/'.  */

char *all_but_ext;
static char *all_but_tab_ext;
char *dir_prefix;
char *mapped_dir_prefix;

/* C source file extension (the parser source).  */
static char *src_extension = NULL;
/* Header file extension (if option '`-d'' is specified).  */
static char *header_extension = NULL;

struct prefix_map
{
  char *oldprefix;
  char *newprefix;
};

static gl_list_t prefix_maps = NULL;

/*-----------------------------------------------------------------.
| Return a newly allocated string composed of the concatenation of |
| STR1, and STR2.                                                  |
`-----------------------------------------------------------------*/

static char *
concat2 (char const *str1, char const *str2)
{
  size_t len = strlen (str1) + strlen (str2);
  char *res = xmalloc (len + 1);
  char *cp;
  cp = stpcpy (res, str1);
  cp = stpcpy (cp, str2);
  return res;
}

/*-----------------------------------------------------------------.
| Try to open file NAME with mode MODE, and print an error message |
| if fails.                                                        |
`-----------------------------------------------------------------*/

FILE *
xfopen (const char *name, const char *mode)
{
  FILE *res = fopen_safer (name, mode);
  if (!res)
    error (EXIT_FAILURE, get_errno (),
           _("%s: cannot open"), quotearg_colon (name));

  return res;
}

/*-------------------------------------------------------------.
| Try to close file PTR, and print an error message if fails.  |
`-------------------------------------------------------------*/

void
xfclose (FILE *ptr)
{
  if (ptr == NULL)
    return;

  if (ferror (ptr))
    error (EXIT_FAILURE, 0, _("input/output error"));

  if (fclose (ptr) != 0)
    error (EXIT_FAILURE, get_errno (), _("cannot close file"));
}


FILE *
xfdopen (int fd, char const *mode)
{
  FILE *res = fdopen (fd, mode);
  if (! res)
    error (EXIT_FAILURE, get_errno (),
           /* On a separate line to please the "unmarked_diagnostics"
              syntax-check. */
           "fdopen");
  return res;
}

/*  Given an input file path, returns a dynamically allocated string that
    contains the path with the file prefix mapping rules applied, or NULL if
    the input was NULL. */
char *
map_file_name (char const *filename)
{
  if (!filename)
    return NULL;

  struct prefix_map const *p = NULL;
  if (prefix_maps)
    {
      void const *ptr;
      gl_list_iterator_t iter = gl_list_iterator (prefix_maps);
      while (gl_list_iterator_next (&iter, &ptr, NULL))
        {
          p = ptr;
          if (strncmp (p->oldprefix, filename, strlen (p->oldprefix)) == 0)
            break;
          p = NULL;
        }
      gl_list_iterator_free (&iter);
    }

  if (!p)
    return xstrdup (filename);

  size_t oldprefix_len = strlen (p->oldprefix);
  size_t newprefix_len = strlen (p->newprefix);
  char *s = xmalloc (newprefix_len + strlen (filename) - oldprefix_len + 1);

  char *end = stpcpy (s, p->newprefix);
  stpcpy (end, filename + oldprefix_len);

  return s;
}

static void
prefix_map_free (struct prefix_map *p)
{
  free (p->oldprefix);
  free (p->newprefix);
  free (p);
}

/*  Adds a new file prefix mapping. If a file path starts with oldprefix, it
    will be replaced with newprefix */
void
add_prefix_map (char const *oldprefix, char const *newprefix)
{
  if (!prefix_maps)
    prefix_maps = gl_list_create_empty (GL_ARRAY_LIST,
                                        /* equals */ NULL,
                                        /* hashcode */ NULL,
                                        (gl_listelement_dispose_fn) prefix_map_free,
                                        true);

  struct prefix_map *p = xmalloc (sizeof (*p));
  p->oldprefix = xstrdup (oldprefix);
  p->newprefix = xstrdup (newprefix);

  gl_list_add_last (prefix_maps, p);
}

/*------------------------------------------------------------------.
| Compute ALL_BUT_EXT, ALL_BUT_TAB_EXT and output files extensions. |
`------------------------------------------------------------------*/

/* Compute extensions from the grammar file extension.  */
static void
compute_exts_from_gf (const char *ext)
{
  if (STREQ (ext, ".y"))
    {
      src_extension = xstrdup (language->src_extension);
      header_extension = xstrdup (language->header_extension);
    }
  else
    {
      src_extension = xstrdup (ext);
      header_extension = xstrdup (ext);
      tr (src_extension, 'y', 'c');
      tr (src_extension, 'Y', 'C');
      tr (header_extension, 'y', 'h');
      tr (header_extension, 'Y', 'H');
    }
}

/* Compute extensions from the given c source file extension.  */
static void
compute_exts_from_src (const char *ext)
{
  /* We use this function when the user specifies `-o' or `--output',
     so the extensions must be computed unconditionally from the file name
     given by this option.  */
  src_extension = xstrdup (ext);
  header_extension = xstrdup (ext);
  tr (header_extension, 'c', 'h');
  tr (header_extension, 'C', 'H');
}


/* Decompose FILE_NAME in four parts: *BASE, *TAB, and *EXT, the fourth
   part, (the directory) is ranging from FILE_NAME to the char before
   *BASE, so we don't need an additional parameter.

   *EXT points to the last period in the basename, or NULL if none.

   If there is no *EXT, *TAB is NULL.  Otherwise, *TAB points to
   '.tab' or '_tab' if present right before *EXT, or is NULL. *TAB
   cannot be equal to *BASE.

   None are allocated, they are simply pointers to parts of FILE_NAME.
   Examples:

   '/tmp/foo.tab.c' -> *BASE = 'foo.tab.c', *TAB = '.tab.c', *EXT =
   '.c'

   'foo.c' -> *BASE = 'foo.c', *TAB = NULL, *EXT = '.c'

   'tab.c' -> *BASE = 'tab.c', *TAB = NULL, *EXT = '.c'

   '.tab.c' -> *BASE = '.tab.c', *TAB = NULL, *EXT = '.c'

   'foo.tab' -> *BASE = 'foo.tab', *TAB = NULL, *EXT = '.tab'

   'foo_tab' -> *BASE = 'foo_tab', *TAB = NULL, *EXT = NULL

   'foo' -> *BASE = 'foo', *TAB = NULL, *EXT = NULL.  */

static void
file_name_split (const char *file_name,
                 const char **base, const char **tab, const char **ext)
{
  *base = last_component (file_name);

  /* Look for the extension, i.e., look for the last dot. */
  *ext = strrchr (*base, '.');
  *tab = NULL;

  /* If there is an extension, check if there is a '.tab' part right
     before.  */
  if (*ext)
    {
      size_t baselen = *ext - *base;
      size_t dottablen = sizeof (TAB_EXT) - 1;
      if (dottablen < baselen
          && STRPREFIX_LIT (TAB_EXT, *ext - dottablen))
        *tab = *ext - dottablen;
    }
}

/* Compute ALL_BUT_EXT and ALL_BUT_TAB_EXT from SPEC_OUTFILE or
   GRAMMAR_FILE.

   The precise -o name will be used for FTABLE.  For other output
   files, remove the ".c" or ".tab.c" suffix.  */

static void
compute_file_name_parts (void)
{
  if (spec_outfile)
    {
      const char *base, *tab, *ext;
      file_name_split (spec_outfile, &base, &tab, &ext);
      dir_prefix = xstrndup (spec_outfile, base - spec_outfile);

      /* ALL_BUT_EXT goes up the EXT, excluding it. */
      all_but_ext =
        xstrndup (spec_outfile,
                  (strlen (spec_outfile) - (ext ? strlen (ext) : 0)));

      /* ALL_BUT_TAB_EXT goes up to TAB, excluding it.  */
      all_but_tab_ext =
        xstrndup (spec_outfile,
                  (strlen (spec_outfile)
                   - (tab ? strlen (tab) : (ext ? strlen (ext) : 0))));

      if (ext)
        compute_exts_from_src (ext);
    }
  else
    {
      const char *base, *tab, *ext;
      file_name_split (grammar_file, &base, &tab, &ext);

      if (spec_file_prefix)
        {
          /* If --file-prefix=foo was specified, ALL_BUT_TAB_EXT = 'foo'.  */
          dir_prefix =
            xstrndup (spec_file_prefix,
                      last_component (spec_file_prefix) - spec_file_prefix);
          all_but_tab_ext = xstrdup (spec_file_prefix);
        }
      else if (! location_empty (yacc_loc))
        {
          /* If --yacc, then the output is 'y.tab.c'.  */
          dir_prefix = xstrdup ("");
          all_but_tab_ext = xstrdup ("y");
        }
      else
        {
          /* Otherwise, ALL_BUT_TAB_EXT is computed from the input
             grammar: 'foo/bar.yy' => 'bar'.  */
          dir_prefix = xstrdup ("");
          all_but_tab_ext =
            xstrndup (base, (strlen (base) - (ext ? strlen (ext) : 0)));
        }

      if (language->add_tab)
        all_but_ext = concat2 (all_but_tab_ext, TAB_EXT);
      else
        all_but_ext = xstrdup (all_but_tab_ext);

      /* Compute the extensions from the grammar file name.  */
      if (ext && location_empty (yacc_loc))
        compute_exts_from_gf (ext);
    }
}


/* Compute the output file names.  Warn if we detect conflicting
   outputs to the same file.  */

void
compute_output_file_names (void)
{
  compute_file_name_parts ();

  /* If not yet done. */
  if (!src_extension)
    src_extension = xstrdup (".c");
  if (!header_extension)
    header_extension = xstrdup (".h");

  parser_file_name =
    (spec_outfile
     ? xstrdup (spec_outfile)
     : concat2 (all_but_ext, src_extension));

  if (defines_flag)
    {
      if (! spec_header_file)
        spec_header_file = concat2 (all_but_ext, header_extension);
    }

  if (graph_flag)
    {
      if (! spec_graph_file)
        spec_graph_file = concat2 (all_but_tab_ext,
                                   304 <= required_version ? ".gv" : ".dot");
      output_file_name_check (&spec_graph_file, false);
    }

  if (xml_flag)
    {
      if (! spec_xml_file)
        spec_xml_file = concat2 (all_but_tab_ext, ".xml");
      output_file_name_check (&spec_xml_file, false);
    }

  if (report_flag)
    {
      if (!spec_verbose_file)
        spec_verbose_file = concat2 (all_but_tab_ext, OUTPUT_EXT);
      output_file_name_check (&spec_verbose_file, false);
    }

  spec_mapped_header_file = map_file_name (spec_header_file);
  mapped_dir_prefix = map_file_name (dir_prefix);

  free (all_but_tab_ext);
  free (src_extension);
  free (header_extension);
}

void
output_file_name_check (char **file_name, bool source)
{
  bool conflict = false;
  if (STREQ (*file_name, grammar_file))
    {
      complain (NULL, complaint, _("refusing to overwrite the input file %s"),
                quote (*file_name));
      conflict = true;
    }
  else
    for (int i = 0; i < generated_files_size; i++)
      if (STREQ (generated_files[i].name, *file_name))
        {
          complain (NULL, Wother, _("conflicting outputs to file %s"),
                    quote (generated_files[i].name));
          conflict = true;
        }
  if (conflict)
    {
      free (*file_name);
      *file_name = strdup ("/dev/null");
    }
  else
    {
      generated_files = xnrealloc (generated_files, ++generated_files_size,
                                   sizeof *generated_files);
      generated_files[generated_files_size-1].name = xstrdup (*file_name);
      generated_files[generated_files_size-1].is_source = source;
    }
}

void
unlink_generated_sources (void)
{
  for (int i = 0; i < generated_files_size; i++)
    if (generated_files[i].is_source)
      /* Ignore errors.  The file might not even exist.  */
      unlink (generated_files[i].name);
}

/* Memory allocated by relocate2, to free.  */
static char *relocate_buffer = NULL;

char const *
pkgdatadir (void)
{
  if (relocate_buffer)
    return relocate_buffer;
  else
    {
      char const *cp = getenv ("BISON_PKGDATADIR");
      return cp ? cp : relocate2 (PKGDATADIR, &relocate_buffer);
    }
}

char const *
m4path (void)
{
  char const *m4 = getenv ("M4");
  if (m4)
    return m4;

  /* We don't use relocate2() to store the temporary buffer and re-use
     it, because m4path() is only called once.  */
  char const *m4_relocated = relocate (M4);
  struct stat buf;
  if (stat (m4_relocated, &buf) == 0)
    return m4_relocated;

  return M4;
}

void
output_file_names_free (void)
{
  free (all_but_ext);
  free (spec_verbose_file);
  free (spec_graph_file);
  free (spec_xml_file);
  free (spec_header_file);
  free (spec_mapped_header_file);
  free (parser_file_name);
  free (dir_prefix);
  free (mapped_dir_prefix);
  for (int i = 0; i < generated_files_size; i++)
    free (generated_files[i].name);
  free (generated_files);
  free (relocate_buffer);

  if (prefix_maps)
    gl_list_free (prefix_maps);
}
