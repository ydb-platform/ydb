/* Provide relocatable packages.
   Copyright (C) 2003, 2005, 2008-2019 Free Software Foundation, Inc.
   Written by Bruno Haible <bruno@clisp.org>, 2003.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

#ifndef _RELOCATABLE_H
#define _RELOCATABLE_H

#ifdef __cplusplus
extern "C" {
#endif


/* This can be enabled through the configure --enable-relocatable option.  */
#if ENABLE_RELOCATABLE

/* When building a DLL, we must export some functions.  Note that because
   this is a private .h file, we don't need to use __declspec(dllimport)
   in any case.  */
#if HAVE_VISIBILITY && BUILDING_DLL
# define RELOCATABLE_DLL_EXPORTED __attribute__((__visibility__("default")))
#elif defined _MSC_VER && BUILDING_DLL
# define RELOCATABLE_DLL_EXPORTED __declspec(dllexport)
#else
# define RELOCATABLE_DLL_EXPORTED
#endif

/* Sets the original and the current installation prefix of the package.
   Relocation simply replaces a pathname starting with the original prefix
   by the corresponding pathname with the current prefix instead.  Both
   prefixes should be directory names without trailing slash (i.e. use ""
   instead of "/").  */
extern RELOCATABLE_DLL_EXPORTED void
       set_relocation_prefix (const char *orig_prefix,
                              const char *curr_prefix);

/* Returns the pathname, relocated according to the current installation
   directory.
   The returned string is either PATHNAME unmodified or a freshly allocated
   string that you can free with free() after casting it to 'char *'.  */
extern const char * relocate (const char *pathname);

/* Returns the pathname, relocated according to the current installation
   directory.
   This function sets *ALLOCATEDP to the allocated memory, or to NULL if
   no memory allocation occurs.  So that, after you're done with the return
   value, to reclaim allocated memory, you can do: free (*ALLOCATEDP).  */
extern const char * relocate2 (const char *pathname, char **allocatedp);

/* Memory management: relocate() potentially allocates memory, because it has
   to construct a fresh pathname.  If this is a problem because your program
   calls relocate() frequently or because you want to fix all potential memory
   leaks anyway, you have three options:
   1) Use this idiom:
        const char *pathname = ...;
        const char *rel_pathname = relocate (pathname);
        ...
        if (rel_pathname != pathname)
          free ((char *) rel_pathname);
   2) Use this idiom:
        char *allocated;
        const char *rel_pathname = relocate2 (..., &allocated);
        ...
        free (allocated);
   3) Think about caching the result.  */

/* Convenience function:
   Computes the current installation prefix, based on the original
   installation prefix, the original installation directory of a particular
   file, and the current pathname of this file.
   Returns it, freshly allocated.  Returns NULL upon failure.  */
extern char * compute_curr_prefix (const char *orig_installprefix,
                                   const char *orig_installdir,
                                   const char *curr_pathname);

#else

/* By default, we use the hardwired pathnames.  */
#define relocate(pathname) (pathname)
#define relocate2(pathname,allocatedp) (*(allocatedp) = NULL, (pathname))

#endif


#ifdef __cplusplus
}
#endif

#endif /* _RELOCATABLE_H */
