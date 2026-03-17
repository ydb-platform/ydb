/*********************************************************************
 *
 * This is based on code created by Peter Harvey,
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * Further modified and extend by Eric Sharkey
 * (sharkey@netrics.com).
 *
 * Any bugs or problems should be considered the fault of Nick or
 * Eric and not Peter.
 *
 * copyright (c) 2005 Eric Sharkey
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 **********************************************************************
 *
 * $Id: uodbc_extras.h,v 1.1 2005/03/04 20:08:45 sharkey Exp $
 *
 **********************************************************************/
#ifndef UODBC__extras_h
#define UODBC__extras_h 1

#if defined(HAVE_STDARG_H)
# include <stdarg.h>
# define HAVE_STDARGS
#else
# if defined(HAVE_VARARGS_H)
#  include <varargs.h>
#  ifdef HAVE_STDARGS
#   undef HAVE_STDARGS
#  endif
# endif
#endif

#if defined(__cplusplus)
         extern  "C" {
#endif

extern int uodbc_vsnprintf (char *str, size_t count, const char *fmt, va_list args);

#ifdef HAVE_STDARGS
 int uodbc_snprintf (char *str,size_t count,const char *fmt,...);
#else
 int uodbc_snprintf (va_alist) va_dcl;
#endif

#ifndef HAVE_SNPRINTF
#define snprintf uodbc_snprintf
#endif

#ifndef HAVE_VSNPRINTF
#define vsnprintf uodbc_vsnprintf
#endif

#ifndef HAVE_STRCASECMP
extern int strcasecmp( const char *s1, const char * s2 );
#endif

#ifndef HAVE_STRNCASECMP
extern int strncasecmp (const char *s1, const char *s2, int n );
#endif


#if defined(__cplusplus)
         }
#endif

#endif /* UODBC__extras_h */

