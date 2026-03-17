/*********************************************************************
 *
 * This is based on code created by Peter Harvey,
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * Any bugs or problems should be considered the fault of Nick and not
 * Peter.
 *
 * copyright (c) 1999 Nick Gorham
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
 * $Id: uodbc_stats.h,v 1.1.1.1 2001/10/17 16:40:28 lurcher Exp $
 *
 * $Log: uodbc_stats.h,v $
 * Revision 1.1.1.1  2001/10/17 16:40:28  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.2  2000/12/19 07:28:45  pharvey
 * - added first pass at Stats page content
 * - wrapped public stats functions for C++
 *
 * Revision 1.1  2000/12/18 11:54:22  martin
 *
 * handle statistic API.
 *
 *
 **********************************************************************/
#ifndef UODBC__stats_h
#define UODBC__stats_h 1

#include <unistd.h>
#include <sys/types.h>

typedef struct uodbc_stats_retentry
{
    unsigned long       type;                   /* type of statistic */
#       define UODBC_STAT_STRING 1
#       define UODBC_STAT_LONG 2
    union
    {
        char            s_value[256];           /* string type */
        long            l_value;                /* number type */
    } value;
    char                name[32];               /* name of statistic */
} uodbc_stats_retentry;

#if defined(__cplusplus)
         extern  "C" {
#endif

int uodbc_open_stats(void **rh, unsigned int mode);
#define UODBC_STATS_READ 0x1
#define UODBC_STATS_WRITE 0x2
int uodbc_close_stats(void *rh);
int uodbc_get_stats(void *h, pid_t request_pid,
                    uodbc_stats_retentry *s, int n_stats);
char *uodbc_stats_error(char *buf, size_t buflen);

#if defined(__cplusplus)
         }
#endif

#endif /* UODBC__stats_h */

