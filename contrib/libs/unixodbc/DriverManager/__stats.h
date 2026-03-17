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
 * $Id: __stats.h,v 1.2 2005/02/01 10:24:24 lurcher Exp $
 *
 * $Log: __stats.h,v $
 * Revision 1.2  2005/02/01 10:24:24  lurcher
 * Cope if SHLIBEXT is not set
 *
 * Revision 1.1.1.1  2001/10/17 16:40:09  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1  2000/12/18 11:53:51  martin
 *
 * handle statistic API.
 *
 *
 **********************************************************************/

#ifndef UNIXODBC__STATS_H
#define UNIXODBC__STATS_H 1

#include <stdio.h>
#include <sys/types.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif

typedef struct uodbc_stats_proc
{
    pid_t       pid;                            /* process ID */
    long        n_env;                          /* # of henvs */
    long        n_dbc;                          /* # of hdbcs */
    long        n_stmt;                         /* # of hstmts */
    long        n_desc;                         /* # of hdescs */
} uodbc_stats_proc_t;

typedef struct uodbc_stats
{
    int                 n_pid;                  /* # of PIDs attached */
    uodbc_stats_proc_t  perpid[20];
} uodbc_stats_t;

typedef struct uodbc_stats_handle
{
    char                id[5];                  /* identifier */
#       define UODBC_STATS_ID "UODBC"
    int                 sem_id;                 /* sempahore ID */
    int                 shm_id;                 /* shared memory ID */
    uodbc_stats_t       *stats;                 /* ptr to stats in shared mem */
    pid_t               pid;
} uodbc_stats_handle_t;

int uodbc_update_stats(void *rh,
                       unsigned int type,
                       void *value);
#define UODBC_STATS_TYPE_TYPE_MASK 0xffff
#define UODBC_STATS_TYPE_HENV 1
#define UODBC_STATS_TYPE_HDBC 2
#define UODBC_STATS_TYPE_HSTMT 3
#define UODBC_STATS_TYPE_HDESC 4
#endif /* UNIXODBC__STATS_H */
