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
 * $Id: __stats.c,v 1.4 2009/02/18 17:59:09 lurcher Exp $
 *
 * $Log: __stats.c,v $
 * Revision 1.4  2009/02/18 17:59:09  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.3  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.2  2004/05/07 09:53:13  lurcher
 *
 *
 * Fix potential problrm in stats if creating a semaphore fails
 * Alter state after SQLParamData from S4 to S5
 *
 * Revision 1.1.1.1  2001/10/17 16:40:09  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.11  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.10  2001/06/04 15:24:49  nick
 *
 * Add port to MAC OSX and QT3 changes
 *
 * Revision 1.9  2001/05/15 13:56:29  jason
 *
 * semaphore header file not requires unless COLLECT_STATS is defined
 *
 * Revision 1.8  2001/05/15 13:29:07  jason
 *
 *
 * Moved COLLECT_STATS define to allow compilation on OpenVMS.
 *
 * Revision 1.7  2001/01/03 10:15:16  martin
 *
 * Fix bug in uodbc_update_stats() which attempted to use the shared memory
 * 	ID to release the semaphore if the array of process info full.
 * Fix bug in release_sem_lock() which called semop saying there were 2 ops
 * 	when there was really only one.
 *
 * Revision 1.6  2000/12/21 16:18:37  martin
 *
 * Add the promised support to return a list of process IDs currently attached
 * 	to the DM.
 *
 * Revision 1.5  2000/12/21 15:58:35  martin
 *
 * Fix problems with any app exiting clearing all stats.
 *
 * Revision 1.4  2000/12/20 12:00:52  nick
 *
 * Add uodbc_update_stats to the non stats build
 *
 * Revision 1.3  2000/12/19 10:28:29  martin
 *
 * Return "not built with stats" in uodbc_error() if stats function called
 * 	when stats not built.
 * Add uodbc_update_stats() calls to SQLFreeHandle.
 *
 * Revision 1.1  2000/12/18 11:53:51  martin
 *
 * handle statistic API.
 *
 *
 **********************************************************************/

#include <config.h>

#ifdef HAVE_SYS_SEM_H
 
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif /* HAVE_SYS_SEM_H */
#ifdef COLLECT_STATS
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#endif
#include <signal.h>
#include "__stats.h"
#include <uodbc_stats.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: __stats.c,v $ $Revision: 1.4 $";

#ifdef COLLECT_STATS
#ifdef HAVE_LIBPTHREAD

#include <pthread.h>

#endif
/*
 *  PROJECT_ID is used in the call to ftok().
 *  The PROJECT_ID reduces the chance of a class between processes using IPC.
 *  Do not change thisnumber as it will make different versions of unixODBC
 *  incompatible.
 */
#define PROJECT_ID 121

/*
 *  Permssions on sempahore/shared memory
 *  These needs to be world readable/writeable or different apps running under
 *  different users we not be able to update/read the stats.
 */
#define IPC_ACCESS_MODE (S_IRUSR | S_IWUSR \
                       | S_IRGRP | S_IWGRP \
                       | S_IROTH | S_IWOTH)

static char errmsg[512]="";

static int release_sem_lock(int sem_id);
static int acquire_sem_lock(int sem_id);


int uodbc_open_stats(
    void **rh,
    unsigned int mode)
{
    key_t                       ipc_key;
    int                         shm_created = 0;
    uodbc_stats_handle_t        *h = NULL;
    uodbc_stats_handle_t        lh;
    char                        odbcini[1024];
    unsigned int                i;
    int                         shmflg;

    if (!rh)
    {
        return -1;
    }

#ifdef STATS_FTOK_NAME

    if ( strcmp( STATS_FTOK_NAME, "odbc.ini" ) == 0 ) {
        if (!_odbcinst_SystemINI(odbcini, FALSE))
        {
            snprintf(errmsg, sizeof(errmsg), "Failed to find system odbc.ini");
            return -1;
        }
    }
    else {
        strcpy( odbcini, STATS_FTOK_NAME );
    }

#else

    if (!_odbcinst_SystemINI(odbcini, FALSE))
    {
        snprintf(errmsg, sizeof(errmsg), "Failed to find system odbc.ini");
        return -1;
    }

#endif

    memset(&lh, '\0', sizeof(lh));
    memcpy(lh.id, UODBC_STATS_ID, 5);
    lh.shm_id = -1;
    lh.sem_id = -1;
    lh.pid = getpid();
    
    /*
     * Check the odbc.ini file used in ftok() exists.
     */
    if (access(odbcini, F_OK) < 0)
    {
        snprintf(errmsg, sizeof(errmsg), "Cannot locate %s", odbcini);
        return -1;
    }
    
    /*
     *  Get a unique IPC key.
     */
    if ((ipc_key = ftok(odbcini, (char)PROJECT_ID)) < 0)
    {
        snprintf(errmsg, sizeof(errmsg),
                 "Failed to obtain IPC key - %s", strerror(errno));
        return -1;
    }
    
    /*
     *  See if the semaphore exists and create if it doesn't.
     */
    lh.sem_id = semget(ipc_key, 1, IPC_ACCESS_MODE | IPC_CREAT | IPC_EXCL);
    if (lh.sem_id < 0)
    {
        if (errno != EEXIST)
        {
            snprintf(errmsg, sizeof(errmsg),
                     "Failed to get semaphore ID - %s",
                     strerror(errno));
            return -1;
        }
        
        lh.sem_id = semget(ipc_key, 1, IPC_ACCESS_MODE | IPC_CREAT);
        if (lh.sem_id < 0)
        {
            snprintf(errmsg, sizeof(errmsg),
                     "Failed to create semaphore - %s", strerror(errno));
            return -1;
        }
    }
    /*
     *  Create/map shared memory
     */
    if (mode & UODBC_STATS_WRITE)
        shmflg = IPC_ACCESS_MODE | IPC_CREAT | IPC_EXCL;
    else
        shmflg = IPC_ACCESS_MODE;        
    
    lh.shm_id = shmget(ipc_key, sizeof(uodbc_stats_t), shmflg);
    if (lh.shm_id < 0)
    {
        if (mode & UODBC_STATS_READ)
        {
            snprintf(errmsg, sizeof(errmsg),
                     "No statistics available yet");
            return -1;
        }
        if (errno == EEXIST)
        { 
            lh.shm_id = shmget(ipc_key, sizeof(uodbc_stats_t), IPC_ACCESS_MODE);
            if (lh.shm_id < 0)
            {
                snprintf(errmsg, sizeof(errmsg),
                         "Shared memory exists but cannot map it - %s",
                         strerror(errno));
                return -1;
            }
        }
        else
        {
            snprintf(errmsg, sizeof(errmsg),
                     "Failed to get shared memory ID - %s",
                     strerror(errno));
            return -1;
        }
    }
    else
    {
        if (mode & UODBC_STATS_WRITE) shm_created = 1;
    }
    
    lh.stats = (uodbc_stats_t *)shmat(lh.shm_id, 0, 0);
    if (lh.stats == (uodbc_stats_t *)-1)
    {
        snprintf(errmsg, sizeof(errmsg),
                 "Failed to attach to shared memory - %s", strerror(errno));
        return -1;
    }
    else if (shm_created)
    {
        unsigned int    i;
        int lk;
        
        lk = acquire_sem_lock(lh.sem_id);
        memset(lh.stats, '\0', sizeof(uodbc_stats_t));
        for (i = 0; i < (sizeof(lh.stats->perpid) / sizeof(lh.stats->perpid[0])); i++)
        {
            lh.stats->perpid[i].pid = (pid_t)0;
        }
        if (lk == 0) release_sem_lock(lh.sem_id);
    }
    if ((h = calloc(1, sizeof(uodbc_stats_handle_t))) == NULL) return -1;
    memcpy(h, &lh, sizeof(uodbc_stats_handle_t));
    /*
     *  If caller asked for write access it is assumed it is going to
     *  change the statistics and so it needs an entry in the stats.
     */
    if (mode & UODBC_STATS_WRITE)
    {
        int lk;
        
        lk = acquire_sem_lock(lh.sem_id);
        for (i = 0;
             i < (sizeof(h->stats->perpid) / sizeof(h->stats->perpid[0]));
             i++)
        {
            if (h->stats->perpid[i].pid == (pid_t)0)
            {
                h->stats->perpid[i].pid = getpid();
                h->stats->perpid[i].n_env = 0;
                h->stats->perpid[i].n_dbc = 0;
                h->stats->perpid[i].n_stmt = 0;
                h->stats->perpid[i].n_desc = 0;
                break;
            }
        }
        if (lk == 0) release_sem_lock(lh.sem_id);
    }
    
    *(uodbc_stats_handle_t **)rh = h;
    return 0;
}


/************************************************************************/
/*                                                                      */
/*  uodbc_close_stats                                                   */
/*  =================                                                   */
/*                                                                      */
/************************************************************************/
int uodbc_close_stats(
    void *h)
{
    uodbc_stats_handle_t        *sh;
    sh = (uodbc_stats_handle_t *)h;

    if (!sh)
    {
        snprintf(errmsg, sizeof(errmsg), "NULL stats handle");
        return -1;
    }
    if (memcmp(sh->id, UODBC_STATS_ID, sizeof(sh->id)) != 0)
    {
        snprintf(errmsg, sizeof(errmsg), "Invalid stats handle %p", sh);
        return -1;
    }
    if ((sh->shm_id != -1) && (sh->stats))
    {
        unsigned int    i;
        
        for (i = 0;
             i < (sizeof(sh->stats->perpid) / sizeof(sh->stats->perpid[0]));
             i++)
        {
            if (sh->stats->perpid[i].pid == sh->pid)
            {
                sh->stats->perpid[i].pid = (pid_t) 0;
                break;
            }
        }
        
        shmdt((char *)sh->stats);
        sh->stats = NULL;
        sh->shm_id = -1;
    }
    /*
     *  Should we examine attach count and delete shared memory?
     */
    memset(sh->id, '\0', sizeof(sh->id));
    free(sh);
    return 0;
}


/************************************************************************/
/*                                                                      */
/*  uodbc_update_stats                                                  */
/*  ==================                                                  */
/*                                                                      */
/************************************************************************/
int uodbc_update_stats(void *h,
                       unsigned int stats_type_mask,
                       void *value)
{
    unsigned long               type;
    unsigned int                i;
    uodbc_stats_handle_t        *sh;
    int lk;
    
    sh = (uodbc_stats_handle_t *)h;
    if (!sh)
    {
        snprintf(errmsg, sizeof(errmsg), "NULL stats handle");
        return -1;
    }
    if (memcmp(sh->id, UODBC_STATS_ID, sizeof(sh->id)) != 0)
    {
        snprintf(errmsg, sizeof(errmsg), "Invalid stats handle %p", h);
        return -1;
    }
    if (!sh->stats)
    {
        snprintf(errmsg, sizeof(errmsg), "stats memory not mapped");
        return -1;
    }

    lk = acquire_sem_lock(sh->sem_id);
    /*
     *  Find this PID in array
     */
    for (i = 0;
         i < (sizeof(sh->stats->perpid) / sizeof(sh->stats->perpid[0]));
         i++)
    {
        if (sh->stats->perpid[i].pid == sh->pid) break;
    }
    /*
     *  Check if array full.
     */
    if ( i >= (sizeof(sh->stats->perpid) / sizeof(sh->stats->perpid[0])))
    {
        /*
         *  array full - process not entered.
         */
        if (lk == 0) release_sem_lock(sh->sem_id);
        return 0;
    }

    type = stats_type_mask & UODBC_STATS_TYPE_TYPE_MASK;
    switch(type)
    {
      case UODBC_STATS_TYPE_HENV:
      {
          sh->stats->perpid[i].n_env += (long)value;
          break;
      }
      case UODBC_STATS_TYPE_HDBC:
      {
          sh->stats->perpid[i].n_dbc += (long)value;
          break;
      }
      case UODBC_STATS_TYPE_HSTMT:
      {
          sh->stats->perpid[i].n_stmt += (long)value;
          break;
      }
      case UODBC_STATS_TYPE_HDESC:
      {
          sh->stats->perpid[i].n_desc += (long)value;
          break;
      }
      default:
      {
          break;
      }
    }
    if (lk == 0) release_sem_lock(sh->sem_id);
    
    return 0;
}

/************************************************************************/
/*                                                                      */
/*  uodbc_stats_error                                                   */
/*  =================                                                   */
/*                                                                      */
/************************************************************************/
char *uodbc_stats_error(
    char *buf,
    size_t buflen)
{
    if (!buf) return NULL;

    if (strlen(errmsg) > buflen)
    {
        memcpy(buf, errmsg, buflen - 1);
        buf[buflen - 1] = '\0';
    }
    else
    {
        strcpy(buf, errmsg);
    }

    return buf;
}
    

/************************************************************************/
/*                                                                      */
/*  uodbc_get_stats                                                     */
/*  ===============                                                     */
/*                                                                      */
/*  This function should be provided with an array of statistic         */
/*  structures which will be filled with the required statistics        */
/*  records.                                                            */
/*                                                                      */
/*  ret_stats = uodbc_get_stats(h, request_pid, s, n_stats);            */
/*                                                                      */
/*  h = a statistics handle returned from uodbc_open_stats().           */
/*  request_pid =                                                       */
/*    -1 = return stats on all attached processes.                      */
/*    n (n > 0) = return stats on specific process request_pid.         */
/*    0 = return list of processes attached.                            */
/*  s = ptr to array of statistics structures.                          */
/*  n_stats = number of statistics structures at s.                     */
/*  ret_stats = number of stats structures filled in at s.              */
/*                                                                      */
/************************************************************************/
int uodbc_get_stats(
    void *h,
    pid_t request_pid,
    uodbc_stats_retentry *s,
    int n_stats)
{
    uodbc_stats_handle_t        *sh;
    unsigned int                i;
    long                        n_env=0;
    long                        n_dbc=0;
    long                        n_stmt=0;
    long                        n_desc=0;
    int                         cur_stat;

    sh = (uodbc_stats_handle_t *)h;

    if (!sh)
    {
        snprintf(errmsg, sizeof(errmsg), "NULL stats return ptr supplied");
        return -1;
    }
    if (n_stats < 1)
    {
        snprintf(errmsg, sizeof(errmsg), "No stats return structures supplied");
        return -1;
    }
    if (memcmp(sh->id, UODBC_STATS_ID, sizeof(sh->id)) != 0)
    {
        snprintf(errmsg, sizeof(errmsg), "Invalid stats handle %p", sh);
        return -1;
    }
    if (!sh->stats)
    {
        snprintf(errmsg, sizeof(errmsg), "stats memory not mapped");
        return -1;
    }
    cur_stat = 0;
    for (i = 0;
         i < (sizeof(sh->stats->perpid) / sizeof(sh->stats->perpid[0]));
         i++)
    {
        if (sh->stats->perpid[i].pid > 0)
        {
            int         sts;

            /*
             *  Check this process still exists and if not zero counts.
             */
            sts = kill(sh->stats->perpid[i].pid, 0);
            if ((sts == 0) || ((sts < 0) && (errno == EPERM)))
            {
                ;
            }
            else
            {
                sh->stats->perpid[i].pid = 0;
                sh->stats->perpid[i].n_env = 0;
                sh->stats->perpid[i].n_dbc = 0;
                sh->stats->perpid[i].n_stmt = 0;
                sh->stats->perpid[i].n_desc = 0;
            }
        }
        
        if (((request_pid == (pid_t)-1) && (sh->stats->perpid[i].pid > 0)) ||
            (sh->stats->perpid[i].pid == request_pid))
        {
            n_env += sh->stats->perpid[i].n_env;
            n_dbc += sh->stats->perpid[i].n_dbc;
            n_stmt += sh->stats->perpid[i].n_stmt;
            n_desc += sh->stats->perpid[i].n_desc;
        }
        else if (request_pid == (pid_t)0)
        {
            s[cur_stat].type = UODBC_STAT_LONG;
            s[cur_stat].value.l_value = sh->stats->perpid[i].pid;
            strcpy(s[cur_stat].name, "PID");
            if (++cur_stat > n_stats) return cur_stat;
        }
    }

    if (request_pid == (pid_t)0) return cur_stat;
    
    s[cur_stat].type = UODBC_STAT_LONG;
    s[cur_stat].value.l_value = n_env;
    strcpy(s[cur_stat].name, "Environments");
    if (++cur_stat > n_stats) return cur_stat;
    
    s[cur_stat].type = UODBC_STAT_LONG;
    s[cur_stat].value.l_value = n_dbc;
    strcpy(s[cur_stat].name, "Connections");
    if (++cur_stat > n_stats) return cur_stat;
    
    s[cur_stat].type = UODBC_STAT_LONG;
    s[cur_stat].value.l_value = n_stmt;
    strcpy(s[cur_stat].name, "Statements");
    if (++cur_stat > n_stats) return cur_stat;
    
    s[cur_stat].type = UODBC_STAT_LONG;
    s[cur_stat].value.l_value = n_desc;
    strcpy(s[cur_stat].name, "Descriptors");
    if (++cur_stat > n_stats) return cur_stat;
    
    return cur_stat;
}


/************************************************************************/
/*                                                                      */
/*  acquire_sem_lock                                                    */
/*  ================                                                    */
/*                                                                      */
/*  This function locks other threads/processes out whilst a change to  */
/*  to the statistics is made. It uses a global semaphore which was     */
/*  created in uodbc_open_stats(). The semaphore set contains only the  */
/*  one semaphore which is incremented to 1 when locked. All semops     */
/*  are performed with SEM_UNDO so if the process unexepctedly exits    */
/*  sempahore operations are undone.                                    */
/*                                                                      */
/*  NOTE: some older platforms have a kernel limit on the number of     */
/*  SEM_UNDOs structures that may be used. If you run out, you will     */
/*  either have to increase the limit or take out the SEM_UNDO in this  */
/*  function and release_sem_lock() and hope uodbc_update_stats() never */
/*  causes a preature exit.                                             */
/*                                                                      */
/************************************************************************/
static int acquire_sem_lock(int sem_id)
{
    /*
     *  Semaphore operations:
     */
    /* lock the semaphore */
    struct sembuf	op_lock[2] =
    {
        {0, 0, 0},          /* Wait for [0] (lock) to equal 0 */
        {0, 1, SEM_UNDO}    /* increment [0] to lock */
    };

    if (semop(sem_id, &op_lock[0], 2) < 0)
    {
        return -1;
    }
    return 0;
}


/************************************************************************/
/*                                                                      */
/*  release_sem_lock                                                    */
/*  ================                                                    */
/*                                                                      */
/*  This function unlocks the semaphore used to lock other              */
/*  threads/processes out whilst a change to the statistics is made.    */
/*  It uses a global semaphore which was created in uodbc_open_stats(). */
/*  The semaphore set contains only the one semaphore which is          */
/*  incremented to 1 when locked and decremented when unlocked.         */
/*  All semops are performed with SEM_UNDO so if the process            */
/*  unexepctedly exits sempahore operations are undone.                 */
/*                                                                      */
/*  NOTE: some older platforms have a kernel limit on the number of     */
/*  SEM_UNDOs structures that may be used. If you run out, you will     */
/*  either have to increase the limit or take out the SEM_UNDO in this  */
/*  function and acquire_sem_lock() and hope uodbc_update_stats() never */
/*  causes a preature exit.                                             */
/*                                                                      */
/************************************************************************/
static int release_sem_lock(int sem_id)
{
    /*
     *  Semaphore operations:
     */
    /* unlock the semaphore */
    struct sembuf	op_unlock[1] =
    {
        {0, -1, SEM_UNDO},  /* Decrement [0] lock back to zero */
    };

    if (semop(sem_id, &op_unlock[0], 1) < 0)
    {
        return -1;
    }
    return 0;
}
#else
int uodbc_open_stats(
    void **rh,
    unsigned int mode)
{
    return -1;
}
int uodbc_close_stats(
    void *h)
{
    return -1;
}
char *uodbc_stats_error(
    char *buf,
    size_t buflen)
{
    const char *notbuilt="unixODBC not built with statistics code";
    
    if (!buf) return NULL;

    if (strlen(notbuilt) > buflen)
    {
        memcpy(buf, notbuilt, buflen - 1);
        buf[buflen - 1] = '\0';
    }
    else
    {
        strcpy(buf, notbuilt);
    }

    return buf;
}
int uodbc_get_stats(
    void *h,
    pid_t request_pid,
    uodbc_stats_retentry *s,
    int n_stats)
{
    return -1;
}
int uodbc_update_stats(void *h,
                       unsigned int stats_type_mask,
                       void *value)
{
    return -1;
}

#endif /* COLLECT_STATS */
