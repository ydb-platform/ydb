/* GLIB - Library of useful routines for C programming
 * Copyright (C) 1995-1997  Peter Mattis, Spencer Kimball and Josh MacDonald
 *
 * gthread.c: solaris thread system implementation
 * Copyright 1998-2001 Sebastian Wilhelmi; University of Karlsruhe
 * Copyright 2001 Hans Breuer
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

/* The GMutex and GCond implementations in this file are some of the
 * lowest-level code in GLib.  All other parts of GLib (messages,
 * memory, slices, etc) assume that they can freely use these facilities
 * without risking recursion.
 *
 * As such, these functions are NOT permitted to call any other part of
 * GLib.
 *
 * The thread manipulation functions (create, exit, join, etc.) have
 * more freedom -- they can do as they please.
 */

#include <contrib/restricted/glib/config.h>

#include "glib.h"
#include "glib-init.h"
#include "gthread.h"
#include "gthreadprivate.h"
#include "gslice.h"

#include <windows.h>

#include <process.h>
#include <stdlib.h>
#include <stdio.h>

static void
g_thread_abort (gint         status,
                const gchar *function)
{
  fprintf (stderr, "GLib (gthread-win32.c): Unexpected error from C library during '%s': %s.  Aborting.\n",
           strerror (status), function);
  g_abort ();
}

/* Starting with Vista and Windows 2008, we have access to the
 * CONDITION_VARIABLE and SRWLock primitives on Windows, which are
 * pretty reasonable approximations of the primitives specified in
 * POSIX 2001 (pthread_cond_t and pthread_mutex_t respectively).
 *
 * Both of these types are structs containing a single pointer.  That
 * pointer is used as an atomic bitfield to support user-space mutexes
 * that only get the kernel involved in cases of contention (similar
 * to how futex()-based mutexes work on Linux).  The biggest advantage
 * of these new types is that they can be statically initialised to
 * zero.  That means that they are completely ABI compatible with our
 * GMutex and GCond APIs.
 */

/* {{{1 GMutex */
void
g_mutex_init (GMutex *mutex)
{
  InitializeSRWLock ((gpointer) mutex);
}

void
g_mutex_clear (GMutex *mutex)
{
}

void
g_mutex_lock (GMutex *mutex)
{
  AcquireSRWLockExclusive ((gpointer) mutex);
}

gboolean
g_mutex_trylock (GMutex *mutex)
{
  return TryAcquireSRWLockExclusive ((gpointer) mutex);
}

void
g_mutex_unlock (GMutex *mutex)
{
  ReleaseSRWLockExclusive ((gpointer) mutex);
}

/* {{{1 GRecMutex */

static CRITICAL_SECTION *
g_rec_mutex_impl_new (void)
{
  CRITICAL_SECTION *cs;

  cs = g_slice_new (CRITICAL_SECTION);
  InitializeCriticalSection (cs);

  return cs;
}

static void
g_rec_mutex_impl_free (CRITICAL_SECTION *cs)
{
  DeleteCriticalSection (cs);
  g_slice_free (CRITICAL_SECTION, cs);
}

static CRITICAL_SECTION *
g_rec_mutex_get_impl (GRecMutex *mutex)
{
  CRITICAL_SECTION *impl = mutex->p;

  if G_UNLIKELY (mutex->p == NULL)
    {
      impl = g_rec_mutex_impl_new ();
      if (InterlockedCompareExchangePointer (&mutex->p, impl, NULL) != NULL)
        g_rec_mutex_impl_free (impl);
      impl = mutex->p;
    }

  return impl;
}

void
g_rec_mutex_init (GRecMutex *mutex)
{
  mutex->p = g_rec_mutex_impl_new ();
}

void
g_rec_mutex_clear (GRecMutex *mutex)
{
  g_rec_mutex_impl_free (mutex->p);
}

void
g_rec_mutex_lock (GRecMutex *mutex)
{
  EnterCriticalSection (g_rec_mutex_get_impl (mutex));
}

void
g_rec_mutex_unlock (GRecMutex *mutex)
{
  LeaveCriticalSection (mutex->p);
}

gboolean
g_rec_mutex_trylock (GRecMutex *mutex)
{
  return TryEnterCriticalSection (g_rec_mutex_get_impl (mutex));
}

/* {{{1 GRWLock */

void
g_rw_lock_init (GRWLock *lock)
{
  InitializeSRWLock ((gpointer) lock);
}

void
g_rw_lock_clear (GRWLock *lock)
{
}

void
g_rw_lock_writer_lock (GRWLock *lock)
{
  AcquireSRWLockExclusive ((gpointer) lock);
}

gboolean
g_rw_lock_writer_trylock (GRWLock *lock)
{
  return TryAcquireSRWLockExclusive ((gpointer) lock);
}

void
g_rw_lock_writer_unlock (GRWLock *lock)
{
  ReleaseSRWLockExclusive ((gpointer) lock);
}

void
g_rw_lock_reader_lock (GRWLock *lock)
{
  AcquireSRWLockShared ((gpointer) lock);
}

gboolean
g_rw_lock_reader_trylock (GRWLock *lock)
{
  return TryAcquireSRWLockShared ((gpointer) lock);
}

void
g_rw_lock_reader_unlock (GRWLock *lock)
{
  ReleaseSRWLockShared ((gpointer) lock);
}

/* {{{1 GCond */
void
g_cond_init (GCond *cond)
{
  InitializeConditionVariable ((gpointer) cond);
}

void
g_cond_clear (GCond *cond)
{
}

void
g_cond_signal (GCond *cond)
{
  WakeConditionVariable ((gpointer) cond);
}

void
g_cond_broadcast (GCond *cond)
{
  WakeAllConditionVariable ((gpointer) cond);
}

void
g_cond_wait (GCond  *cond,
             GMutex *entered_mutex)
{
  SleepConditionVariableSRW ((gpointer) cond, (gpointer) entered_mutex, INFINITE, 0);
}

gboolean
g_cond_wait_until (GCond  *cond,
                   GMutex *entered_mutex,
                   gint64  end_time)
{
  gint64 span, start_time;
  DWORD span_millis;
  gboolean signalled;

  start_time = g_get_monotonic_time ();
  do
    {
      span = end_time - start_time;

      if G_UNLIKELY (span < 0)
        span_millis = 0;
      else if G_UNLIKELY (span > G_GINT64_CONSTANT (1000) * (DWORD) INFINITE)
        span_millis = INFINITE;
      else
        /* Round up so we don't time out too early */
        span_millis = (span + 1000 - 1) / 1000;

      /* We never want to wait infinitely */
      if (span_millis >= INFINITE)
        span_millis = INFINITE - 1;

      signalled = SleepConditionVariableSRW ((gpointer) cond, (gpointer) entered_mutex, span_millis, 0);
      if (signalled)
        break;

      /* In case we didn't wait long enough after a timeout, wait again for the
       * remaining time */
      start_time = g_get_monotonic_time ();
    }
  while (start_time < end_time);

  return signalled;
}

/* {{{1 GPrivate */

typedef struct _GPrivateDestructor GPrivateDestructor;

struct _GPrivateDestructor
{
  DWORD               index;
  GDestroyNotify      notify;
  GPrivateDestructor *next;
};

static GPrivateDestructor *g_private_destructors;  /* (atomic) prepend-only */
static CRITICAL_SECTION g_private_lock;

static DWORD
g_private_get_impl (GPrivate *key)
{
  DWORD impl = (DWORD) GPOINTER_TO_UINT(key->p);

  if G_UNLIKELY (impl == 0)
    {
      EnterCriticalSection (&g_private_lock);
      impl = (UINT_PTR) key->p;
      if (impl == 0)
        {
          GPrivateDestructor *destructor;

          impl = TlsAlloc ();

          if G_UNLIKELY (impl == 0)
            {
              /* Ignore TLS index 0 temporarily (as 0 is the indicator that we
               * haven't allocated TLS yet) and alloc again;
               * See https://gitlab.gnome.org/GNOME/glib/-/issues/2058 */
              DWORD impl2 = TlsAlloc ();
              TlsFree (impl);
              impl = impl2;
            }

          if (impl == TLS_OUT_OF_INDEXES || impl == 0)
            g_thread_abort (0, "TlsAlloc");

          if (key->notify != NULL)
            {
              destructor = malloc (sizeof (GPrivateDestructor));
              if G_UNLIKELY (destructor == NULL)
                g_thread_abort (errno, "malloc");
              destructor->index = impl;
              destructor->notify = key->notify;
              destructor->next = g_atomic_pointer_get (&g_private_destructors);

              /* We need to do an atomic store due to the unlocked
               * access to the destructor list from the thread exit
               * function.
               *
               * It can double as a sanity check...
               */
              if (!g_atomic_pointer_compare_and_exchange (&g_private_destructors,
                                                          destructor->next,
                                                          destructor))
                g_thread_abort (0, "g_private_get_impl(1)");
            }

          /* Ditto, due to the unlocked access on the fast path */
          if (!g_atomic_pointer_compare_and_exchange (&key->p, NULL, impl))
            g_thread_abort (0, "g_private_get_impl(2)");
        }
      LeaveCriticalSection (&g_private_lock);
    }

  return impl;
}

gpointer
g_private_get (GPrivate *key)
{
  return TlsGetValue (g_private_get_impl (key));
}

void
g_private_set (GPrivate *key,
               gpointer  value)
{
  TlsSetValue (g_private_get_impl (key), value);
}

void
g_private_replace (GPrivate *key,
                   gpointer  value)
{
  DWORD impl = g_private_get_impl (key);
  gpointer old;

  old = TlsGetValue (impl);
  TlsSetValue (impl, value);
  if (old && key->notify)
    key->notify (old);
}

/* {{{1 GThread */

#define win32_check_for_error(what) G_STMT_START{			\
  if (!(what))								\
    g_error ("file %s: line %d (%s): error %s during %s",		\
	     __FILE__, __LINE__, G_STRFUNC,				\
	     g_win32_error_message (GetLastError ()), #what);		\
  }G_STMT_END

#define G_MUTEX_SIZE (sizeof (gpointer))

typedef BOOL (__stdcall *GTryEnterCriticalSectionFunc) (CRITICAL_SECTION *);

typedef struct
{
  GRealThread thread;

  GThreadFunc proxy;
  HANDLE      handle;
} GThreadWin32;

void
g_system_thread_free (GRealThread *thread)
{
  GThreadWin32 *wt = (GThreadWin32 *) thread;

  win32_check_for_error (CloseHandle (wt->handle));
  g_slice_free (GThreadWin32, wt);
}

void
g_system_thread_exit (void)
{
  /* In static compilation, DllMain doesn't exist and so DLL_THREAD_DETACH
   * case is never called and thread destroy notifications are not triggered.
   * To ensure that notifications are correctly triggered in static
   * compilation mode, we call directly the "detach" function here right
   * before terminating the thread.
   * As all win32 threads initialized through the glib API are run through
   * the same proxy function g_thread_win32_proxy() which calls systematically
   * g_system_thread_exit() when finishing, we obtain the same behavior as
   * with dynamic compilation.
   *
   * WARNING: unfortunately this mechanism cannot work with threads created
   * directly from the Windows API using CreateThread() or _beginthread/ex().
   * It only works with threads created by using the glib API with
   * g_system_thread_new(). If users need absolutely to use a thread NOT
   * created with glib API under Windows and in static compilation mode, they
   * should not use glib functions within their thread or they may encounter
   * memory leaks when the thread finishes.
   */
#ifdef GLIB_STATIC_COMPILATION
  g_thread_win32_thread_detach ();
#endif

  _endthreadex (0);
}

static guint __stdcall
g_thread_win32_proxy (gpointer data)
{
  GThreadWin32 *self = data;

  self->proxy (self);

  g_system_thread_exit ();

  g_assert_not_reached ();

  return 0;
}

gboolean
g_system_thread_get_scheduler_settings (GThreadSchedulerSettings *scheduler_settings)
{
  HANDLE current_thread = GetCurrentThread ();
  scheduler_settings->thread_prio = GetThreadPriority (current_thread);

  return TRUE;
}

GRealThread *
g_system_thread_new (GThreadFunc proxy,
                     gulong stack_size,
                     const GThreadSchedulerSettings *scheduler_settings,
                     const char *name,
                     GThreadFunc func,
                     gpointer data,
                     GError **error)
{
  GThreadWin32 *thread;
  GRealThread *base_thread;
  guint ignore;
  const gchar *message = NULL;
  int thread_prio;

  thread = g_slice_new0 (GThreadWin32);
  thread->proxy = proxy;
  thread->handle = (HANDLE) NULL;
  base_thread = (GRealThread*)thread;
  base_thread->ref_count = 2;
  base_thread->ours = TRUE;
  base_thread->thread.joinable = TRUE;
  base_thread->thread.func = func;
  base_thread->thread.data = data;
  base_thread->name = g_strdup (name);

  thread->handle = (HANDLE) _beginthreadex (NULL, stack_size, g_thread_win32_proxy, thread,
                                            CREATE_SUSPENDED, &ignore);

  if (thread->handle == NULL)
    {
      message = "Error creating thread";
      goto error;
    }

  /* For thread priority inheritance we need to manually set the thread
   * priority of the new thread to the priority of the current thread. We
   * also have to start the thread suspended and resume it after actually
   * setting the priority here.
   *
   * On Windows, by default all new threads are created with NORMAL thread
   * priority.
   */

  if (scheduler_settings)
    {
      thread_prio = scheduler_settings->thread_prio;
    }
  else
    {
      HANDLE current_thread = GetCurrentThread ();
      thread_prio = GetThreadPriority (current_thread);
    }

  if (thread_prio == THREAD_PRIORITY_ERROR_RETURN)
    {
      message = "Error getting current thread priority";
      goto error;
    }

  if (SetThreadPriority (thread->handle, thread_prio) == 0)
    {
      message = "Error setting new thread priority";
      goto error;
    }

  if (ResumeThread (thread->handle) == (DWORD) -1)
    {
      message = "Error resuming new thread";
      goto error;
    }

  return (GRealThread *) thread;

error:
  {
    gchar *win_error = g_win32_error_message (GetLastError ());
    g_set_error (error, G_THREAD_ERROR, G_THREAD_ERROR_AGAIN,
                 "%s: %s", message, win_error);
    g_free (win_error);
    if (thread->handle)
      CloseHandle (thread->handle);
    g_slice_free (GThreadWin32, thread);
    return NULL;
  }
}

void
g_thread_yield (void)
{
  Sleep(0);
}

void
g_system_thread_wait (GRealThread *thread)
{
  GThreadWin32 *wt = (GThreadWin32 *) thread;

  win32_check_for_error (WAIT_FAILED != WaitForSingleObject (wt->handle, INFINITE));
}

#define EXCEPTION_SET_THREAD_NAME ((DWORD) 0x406D1388)

#ifndef _MSC_VER
static void *SetThreadName_VEH_handle = NULL;

static LONG __stdcall
SetThreadName_VEH (PEXCEPTION_POINTERS ExceptionInfo)
{
  if (ExceptionInfo->ExceptionRecord != NULL &&
      ExceptionInfo->ExceptionRecord->ExceptionCode == EXCEPTION_SET_THREAD_NAME)
    return EXCEPTION_CONTINUE_EXECUTION;

  return EXCEPTION_CONTINUE_SEARCH;
}
#endif

typedef struct _THREADNAME_INFO
{
  DWORD  dwType;	/* must be 0x1000 */
  LPCSTR szName;	/* pointer to name (in user addr space) */
  DWORD  dwThreadID;	/* thread ID (-1=caller thread) */
  DWORD  dwFlags;	/* reserved for future use, must be zero */
} THREADNAME_INFO;

static void
SetThreadName (DWORD  dwThreadID,
               LPCSTR szThreadName)
{
   THREADNAME_INFO info;
   DWORD infosize;

   info.dwType = 0x1000;
   info.szName = szThreadName;
   info.dwThreadID = dwThreadID;
   info.dwFlags = 0;

   infosize = sizeof (info) / sizeof (DWORD);

#ifdef _MSC_VER
   __try
     {
       RaiseException (EXCEPTION_SET_THREAD_NAME, 0, infosize,
                       (const ULONG_PTR *) &info);
     }
   __except (EXCEPTION_EXECUTE_HANDLER)
     {
     }
#else
   /* Without a debugger we *must* have an exception handler,
    * otherwise raising an exception will crash the process.
    */
   if ((!IsDebuggerPresent ()) && (SetThreadName_VEH_handle == NULL))
     return;

   RaiseException (EXCEPTION_SET_THREAD_NAME, 0, infosize, (const ULONG_PTR *) &info);
#endif
}

typedef HRESULT (WINAPI *pSetThreadDescription) (HANDLE hThread,
                                                 PCWSTR lpThreadDescription);
static pSetThreadDescription SetThreadDescriptionFunc = NULL;
static HMODULE kernel32_module = NULL;

static gboolean
g_thread_win32_load_library (void)
{
  /* FIXME: Add support for UWP app */
#if !defined(G_WINAPI_ONLY_APP)
  static gsize _init_once = 0;
  if (g_once_init_enter (&_init_once))
    {
      kernel32_module = LoadLibraryW (L"kernel32.dll");
      if (kernel32_module)
        {
          SetThreadDescriptionFunc =
              (pSetThreadDescription) GetProcAddress (kernel32_module,
                                                      "SetThreadDescription");
          if (!SetThreadDescriptionFunc)
            FreeLibrary (kernel32_module);
        }
      g_once_init_leave (&_init_once, 1);
    }
#endif

  return !!SetThreadDescriptionFunc;
}

static gboolean
g_thread_win32_set_thread_desc (const gchar *name)
{
  HRESULT hr;
  wchar_t *namew;

  if (!g_thread_win32_load_library () || !name)
    return FALSE;

  namew = g_utf8_to_utf16 (name, -1, NULL, NULL, NULL);
  if (!namew)
    return FALSE;

  hr = SetThreadDescriptionFunc (GetCurrentThread (), namew);

  g_free (namew);
  return SUCCEEDED (hr);
}

void
g_system_thread_set_name (const gchar *name)
{
  /* Prefer SetThreadDescription over exception based way if available,
   * since thread description set by SetThreadDescription will be preserved
   * in dump file */
  if (!g_thread_win32_set_thread_desc (name))
    SetThreadName ((DWORD) -1, name);
}

/* {{{1 Epilogue */

void
g_thread_win32_init (void)
{
  InitializeCriticalSection (&g_private_lock);

#ifndef _MSC_VER
  SetThreadName_VEH_handle = AddVectoredExceptionHandler (1, &SetThreadName_VEH);
  if (SetThreadName_VEH_handle == NULL)
    {
      /* This is bad, but what can we do? */
    }
#endif
}

void
g_thread_win32_thread_detach (void)
{
  gboolean dtors_called;

  do
    {
      GPrivateDestructor *dtor;

      /* We go by the POSIX book on this one.
       *
       * If we call a destructor then there is a chance that some new
       * TLS variables got set by code called in that destructor.
       *
       * Loop until nothing is left.
       */
      dtors_called = FALSE;

      for (dtor = g_atomic_pointer_get (&g_private_destructors); dtor; dtor = dtor->next)
        {
          gpointer value;

          value = TlsGetValue (dtor->index);
          if (value != NULL && dtor->notify != NULL)
            {
              /* POSIX says to clear this before the call */
              TlsSetValue (dtor->index, NULL);
              dtor->notify (value);
              dtors_called = TRUE;
            }
        }
    }
  while (dtors_called);
}

void
g_thread_win32_process_detach (void)
{
#ifndef _MSC_VER
  if (SetThreadName_VEH_handle != NULL)
    {
      RemoveVectoredExceptionHandler (SetThreadName_VEH_handle);
      SetThreadName_VEH_handle = NULL;
    }
#endif
}

/* vim:set foldmethod=marker: */
