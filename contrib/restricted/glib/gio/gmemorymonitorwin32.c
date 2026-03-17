/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2022 Red Hat, Inc.
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
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include "gmemorymonitor.h"
#include "gioerror.h"
#include "ginitable.h"
#include "giomodule-priv.h"
#include "glibintl.h"
#include "glib/gstdio.h"
#include "gcancellable.h"

#include <windows.h>

#define G_TYPE_MEMORY_MONITOR_WIN32 (g_memory_monitor_win32_get_type ())
G_DECLARE_FINAL_TYPE (GMemoryMonitorWin32, g_memory_monitor_win32, G, MEMORY_MONITOR_WIN32, GObject)

#define G_MEMORY_MONITOR_WIN32_GET_INITABLE_IFACE(o) (G_TYPE_INSTANCE_GET_INTERFACE ((o), G_TYPE_INITABLE, GInitable))

static void g_memory_monitor_win32_iface_init (GMemoryMonitorInterface *iface);
static void g_memory_monitor_win32_initable_iface_init (GInitableIface *iface);

struct _GMemoryMonitorWin32
{
  GObject parent_instance;

  HANDLE event;
  HANDLE mem;
  HANDLE thread;
};

G_DEFINE_TYPE_WITH_CODE (GMemoryMonitorWin32, g_memory_monitor_win32, G_TYPE_OBJECT,
                         G_IMPLEMENT_INTERFACE (G_TYPE_INITABLE,
                                                g_memory_monitor_win32_initable_iface_init)
                         G_IMPLEMENT_INTERFACE (G_TYPE_MEMORY_MONITOR,
                                                g_memory_monitor_win32_iface_init)
                         _g_io_modules_ensure_extension_points_registered ();
                         g_io_extension_point_implement (G_MEMORY_MONITOR_EXTENSION_POINT_NAME,
                                                         g_define_type_id,
                                                         "win32",
                                                         30))

static void
g_memory_monitor_win32_init (GMemoryMonitorWin32 *win32)
{
}

static gboolean
watch_handler (gpointer user_data)
{
  GMemoryMonitorWin32 *win32 = user_data;

  g_signal_emit_by_name (win32, "low-memory-warning",
                         G_MEMORY_MONITOR_WARNING_LEVEL_LOW);

  return G_SOURCE_REMOVE;
}

/* Thread which watches for win32 memory resource events */
static DWORD WINAPI
watch_thread_function (LPVOID parameter)
{
  GWeakRef *weak_ref = parameter;
  GMemoryMonitorWin32 *win32 = NULL;
  HANDLE handles[2] = { 0, };
  DWORD result;
  BOOL low_memory_state;

  win32 = g_weak_ref_get (weak_ref);
  if (!win32)
    goto end;

  if (!DuplicateHandle (GetCurrentProcess (),
                        win32->event,
                        GetCurrentProcess (),
                        &handles[0],
                        0,
                        FALSE,
                        DUPLICATE_SAME_ACCESS))
    {
      gchar *emsg;

      emsg = g_win32_error_message (GetLastError ());
      g_debug ("DuplicateHandle failed: %s", emsg);
      g_free (emsg);
      goto end;
    }

  if (!DuplicateHandle (GetCurrentProcess (),
                        win32->mem,
                        GetCurrentProcess (),
                        &handles[1],
                        0,
                        FALSE,
                        DUPLICATE_SAME_ACCESS))
    {
      gchar *emsg;

      emsg = g_win32_error_message (GetLastError ());
      g_debug ("DuplicateHandle failed: %s", emsg);
      g_free (emsg);
      goto end;
    }

  g_clear_object (&win32);

  while (1)
    {
      if (!QueryMemoryResourceNotification (handles[1], &low_memory_state))
        {
          gchar *emsg;

          emsg = g_win32_error_message (GetLastError ());
          g_debug ("QueryMemoryResourceNotification failed: %s", emsg);
          g_free (emsg);
          break;
        }

      win32 = g_weak_ref_get (weak_ref);
      if (!win32)
        break;

      if (low_memory_state)
        {
          g_idle_add_full (G_PRIORITY_DEFAULT,
                           watch_handler,
                           g_steal_pointer (&win32),
                           g_object_unref);
          /* throttle a bit the loop */
          g_usleep (G_USEC_PER_SEC);
          continue;
        }

      g_clear_object (&win32);

      result = WaitForMultipleObjects (G_N_ELEMENTS (handles), handles, FALSE, INFINITE);
      switch (result)
        {
          case WAIT_OBJECT_0 + 1:
            continue;

          case WAIT_FAILED:
            {
              gchar *emsg;

              emsg = g_win32_error_message (GetLastError ());
              g_debug ("WaitForMultipleObjects failed: %s", emsg);
              g_free (emsg);
            }
            G_GNUC_FALLTHROUGH;
          default:
            goto end;
        }
    }

end:
  if (handles[0])
    CloseHandle (handles[0]);
  if (handles[1])
    CloseHandle (handles[1]);
  g_clear_object (&win32);
  g_weak_ref_clear (weak_ref);
  g_free (weak_ref);
  return 0;
}

static gboolean
g_memory_monitor_win32_initable_init (GInitable     *initable,
                                      GCancellable  *cancellable,
                                      GError       **error)
{
  GMemoryMonitorWin32 *win32 = G_MEMORY_MONITOR_WIN32 (initable);
  GWeakRef *weak_ref = NULL;

  win32->event = CreateEvent (NULL, FALSE, FALSE, NULL);
  if (win32->event == NULL)
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to create event");
      return FALSE;
    }

  win32->mem = CreateMemoryResourceNotification (LowMemoryResourceNotification);
  if (win32->mem == NULL)
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to create resource notification handle");
      return FALSE;
    }

  weak_ref = g_new0 (GWeakRef, 1);
  g_weak_ref_init (weak_ref, win32);
  /* Use CreateThread (not GThread) with a small stack to make it more lightweight. */
  win32->thread = CreateThread (NULL, 1024, watch_thread_function, weak_ref, 0, NULL);
  if (win32->thread == NULL)
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to create memory resource notification thread");
      g_weak_ref_clear (weak_ref);
      g_free (weak_ref);
      return FALSE;
    }

  return TRUE;
}

static void
g_memory_monitor_win32_finalize (GObject *object)
{
  GMemoryMonitorWin32 *win32 = G_MEMORY_MONITOR_WIN32 (object);

  if (win32->thread)
    {
      SetEvent (win32->event);
      WaitForSingleObject (win32->thread, INFINITE);
      CloseHandle (win32->thread);
    }

  if (win32->event)
    CloseHandle (win32->event);

  if (win32->mem)
    CloseHandle (win32->mem);

  G_OBJECT_CLASS (g_memory_monitor_win32_parent_class)->finalize (object);
}

static void
g_memory_monitor_win32_class_init (GMemoryMonitorWin32Class *nl_class)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (nl_class);

  gobject_class->finalize = g_memory_monitor_win32_finalize;
}

static void
g_memory_monitor_win32_iface_init (GMemoryMonitorInterface *monitor_iface)
{
}

static void
g_memory_monitor_win32_initable_iface_init (GInitableIface *iface)
{
  iface->init = g_memory_monitor_win32_initable_init;
}
