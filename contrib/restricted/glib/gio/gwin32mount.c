/* GIO - GLib Input, Output and Streaming Library
 * 
 * Copyright (C) 2006-2007 Red Hat, Inc.
 * Copyright (C) 2008 Hans Breuer
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
 *
 * Author: Alexander Larsson <alexl@redhat.com>
 *         David Zeuthen <davidz@redhat.com>
 *         Hans Breuer <hans@breuer.org>
 */

#include <contrib/restricted/glib/config.h>

#include <string.h>
#define WIN32_MEAN_AND_LEAN
#define COBJMACROS
#include <windows.h>
#include <shlobj.h>
#include <shlwapi.h>

/* At the moment of writing IExtractIconW interface in Mingw-w64
 * is missing IUnknown members in its vtable. Use our own
 * fixed declaration for now.
 */
#undef INTERFACE
#define INTERFACE IMyExtractIconW
DECLARE_INTERFACE_(IMyExtractIconW,IUnknown)
{
    /*** IUnknown methods ***/
    STDMETHOD_(HRESULT,QueryInterface)(THIS_ REFIID riid, void** ppvObject) PURE;
    STDMETHOD_(ULONG,AddRef)(THIS) PURE;
    STDMETHOD_(ULONG,Release)(THIS) PURE;
    /*** IMyExtractIconW methods ***/
    STDMETHOD(GetIconLocation)(THIS_ UINT uFlags, LPWSTR pszIconFile, UINT cchMax, int *piIndex, PUINT pwFlags) PURE;
    STDMETHOD(Extract)(THIS_ LPCWSTR pszFile, UINT nIconIndex, HICON *phiconLarge, HICON *phiconSmall, UINT nIconSize) PURE;
};
#undef INTERFACE

#if !defined(__cplusplus) || defined(CINTERFACE)
/*** IUnknown methods ***/
#define IMyExtractIconW_QueryInterface(p,a,b) (p)->lpVtbl->QueryInterface(p,a,b)
#define IMyExtractIconW_AddRef(p)             (p)->lpVtbl->AddRef(p)
#define IMyExtractIconW_Release(p)            (p)->lpVtbl->Release(p)
/*** IMyExtractIconW methods ***/
#define IMyExtractIconW_GetIconLocation(p,a,b,c,d,e) (p)->lpVtbl->GetIconLocation(p,a,b,c,d,e)
#define IMyExtractIconW_Extract(p,a,b,c,d,e)         (p)->lpVtbl->Extract(p,a,b,c,d,e)
#else
/*** IUnknown methods ***/
#define IMyExtractIconW_QueryInterface(p,a,b) (p)->QueryInterface(a,b)
#define IMyExtractIconW_AddRef(p)             (p)->AddRef()
#define IMyExtractIconW_Release(p)            (p)->Release()
/*** IMyExtractIconW methods ***/
#define IMyExtractIconW_GetIconLocation(p,a,b,c,d,e) (p)->GetIconLocation(p,a,b,c,d,e)
#define IMyExtractIconW_Extract(p,a,b,c,d,e)         (p)->Extract(p,a,b,c,d,e)
#endif


#include <glib.h>
#include "gwin32volumemonitor.h"
#include "gwin32mount.h"
#include "gmount.h"
#include "gfile.h"
#include "gmountprivate.h"
#include "gvolumemonitor.h"
#include "gthemedicon.h"
#include "glibintl.h"


struct _GWin32Mount {
  GObject parent;

  GVolumeMonitor   *volume_monitor;

  GWin32Volume      *volume; /* owned by volume monitor */
  int   drive_type;

  /* why does all this stuff need to be duplicated? It is in volume already! */
  char *name;
  GIcon *icon;
  GIcon *symbolic_icon;
  char *mount_path;

  gboolean can_eject;
};

static void g_win32_mount_mount_iface_init (GMountIface *iface);

#define g_win32_mount_get_type _g_win32_mount_get_type
G_DEFINE_TYPE_WITH_CODE (GWin32Mount, g_win32_mount, G_TYPE_OBJECT,
			 G_IMPLEMENT_INTERFACE (G_TYPE_MOUNT,
						g_win32_mount_mount_iface_init))


static void
g_win32_mount_finalize (GObject *object)
{
  GWin32Mount *mount;
  
  mount = G_WIN32_MOUNT (object);

  if (mount->volume_monitor != NULL)
    g_object_unref (mount->volume_monitor);
#if 0
  if (mount->volume)
    _g_win32_volume_unset_mount (mount->volume, mount);
#endif
  /* TODO: g_warn_if_fail (volume->volume == NULL); */

  if (mount->icon != NULL)
    g_object_unref (mount->icon);
  if (mount->symbolic_icon != NULL)
    g_object_unref (mount->symbolic_icon);

  g_free (mount->name);
  g_free (mount->mount_path);
  
  if (G_OBJECT_CLASS (g_win32_mount_parent_class)->finalize)
    (*G_OBJECT_CLASS (g_win32_mount_parent_class)->finalize) (object);
}

static void
g_win32_mount_class_init (GWin32MountClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->finalize = g_win32_mount_finalize;
}

static void
g_win32_mount_init (GWin32Mount *win32_mount)
{
}

/* wdrive doesn't need to end with a path separator.
   wdrive must use backslashes as path separators, not slashes.
   IShellFolder::ParseDisplayName() takes non-const string as input,
   so wdrive can't be a const string.
   Returns the name on success (free with g_free),
   NULL otherwise.
 */
static gchar *
get_mount_display_name (gunichar2 *wdrive)
{
  IShellFolder *desktop;
  PIDLIST_RELATIVE volume;
  STRRET volume_name;
  gchar *result = NULL;

  /* Get the desktop folder object reference */
  if (!SUCCEEDED (SHGetDesktopFolder (&desktop)))
    return result;

  if (SUCCEEDED (IShellFolder_ParseDisplayName (desktop, NULL, NULL, wdrive, NULL, &volume, NULL)))
    {
      volume_name.uType = STRRET_WSTR;

      if (SUCCEEDED (IShellFolder_GetDisplayNameOf (desktop, volume, SHGDN_FORADDRESSBAR, &volume_name)))
        {
          wchar_t *volume_name_wchar;

          if (SUCCEEDED (StrRetToStrW (&volume_name, volume, &volume_name_wchar)))
            {
              result = g_utf16_to_utf8 (volume_name_wchar, -1, NULL, NULL, NULL);
              CoTaskMemFree (volume_name_wchar);
            }
        }
      CoTaskMemFree (volume);
    }

  IShellFolder_Release (desktop);

  return result;
}

static gchar *
_win32_get_displayname (const char *drive)
{
  gunichar2 *wdrive = g_utf8_to_utf16 (drive, -1, NULL, NULL, NULL);
  gchar *name = get_mount_display_name (wdrive);

  g_free (wdrive);
  return name ? name : g_strdup (drive);
}

/*
 * _g_win32_mount_new:
 * @volume_monitor: a #GVolumeMonitor.
 * @path: a win32 path.
 * @volume: usually NULL
 * 
 * Returns: a #GWin32Mount for the given win32 path.
 **/
GWin32Mount *
_g_win32_mount_new (GVolumeMonitor  *volume_monitor,
                    const char      *path,
                    GWin32Volume    *volume)
{
  GWin32Mount *mount;
  const gchar *drive = path; //fixme
  WCHAR *drive_utf16;

  drive_utf16 = g_utf8_to_utf16 (drive, -1, NULL, NULL, NULL);

#if 0  
  /* No volume for mount: Ignore internal things */
  if (volume == NULL && !g_win32_mount_guess_should_display (mount_entry))
    return NULL;
#endif

  mount = g_object_new (G_TYPE_WIN32_MOUNT, NULL);
  mount->volume_monitor = volume_monitor != NULL ? g_object_ref (volume_monitor) : NULL;
  mount->mount_path = g_strdup (path);
  mount->drive_type = GetDriveTypeW (drive_utf16);
  mount->can_eject = FALSE; /* TODO */
  mount->name = _win32_get_displayname (drive);

  /* need to do this last */
  mount->volume = volume;
#if 0
  if (volume != NULL)
    _g_win32_volume_set_mount (volume, mount);
#endif

  g_free (drive_utf16);

  return mount;
}

void
_g_win32_mount_unmounted (GWin32Mount *mount)
{
  if (mount->volume != NULL)
    {
#if 0
      _g_win32_volume_unset_mount (mount->volume, mount);
#endif
      mount->volume = NULL;
      g_signal_emit_by_name (mount, "changed");
      /* there's really no need to emit mount_changed on the volume monitor 
       * as we're going to be deleted.. */
    }
}

void
_g_win32_mount_unset_volume (GWin32Mount  *mount,
			     GWin32Volume *volume)
{
  if (mount->volume == volume)
    {
      mount->volume = NULL;
      /* TODO: Emit changed in idle to avoid locking issues */
      g_signal_emit_by_name (mount, "changed");
      if (mount->volume_monitor != NULL)
        g_signal_emit_by_name (mount->volume_monitor, "mount-changed", mount);
    }
}

static GFile *
g_win32_mount_get_root (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);

  return g_file_new_for_path (win32_mount->mount_path);
}

static const char *
_win32_drive_type_to_icon (int type, gboolean use_symbolic)
{
  switch (type)
  {
  case DRIVE_REMOVABLE : return use_symbolic ? "drive-removable-media-symbolic" : "drive-removable-media";
  case DRIVE_FIXED : return use_symbolic ? "drive-harddisk-symbolic" : "drive-harddisk";
  case DRIVE_REMOTE : return use_symbolic ? "folder-remote-symbolic" : "folder-remote";
  case DRIVE_CDROM : return use_symbolic ? "drive-optical-symbolic" : "drive-optical";
  default : return use_symbolic ? "folder-symbolic" : "folder";
  }
}

/* mount_path doesn't need to end with a path separator.
   mount_path must use backslashes as path separators, not slashes.
   IShellFolder::ParseDisplayName() takes non-const string as input,
   so mount_path can't be a const string.
   result_name and result_index must not be NULL.
   Returns TRUE when result_name is set (free with g_free),
   FALSE otherwise.
 */
static gboolean
get_icon_name_index (wchar_t  *mount_path,
                     wchar_t **result_name,
                     int      *result_index)
{
  IShellFolder *desktop;
  PIDLIST_RELATIVE volume;
  IShellFolder *volume_parent;
  PCUITEMID_CHILD volume_relative;
  IMyExtractIconW *eicon;
  int icon_index;
  UINT icon_flags;
  wchar_t *name_buffer;
  gsize name_buffer_size;
  gsize arbitrary_reasonable_limit = 5000;
  gboolean result = FALSE;

  *result_name = NULL;

  /* Get the desktop folder object reference */
  if (!SUCCEEDED (SHGetDesktopFolder (&desktop)))
    return FALSE;

  /* Construct the volume IDList relative to desktop */
  if (SUCCEEDED (IShellFolder_ParseDisplayName (desktop, NULL, NULL, mount_path, NULL, &volume, NULL)))
    {
      /* Get the parent of the volume (transfer-full) and the IDList relative to parent (transfer-none) */
      if (SUCCEEDED (SHBindToParent (volume, &IID_IShellFolder, (void **) &volume_parent, &volume_relative)))
        {
          /* Get a reference to IExtractIcon object for the volume */
          if (SUCCEEDED (IShellFolder_GetUIObjectOf (volume_parent, NULL, 1, (LPCITEMIDLIST *) &volume_relative, &IID_IExtractIconW, NULL, (void **) &eicon)))
            {
              gboolean keep_going = TRUE;
              name_buffer = NULL;
              name_buffer_size = MAX_PATH / 2;
              while (keep_going)
                {
                  name_buffer_size *= 2;
                  name_buffer = g_renew (wchar_t, name_buffer, name_buffer_size);
                  name_buffer[name_buffer_size - 1] = 0x1; /* sentinel */
                  keep_going = FALSE;

                  /* Try to get the icon location */
                  if (SUCCEEDED (IMyExtractIconW_GetIconLocation (eicon, GIL_FORSHELL, name_buffer, name_buffer_size, &icon_index, &icon_flags)))
                    {
                      if (name_buffer[name_buffer_size - 1] != 0x1)
                        {
                          if (name_buffer_size < arbitrary_reasonable_limit)
                            {
                              /* Buffer was too small, keep going */
                              keep_going = TRUE;
                              continue;
                            }
                          /* Else stop trying */
                        }
                      /* name_buffer might not contain a name */
                      else if ((icon_flags & GIL_NOTFILENAME) != GIL_NOTFILENAME)
                        {
                          *result_name = g_steal_pointer (&name_buffer);
                          *result_index = icon_index;
                          result = TRUE;
                        }
                    }
                }

              g_free (name_buffer);
              IMyExtractIconW_Release (eicon);
            }
          IShellFolder_Release (volume_parent);
        }
      CoTaskMemFree (volume);
    }

  IShellFolder_Release (desktop);

  return result;
}

static GIcon *
g_win32_mount_get_icon (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);

  g_return_val_if_fail (win32_mount->mount_path != NULL, NULL);

  /* lazy creation */
  if (!win32_mount->icon)
    {
      wchar_t *icon_path;
      int icon_index;
      wchar_t *p;
      wchar_t *wfn = g_utf8_to_utf16 (win32_mount->mount_path, -1, NULL, NULL, NULL);

      for (p = wfn; p != NULL && *p != 0; p++)
        if (*p == L'/')
          *p = L'\\';

      if (get_icon_name_index (wfn, &icon_path, &icon_index))
        {
	  gchar *id = g_strdup_printf ("%S,%i", icon_path, icon_index);
	  g_free (icon_path);
	  win32_mount->icon = g_themed_icon_new (id);
	  g_free (id);
	}
      else
        {
          win32_mount->icon = g_themed_icon_new_with_default_fallbacks (_win32_drive_type_to_icon (win32_mount->drive_type, FALSE));
	}
    }

  return g_object_ref (win32_mount->icon);
}

static GIcon *
g_win32_mount_get_symbolic_icon (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);

  g_return_val_if_fail (win32_mount->mount_path != NULL, NULL);

  /* lazy creation */
  if (!win32_mount->symbolic_icon)
    {
      win32_mount->symbolic_icon = g_themed_icon_new_with_default_fallbacks (_win32_drive_type_to_icon (win32_mount->drive_type, TRUE));
    }

  return g_object_ref (win32_mount->symbolic_icon);
}

static char *
g_win32_mount_get_uuid (GMount *mount)
{
  return NULL;
}

static char *
g_win32_mount_get_name (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);
  
  return g_strdup (win32_mount->name);
}

static GDrive *
g_win32_mount_get_drive (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);

  if (win32_mount->volume != NULL)
    return g_volume_get_drive (G_VOLUME (win32_mount->volume));

  return NULL;
}

static GVolume *
g_win32_mount_get_volume (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);

  if (win32_mount->volume)
    return G_VOLUME (g_object_ref (win32_mount->volume));
  
  return NULL;
}

static gboolean
g_win32_mount_can_unmount (GMount *mount)
{
  return FALSE;
}

static gboolean
g_win32_mount_can_eject (GMount *mount)
{
  GWin32Mount *win32_mount = G_WIN32_MOUNT (mount);
  return win32_mount->can_eject;
}


typedef struct {
  GWin32Mount *win32_mount;
  GAsyncReadyCallback callback;
  gpointer user_data;
  GCancellable *cancellable;
  int error_fd;
  GIOChannel *error_channel;
  guint error_channel_source_id;
  GString *error_string;
} UnmountEjectOp;

static void
g_win32_mount_unmount (GMount              *mount,
		       GMountUnmountFlags   flags,
		       GCancellable        *cancellable,
		       GAsyncReadyCallback  callback,
		       gpointer             user_data)
{
}

static gboolean
g_win32_mount_unmount_finish (GMount        *mount,
			      GAsyncResult  *result,
			      GError       **error)
{
  return FALSE;
}

static void
g_win32_mount_eject (GMount              *mount,
		     GMountUnmountFlags   flags,
		     GCancellable        *cancellable,
		     GAsyncReadyCallback  callback,
		     gpointer             user_data)
{
}

static gboolean
g_win32_mount_eject_finish (GMount        *mount,
			    GAsyncResult  *result,
			    GError       **error)
{
  return FALSE;
}

static void
g_win32_mount_mount_iface_init (GMountIface *iface)
{
  iface->get_root = g_win32_mount_get_root;
  iface->get_name = g_win32_mount_get_name;
  iface->get_icon = g_win32_mount_get_icon;
  iface->get_symbolic_icon = g_win32_mount_get_symbolic_icon;
  iface->get_uuid = g_win32_mount_get_uuid;
  iface->get_drive = g_win32_mount_get_drive;
  iface->get_volume = g_win32_mount_get_volume;
  iface->can_unmount = g_win32_mount_can_unmount;
  iface->can_eject = g_win32_mount_can_eject;
  iface->unmount = g_win32_mount_unmount;
  iface->unmount_finish = g_win32_mount_unmount_finish;
  iface->eject = g_win32_mount_eject;
  iface->eject_finish = g_win32_mount_eject_finish;
}
