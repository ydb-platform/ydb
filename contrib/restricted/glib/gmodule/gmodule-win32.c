/* GMODULE - GLIB wrapper code for dynamic module loading
 * Copyright (C) 1998, 2000 Tim Janik
 *
 * Win32 GMODULE implementation
 * Copyright (C) 1998 Tor Lillqvist
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
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

/* 
 * MT safe
 */
#include <contrib/restricted/glib/config.h>

#include <glib.h>
#include <stdio.h>
#include <windows.h>

#include <tlhelp32.h>

#ifdef G_WITH_CYGWIN
#error #include <sys/cygwin.h>
#endif

static void G_GNUC_PRINTF (2, 3)
set_error (GError      **error,
           const gchar  *format,
           ...)
{
  gchar *win32_error;
  gchar *detail;
  gchar *message;
  va_list args;

  win32_error = g_win32_error_message (GetLastError ());

  va_start (args, format);
  detail = g_strdup_vprintf (format, args);
  va_end (args);

  message = g_strconcat (detail, win32_error, NULL);

  g_module_set_error (message);
  g_set_error_literal (error, G_MODULE_ERROR, G_MODULE_ERROR_FAILED, message);

  g_free (message);
  g_free (detail);
  g_free (win32_error);
}

/* --- functions --- */
static gpointer
_g_module_open (const gchar *file_name,
		gboolean     bind_lazy,
		gboolean     bind_local,
                GError     **error)
{
  HINSTANCE handle;
  wchar_t *wfilename;
  DWORD old_mode;
  BOOL success;
#ifdef G_WITH_CYGWIN
  gchar tmp[MAX_PATH];

  cygwin_conv_to_win32_path(file_name, tmp);
  file_name = tmp;
#endif
  wfilename = g_utf8_to_utf16 (file_name, -1, NULL, NULL, NULL);

  /* suppress error dialog */
  success = SetThreadErrorMode (SEM_NOOPENFILEERRORBOX | SEM_FAILCRITICALERRORS, &old_mode);
  if (!success)
    set_error (error, "");

  /* When building for UWP, load app asset DLLs instead of filesystem DLLs.
   * Needs MSVC, Windows 8 and newer, and is only usable from apps. */
#if _WIN32_WINNT >= 0x0602 && defined(G_WINAPI_ONLY_APP)
  handle = LoadPackagedLibrary (wfilename, 0);
#else
  handle = LoadLibraryW (wfilename);
#endif

  if (success)
    SetThreadErrorMode (old_mode, NULL);
  g_free (wfilename);
      
  if (!handle)
    set_error (error, "'%s': ", file_name);

  return handle;
}

static gint dummy;
static gpointer null_module_handle = &dummy;
  
static gpointer
_g_module_self (void)
{
  return null_module_handle;
}

static void
_g_module_close (gpointer handle)
{
  if (handle != null_module_handle)
    if (!FreeLibrary (handle))
      set_error (NULL, "");
}

static gpointer
find_in_any_module_using_toolhelp (const gchar *symbol_name)
{
  HANDLE snapshot; 
  MODULEENTRY32 me32;

  gpointer p = NULL;

  /* Under UWP, Module32Next and Module32First are not available since we're
   * not allowed to search in the address space of arbitrary loaded DLLs */
#if !defined(G_WINAPI_ONLY_APP)
  /* https://docs.microsoft.com/en-us/windows/win32/api/tlhelp32/nf-tlhelp32-createtoolhelp32snapshot#remarks
   * If the function fails with ERROR_BAD_LENGTH, retry the function until it succeeds. */
  while (TRUE)
    {
      snapshot = CreateToolhelp32Snapshot (TH32CS_SNAPMODULE, 0);
      if (snapshot == INVALID_HANDLE_VALUE && GetLastError () == ERROR_BAD_LENGTH)
        {
          g_thread_yield ();
          continue;
        }
      break;
    }

  if (snapshot == INVALID_HANDLE_VALUE)
    return NULL;

  me32.dwSize = sizeof (me32);
  p = NULL;
  if (Module32First (snapshot, &me32))
    {
      do {
	if ((p = GetProcAddress (me32.hModule, symbol_name)) != NULL)
	  break;
      } while (Module32Next (snapshot, &me32));
    }

  CloseHandle (snapshot);
#endif

  return p;
}

static gpointer
find_in_any_module (const gchar *symbol_name)
{
  gpointer result;

  if ((result = find_in_any_module_using_toolhelp (symbol_name)) == NULL)
    return NULL;
  else
    return result;
}

static gpointer
_g_module_symbol (gpointer     handle,
		  const gchar *symbol_name)
{
  gpointer p;
  
  if (handle == null_module_handle)
    {
      if ((p = GetProcAddress (GetModuleHandle (NULL), symbol_name)) == NULL)
	p = find_in_any_module (symbol_name);
    }
  else
    p = GetProcAddress (handle, symbol_name);

  if (!p)
    set_error (NULL, "");

  return p;
}

static gchar*
_g_module_build_path (const gchar *directory,
		      const gchar *module_name)
{
  gint k;

  k = strlen (module_name);
    
  if (directory && *directory)
    if (k > 4 && g_ascii_strcasecmp (module_name + k - 4, ".dll") == 0)
      return g_strconcat (directory, G_DIR_SEPARATOR_S, module_name, NULL);
#ifdef G_WITH_CYGWIN
    else if (strncmp (module_name, "lib", 3) == 0 || strncmp (module_name, "cyg", 3) == 0)
      return g_strconcat (directory, G_DIR_SEPARATOR_S, module_name, ".dll", NULL);
    else
      return g_strconcat (directory, G_DIR_SEPARATOR_S, "cyg", module_name, ".dll", NULL);
#else
    else if (strncmp (module_name, "lib", 3) == 0)
      return g_strconcat (directory, G_DIR_SEPARATOR_S, module_name, ".dll", NULL);
    else
      return g_strconcat (directory, G_DIR_SEPARATOR_S, "lib", module_name, ".dll", NULL);
#endif
  else if (k > 4 && g_ascii_strcasecmp (module_name + k - 4, ".dll") == 0)
    return g_strdup (module_name);
#ifdef G_WITH_CYGWIN
  else if (strncmp (module_name, "lib", 3) == 0 || strncmp (module_name, "cyg", 3) == 0)
    return g_strconcat (module_name, ".dll", NULL);
  else
    return g_strconcat ("cyg", module_name, ".dll", NULL);
#else
  else if (strncmp (module_name, "lib", 3) == 0)
    return g_strconcat (module_name, ".dll", NULL);
  else
    return g_strconcat ("lib", module_name, ".dll", NULL);
#endif
}
