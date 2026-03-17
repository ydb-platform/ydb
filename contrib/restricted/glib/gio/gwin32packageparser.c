/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright (C) 2020 Руслан Ижбулатов <lrn1986@gmail.com>
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
 */

/* Queries the system (Windows 8 or newer) for the list
 * of UWP packages, parses their manifests and invokes
 * a user-provided callback with the needed application
 * info.
 */

#include <contrib/restricted/glib/config.h>

#define COBJMACROS
#define INITGUID
#include <windows.h>
#include <winstring.h>
#include <roapi.h>
#include <stdio.h>
#include <shlwapi.h>

#include "gwin32api-storage.h"
#include "gwin32api-misc.h"
#include "gwin32api-iterator.h"
#include "gwin32api-package.h"

#include <xmllite.h>

#include <glib.h>

#include "gwin32file-sync-stream.h"
#include "gwin32packageparser.h"

#ifdef G_WINAPI_ONLY_APP
#define LoadedRoActivateInstance RoActivateInstance
#define LoadedWindowsCreateStringReference WindowsCreateStringReference
#define LoadedWindowsDeleteString WindowsDeleteString
#define sax_WindowsGetStringRawBuffer WindowsGetStringRawBuffer
#define LoadedWindowsGetStringRawBuffer WindowsGetStringRawBuffer
#define sax_CreateXmlReader CreateXmlReader
#else
typedef HRESULT (WINAPI *RoActivateInstance_func)(HSTRING activatableClassId, IInspectable **instance);
typedef HRESULT (WINAPI *WindowsCreateStringReference_func)(PCWSTR sourceString, UINT32 length, HSTRING_HEADER *hstringHeader, HSTRING *string);
typedef HRESULT (WINAPI *WindowsDeleteString_func)(HSTRING string);
typedef PCWSTR (WINAPI *WindowsGetStringRawBuffer_func)(HSTRING string, UINT32 *length);
typedef HRESULT (STDAPICALLTYPE *CreateXmlReader_func)(REFIID riid, void **ppvObject, IMalloc *pMalloc);
#define sax_WindowsGetStringRawBuffer sax->WindowsGetStringRawBuffer
#define sax_CreateXmlReader sax->CreateXmlReader
#endif

static gsize
g_utf16_len (const gunichar2 *str)
{
  gsize result;

  for (result = 0; str[0] != 0; str++, result++)
    ;

  return result;
}

static gunichar2 *
g_wcsdup (const gunichar2 *str, gssize str_len)
{
  gsize str_len_unsigned;
  gsize str_size;

  g_return_val_if_fail (str != NULL, NULL);

  if (str_len < 0)
    str_len_unsigned = g_utf16_len (str);
  else
    str_len_unsigned = (gsize) str_len;

  g_assert (str_len_unsigned <= G_MAXSIZE / sizeof (gunichar2) - 1);
  str_size = (str_len_unsigned + 1) * sizeof (gunichar2);

  return g_memdup2 (str, str_size);
}

static BOOL
WIN32_FROM_HRESULT (HRESULT hresult,
                    DWORD  *win32_error_code)
{
  if ((hresult & (HRESULT) 0xFFFF0000) == MAKE_HRESULT (SEVERITY_ERROR, FACILITY_WIN32, 0) ||
      hresult == S_OK)
    {
      *win32_error_code = HRESULT_CODE (hresult);
      return TRUE;
    }

  return FALSE;
}

static GIOErrorEnum
gio_error_from_hresult (HRESULT hresult)
{
  DWORD error;

  if (WIN32_FROM_HRESULT (hresult, &error))
    return g_io_error_from_errno (error);

  return G_IO_ERROR_FAILED;
}

static void
free_extgroup (GWin32PackageExtGroup *g)
{
  g_ptr_array_unref (g->verbs);
  g_ptr_array_unref (g->extensions);
  g_free (g);
}

struct _xml_sax_state
{
#ifndef G_WINAPI_ONLY_APP
  WindowsGetStringRawBuffer_func WindowsGetStringRawBuffer;
  CreateXmlReader_func CreateXmlReader;
#endif

  GWin32PackageParserCallback callback;
  gpointer user_data;

  const wchar_t *manifest_filename;
  gsize          package_index;
  const wchar_t *wcs_full_name;
  const wchar_t *wcs_name;
  HSTRING        package_family;

  gboolean       applist;
  gboolean       exit_early;

  UINT64         in_package;
  UINT64         in_applications;
  UINT64         in_application;
  UINT64         in_extensions;
  UINT64         in_extension_protocol;
  UINT64         in_extension_fta;
  UINT64         in_fta_group;
  UINT64         in_sfp;
  UINT64         in_filetype;
  UINT64         in_sv;
  GPtrArray     *supported_extensions;
  GPtrArray     *supported_protocols;
  GPtrArray     *supported_verbs;
  GPtrArray     *supported_extgroups;
  wchar_t       *application_usermodelid;
};

static gboolean parse_manifest_file          (struct _xml_sax_state  *sax);
static gboolean xml_parser_iteration         (struct _xml_sax_state  *sax,
                                              IXmlReader             *xml_reader);
static gboolean xml_parser_get_current_state (struct _xml_sax_state  *sax,
                                              IXmlReader             *xml_reader,
                                              const wchar_t         **local_name,
                                              const wchar_t         **prefix,
                                              const wchar_t         **value);

gboolean
g_win32_package_parser_enum_packages (GWin32PackageParserCallback   callback,
                                      gpointer                      user_data,
                                      GError                      **error)
{
  gboolean result = TRUE;
  HRESULT hr;
  HSTRING packagemanager_name = NULL;
  HSTRING_HEADER packageanager_name_header;
  const wchar_t *packman_id = L"Windows.Management.Deployment.PackageManager";
  IInspectable *ii_pm = NULL;
  IPackageManager *pm = NULL;
  IIterable *packages_iterable = NULL;
  IIterator *packages_iterator = NULL;
  CHAR has_current;
  gsize package_index;
  const wchar_t *bslash_appmanifest = L"\\AppxManifest.xml";
  struct _xml_sax_state sax_stack;
  struct _xml_sax_state *sax = &sax_stack;

#ifndef G_WINAPI_ONLY_APP
  HMODULE xmllite = NULL;
  HMODULE combase = NULL;
  HMODULE winrt = NULL;
  RoActivateInstance_func LoadedRoActivateInstance;
  WindowsCreateStringReference_func LoadedWindowsCreateStringReference;
  WindowsDeleteString_func LoadedWindowsDeleteString;
  WindowsGetStringRawBuffer_func LoadedWindowsGetStringRawBuffer;
  CreateXmlReader_func LoadedCreateXmlReader;

  winrt = LoadLibraryW (L"api-ms-win-core-winrt-l1-1-0.dll");
  if (winrt == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to load api-ms-win-core-winrt-l1-1-0.dll");
      goto cleanup;
    }

  combase = LoadLibraryW (L"combase.dll");
  if (combase == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to load combase.dll");
      goto cleanup;
    }

  xmllite = LoadLibraryW (L"xmllite.dll");
  if (xmllite == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to load xmllite.dll");
      goto cleanup;
    }

  LoadedCreateXmlReader = (CreateXmlReader_func) GetProcAddress (xmllite, "CreateXmlReader");
  if (LoadedCreateXmlReader == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "CreateXmlReader entry point is not found in xmllite.dll");
      goto cleanup;
    }

  LoadedRoActivateInstance = (RoActivateInstance_func) GetProcAddress (winrt, "RoActivateInstance");
  if (LoadedRoActivateInstance == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "RoActivateInstance entry point is not found in api-ms-win-core-winrt-l1-1-0.dll");
      goto cleanup;
    }

  LoadedWindowsCreateStringReference = (WindowsCreateStringReference_func) GetProcAddress (combase, "WindowsCreateStringReference");
  if (LoadedWindowsCreateStringReference == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "WindowsCreateStringReference entry point is not found in combase.dll");
      goto cleanup;
    }

  LoadedWindowsDeleteString = (WindowsDeleteString_func) GetProcAddress (combase, "WindowsDeleteString");
  if (LoadedWindowsDeleteString == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "WindowsDeleteString entry point is not found in combase.dll");
      goto cleanup;
    }

  LoadedWindowsGetStringRawBuffer = (WindowsGetStringRawBuffer_func) GetProcAddress (combase, "WindowsGetStringRawBuffer");
  if (LoadedWindowsGetStringRawBuffer == NULL)
    {
      result = FALSE;
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "WindowsGetStringRawBuffer entry point is not found in combase.dll");
      goto cleanup;
    }
#endif

  /* This essentially locks current GLib thread into apartment COM model. */
  hr = CoInitializeEx (NULL, COINIT_APARTMENTTHREADED);
  /* Can return S_FALSE if COM is already initialized,
   * which is not an error, and we still need to uninitialize after that.
   */
  if (hr != S_OK && hr != S_FALSE)
    {
      result = FALSE;
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_NOT_INITIALIZED,
                   "CoInitializeEx(COINIT_APARTMENTTHREADED) failed with code 0x%lx", hr);
      goto cleanup;
    }

#define canned_com_error_handler(function_name_literal, where_to_go) \
  do \
  { \
    if (FAILED (hr)) \
      { \
        result = FALSE; \
        g_set_error (error, G_IO_ERROR, gio_error_from_hresult (hr), \
                     function_name_literal " failed with code 0x%lx", hr); \
        goto where_to_go; \
      } \
  } while (0)

  hr = LoadedWindowsCreateStringReference (packman_id, wcslen (packman_id), &packageanager_name_header, &packagemanager_name);
  canned_com_error_handler ("WindowsCreateStringReference()", cleanup);

  hr = LoadedRoActivateInstance (packagemanager_name, &ii_pm);
  canned_com_error_handler ("RoActivateInstance()", cleanup);

  hr = IInspectable_QueryInterface (ii_pm, &IID_IPackageManager, (void**) &pm);
  canned_com_error_handler ("IInspectable_QueryInterface()", cleanup);

  hr = IPackageManager_FindPackagesByUserSecurityId (pm, 0, &packages_iterable);
  canned_com_error_handler ("IPackageManager_FindPackagesByUserSecurityId()", cleanup);

  hr = IIterable_First (packages_iterable, &packages_iterator);
  canned_com_error_handler ("IIterable_First()", cleanup);

  hr = IIterator_get_HasCurrent (packages_iterator, &has_current);
  canned_com_error_handler ("IIterator_get_HasCurrent()", cleanup);

  for (package_index = 0; SUCCEEDED (hr) && has_current; package_index++)
    {
      IUnknown *item = NULL;
      IPackage *ipackage = NULL;
      IPackageId *ipackageid = NULL;
      IUnknown *package_install_location = NULL;
      IStorageItem *storage_item = NULL;
      HSTRING path = NULL;
      HSTRING name = NULL;
      HSTRING full_name = NULL;
      HSTRING package_family = NULL;
      size_t manifest_filename_size;
      const wchar_t *wcs_path;
      const wchar_t *wcs_full_name;
      const wchar_t *wcs_name;
      wchar_t *manifest_filename = NULL;

#define canned_com_error_handler_pkg(function_name_literal, where_to_go) \
      do \
      { \
        if (FAILED (hr)) \
          { \
            result = FALSE; \
            g_set_error (error, G_IO_ERROR, gio_error_from_hresult (hr), \
                         function_name_literal " for package #%zu failed with code 0x%lx", package_index, hr); \
            goto where_to_go; \
          } \
      } while (0)

      hr = IIterator_get_Current (packages_iterator, &item);
      canned_com_error_handler_pkg ("IIterator_get_Current()", package_cleanup);

      hr = IUnknown_QueryInterface (item, &IID_IPackage, (void **) &ipackage);
      canned_com_error_handler_pkg ("IUnknown_QueryInterface(IID_IPackage)", package_cleanup);

      hr = IPackage_get_Id (ipackage, &ipackageid);
      canned_com_error_handler_pkg ("IPackage_get_Id()", package_cleanup);

      hr = IPackageId_get_FullName (ipackageid, &full_name);
      canned_com_error_handler_pkg ("IPackageId_get_FullName()", package_cleanup);

      hr = IPackageId_get_Name (ipackageid, &name);
      canned_com_error_handler_pkg ("IPackageId_get_Name()", package_cleanup);

      wcs_full_name = LoadedWindowsGetStringRawBuffer (full_name, NULL);
      wcs_name = LoadedWindowsGetStringRawBuffer (name, NULL);

#define canned_com_error_handler_pkg_named(function_name_literal, where_to_go) \
      do \
      { \
        if (FAILED (hr)) \
          { \
            result = FALSE; \
            g_set_error (error, G_IO_ERROR, gio_error_from_hresult (hr), \
                         function_name_literal " for package #%zu (`%S') failed with code 0x%lx", package_index, wcs_full_name, hr); \
            goto where_to_go; \
          } \
      } while (0)

      hr = IPackage_get_InstalledLocation (ipackage, &package_install_location);
      canned_com_error_handler_pkg_named ("IPackage_get_InstalledLocation()", package_cleanup);

      hr = IUnknown_QueryInterface (package_install_location, &IID_IStorageItem, (void **) &storage_item);
      canned_com_error_handler_pkg_named ("IUnknown_QueryInterface(IID_IStorageItem)", package_cleanup);

      hr = IPackageId_get_FamilyName (ipackageid, &package_family);
      canned_com_error_handler_pkg_named ("IPackageId_get_FamilyName()", package_cleanup);

      hr = IStorageItem_get_Path (storage_item, &path);
      canned_com_error_handler_pkg_named ("IStorageItem_get_Path()", package_cleanup);

      wcs_path = LoadedWindowsGetStringRawBuffer (path, NULL);
      manifest_filename_size = wcslen (wcs_path) + wcslen (bslash_appmanifest);
      manifest_filename = g_new (wchar_t, manifest_filename_size + 1);
      memcpy (manifest_filename, wcs_path, wcslen (wcs_path) * sizeof (wchar_t));
      memcpy (&manifest_filename[wcslen (wcs_path)], bslash_appmanifest, (wcslen (bslash_appmanifest) + 1) * sizeof (wchar_t));

      memset (sax, 0, sizeof (*sax));
      sax->callback = callback;
      sax->user_data = user_data;
      sax->manifest_filename = manifest_filename;
      sax->package_index = package_index;
      sax->wcs_full_name = wcs_full_name;
      sax->wcs_name = wcs_name;
      sax->package_family = package_family;
      sax->applist = TRUE;
      sax->exit_early = FALSE;
#ifndef G_WINAPI_ONLY_APP
      sax->CreateXmlReader = LoadedCreateXmlReader;
      sax->WindowsGetStringRawBuffer = LoadedWindowsGetStringRawBuffer;
#endif
      /* Result isn't checked. If we fail to parse the manifest,
       * just try the next package, no need to bail out.
       */
      parse_manifest_file (sax);

      hr = IIterator_MoveNext (packages_iterator, &has_current);
      canned_com_error_handler_pkg_named ("IIterator_MoveNext()", package_cleanup);

#undef canned_com_error_handler_pkg_named
#undef canned_com_error_handler_pkg
#undef canned_com_error_handler

      package_cleanup:
      g_clear_pointer (&manifest_filename, g_free);

      if (path)
        LoadedWindowsDeleteString (path);
      if (storage_item)
        (void) IStorageItem_Release (storage_item);
      if (package_install_location)
        (void) IUnknown_Release (package_install_location);
      if (ipackage)
        (void) IPackage_Release (ipackage);
      if (item)
        (void) IUnknown_Release (item);

      if (package_family)
        LoadedWindowsDeleteString (package_family);
      if (name)
        LoadedWindowsDeleteString (name);
      if (full_name)
        LoadedWindowsDeleteString (full_name);

      if (ipackageid)
        (void) IPackageId_Release (ipackageid);
      if (sax->exit_early)
        break;
    }

  cleanup:
  if (packages_iterator)
    (void) IIterator_Release (packages_iterator);
  if (packages_iterable)
    (void) IIterable_Release (packages_iterable);
  if (pm)
    (void) IPackageManager_Release (pm);
  if (ii_pm)
    (void) IInspectable_Release (ii_pm);

  CoUninitialize ();

#ifndef G_WINAPI_ONLY_APP
  if (xmllite)
    (void) FreeLibrary (xmllite);
  if (combase)
    (void) FreeLibrary (combase);
  if (winrt)
    (void) FreeLibrary (winrt);
#endif

  return result;
}

static gboolean
parse_manifest_file (struct _xml_sax_state *sax)
{
  HRESULT hr;
  HANDLE file_handle = INVALID_HANDLE_VALUE;
  IStream *file_stream = NULL;
  gboolean result;
  IXmlReader *xml_reader;

  file_handle = CreateFileW (sax->manifest_filename, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
  if (file_handle == INVALID_HANDLE_VALUE)
    {
      g_warning ("Failed to open application manifest `%S' for package #%zu (`%S'): error code 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, GetLastError ());
      return FALSE;
    }

  file_stream = g_win32_file_sync_stream_new (file_handle, TRUE, STGM_READ | STGM_SHARE_DENY_WRITE, &hr);
  if (file_stream == NULL)
    {
      g_warning ("Failed to create an IStream for application manifest `%S' for package #%zu (`%S'): HRESULT 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, hr);
      CloseHandle (file_handle);
      return FALSE;
    }

  /* file_stream owns it now */
  file_handle = NULL;

  hr = sax_CreateXmlReader (&IID_IXmlReader, (void **) &xml_reader, NULL);
  /* Slightly incorrect - xml reader is not created for any particular file,
   * in theory we could re-use the same xml reader instance for all files.
   */
  if (FAILED (hr))
    {
      g_warning ("CreateXmlReader() for application manifest `%S' for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, hr);
      (void) IStream_Release (file_stream);
      return FALSE;
    }

  hr = IXmlReader_SetInput (xml_reader, (IUnknown *) file_stream);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_SetInput() for application manifest `%S' for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, hr);
      (void) IXmlReader_Release (xml_reader);
      (void) IStream_Release (file_stream);
      return FALSE;
    }

  sax->supported_extensions = g_ptr_array_new_full (0, (GDestroyNotify) g_free);
  sax->supported_protocols = g_ptr_array_new_full (0, (GDestroyNotify) g_free);
  sax->supported_verbs = g_ptr_array_new_full (0, (GDestroyNotify) g_free);
  sax->supported_extgroups = g_ptr_array_new_full (0, (GDestroyNotify) free_extgroup);

  result = TRUE;

  while (!sax->exit_early && result && !IXmlReader_IsEOF (xml_reader))
    result = xml_parser_iteration (sax, xml_reader);

  g_clear_pointer (&sax->application_usermodelid, g_free);
  g_clear_pointer (&sax->supported_extensions, g_ptr_array_unref);
  g_clear_pointer (&sax->supported_verbs, g_ptr_array_unref);
  g_clear_pointer (&sax->supported_extgroups, g_ptr_array_unref);
  g_clear_pointer (&sax->supported_protocols, g_ptr_array_unref);

  (void) IXmlReader_Release (xml_reader);
  (void) IStream_Release (file_stream);

  return result;
}

static gboolean
xml_parser_get_current_state (struct _xml_sax_state  *sax,
                              IXmlReader             *xml_reader,
                              const wchar_t         **local_name,
                              const wchar_t         **prefix,
                              const wchar_t         **value)
{
  HRESULT hr;
  UINT xml_line_number;
  UINT xml_line_position;

  hr = IXmlReader_GetLineNumber (xml_reader, &xml_line_number);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_GetLineNumber() for application manifest `%S' for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, hr);
      return FALSE;
    }

  hr = IXmlReader_GetLinePosition (xml_reader, &xml_line_position);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_GetLinePosition() for application manifest `%S' for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, hr);
      return FALSE;
    }

  hr = IXmlReader_GetLocalName (xml_reader, local_name, NULL);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_GetLocalName() for application manifest `%S':%u (column %u) for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, xml_line_number, xml_line_position, sax->package_index, sax->wcs_full_name, hr);
      return FALSE;
    }

  hr = IXmlReader_GetPrefix (xml_reader, prefix, NULL);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_GetPrefix() for application manifest `%S':%u (column %u) for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, xml_line_number, xml_line_position, sax->package_index, sax->wcs_full_name, hr);
      return FALSE;
    }

  hr = IXmlReader_GetValue (xml_reader, value, NULL);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_GetValue() for application manifest `%S':%u (column %u) for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, xml_line_number, xml_line_position, sax->package_index, sax->wcs_full_name, hr);
      return FALSE;
    }

  return TRUE;
}

static gboolean
xml_parser_iteration (struct _xml_sax_state  *sax,
                      IXmlReader             *xml_reader)
{
  HRESULT hr;
  XmlNodeType xml_node_type;
  const wchar_t *local_name;
  const wchar_t *prefix;
  const wchar_t *value;
  BOOL is_visual_elements = FALSE;
  BOOL is_extension = FALSE;
  BOOL is_protocol = FALSE;
  BOOL is_empty;
  BOOL is_application = FALSE;
  BOOL is_verb = FALSE;

  hr = IXmlReader_Read (xml_reader, &xml_node_type);
  if (FAILED (hr))
    {
      g_warning ("IXmlReader_Read() for application manifest `%S' for package #%zu (`%S') failed with HRESULT 0x%lx",
                 sax->manifest_filename, sax->package_index, sax->wcs_full_name, hr);
      return FALSE;
    }

  if (!xml_parser_get_current_state (sax, xml_reader, &local_name, &prefix, &value))
    return FALSE;

  switch (xml_node_type)
    {
    case XmlNodeType_Element:
      is_empty = IXmlReader_IsEmptyElement (xml_reader);
      g_assert (local_name != NULL);

      if (!is_empty &&
          _wcsicmp (local_name, L"Package") == 0 &&
          prefix[0] == 0)
        sax->in_package += 1;
      else if (!is_empty &&
               sax->in_package == 1 &&
               _wcsicmp (local_name, L"Applications") == 0 &&
               prefix[0] == 0)
        sax->in_applications += 1;
      else if (!is_empty &&
               sax->in_applications == 1 &&
               _wcsicmp (local_name, L"Application") == 0 &&
               prefix[0] == 0)
        {
          sax->in_application += 1;
          is_application = TRUE;
          sax->applist = TRUE;
          g_clear_pointer (&sax->application_usermodelid, g_free);
        }
      else if (sax->in_application == 1 &&
               _wcsicmp (local_name, L"VisualElements") == 0 &&
               (_wcsicmp (prefix, L"uap") == 0 || _wcsicmp (prefix, L"uap3") == 0))
        is_visual_elements = TRUE;
      else if (!is_empty &&
               sax->in_application == 1 &&
               _wcsicmp (local_name, L"Extensions") == 0 &&
               prefix[0] == 0)
        sax->in_extensions += 1;
      else if (!is_empty &&
               sax->in_application == 1 &&
               _wcsicmp (local_name, L"Extension") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        is_extension = TRUE;
      else if (sax->in_extension_protocol == 1 &&
               _wcsicmp (local_name, L"Protocol") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        is_protocol = TRUE;
      else if (!is_empty &&
               sax->in_extension_fta == 1 &&
               _wcsicmp (local_name, L"FileTypeAssociation") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_fta_group += 1;
      else if (!is_empty &&
               sax->in_fta_group == 1 &&
               _wcsicmp (local_name, L"SupportedFileTypes") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_sfp += 1;
      else if (!is_empty &&
               sax->in_fta_group == 1 &&
               _wcsicmp (local_name, L"SupportedVerbs") == 0 &&
               _wcsicmp (prefix, L"uap2") == 0)
        sax->in_sv += 1;
      else if (!is_empty &&
               sax->in_sfp == 1 &&
               _wcsicmp (local_name, L"FileType") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_filetype += 1;
      else if (!is_empty &&
               sax->in_sv == 1 &&
               _wcsicmp (local_name, L"Verb") == 0 &&
               _wcsicmp (prefix, L"uap3") == 0)
        is_verb = TRUE;

      hr = IXmlReader_MoveToFirstAttribute (xml_reader);
      while (hr == S_OK)
        {
          if (!xml_parser_get_current_state (sax, xml_reader, &local_name, &prefix, &value))
            return FALSE;

          g_assert (local_name != NULL);
          g_assert (value != NULL);
          g_assert (prefix != NULL);

          if (is_application &&
              sax->application_usermodelid == NULL &&
              _wcsicmp (local_name, L"Id") == 0)
            {
              size_t id_len = 0;
              size_t value_len = wcslen (value);
              const wchar_t *wcs_package_family;
              size_t wcs_package_family_len;

              wcs_package_family = sax_WindowsGetStringRawBuffer (sax->package_family, NULL);
              wcs_package_family_len = wcslen (wcs_package_family);
              id_len += wcs_package_family_len + 1 + value_len;
              sax->application_usermodelid = g_new (wchar_t, id_len + 1);
              /* AppUserModelId = <family>!<id> */
              memcpy (&sax->application_usermodelid[0], wcs_package_family, wcs_package_family_len * sizeof (wchar_t));
              memcpy (&sax->application_usermodelid[wcs_package_family_len], L"!", sizeof (wchar_t));
              memcpy (&sax->application_usermodelid[wcs_package_family_len + 1], value, (value_len + 1) * sizeof (wchar_t));
            }
          else if (is_visual_elements &&
                   _wcsicmp (local_name, L"AppListEntry") == 0 &&
                   _wcsicmp (value, L"none") == 0)
            sax->applist = FALSE;
          else if (is_extension &&
                   _wcsicmp (local_name, L"Category") == 0 &&
                   _wcsicmp (value, L"windows.protocol") == 0)
            sax->in_extension_protocol += 1;
          else if (is_extension &&
                   _wcsicmp (local_name, L"Category") == 0 &&
                   _wcsicmp (value, L"windows.fileTypeAssociation") == 0)
            sax->in_extension_fta += 1;
          else if (is_protocol &&
                   _wcsicmp (local_name, L"Name") == 0)
            g_ptr_array_add (sax->supported_protocols, g_wcsdup (value, -1));
          else if (is_verb &&
                   _wcsicmp (local_name, L"Id") == 0)
            g_ptr_array_add (sax->supported_verbs, g_wcsdup (value, -1));

          hr = IXmlReader_MoveToNextAttribute (xml_reader);
        }
      break;
    case XmlNodeType_Text:
      g_assert (value != NULL);

      if (sax->in_filetype && value[0] != 0)
        g_ptr_array_add (sax->supported_extensions, g_wcsdup (value, -1));
      break;
    case XmlNodeType_EndElement:
      g_assert (local_name != NULL);

      if (_wcsicmp (local_name, L"Package") == 0 &&
          prefix[0] == 0)
        sax->in_package -= 1;
      else if (sax->in_package == 1 &&
               _wcsicmp (local_name, L"Applications") == 0 &&
               prefix[0] == 0)
        sax->in_applications -= 1;
      else if (sax->in_application == 1 &&
               _wcsicmp (local_name, L"Extensions") == 0 &&
               prefix[0] == 0)
        sax->in_extensions -= 1;
      else if (sax->in_extension_protocol == 1 &&
               _wcsicmp (local_name, L"Extension") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_extension_protocol -= 1;
      else if (sax->in_extension_fta == 1 &&
               _wcsicmp (local_name, L"Extension") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_extension_fta -= 1;
      else if (sax->in_fta_group == 1 &&
               _wcsicmp (local_name, L"SupportedFileTypes") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_sfp -= 1;
      else if (sax->in_sfp == 1 &&
               _wcsicmp (local_name, L"FileType") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        sax->in_filetype -= 1;
      else if (sax->in_fta_group == 1 &&
               _wcsicmp (local_name, L"SupportedVerbs") == 0 &&
               _wcsicmp (prefix, L"uap2") == 0)
        sax->in_sv -= 1;
      else if (sax->in_applications == 1 &&
               _wcsicmp (local_name, L"Application") == 0 &&
               prefix[0] == 0)
        {
          if (sax->application_usermodelid != NULL)
            sax->exit_early = !sax->callback (sax->user_data, sax->wcs_full_name, sax->wcs_name,
                                              sax->application_usermodelid, sax->applist,
                                              sax->supported_extgroups, sax->supported_protocols);
          g_clear_pointer (&sax->supported_extgroups, g_ptr_array_unref);
          g_clear_pointer (&sax->supported_protocols, g_ptr_array_unref);
          sax->supported_protocols = g_ptr_array_new_full (0, (GDestroyNotify) g_free);
          sax->supported_extgroups = g_ptr_array_new_full (0, (GDestroyNotify) free_extgroup);
          sax->in_application -= 1;
        }
      else if (sax->in_extension_fta == 1 &&
               _wcsicmp (local_name, L"FileTypeAssociation") == 0 &&
               _wcsicmp (prefix, L"uap") == 0)
        {
          GWin32PackageExtGroup *new_group = g_new0 (GWin32PackageExtGroup, 1);
          new_group->extensions = g_steal_pointer (&sax->supported_extensions);
          sax->supported_extensions = g_ptr_array_new_full (0, (GDestroyNotify) g_free);
          new_group->verbs = g_steal_pointer (&sax->supported_verbs);
          sax->supported_verbs  = g_ptr_array_new_full (0, (GDestroyNotify) g_free);
          g_ptr_array_add (sax->supported_extgroups, new_group);
          sax->in_fta_group -= 1;
        }
      break;
    default:
      break;
    }

  return TRUE;
}
