/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright (C) 2018 Руслан Ижбулатов
 * Copyright (C) 2022 Red Hat, Inc.
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
 * Author: Руслан Ижбулатов <lrn1986@gmail.com>
 */

#include <contrib/restricted/glib/config.h>

#include "gwin32sid.h"
#include "gioerror.h"

#include <sddl.h>

/**
 * _g_win32_sid_replace: (skip)
 * @dest: A pointer to a SID storage
 * @src: Existing SID
 * @error: return location for a #GError, or %NULL
 *
 * Creates a copy of the @src SID and puts that into @dest, after freeing
 * existing SID in @dest (if any).
 *
 * The @src SID must be valid (use IsValidSid() to ensure that).
 *
 * Returns: TRUE on success, FALSE otherwise
 */
static gboolean
_g_win32_sid_replace (SID **dest,
                      SID  *src,
                      GError **error)
{
  DWORD sid_len;
  SID *new_sid;

  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);
  g_return_val_if_fail (src != NULL, FALSE);
  g_return_val_if_fail (dest && *dest == NULL, FALSE);

  sid_len = GetLengthSid (src);
  new_sid = g_malloc (sid_len);

  if (!CopySid (sid_len, new_sid, src))
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to copy SID");

      g_free (new_sid);
      return FALSE;
    }
  else
    {
      g_free (*dest);
      *dest = g_steal_pointer (&new_sid);

      return TRUE;
    }
}

/**
 * _g_win32_token_get_sid: (skip)
 * @token: A handle of an access token
 * @error: return location for a #GError, or %NULL
 *
 * Gets user SID of the @token and returns a copy of that SID.
 *
 * Returns: A newly-allocated SID, or NULL in case of an error.
 *          Free the returned SID with g_free().
 */
static SID *
_g_win32_token_get_sid (HANDLE token,
                        GError **error)
{
  TOKEN_USER *token_user = NULL;
  DWORD n;
  PSID psid;
  SID *result = NULL;

  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (!GetTokenInformation (token, TokenUser, NULL, 0, &n)
      && GetLastError () != ERROR_INSUFFICIENT_BUFFER)
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to GetTokenInformation");

      return NULL;
    }

  token_user = g_alloca (n);

  if (!GetTokenInformation (token, TokenUser, token_user, n, &n))
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to GetTokenInformation");

      return NULL;
    }

  psid = token_user->User.Sid;

  if (!IsValidSid (psid))
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Invalid SID token");

      return NULL;
    }

  _g_win32_sid_replace (&result, psid, error);

  return result;
}

/**
 * _g_win32_process_get_access_token_sid: (skip)
 * @process_id: Identifier of a process to get an access token of
 *              (use 0 to get a token of the current process)
 * @error: return location for a #GError, or %NULL
 *
 * Opens the process identified by @process_id and opens its token,
 * then retrieves SID of the token user and returns a copy of that SID.
 *
 * Returns: A newly-allocated SID, or NULL in case of an error.
 *          Free the returned SID with g_free().
 */
SID *
_g_win32_process_get_access_token_sid (DWORD process_id,
                                       GError **error)
{
  HANDLE process_handle;
  HANDLE process_token;
  SID *result = NULL;

  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (process_id == 0)
    process_handle = GetCurrentProcess ();
  else
    process_handle = OpenProcess (PROCESS_QUERY_LIMITED_INFORMATION, FALSE, process_id);

  if (process_handle == NULL)
    {
      g_set_error (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                   "%s failed", process_id == 0 ? "GetCurrentProcess" : "OpenProcess");

      return NULL;
    }

  if (!OpenProcessToken (process_handle, TOKEN_QUERY, &process_token))
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "OpenProcessToken failed");

      CloseHandle (process_handle);
      return NULL;
    }

  result = _g_win32_token_get_sid (process_token, error);

  CloseHandle (process_token);
  CloseHandle (process_handle);

  return result;
}

/**
 * _g_win32_sid_to_string: (skip)
 * @sid: a SID.
 * @error: return location for a #GError, or %NULL
 *
 * Convert a SID to its string form.
 *
 * Returns: A newly-allocated string, or NULL in case of an error.
 */
gchar *
_g_win32_sid_to_string (SID *sid, GError **error)
{
  gchar *tmp, *ret;

  g_return_val_if_fail (sid != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (!ConvertSidToStringSidA (sid, &tmp))
    {
      g_set_error_literal (error, G_IO_ERROR, g_io_error_from_errno (GetLastError ()),
                           "Failed to ConvertSidToString");

      return NULL;
    }

  ret = g_strdup (tmp);
  LocalFree (tmp);
  return ret;
}

/**
 * _g_win32_current_process_sid_string: (skip)
 * @error: return location for a #GError, or %NULL
 *
 * Get the current process SID, as a string.
 *
 * Returns: A newly-allocated string, or NULL in case of an error.
 */
gchar *
_g_win32_current_process_sid_string (GError **error)
{
  SID *sid;
  gchar *ret;

  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  sid = _g_win32_process_get_access_token_sid (0, error);
  if (!sid)
    return NULL;

  ret = _g_win32_sid_to_string (sid, error);
  g_free (sid);
  return ret;
}
