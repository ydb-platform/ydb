/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright 2016 Red Hat, Inc.
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

#include "gportalsupport.h"

static gboolean use_portal;
static gboolean network_available;
static gboolean dconf_access;

static void
read_flatpak_info (void)
{
  static gsize flatpak_info_read = 0;
  const gchar *path = "/.flatpak-info";

  if (!g_once_init_enter (&flatpak_info_read))
    return;

  if (g_file_test (path, G_FILE_TEST_EXISTS))
    {
      GKeyFile *keyfile;

      use_portal = TRUE;
      network_available = FALSE;
      dconf_access = FALSE;

      keyfile = g_key_file_new ();
      if (g_key_file_load_from_file (keyfile, path, G_KEY_FILE_NONE, NULL))
        {
          char **shared = NULL;
          char *dconf_policy = NULL;

          shared = g_key_file_get_string_list (keyfile, "Context", "shared", NULL, NULL);
          if (shared)
            {
              network_available = g_strv_contains ((const char * const *)shared, "network");
              g_strfreev (shared);
            }

          dconf_policy = g_key_file_get_string (keyfile, "Session Bus Policy", "ca.desrt.dconf", NULL);
          if (dconf_policy)
            {
              if (strcmp (dconf_policy, "talk") == 0)
                dconf_access = TRUE;
              g_free (dconf_policy);
            }
        }

      g_key_file_unref (keyfile);
    }
  else
    {
      const char *var;

      var = g_getenv ("GTK_USE_PORTAL");
      if (var && var[0] == '1')
        use_portal = TRUE;
      network_available = TRUE;
      dconf_access = TRUE;
    }

  g_once_init_leave (&flatpak_info_read, 1);
}

gboolean
glib_should_use_portal (void)
{
  read_flatpak_info ();
  return use_portal;
}

gboolean
glib_network_available_in_sandbox (void)
{
  read_flatpak_info ();
  return network_available;
}

gboolean
glib_has_dconf_access_in_sandbox (void)
{
  read_flatpak_info ();
  return dconf_access;
}
