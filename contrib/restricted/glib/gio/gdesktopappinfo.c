/* GIO - GLib Input, Output and Streaming Library
 *
 * Copyright (C) 2006-2007 Red Hat, Inc.
 * Copyright © 2007 Ryan Lortie
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
 *         Ryan Lortie <desrt@desrt.ca>
 */

/* Prelude {{{1 */

#include <contrib/restricted/glib/config.h>

/* For the #GDesktopAppInfoLookup macros; since macro deprecation is implemented
 * in the preprocessor, we need to define this before including glib.h*/
#ifndef GLIB_DISABLE_DEPRECATION_WARNINGS
#define GLIB_DISABLE_DEPRECATION_WARNINGS
#endif

#include <errno.h>
#include <string.h>
#include <unistd.h>

#ifdef HAVE_CRT_EXTERNS_H
#include <crt_externs.h>
#endif

#include "gcontenttypeprivate.h"
#include "gdesktopappinfo.h"
#ifdef G_OS_UNIX
#include "glib-unix.h"
#endif
#include "gfile.h"
#include "gioerror.h"
#include "gthemedicon.h"
#include "gfileicon.h"
#include <glib/gstdio.h>
#include "glibintl.h"
#include "giomodule-priv.h"
#include "gappinfo.h"
#include "gappinfoprivate.h"
#include "glocalfilemonitor.h"

#ifdef G_OS_UNIX
#include "gdocumentportal.h"
#endif

/**
 * SECTION:gdesktopappinfo
 * @title: GDesktopAppInfo
 * @short_description: Application information from desktop files
 * @include: gio/gdesktopappinfo.h
 *
 * #GDesktopAppInfo is an implementation of #GAppInfo based on
 * desktop files.
 *
 * Note that `<gio/gdesktopappinfo.h>` belongs to the UNIX-specific
 * GIO interfaces, thus you have to use the `gio-unix-2.0.pc` pkg-config
 * file when using it.
 */

#define DEFAULT_APPLICATIONS_GROUP  "Default Applications"
#define ADDED_ASSOCIATIONS_GROUP    "Added Associations"
#define REMOVED_ASSOCIATIONS_GROUP  "Removed Associations"
#define MIME_CACHE_GROUP            "MIME Cache"
#define GENERIC_NAME_KEY            "GenericName"
#define FULL_NAME_KEY               "X-GNOME-FullName"
#define KEYWORDS_KEY                "Keywords"
#define STARTUP_WM_CLASS_KEY        "StartupWMClass"

enum {
  PROP_0,
  PROP_FILENAME
};

static void     g_desktop_app_info_iface_init         (GAppInfoIface    *iface);
static gboolean g_desktop_app_info_ensure_saved       (GDesktopAppInfo  *info,
                                                       GError          **error);
static gboolean g_desktop_app_info_load_file (GDesktopAppInfo *self);

/**
 * GDesktopAppInfo:
 *
 * Information about an installed application from a desktop file.
 */
struct _GDesktopAppInfo
{
  GObject parent_instance;

  char *desktop_id;
  char *filename;
  char *app_id;

  GKeyFile *keyfile;

  char *name;
  char *generic_name;
  char *fullname;
  char *comment;
  char *icon_name;
  GIcon *icon;
  char **keywords;
  char **only_show_in;
  char **not_show_in;
  char *try_exec;
  char *exec;
  char *binary;
  char *path;
  char *categories;
  char *startup_wm_class;
  char **mime_types;
  char **actions;

  guint nodisplay       : 1;
  guint hidden          : 1;
  guint terminal        : 1;
  guint startup_notify  : 1;
  guint no_fuse         : 1;
};

typedef enum {
  UPDATE_MIME_NONE = 1 << 0,
  UPDATE_MIME_SET_DEFAULT = 1 << 1,
  UPDATE_MIME_SET_NON_DEFAULT = 1 << 2,
  UPDATE_MIME_REMOVE = 1 << 3,
  UPDATE_MIME_SET_LAST_USED = 1 << 4,
} UpdateMimeFlags;

G_DEFINE_TYPE_WITH_CODE (GDesktopAppInfo, g_desktop_app_info, G_TYPE_OBJECT,
                         G_IMPLEMENT_INTERFACE (G_TYPE_APP_INFO, g_desktop_app_info_iface_init))

/* DesktopFileDir implementation {{{1 */

typedef struct
{
  gatomicrefcount             ref_count;
  gchar                      *path;
  gchar                      *alternatively_watching;
  gboolean                    is_config;
  gboolean                    is_setup;
  GFileMonitor               *monitor;
  GHashTable                 *app_names;
  GHashTable                 *mime_tweaks;
  GHashTable                 *memory_index;
  GHashTable                 *memory_implementations;
} DesktopFileDir;

static GPtrArray      *desktop_file_dirs = NULL;
static const gchar    *desktop_file_dirs_config_dir = NULL;
static DesktopFileDir *desktop_file_dir_user_config = NULL;  /* (owned) */
static DesktopFileDir *desktop_file_dir_user_data = NULL;  /* (owned) */
static GMutex          desktop_file_dir_lock;

/* Monitor 'changed' signal handler {{{2 */
static void desktop_file_dir_reset (DesktopFileDir *dir);

static DesktopFileDir *
desktop_file_dir_ref (DesktopFileDir *dir)
{
  g_atomic_ref_count_inc (&dir->ref_count);

  return dir;
}

static void
desktop_file_dir_unref (DesktopFileDir *dir)
{
  if (g_atomic_ref_count_dec (&dir->ref_count))
    {
      desktop_file_dir_reset (dir);
      g_free (dir->path);
      g_free (dir);
    }
}

/*< internal >
 * desktop_file_dir_get_alternative_dir:
 * @dir: a #DesktopFileDir
 *
 * Gets the "alternative" directory to monitor in case the path
 * doesn't exist.
 *
 * If the path exists this will return NULL, otherwise it will return a
 * parent directory of the path.
 *
 * This is used to avoid inotify on a non-existent directory (which
 * results in polling).
 *
 * See https://bugzilla.gnome.org/show_bug.cgi?id=522314 for more info.
 */
static gchar *
desktop_file_dir_get_alternative_dir (DesktopFileDir *dir)
{
  gchar *parent;

  /* If the directory itself exists then we need no alternative. */
  if (g_access (dir->path, R_OK | X_OK) == 0)
    return NULL;

  /* Otherwise, try the parent directories until we find one. */
  parent = g_path_get_dirname (dir->path);

  while (g_access (parent, R_OK | X_OK) != 0)
    {
      gchar *tmp = parent;

      parent = g_path_get_dirname (tmp);

      /* If somehow we get to '/' or '.' then just stop... */
      if (g_str_equal (parent, tmp))
        {
          g_free (tmp);
          break;
        }

      g_free (tmp);
    }

  return parent;
}

static void
desktop_file_dir_changed (GFileMonitor      *monitor,
                          GFile             *file,
                          GFile             *other_file,
                          GFileMonitorEvent  event_type,
                          gpointer           user_data)
{
  DesktopFileDir *dir = user_data;
  gboolean do_nothing = FALSE;

  /* We are not interested in receiving notifications forever just
   * because someone asked about one desktop file once.
   *
   * After we receive the first notification, reset the dir, destroying
   * the monitor.  We will take this as a hint, next time that we are
   * asked, that we need to check if everything is up to date.
   *
   * If this is a notification for a parent directory (because the
   * desktop directory didn't exist) then we shouldn't fire the signal
   * unless something actually changed.
   */
  g_mutex_lock (&desktop_file_dir_lock);

  if (dir->alternatively_watching)
    {
      gchar *alternative_dir;

      alternative_dir = desktop_file_dir_get_alternative_dir (dir);
      do_nothing = alternative_dir && g_str_equal (dir->alternatively_watching, alternative_dir);
      g_free (alternative_dir);
    }

  if (!do_nothing)
    desktop_file_dir_reset (dir);

  g_mutex_unlock (&desktop_file_dir_lock);

  /* Notify anyone else who may be interested */
  if (!do_nothing)
    g_app_info_monitor_fire ();
}

/* Internal utility functions {{{2 */

/*< internal >
 * desktop_file_dir_app_name_is_masked:
 * @dir: a #DesktopFileDir
 * @app_name: an application ID
 *
 * Checks if @app_name is masked for @dir.
 *
 * An application is masked if a similarly-named desktop file exists in
 * a desktop file directory with higher precedence.  Masked desktop
 * files should be ignored.
 */
static gboolean
desktop_file_dir_app_name_is_masked (DesktopFileDir *dir,
                                     const gchar    *app_name)
{
  guint i;

  for (i = 0; i < desktop_file_dirs->len; i++)
    {
      DesktopFileDir *i_dir = g_ptr_array_index (desktop_file_dirs, i);

      if (dir == i_dir)
        return FALSE;
      if (i_dir->app_names && g_hash_table_contains (i_dir->app_names, app_name))
        return TRUE;
    }

  return FALSE;
}

/* Not much to go on from https://specifications.freedesktop.org/desktop-entry-spec/desktop-entry-spec-latest.html
 * so validate it as a non-empty alphanumeric ASCII string with `-` and `_` allowed.
 *
 * Validation is important as the desktop IDs are used to construct filenames,
 * and may be set by an unprivileged caller if running in a setuid program. */
static gboolean
validate_xdg_desktop (const gchar *desktop)
{
  gsize i;

  for (i = 0; desktop[i] != '\0'; i++)
    if (desktop[i] != '-' && desktop[i] != '_' &&
        !g_ascii_isalnum (desktop[i]))
      return FALSE;

  if (i == 0)
    return FALSE;

  return TRUE;
}

static char **
get_valid_current_desktops (const char *value)
{
  char **tmp;
  gsize i;
  GPtrArray *valid_desktops;

  if (value == NULL)
    value = g_getenv ("XDG_CURRENT_DESKTOP");
  if (value == NULL)
    value = "";

  tmp = g_strsplit (value, G_SEARCHPATH_SEPARATOR_S, 0);
  valid_desktops = g_ptr_array_new_full (g_strv_length (tmp) + 1, g_free);
  for (i = 0; tmp[i]; i++)
    {
      if (validate_xdg_desktop (tmp[i]))
        g_ptr_array_add (valid_desktops, tmp[i]);
      else
        g_free (tmp[i]);
    }
  g_ptr_array_add (valid_desktops, NULL);
  g_free (tmp);
  tmp = (char **) g_ptr_array_steal (valid_desktops, NULL);
  g_ptr_array_unref (valid_desktops);
  return tmp;
}

static const gchar * const *
get_lowercase_current_desktops (void)
{
  static gchar **result;

  if (g_once_init_enter (&result))
    {
      char **tmp = get_valid_current_desktops (NULL);
      gsize i, j;

      for (i = 0; tmp[i]; i++)
        {
          /* Convert to lowercase. */
          for (j = 0; tmp[i][j]; j++)
            tmp[i][j] = g_ascii_tolower (tmp[i][j]);
        }

      g_once_init_leave (&result, tmp);
    }

  return (const gchar **) result;
}

static const gchar * const *
get_current_desktops (const gchar *value)
{
  static gchar **result;

  if (g_once_init_enter (&result))
    {
      char **tmp = get_valid_current_desktops (value);

      g_once_init_leave (&result, tmp);
    }

  return (const gchar **) result;
}

/*< internal >
 * add_to_table_if_appropriate:
 * @apps: a string to GDesktopAppInfo hash table
 * @app_name: the name of the application
 * @info: a #GDesktopAppInfo, or NULL
 *
 * If @info is non-%NULL and non-hidden, then add it to @apps, using
 * @app_name as a key.
 *
 * If @info is non-%NULL then this function will consume the passed-in
 * reference.
 */
static void
add_to_table_if_appropriate (GHashTable      *apps,
                             const gchar     *app_name,
                             GDesktopAppInfo *info)
{
  if (!info)
    return;

  if (info->hidden)
    {
      g_object_unref (info);
      return;
    }

  g_free (info->desktop_id);
  info->desktop_id = g_strdup (app_name);

  g_hash_table_insert (apps, g_strdup (info->desktop_id), info);
}

enum
{
  DESKTOP_KEY_Comment,
  DESKTOP_KEY_Exec,
  DESKTOP_KEY_GenericName,
  DESKTOP_KEY_Keywords,
  DESKTOP_KEY_Name,
  DESKTOP_KEY_X_GNOME_FullName,

  N_DESKTOP_KEYS
};

const gchar desktop_key_match_category[N_DESKTOP_KEYS] = {
  /* Note: lower numbers are a better match.
   *
   * In case we want two keys to match at the same level, we can just
   * use the same number for the two different keys.
   */
  [DESKTOP_KEY_Name]             = 1,
  [DESKTOP_KEY_Exec]             = 2,
  [DESKTOP_KEY_Keywords]         = 3,
  [DESKTOP_KEY_GenericName]      = 4,
  [DESKTOP_KEY_X_GNOME_FullName] = 5,
  [DESKTOP_KEY_Comment]          = 6
};

/* Common prefix commands to ignore from Exec= lines */
const char * const exec_key_match_blocklist[] = {
  "bash",
  "env",
  "flatpak",
  "gjs",
  "pkexec",
  "python",
  "python2",
  "python3",
  "sh",
  "wine",
  "wine64",
  NULL
};

static gchar *
desktop_key_get_name (guint key_id)
{
  switch (key_id)
    {
    case DESKTOP_KEY_Comment:
      return "Comment";
    case DESKTOP_KEY_Exec:
      return "Exec";
    case DESKTOP_KEY_GenericName:
      return GENERIC_NAME_KEY;
    case DESKTOP_KEY_Keywords:
      return KEYWORDS_KEY;
    case DESKTOP_KEY_Name:
      return "Name";
    case DESKTOP_KEY_X_GNOME_FullName:
      return FULL_NAME_KEY;
    default:
      g_assert_not_reached ();
    }
}

/* Search global state {{{2
 *
 * We only ever search under a global lock, so we can use (and reuse)
 * some global data to reduce allocations made while searching.
 *
 * In short, we keep around arrays of results that we expand as needed
 * (and never shrink).
 *
 * static_token_results: this is where we append the results for each
 *     token within a given desktop directory, as we handle it (which is
 *     a union of all matches for this term)
 *
 * static_search_results: this is where we build the complete results
 *     for a single directory (which is an intersection of the matches
 *     found for each term)
 *
 * static_total_results: this is where we build the complete results
 *     across all directories (which is a union of the matches found in
 *     each directory)
 *
 * The app_names that enter these tables are always pointer-unique (in
 * the sense that string equality is the same as pointer equality).
 * This can be guaranteed for two reasons:
 *
 *   - we mask appids so that a given appid will only ever appear within
 *     the highest-precedence directory that contains it.  We never
 *     return search results from a lower-level directory if a desktop
 *     file exists in a higher-level one.
 *
 *   - within a given directory, the string is unique because it's the
 *     key in the hashtable of all app_ids for that directory.
 *
 * We perform a merging of the results in merge_token_results().  This
 * works by ordering the two lists and moving through each of them (at
 * the same time) looking for common elements, rejecting uncommon ones.
 * "Order" here need not mean any particular thing, as long as it is
 * some order.  Because of the uniqueness of our strings, we can use
 * pointer order.  That's what's going on in compare_results() below.
 */
struct search_result
{
  const gchar *app_name;
  gint         category;
};

static struct search_result *static_token_results;
static gint                  static_token_results_size;
static gint                  static_token_results_allocated;
static struct search_result *static_search_results;
static gint                  static_search_results_size;
static gint                  static_search_results_allocated;
static struct search_result *static_total_results;
static gint                  static_total_results_size;
static gint                  static_total_results_allocated;

/* And some functions for performing nice operations against it */
static gint
compare_results (gconstpointer a,
                 gconstpointer b)
{
  const struct search_result *ra = a;
  const struct search_result *rb = b;

  if (ra->app_name < rb->app_name)
    return -1;

  else if (ra->app_name > rb->app_name)
    return 1;

  else
    return ra->category - rb->category;
}

static gint
compare_categories (gconstpointer a,
                    gconstpointer b)
{
  const struct search_result *ra = a;
  const struct search_result *rb = b;

  return ra->category - rb->category;
}

static void
add_token_result (const gchar *app_name,
                  guint16      category)
{
  if G_UNLIKELY (static_token_results_size == static_token_results_allocated)
    {
      static_token_results_allocated = MAX (16, static_token_results_allocated * 2);
      static_token_results = g_renew (struct search_result, static_token_results, static_token_results_allocated);
    }

  static_token_results[static_token_results_size].app_name = app_name;
  static_token_results[static_token_results_size].category = category;
  static_token_results_size++;
}

static void
merge_token_results (gboolean first)
{
  if (static_token_results_size != 0)
    qsort (static_token_results, static_token_results_size, sizeof (struct search_result), compare_results);

  /* If this is the first token then we are basically merging a list with
   * itself -- we only perform de-duplication.
   *
   * If this is not the first token then we are doing a real merge.
   */
  if (first)
    {
      const gchar *last_name = NULL;
      gint i;

      /* We must de-duplicate, but we do so by taking the best category
       * in each case.
       *
       * The final list can be as large as the input here, so make sure
       * we have enough room (even if it's too much room).
       */

      if G_UNLIKELY (static_search_results_allocated < static_token_results_size)
        {
          static_search_results_allocated = static_token_results_allocated;
          static_search_results = g_renew (struct search_result,
                                           static_search_results,
                                           static_search_results_allocated);
        }

      for (i = 0; i < static_token_results_size; i++)
        {
          /* The list is sorted so that the best match for a given id
           * will be at the front, so once we have copied an id, skip
           * the rest of the entries for the same id.
           */
          if (static_token_results[i].app_name == last_name)
            continue;

          last_name = static_token_results[i].app_name;

          static_search_results[static_search_results_size++] = static_token_results[i];
        }
    }
  else
    {
      const gchar *last_name = NULL;
      gint i, j = 0;
      gint k = 0;

      /* We only ever remove items from the results list, so no need to
       * resize to ensure that we have enough room.
       */
      for (i = 0; i < static_token_results_size; i++)
        {
          if (static_token_results[i].app_name == last_name)
            continue;

          last_name = static_token_results[i].app_name;

          /* Now we only want to have a result in static_search_results
           * if we already have it there *and* we have it in
           * static_token_results as well.  The category will be the
           * lesser of the two.
           *
           * Skip past the results in static_search_results that are not
           * going to be matches.
           */
          while (k < static_search_results_size &&
                 static_search_results[k].app_name < static_token_results[i].app_name)
            k++;

          if (k < static_search_results_size &&
              static_search_results[k].app_name == static_token_results[i].app_name)
            {
              /* We have a match.
               *
               * Category should be the worse of the two (ie:
               * numerically larger).
               */
              static_search_results[j].app_name = static_search_results[k].app_name;
              static_search_results[j].category = MAX (static_search_results[k].category,
                                                       static_token_results[i].category);
              j++;
            }
        }

      static_search_results_size = j;
    }

  /* Clear it out for next time... */
  static_token_results_size = 0;
}

static void
reset_total_search_results (void)
{
  static_total_results_size = 0;
}

static void
sort_total_search_results (void)
{
  if (static_total_results_size != 0)
    qsort (static_total_results, static_total_results_size, sizeof (struct search_result), compare_categories);
}

static void
merge_directory_results (void)
{
  if G_UNLIKELY (static_total_results_size + static_search_results_size > static_total_results_allocated)
    {
      static_total_results_allocated = MAX (16, static_total_results_allocated);
      while (static_total_results_allocated < static_total_results_size + static_search_results_size)
        static_total_results_allocated *= 2;
      static_total_results = g_renew (struct search_result, static_total_results, static_total_results_allocated);
    }

  if (static_total_results + static_total_results_size != 0)
    memcpy (static_total_results + static_total_results_size,
            static_search_results,
            static_search_results_size * sizeof (struct search_result));

  static_total_results_size += static_search_results_size;

  /* Clear it out for next time... */
  static_search_results_size = 0;
}

/* Support for unindexed DesktopFileDirs {{{2 */
static void
get_apps_from_dir (GHashTable **apps,
                   const char  *dirname,
                   const char  *prefix)
{
  const char *basename;
  GDir *dir;

  dir = g_dir_open (dirname, 0, NULL);

  if (dir == NULL)
    return;

  while ((basename = g_dir_read_name (dir)) != NULL)
    {
      gchar *filename;

      filename = g_build_filename (dirname, basename, NULL);

      if (g_str_has_suffix (basename, ".desktop"))
        {
          gchar *app_name;

          app_name = g_strconcat (prefix, basename, NULL);

          if (*apps == NULL)
            *apps = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, g_free);

          g_hash_table_insert (*apps, app_name, g_strdup (filename));
        }
      else if (g_file_test (filename, G_FILE_TEST_IS_DIR))
        {
          gchar *subprefix;

          subprefix = g_strconcat (prefix, basename, "-", NULL);
          get_apps_from_dir (apps, filename, subprefix);
          g_free (subprefix);
        }

      g_free (filename);
    }

  g_dir_close (dir);
}

typedef struct
{
  gchar **additions;
  gchar **removals;
  gchar **defaults;
} UnindexedMimeTweaks;

static void
free_mime_tweaks (gpointer data)
{
  UnindexedMimeTweaks *tweaks = data;

  g_strfreev (tweaks->additions);
  g_strfreev (tweaks->removals);
  g_strfreev (tweaks->defaults);

  g_slice_free (UnindexedMimeTweaks, tweaks);
}

static UnindexedMimeTweaks *
desktop_file_dir_unindexed_get_tweaks (DesktopFileDir *dir,
                                       const gchar    *mime_type)
{
  UnindexedMimeTweaks *tweaks;
  gchar *unaliased_type;

  unaliased_type = _g_unix_content_type_unalias (mime_type);
  tweaks = g_hash_table_lookup (dir->mime_tweaks, unaliased_type);

  if (tweaks == NULL)
    {
      tweaks = g_slice_new0 (UnindexedMimeTweaks);
      g_hash_table_insert (dir->mime_tweaks, unaliased_type, tweaks);
    }
  else
    g_free (unaliased_type);

  return tweaks;
}

/* consumes 'to_add' */
static void
expand_strv (gchar         ***strv_ptr,
             gchar          **to_add,
             gchar * const   *blocklist)
{
  guint strv_len, add_len;
  gchar **strv;
  guint i, j;

  if (!*strv_ptr)
    {
      *strv_ptr = to_add;
      return;
    }

  strv = *strv_ptr;
  strv_len = g_strv_length (strv);
  add_len = g_strv_length (to_add);
  strv = g_renew (gchar *, strv, strv_len + add_len + 1);

  for (i = 0; to_add[i]; i++)
    {
      /* Don't add blocklisted strings */
      if (blocklist)
        for (j = 0; blocklist[j]; j++)
          if (g_str_equal (to_add[i], blocklist[j]))
            goto no_add;

      /* Don't add duplicates already in the list */
      for (j = 0; j < strv_len; j++)
        if (g_str_equal (to_add[i], strv[j]))
          goto no_add;

      strv[strv_len++] = to_add[i];
      continue;

no_add:
      g_free (to_add[i]);
    }

  strv[strv_len] = NULL;
  *strv_ptr = strv;

  g_free (to_add);
}

static void
desktop_file_dir_unindexed_read_mimeapps_list (DesktopFileDir *dir,
                                               const gchar    *filename,
                                               const gchar    *added_group,
                                               gboolean        tweaks_permitted)
{
  UnindexedMimeTweaks *tweaks;
  char **desktop_file_ids;
  GKeyFile *key_file;
  gchar **mime_types;
  int i;

  key_file = g_key_file_new ();
  if (!g_key_file_load_from_file (key_file, filename, G_KEY_FILE_NONE, NULL))
    {
      g_key_file_free (key_file);
      return;
    }

  mime_types = g_key_file_get_keys (key_file, added_group, NULL, NULL);

  if G_UNLIKELY (mime_types != NULL && !tweaks_permitted)
    {
      g_warning ("%s contains a [%s] group, but it is not permitted here.  Only the non-desktop-specific "
                 "mimeapps.list file may add or remove associations.", filename, added_group);
      g_strfreev (mime_types);
      mime_types = NULL;
    }

  if (mime_types != NULL)
    {
      for (i = 0; mime_types[i] != NULL; i++)
        {
          desktop_file_ids = g_key_file_get_string_list (key_file, added_group, mime_types[i], NULL, NULL);

          if (desktop_file_ids)
            {
              tweaks = desktop_file_dir_unindexed_get_tweaks (dir, mime_types[i]);
              expand_strv (&tweaks->additions, desktop_file_ids, tweaks->removals);
            }
        }

      g_strfreev (mime_types);
    }

  mime_types = g_key_file_get_keys (key_file, REMOVED_ASSOCIATIONS_GROUP, NULL, NULL);

  if G_UNLIKELY (mime_types != NULL && !tweaks_permitted)
    {
      g_warning ("%s contains a [%s] group, but it is not permitted here.  Only the non-desktop-specific "
                 "mimeapps.list file may add or remove associations.", filename, REMOVED_ASSOCIATIONS_GROUP);
      g_strfreev (mime_types);
      mime_types = NULL;
    }

  if (mime_types != NULL)
    {
      for (i = 0; mime_types[i] != NULL; i++)
        {
          desktop_file_ids = g_key_file_get_string_list (key_file, REMOVED_ASSOCIATIONS_GROUP, mime_types[i], NULL, NULL);

          if (desktop_file_ids)
            {
              tweaks = desktop_file_dir_unindexed_get_tweaks (dir, mime_types[i]);
              expand_strv (&tweaks->removals, desktop_file_ids, tweaks->additions);
            }
        }

      g_strfreev (mime_types);
    }

  mime_types = g_key_file_get_keys (key_file, DEFAULT_APPLICATIONS_GROUP, NULL, NULL);

  if (mime_types != NULL)
    {
      for (i = 0; mime_types[i] != NULL; i++)
        {
          desktop_file_ids = g_key_file_get_string_list (key_file, DEFAULT_APPLICATIONS_GROUP, mime_types[i], NULL, NULL);

          if (desktop_file_ids)
            {
              tweaks = desktop_file_dir_unindexed_get_tweaks (dir, mime_types[i]);
              expand_strv (&tweaks->defaults, desktop_file_ids, NULL);
            }
        }

      g_strfreev (mime_types);
    }

  g_key_file_free (key_file);
}

static void
desktop_file_dir_unindexed_read_mimeapps_lists (DesktopFileDir *dir)
{
  const gchar * const *desktops;
  gchar *filename;
  gint i;

  dir->mime_tweaks = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, free_mime_tweaks);

  /* We process in order of precedence, using a blocklisting approach to
   * avoid recording later instructions that conflict with ones we found
   * earlier.
   *
   * We first start with the XDG_CURRENT_DESKTOP files, in precedence
   * order.
   */
  desktops = get_lowercase_current_desktops ();
  for (i = 0; desktops[i]; i++)
    {
      filename = g_strdup_printf ("%s/%s-mimeapps.list", dir->path, desktops[i]);
      desktop_file_dir_unindexed_read_mimeapps_list (dir, filename, ADDED_ASSOCIATIONS_GROUP, FALSE);
      g_free (filename);
    }

  /* Next, the non-desktop-specific mimeapps.list */
  filename = g_strdup_printf ("%s/mimeapps.list", dir->path);
  desktop_file_dir_unindexed_read_mimeapps_list (dir, filename, ADDED_ASSOCIATIONS_GROUP, TRUE);
  g_free (filename);

  /* The remaining files are only checked for in directories that might
   * contain desktop files (ie: not the config dirs).
   */
  if (dir->is_config)
    return;

  /* We have 'defaults.list' which was only ever understood by GLib.  It
   * exists widely, but it has never been part of any spec and it should
   * be treated as deprecated.  This will be removed in a future
   * version.
   */
  filename = g_strdup_printf ("%s/defaults.list", dir->path);
  desktop_file_dir_unindexed_read_mimeapps_list (dir, filename, ADDED_ASSOCIATIONS_GROUP, FALSE);
  g_free (filename);

  /* Finally, the mimeinfo.cache, which is just a cached copy of what we
   * would find in the MimeTypes= lines of all of the desktop files.
   */
  filename = g_strdup_printf ("%s/mimeinfo.cache", dir->path);
  desktop_file_dir_unindexed_read_mimeapps_list (dir, filename, MIME_CACHE_GROUP, TRUE);
  g_free (filename);
}

static void
desktop_file_dir_unindexed_init (DesktopFileDir *dir)
{
  if (!dir->is_config)
    get_apps_from_dir (&dir->app_names, dir->path, "");

  desktop_file_dir_unindexed_read_mimeapps_lists (dir);
}

static GDesktopAppInfo *
g_desktop_app_info_new_from_filename_unlocked (const char *filename)
{
  GDesktopAppInfo *info = NULL;

  info = g_object_new (G_TYPE_DESKTOP_APP_INFO, "filename", filename, NULL);

  if (!g_desktop_app_info_load_file (info))
    g_clear_object (&info);

  return info;
}

static GDesktopAppInfo *
desktop_file_dir_unindexed_get_app (DesktopFileDir *dir,
                                    const gchar    *desktop_id)
{
  const gchar *filename;

  filename = g_hash_table_lookup (dir->app_names, desktop_id);

  if (!filename)
    return NULL;

  return g_desktop_app_info_new_from_filename_unlocked (filename);
}

static void
desktop_file_dir_unindexed_get_all (DesktopFileDir *dir,
                                    GHashTable     *apps)
{
  GHashTableIter iter;
  gpointer app_name;
  gpointer filename;

  if (dir->app_names == NULL)
    return;

  g_hash_table_iter_init (&iter, dir->app_names);
  while (g_hash_table_iter_next (&iter, &app_name, &filename))
    {
      if (desktop_file_dir_app_name_is_masked (dir, app_name))
        continue;

      add_to_table_if_appropriate (apps, app_name, g_desktop_app_info_new_from_filename_unlocked (filename));
    }
}

typedef struct _MemoryIndexEntry MemoryIndexEntry;
typedef GHashTable MemoryIndex;

struct _MemoryIndexEntry
{
  const gchar      *app_name; /* pointer to the hashtable key */
  gint              match_category;
  MemoryIndexEntry *next;
};

static void
memory_index_entry_free (gpointer data)
{
  MemoryIndexEntry *mie = data;

  while (mie)
    {
      MemoryIndexEntry *next = mie->next;

      g_slice_free (MemoryIndexEntry, mie);
      mie = next;
    }
}

static void
memory_index_add_token (MemoryIndex *mi,
                        const gchar *token,
                        gint         match_category,
                        const gchar *app_name)
{
  MemoryIndexEntry *mie, *first;

  mie = g_slice_new (MemoryIndexEntry);
  mie->app_name = app_name;
  mie->match_category = match_category;

  first = g_hash_table_lookup (mi, token);

  if (first)
    {
      mie->next = first->next;
      first->next = mie;
    }
  else
    {
      mie->next = NULL;
      g_hash_table_insert (mi, g_strdup (token), mie);
    }
}

static void
memory_index_add_string (MemoryIndex *mi,
                         const gchar *string,
                         gint         match_category,
                         const gchar *app_name)
{
  gchar **tokens, **alternates;
  gint i;

  tokens = g_str_tokenize_and_fold (string, NULL, &alternates);

  for (i = 0; tokens[i]; i++)
    memory_index_add_token (mi, tokens[i], match_category, app_name);

  for (i = 0; alternates[i]; i++)
    memory_index_add_token (mi, alternates[i], match_category, app_name);

  g_strfreev (alternates);
  g_strfreev (tokens);
}

static MemoryIndex *
memory_index_new (void)
{
  return g_hash_table_new_full (g_str_hash, g_str_equal, g_free, memory_index_entry_free);
}

static void
desktop_file_dir_unindexed_setup_search (DesktopFileDir *dir)
{
  GHashTableIter iter;
  gpointer app, path;

  dir->memory_index = memory_index_new ();
  dir->memory_implementations = memory_index_new ();

  /* Nothing to search? */
  if (dir->app_names == NULL)
    return;

  g_hash_table_iter_init (&iter, dir->app_names);
  while (g_hash_table_iter_next (&iter, &app, &path))
    {
      GKeyFile *key_file;

      if (desktop_file_dir_app_name_is_masked (dir, app))
        continue;

      key_file = g_key_file_new ();

      if (g_key_file_load_from_file (key_file, path, G_KEY_FILE_NONE, NULL) &&
          !g_key_file_get_boolean (key_file, "Desktop Entry", "Hidden", NULL))
        {
          /* Index the interesting keys... */
          gchar **implements;
          gsize i;

          for (i = 0; i < G_N_ELEMENTS (desktop_key_match_category); i++)
            {
              const gchar *value;
              gchar *raw;

              if (!desktop_key_match_category[i])
                continue;

              raw = g_key_file_get_locale_string (key_file, "Desktop Entry", desktop_key_get_name (i), NULL, NULL);
              value = raw;

              if (i == DESKTOP_KEY_Exec && raw != NULL)
                {
                  /* Special handling: only match basename of first field */
                  gchar *space;
                  gchar *slash;

                  /* Remove extra arguments, if any */
                  space = raw + strcspn (raw, " \t\n"); /* IFS */
                  *space = '\0';

                  /* Skip the pathname, if any */
                  if ((slash = strrchr (raw, '/')))
                    value = slash + 1;

                  /* Don't match on blocklisted binaries like interpreters */
                  if (g_strv_contains (exec_key_match_blocklist, value))
		    value = NULL;
                }

              if (value)
                memory_index_add_string (dir->memory_index, value, desktop_key_match_category[i], app);

              g_free (raw);
            }

          /* Make note of the Implements= line */
          implements = g_key_file_get_string_list (key_file, "Desktop Entry", "Implements", NULL, NULL);
          for (i = 0; implements && implements[i]; i++)
            memory_index_add_token (dir->memory_implementations, implements[i], 0, app);
          g_strfreev (implements);
        }

      g_key_file_free (key_file);
    }
}

static void
desktop_file_dir_unindexed_search (DesktopFileDir  *dir,
                                   const gchar     *search_token)
{
  GHashTableIter iter;
  gpointer key, value;

  if (!dir->memory_index)
    desktop_file_dir_unindexed_setup_search (dir);

  g_hash_table_iter_init (&iter, dir->memory_index);
  while (g_hash_table_iter_next (&iter, &key, &value))
    {
      MemoryIndexEntry *mie = value;

      if (!g_str_has_prefix (key, search_token))
        continue;

      while (mie)
        {
          add_token_result (mie->app_name, mie->match_category);
          mie = mie->next;
        }
    }
}

static gboolean
array_contains (GPtrArray *array,
                const gchar *str)
{
  guint i;

  for (i = 0; i < array->len; i++)
    if (g_str_equal (array->pdata[i], str))
      return TRUE;

  return FALSE;
}

static void
desktop_file_dir_unindexed_mime_lookup (DesktopFileDir *dir,
                                        const gchar    *mime_type,
                                        GPtrArray      *hits,
                                        GPtrArray      *blocklist)
{
  UnindexedMimeTweaks *tweaks;
  gint i;

  tweaks = g_hash_table_lookup (dir->mime_tweaks, mime_type);

  if (!tweaks)
    return;

  if (tweaks->additions)
    {
      for (i = 0; tweaks->additions[i]; i++)
        {
          gchar *app_name = tweaks->additions[i];

          if (!desktop_file_dir_app_name_is_masked (dir, app_name) &&
              !array_contains (blocklist, app_name) && !array_contains (hits, app_name))
            g_ptr_array_add (hits, app_name);
        }
    }

  if (tweaks->removals)
    {
      for (i = 0; tweaks->removals[i]; i++)
        {
          gchar *app_name = tweaks->removals[i];

          if (!desktop_file_dir_app_name_is_masked (dir, app_name) &&
              !array_contains (blocklist, app_name) && !array_contains (hits, app_name))
            g_ptr_array_add (blocklist, app_name);
        }
    }
}

static void
desktop_file_dir_unindexed_default_lookup (DesktopFileDir *dir,
                                           const gchar    *mime_type,
                                           GPtrArray      *results)
{
  UnindexedMimeTweaks *tweaks;
  gint i;

  tweaks = g_hash_table_lookup (dir->mime_tweaks, mime_type);

  if (!tweaks || !tweaks->defaults)
    return;

  for (i = 0; tweaks->defaults[i]; i++)
    {
      gchar *app_name = tweaks->defaults[i];

      if (!array_contains (results, app_name))
        g_ptr_array_add (results, app_name);
    }
}

static void
desktop_file_dir_unindexed_get_implementations (DesktopFileDir  *dir,
                                                GList          **results,
                                                const gchar     *interface)
{
  MemoryIndexEntry *mie;

  if (!dir->memory_index)
    desktop_file_dir_unindexed_setup_search (dir);

  for (mie = g_hash_table_lookup (dir->memory_implementations, interface); mie; mie = mie->next)
    *results = g_list_prepend (*results, g_strdup (mie->app_name));
}

/* DesktopFileDir "API" {{{2 */

/*< internal >
 * desktop_file_dir_new:
 * @data_dir: an XDG_DATA_DIR
 *
 * Creates a #DesktopFileDir for the corresponding @data_dir.
 */
static DesktopFileDir *
desktop_file_dir_new (const gchar *data_dir)
{
  DesktopFileDir *dir = g_new0 (DesktopFileDir, 1);

  g_atomic_ref_count_init (&dir->ref_count);
  dir->path = g_build_filename (data_dir, "applications", NULL);

  return g_steal_pointer (&dir);
}

/*< internal >
 * desktop_file_dir_new_for_config:
 * @config_dir: an XDG_CONFIG_DIR
 *
 * Just the same as desktop_file_dir_new() except that it does not
 * add the "applications" directory.  It also marks the directory as
 * config-only, which prevents us from attempting to find desktop files
 * here.
 */
static DesktopFileDir *
desktop_file_dir_new_for_config (const gchar *config_dir)
{
  DesktopFileDir *dir = g_new0 (DesktopFileDir, 1);

  g_atomic_ref_count_init (&dir->ref_count);
  dir->path = g_strdup (config_dir);
  dir->is_config = TRUE;

  return g_steal_pointer (&dir);
}

/*< internal >
 * desktop_file_dir_reset:
 * @dir: a #DesktopFileDir
 *
 * Cleans up @dir, releasing most resources that it was using.
 */
static void
desktop_file_dir_reset (DesktopFileDir *dir)
{
  if (dir->alternatively_watching)
    {
      g_free (dir->alternatively_watching);
      dir->alternatively_watching = NULL;
    }

  if (dir->monitor)
    {
      g_signal_handlers_disconnect_by_func (dir->monitor, desktop_file_dir_changed, dir);
      g_file_monitor_cancel (dir->monitor);
      g_object_unref (dir->monitor);
      dir->monitor = NULL;
    }

  if (dir->app_names)
    {
      g_hash_table_unref (dir->app_names);
      dir->app_names = NULL;
    }

  if (dir->memory_index)
    {
      g_hash_table_unref (dir->memory_index);
      dir->memory_index = NULL;
    }

  if (dir->mime_tweaks)
    {
      g_hash_table_unref (dir->mime_tweaks);
      dir->mime_tweaks = NULL;
    }

  if (dir->memory_implementations)
    {
      g_hash_table_unref (dir->memory_implementations);
      dir->memory_implementations = NULL;
    }

  dir->is_setup = FALSE;
}

static void
closure_notify_cb (gpointer  data,
                   GClosure *closure)
{
  DesktopFileDir *dir = data;
  desktop_file_dir_unref (dir);
}

/*< internal >
 * desktop_file_dir_init:
 * @dir: a #DesktopFileDir
 *
 * Does initial setup for @dir
 *
 * You should only call this if @dir is not already setup.
 */
static void
desktop_file_dir_init (DesktopFileDir *dir)
{
  const gchar *watch_dir;

  g_assert (!dir->is_setup);

  g_assert (!dir->alternatively_watching);
  g_assert (!dir->monitor);

  dir->alternatively_watching = desktop_file_dir_get_alternative_dir (dir);
  watch_dir = dir->alternatively_watching ? dir->alternatively_watching : dir->path;

  /* There is a very thin race here if the watch_dir has been _removed_
   * between when we checked for it and when we establish the watch.
   * Removes probably don't happen in usual operation, and even if it
   * does (and we catch the unlikely race), the only degradation is that
   * we will fall back to polling.
   */
  dir->monitor = g_local_file_monitor_new_in_worker (watch_dir, TRUE, G_FILE_MONITOR_NONE,
                                                     desktop_file_dir_changed,
                                                     desktop_file_dir_ref (dir),
                                                     closure_notify_cb, NULL);

  desktop_file_dir_unindexed_init (dir);

  dir->is_setup = TRUE;
}

/*< internal >
 * desktop_file_dir_get_app:
 * @dir: a DesktopFileDir
 * @desktop_id: the desktop ID to load
 *
 * Creates the #GDesktopAppInfo for the given @desktop_id if it exists
 * within @dir, even if it is hidden.
 *
 * This function does not check if @desktop_id would be masked by a
 * directory with higher precedence.  The caller must do so.
 */
static GDesktopAppInfo *
desktop_file_dir_get_app (DesktopFileDir *dir,
                          const gchar    *desktop_id)
{
  if (!dir->app_names)
    return NULL;

  return desktop_file_dir_unindexed_get_app (dir, desktop_id);
}

/*< internal >
 * desktop_file_dir_get_all:
 * @dir: a DesktopFileDir
 * @apps: a #GHashTable<string, GDesktopAppInfo>
 *
 * Loads all desktop files in @dir and adds them to @apps, careful to
 * ensure we don't add any files masked by a similarly-named file in a
 * higher-precedence directory.
 */
static void
desktop_file_dir_get_all (DesktopFileDir *dir,
                          GHashTable     *apps)
{
  desktop_file_dir_unindexed_get_all (dir, apps);
}

/*< internal >
 * desktop_file_dir_mime_lookup:
 * @dir: a #DesktopFileDir
 * @mime_type: the mime type to look up
 * @hits: the array to store the hits
 * @blocklist: the array to store the blocklist
 *
 * Does a lookup of a mimetype against one desktop file directory,
 * recording any hits and blocklisting and "Removed" associations (so
 * later directories don't record them as hits).
 *
 * The items added to @hits are duplicated, but the ones in @blocklist
 * are weak pointers.  This facilitates simply freeing the blocklist
 * (which is only used for internal bookkeeping) but using the pdata of
 * @hits as the result of the operation.
 */
static void
desktop_file_dir_mime_lookup (DesktopFileDir *dir,
                              const gchar    *mime_type,
                              GPtrArray      *hits,
                              GPtrArray      *blocklist)
{
  desktop_file_dir_unindexed_mime_lookup (dir, mime_type, hits, blocklist);
}

/*< internal >
 * desktop_file_dir_default_lookup:
 * @dir: a #DesktopFileDir
 * @mime_type: the mime type to look up
 * @results: an array to store the results in
 *
 * Collects the "default" applications for a given mime type from @dir.
 */
static void
desktop_file_dir_default_lookup (DesktopFileDir *dir,
                                 const gchar    *mime_type,
                                 GPtrArray      *results)
{
  desktop_file_dir_unindexed_default_lookup (dir, mime_type, results);
}

/*< internal >
 * desktop_file_dir_search:
 * @dir: a #DesktopFileDir
 * @term: a normalised and casefolded search term
 *
 * Finds the names of applications in @dir that match @term.
 */
static void
desktop_file_dir_search (DesktopFileDir *dir,
                         const gchar    *search_token)
{
  desktop_file_dir_unindexed_search (dir, search_token);
}

static void
desktop_file_dir_get_implementations (DesktopFileDir  *dir,
                                      GList          **results,
                                      const gchar     *interface)
{
  desktop_file_dir_unindexed_get_implementations (dir, results, interface);
}

/* Lock/unlock and global setup API {{{2 */

static void
desktop_file_dirs_lock (void)
{
  guint i;
  const gchar *user_config_dir = g_get_user_config_dir ();

  g_mutex_lock (&desktop_file_dir_lock);

  /* If the XDG dirs configuration has changed (expected only during tests),
   * clear and reload the state. */
  if (desktop_file_dirs_config_dir != NULL &&
      g_strcmp0 (desktop_file_dirs_config_dir, user_config_dir) != 0)
    {
      g_debug ("%s: Resetting desktop app info dirs from %s to %s",
               G_STRFUNC, desktop_file_dirs_config_dir, user_config_dir);

      g_ptr_array_set_size (desktop_file_dirs, 0);
      g_clear_pointer (&desktop_file_dir_user_config, desktop_file_dir_unref);
      g_clear_pointer (&desktop_file_dir_user_data, desktop_file_dir_unref);
    }

  if (desktop_file_dirs == NULL || desktop_file_dirs->len == 0)
    {
      const char * const *dirs;
      gint i;

      if (desktop_file_dirs == NULL)
        desktop_file_dirs = g_ptr_array_new_with_free_func ((GDestroyNotify) desktop_file_dir_unref);

      /* First, the configs.  Highest priority: the user's ~/.config */
      desktop_file_dir_user_config = desktop_file_dir_new_for_config (user_config_dir);
      g_ptr_array_add (desktop_file_dirs, desktop_file_dir_ref (desktop_file_dir_user_config));

      /* Next, the system configs (/etc/xdg, and so on). */
      dirs = g_get_system_config_dirs ();
      for (i = 0; dirs[i]; i++)
        g_ptr_array_add (desktop_file_dirs, desktop_file_dir_new_for_config (dirs[i]));

      /* Now the data.  Highest priority: the user's ~/.local/share/applications */
      desktop_file_dir_user_data = desktop_file_dir_new (g_get_user_data_dir ());
      g_ptr_array_add (desktop_file_dirs, desktop_file_dir_ref (desktop_file_dir_user_data));

      /* Following that, XDG_DATA_DIRS/applications, in order */
      dirs = g_get_system_data_dirs ();
      for (i = 0; dirs[i]; i++)
        g_ptr_array_add (desktop_file_dirs, desktop_file_dir_new (dirs[i]));

      /* The list of directories will never change after this, unless
       * g_get_user_config_dir() changes due to %G_TEST_OPTION_ISOLATE_DIRS. */
      desktop_file_dirs_config_dir = user_config_dir;
    }

  for (i = 0; i < desktop_file_dirs->len; i++)
    if (!((DesktopFileDir *) g_ptr_array_index (desktop_file_dirs, i))->is_setup)
      desktop_file_dir_init (g_ptr_array_index (desktop_file_dirs, i));
}

static void
desktop_file_dirs_unlock (void)
{
  g_mutex_unlock (&desktop_file_dir_lock);
}

static void
desktop_file_dirs_invalidate_user_config (void)
{
  g_mutex_lock (&desktop_file_dir_lock);

  if (desktop_file_dir_user_config != NULL)
    desktop_file_dir_reset (desktop_file_dir_user_config);

  g_mutex_unlock (&desktop_file_dir_lock);
}

static void
desktop_file_dirs_invalidate_user_data (void)
{
  g_mutex_lock (&desktop_file_dir_lock);

  if (desktop_file_dir_user_data != NULL)
    desktop_file_dir_reset (desktop_file_dir_user_data);

  g_mutex_unlock (&desktop_file_dir_lock);
}

/* GDesktopAppInfo implementation {{{1 */
/* GObject implementation {{{2 */
static void
g_desktop_app_info_finalize (GObject *object)
{
  GDesktopAppInfo *info;

  info = G_DESKTOP_APP_INFO (object);

  g_free (info->desktop_id);
  g_free (info->filename);

  if (info->keyfile)
    g_key_file_unref (info->keyfile);

  g_free (info->name);
  g_free (info->generic_name);
  g_free (info->fullname);
  g_free (info->comment);
  g_free (info->icon_name);
  if (info->icon)
    g_object_unref (info->icon);
  g_strfreev (info->keywords);
  g_strfreev (info->only_show_in);
  g_strfreev (info->not_show_in);
  g_free (info->try_exec);
  g_free (info->exec);
  g_free (info->binary);
  g_free (info->path);
  g_free (info->categories);
  g_free (info->startup_wm_class);
  g_strfreev (info->mime_types);
  g_free (info->app_id);
  g_strfreev (info->actions);

  G_OBJECT_CLASS (g_desktop_app_info_parent_class)->finalize (object);
}

static void
g_desktop_app_info_set_property (GObject      *object,
                                 guint         prop_id,
                                 const GValue *value,
                                 GParamSpec   *pspec)
{
  GDesktopAppInfo *self = G_DESKTOP_APP_INFO (object);

  switch (prop_id)
    {
    case PROP_FILENAME:
      self->filename = g_value_dup_string (value);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
    }
}

static void
g_desktop_app_info_get_property (GObject    *object,
                                 guint       prop_id,
                                 GValue     *value,
                                 GParamSpec *pspec)
{
  GDesktopAppInfo *self = G_DESKTOP_APP_INFO (object);

  switch (prop_id)
    {
    case PROP_FILENAME:
      g_value_set_string (value, self->filename);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
    }
}

static void
g_desktop_app_info_class_init (GDesktopAppInfoClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->get_property = g_desktop_app_info_get_property;
  gobject_class->set_property = g_desktop_app_info_set_property;
  gobject_class->finalize = g_desktop_app_info_finalize;

  /**
   * GDesktopAppInfo:filename:
   *
   * The origin filename of this #GDesktopAppInfo
   */
  g_object_class_install_property (gobject_class,
                                   PROP_FILENAME,
                                   g_param_spec_string ("filename", "Filename", "", NULL,
                                                        G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
}

static void
g_desktop_app_info_init (GDesktopAppInfo *local)
{
}

/* Construction... {{{2 */

/*< internal >
 * binary_from_exec:
 * @exec: an exec line
 *
 * Returns the first word in an exec line (ie: the binary name).
 *
 * If @exec is "  progname --foo %F" then returns "progname".
 */
static char *
binary_from_exec (const char *exec)
{
  const char *p, *start;

  p = exec;
  while (*p == ' ')
    p++;
  start = p;
  while (*p != ' ' && *p != 0)
    p++;

  return g_strndup (start, p - start);
}

/*< internal >
 * g_desktop_app_info_get_desktop_id_for_filename
 * @self: #GDesktopAppInfo to get desktop id of
 *
 * Tries to find the desktop ID for a particular `.desktop` filename, as per the
 * [Desktop Entry Specification](https://specifications.freedesktop.org/desktop-
 * entry-spec/desktop-entry-spec-latest.html#desktop-file-id).
 *
 * Returns: desktop id or basename if filename is unknown.
 */
static char *
g_desktop_app_info_get_desktop_id_for_filename (GDesktopAppInfo *self)
{
  guint i;
  gchar *desktop_id = NULL;

  g_return_val_if_fail (self->filename != NULL, NULL);

  for (i = 0; i < desktop_file_dirs->len; i++)
    {
      DesktopFileDir *dir = g_ptr_array_index (desktop_file_dirs, i);
      GHashTable *app_names;
      GHashTableIter iter;
      gpointer key, value;

      app_names = dir->app_names;

      if (!app_names)
        continue;

      g_hash_table_iter_init (&iter, app_names);
      while (g_hash_table_iter_next (&iter, &key, &value))
        {
          if (!strcmp (value, self->filename))
            {
              desktop_id = g_strdup (key);
              break;
            }
        }

      if (desktop_id)
        break;
    }

  if (!desktop_id)
    desktop_id = g_path_get_basename (self->filename);

  return g_steal_pointer (&desktop_id);
}

static gboolean
g_desktop_app_info_load_from_keyfile (GDesktopAppInfo *info,
                                      GKeyFile        *key_file)
{
  char *start_group;
  char *type;
  char *try_exec;
  char *exec;
  gboolean bus_activatable;

  start_group = g_key_file_get_start_group (key_file);
  if (start_group == NULL || strcmp (start_group, G_KEY_FILE_DESKTOP_GROUP) != 0)
    {
      g_free (start_group);
      return FALSE;
    }
  g_free (start_group);

  type = g_key_file_get_string (key_file,
                                G_KEY_FILE_DESKTOP_GROUP,
                                G_KEY_FILE_DESKTOP_KEY_TYPE,
                                NULL);
  if (type == NULL || strcmp (type, G_KEY_FILE_DESKTOP_TYPE_APPLICATION) != 0)
    {
      g_free (type);
      return FALSE;
    }
  g_free (type);

  try_exec = g_key_file_get_string (key_file,
                                    G_KEY_FILE_DESKTOP_GROUP,
                                    G_KEY_FILE_DESKTOP_KEY_TRY_EXEC,
                                    NULL);
  if (try_exec && try_exec[0] != '\0')
    {
      char *t;
      t = g_find_program_in_path (try_exec);
      if (t == NULL)
        {
          g_free (try_exec);
          return FALSE;
        }
      g_free (t);
    }

  exec = g_key_file_get_string (key_file,
                                G_KEY_FILE_DESKTOP_GROUP,
                                G_KEY_FILE_DESKTOP_KEY_EXEC,
                                NULL);
  if (exec && exec[0] != '\0')
    {
      gint argc;
      char **argv;
      if (!g_shell_parse_argv (exec, &argc, &argv, NULL))
        {
          g_free (exec);
          g_free (try_exec);
          return FALSE;
        }
      else
        {
          char *t;

          /* Since @exec is not an empty string, there must be at least one
           * argument, so dereferencing argv[0] should return non-NULL. */
          g_assert (argc > 0);
          t = g_find_program_in_path (argv[0]);
          g_strfreev (argv);

          if (t == NULL)
            {
              g_free (exec);
              g_free (try_exec);
              return FALSE;
            }
          g_free (t);
        }
    }

  info->name = g_key_file_get_locale_string (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_NAME, NULL, NULL);
  info->generic_name = g_key_file_get_locale_string (key_file, G_KEY_FILE_DESKTOP_GROUP, GENERIC_NAME_KEY, NULL, NULL);
  info->fullname = g_key_file_get_locale_string (key_file, G_KEY_FILE_DESKTOP_GROUP, FULL_NAME_KEY, NULL, NULL);
  info->keywords = g_key_file_get_locale_string_list (key_file, G_KEY_FILE_DESKTOP_GROUP, KEYWORDS_KEY, NULL, NULL, NULL);
  info->comment = g_key_file_get_locale_string (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_COMMENT, NULL, NULL);
  info->nodisplay = g_key_file_get_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_NO_DISPLAY, NULL) != FALSE;
  info->icon_name =  g_key_file_get_locale_string (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_ICON, NULL, NULL);
  info->only_show_in = g_key_file_get_string_list (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_ONLY_SHOW_IN, NULL, NULL);
  info->not_show_in = g_key_file_get_string_list (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_NOT_SHOW_IN, NULL, NULL);
  info->try_exec = try_exec;
  info->exec = exec;
  info->path = g_key_file_get_string (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_PATH, NULL);
  info->terminal = g_key_file_get_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_TERMINAL, NULL) != FALSE;
  info->startup_notify = g_key_file_get_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_STARTUP_NOTIFY, NULL) != FALSE;
  info->no_fuse = g_key_file_get_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP, "X-GIO-NoFuse", NULL) != FALSE;
  info->hidden = g_key_file_get_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_HIDDEN, NULL) != FALSE;
  info->categories = g_key_file_get_string (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_CATEGORIES, NULL);
  info->startup_wm_class = g_key_file_get_string (key_file, G_KEY_FILE_DESKTOP_GROUP, STARTUP_WM_CLASS_KEY, NULL);
  info->mime_types = g_key_file_get_string_list (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_MIME_TYPE, NULL, NULL);
  bus_activatable = g_key_file_get_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_DBUS_ACTIVATABLE, NULL);
  info->actions = g_key_file_get_string_list (key_file, G_KEY_FILE_DESKTOP_GROUP, G_KEY_FILE_DESKTOP_KEY_ACTIONS, NULL, NULL);

  /* Remove the special-case: no Actions= key just means 0 extra actions */
  if (info->actions == NULL)
    info->actions = g_new0 (gchar *, 0 + 1);

  info->icon = NULL;
  if (info->icon_name)
    {
      if (g_path_is_absolute (info->icon_name))
        {
          GFile *file;

          file = g_file_new_for_path (info->icon_name);
          info->icon = g_file_icon_new (file);
          g_object_unref (file);
        }
      else
        {
          char *p;

          /* Work around a common mistake in desktop files */
          if ((p = strrchr (info->icon_name, '.')) != NULL &&
              (strcmp (p, ".png") == 0 ||
               strcmp (p, ".xpm") == 0 ||
               strcmp (p, ".svg") == 0))
            *p = 0;

          info->icon = g_themed_icon_new (info->icon_name);
        }
    }

  if (info->exec)
    info->binary = binary_from_exec (info->exec);

  if (info->path && info->path[0] == '\0')
    {
      g_free (info->path);
      info->path = NULL;
    }

  /* Can only be DBusActivatable if we know the filename, which means
   * that this won't work for the load-from-keyfile case.
   */
  if (bus_activatable && info->filename)
    {
      gchar *basename;
      gchar *last_dot;

      basename = g_path_get_basename (info->filename);
      last_dot = strrchr (basename, '.');

      if (last_dot && g_str_equal (last_dot, ".desktop"))
        {
          *last_dot = '\0';

          if (g_dbus_is_name (basename) && basename[0] != ':')
            info->app_id = g_strdup (basename);
        }

      g_free (basename);
    }

  if (info->filename)
    info->desktop_id = g_desktop_app_info_get_desktop_id_for_filename (info);

  info->keyfile = g_key_file_ref (key_file);

  return TRUE;
}

static gboolean
g_desktop_app_info_load_file (GDesktopAppInfo *self)
{
  GKeyFile *key_file;
  gboolean retval = FALSE;

  g_return_val_if_fail (self->filename != NULL, FALSE);

  key_file = g_key_file_new ();

  if (g_key_file_load_from_file (key_file, self->filename, G_KEY_FILE_NONE, NULL))
    retval = g_desktop_app_info_load_from_keyfile (self, key_file);

  g_key_file_unref (key_file);
  return retval;
}

/**
 * g_desktop_app_info_new_from_keyfile:
 * @key_file: an opened #GKeyFile
 *
 * Creates a new #GDesktopAppInfo.
 *
 * Returns: (nullable): a new #GDesktopAppInfo or %NULL on error.
 *
 * Since: 2.18
 **/
GDesktopAppInfo *
g_desktop_app_info_new_from_keyfile (GKeyFile *key_file)
{
  GDesktopAppInfo *info;

  info = g_object_new (G_TYPE_DESKTOP_APP_INFO, NULL);
  info->filename = NULL;

  desktop_file_dirs_lock ();

  if (!g_desktop_app_info_load_from_keyfile (info, key_file))
    g_clear_object (&info);

  desktop_file_dirs_unlock ();

  return info;
}

/**
 * g_desktop_app_info_new_from_filename:
 * @filename: (type filename): the path of a desktop file, in the GLib
 *      filename encoding
 *
 * Creates a new #GDesktopAppInfo.
 *
 * Returns: (nullable): a new #GDesktopAppInfo or %NULL on error.
 **/
GDesktopAppInfo *
g_desktop_app_info_new_from_filename (const char *filename)
{
  GDesktopAppInfo *info = NULL;

  desktop_file_dirs_lock ();

  info = g_desktop_app_info_new_from_filename_unlocked (filename);

  desktop_file_dirs_unlock ();

  return info;
}

/**
 * g_desktop_app_info_new:
 * @desktop_id: the desktop file id
 *
 * Creates a new #GDesktopAppInfo based on a desktop file id.
 *
 * A desktop file id is the basename of the desktop file, including the
 * .desktop extension. GIO is looking for a desktop file with this name
 * in the `applications` subdirectories of the XDG
 * data directories (i.e. the directories specified in the `XDG_DATA_HOME`
 * and `XDG_DATA_DIRS` environment variables). GIO also supports the
 * prefix-to-subdirectory mapping that is described in the
 * [Menu Spec](http://standards.freedesktop.org/menu-spec/latest/)
 * (i.e. a desktop id of kde-foo.desktop will match
 * `/usr/share/applications/kde/foo.desktop`).
 *
 * Returns: (nullable): a new #GDesktopAppInfo, or %NULL if no desktop
 *     file with that id exists.
 */
GDesktopAppInfo *
g_desktop_app_info_new (const char *desktop_id)
{
  GDesktopAppInfo *appinfo = NULL;
  guint i;

  desktop_file_dirs_lock ();

  for (i = 0; i < desktop_file_dirs->len; i++)
    {
      appinfo = desktop_file_dir_get_app (g_ptr_array_index (desktop_file_dirs, i), desktop_id);

      if (appinfo)
        break;
    }

  desktop_file_dirs_unlock ();

  if (appinfo == NULL)
    return NULL;

  g_free (appinfo->desktop_id);
  appinfo->desktop_id = g_strdup (desktop_id);

  if (g_desktop_app_info_get_is_hidden (appinfo))
    {
      g_object_unref (appinfo);
      appinfo = NULL;
    }

  return appinfo;
}

static GAppInfo *
g_desktop_app_info_dup (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);
  GDesktopAppInfo *new_info;

  new_info = g_object_new (G_TYPE_DESKTOP_APP_INFO, NULL);

  new_info->filename = g_strdup (info->filename);
  new_info->desktop_id = g_strdup (info->desktop_id);

  if (info->keyfile)
    new_info->keyfile = g_key_file_ref (info->keyfile);

  new_info->name = g_strdup (info->name);
  new_info->generic_name = g_strdup (info->generic_name);
  new_info->fullname = g_strdup (info->fullname);
  new_info->keywords = g_strdupv (info->keywords);
  new_info->comment = g_strdup (info->comment);
  new_info->nodisplay = info->nodisplay;
  new_info->icon_name = g_strdup (info->icon_name);
  if (info->icon)
    new_info->icon = g_object_ref (info->icon);
  new_info->only_show_in = g_strdupv (info->only_show_in);
  new_info->not_show_in = g_strdupv (info->not_show_in);
  new_info->try_exec = g_strdup (info->try_exec);
  new_info->exec = g_strdup (info->exec);
  new_info->binary = g_strdup (info->binary);
  new_info->path = g_strdup (info->path);
  new_info->app_id = g_strdup (info->app_id);
  new_info->hidden = info->hidden;
  new_info->terminal = info->terminal;
  new_info->startup_notify = info->startup_notify;

  return G_APP_INFO (new_info);
}

/* GAppInfo interface implementation functions {{{2 */

static gboolean
g_desktop_app_info_equal (GAppInfo *appinfo1,
                          GAppInfo *appinfo2)
{
  GDesktopAppInfo *info1 = G_DESKTOP_APP_INFO (appinfo1);
  GDesktopAppInfo *info2 = G_DESKTOP_APP_INFO (appinfo2);

  if (info1->desktop_id == NULL ||
      info2->desktop_id == NULL)
    return info1 == info2;

  return strcmp (info1->desktop_id, info2->desktop_id) == 0;
}

static const char *
g_desktop_app_info_get_id (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->desktop_id;
}

static const char *
g_desktop_app_info_get_name (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (info->name == NULL)
    return _("Unnamed");
  return info->name;
}

static const char *
g_desktop_app_info_get_display_name (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (info->fullname == NULL)
    return g_desktop_app_info_get_name (appinfo);
  return info->fullname;
}

/**
 * g_desktop_app_info_get_is_hidden:
 * @info: a #GDesktopAppInfo.
 *
 * A desktop file is hidden if the Hidden key in it is
 * set to True.
 *
 * Returns: %TRUE if hidden, %FALSE otherwise.
 **/
gboolean
g_desktop_app_info_get_is_hidden (GDesktopAppInfo *info)
{
  return info->hidden;
}

/**
 * g_desktop_app_info_get_filename:
 * @info: a #GDesktopAppInfo
 *
 * When @info was created from a known filename, return it.  In some
 * situations such as the #GDesktopAppInfo returned from
 * g_desktop_app_info_new_from_keyfile(), this function will return %NULL.
 *
 * Returns: (nullable) (type filename): The full path to the file for @info,
 *     or %NULL if not known.
 * Since: 2.24
 */
const char *
g_desktop_app_info_get_filename (GDesktopAppInfo *info)
{
  return info->filename;
}

static const char *
g_desktop_app_info_get_description (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->comment;
}

static const char *
g_desktop_app_info_get_executable (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->binary;
}

static const char *
g_desktop_app_info_get_commandline (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->exec;
}

static GIcon *
g_desktop_app_info_get_icon (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->icon;
}

/**
 * g_desktop_app_info_get_categories:
 * @info: a #GDesktopAppInfo
 *
 * Gets the categories from the desktop file.
 *
 * Returns: (nullable): The unparsed Categories key from the desktop file;
 *     i.e. no attempt is made to split it by ';' or validate it.
 */
const char *
g_desktop_app_info_get_categories (GDesktopAppInfo *info)
{
  return info->categories;
}

/**
 * g_desktop_app_info_get_keywords:
 * @info: a #GDesktopAppInfo
 *
 * Gets the keywords from the desktop file.
 *
 * Returns: (transfer none): The value of the Keywords key
 *
 * Since: 2.32
 */
const char * const *
g_desktop_app_info_get_keywords (GDesktopAppInfo *info)
{
  return (const char * const *)info->keywords;
}

/**
 * g_desktop_app_info_get_generic_name:
 * @info: a #GDesktopAppInfo
 *
 * Gets the generic name from the desktop file.
 *
 * Returns: (nullable): The value of the GenericName key
 */
const char *
g_desktop_app_info_get_generic_name (GDesktopAppInfo *info)
{
  return info->generic_name;
}

/**
 * g_desktop_app_info_get_nodisplay:
 * @info: a #GDesktopAppInfo
 *
 * Gets the value of the NoDisplay key, which helps determine if the
 * application info should be shown in menus. See
 * %G_KEY_FILE_DESKTOP_KEY_NO_DISPLAY and g_app_info_should_show().
 *
 * Returns: The value of the NoDisplay key
 *
 * Since: 2.30
 */
gboolean
g_desktop_app_info_get_nodisplay (GDesktopAppInfo *info)
{
  return info->nodisplay;
}

/**
 * g_desktop_app_info_get_show_in:
 * @info: a #GDesktopAppInfo
 * @desktop_env: (nullable): a string specifying a desktop name
 *
 * Checks if the application info should be shown in menus that list available
 * applications for a specific name of the desktop, based on the
 * `OnlyShowIn` and `NotShowIn` keys.
 *
 * @desktop_env should typically be given as %NULL, in which case the
 * `XDG_CURRENT_DESKTOP` environment variable is consulted.  If you want
 * to override the default mechanism then you may specify @desktop_env,
 * but this is not recommended.
 *
 * Note that g_app_info_should_show() for @info will include this check (with
 * %NULL for @desktop_env) as well as additional checks.
 *
 * Returns: %TRUE if the @info should be shown in @desktop_env according to the
 * `OnlyShowIn` and `NotShowIn` keys, %FALSE
 * otherwise.
 *
 * Since: 2.30
 */
gboolean
g_desktop_app_info_get_show_in (GDesktopAppInfo *info,
                                const gchar     *desktop_env)
{
  const gchar *specified_envs[] = { desktop_env, NULL };
  const gchar * const *envs;
  gint i;

  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), FALSE);

  if (desktop_env)
    envs = specified_envs;
  else
    envs = get_current_desktops (NULL);

  for (i = 0; envs[i]; i++)
    {
      gint j;

      if (info->only_show_in)
        for (j = 0; info->only_show_in[j]; j++)
          if (g_str_equal (info->only_show_in[j], envs[i]))
            return TRUE;

      if (info->not_show_in)
        for (j = 0; info->not_show_in[j]; j++)
          if (g_str_equal (info->not_show_in[j], envs[i]))
            return FALSE;
    }

  return info->only_show_in == NULL;
}

/* Launching... {{{2 */

static char *
expand_macro_single (char macro, const char *uri)
{
  GFile *file;
  char *result = NULL;
  char *path = NULL;
  char *name;

  file = g_file_new_for_uri (uri);

  switch (macro)
    {
    case 'u':
    case 'U':
      result = g_shell_quote (uri);
      break;
    case 'f':
    case 'F':
      path = g_file_get_path (file);
      if (path)
        result = g_shell_quote (path);
      break;
    case 'd':
    case 'D':
      path = g_file_get_path (file);
      if (path)
        {
          name = g_path_get_dirname (path);
          result = g_shell_quote (name);
          g_free (name);
        }
      break;
    case 'n':
    case 'N':
      path = g_file_get_path (file);
      if (path)
        {
          name = g_path_get_basename (path);
          result = g_shell_quote (name);
          g_free (name);
        }
      break;
    }

  g_object_unref (file);
  g_free (path);

  return result;
}

static char *
expand_macro_uri (char macro, const char *uri, gboolean force_file_uri, char force_file_uri_macro)
{
  char *expanded = NULL;

  g_return_val_if_fail (uri != NULL, NULL);

  if (!force_file_uri ||
      /* Pass URI if it contains an anchor */
      strchr (uri, '#') != NULL)
    {
      expanded = expand_macro_single (macro, uri);
    }
  else
    {
      expanded = expand_macro_single (force_file_uri_macro, uri);
      if (expanded == NULL)
        expanded = expand_macro_single (macro, uri);
    }

  return expanded;
}

static void
expand_macro (char              macro,
              GString          *exec,
              GDesktopAppInfo  *info,
              GList           **uri_list)
{
  GList *uris = *uri_list;
  char *expanded = NULL;
  gboolean force_file_uri;
  char force_file_uri_macro;
  const char *uri;

  g_return_if_fail (exec != NULL);

  /* On %u and %U, pass POSIX file path pointing to the URI via
   * the FUSE mount in ~/.gvfs. Note that if the FUSE daemon isn't
   * running or the URI doesn't have a POSIX file path via FUSE
   * we'll just pass the URI.
   */
  force_file_uri_macro = macro;
  force_file_uri = FALSE;
  if (!info->no_fuse)
    {
      switch (macro)
        {
        case 'u':
          force_file_uri_macro = 'f';
          force_file_uri = TRUE;
          break;
        case 'U':
          force_file_uri_macro = 'F';
          force_file_uri = TRUE;
          break;
        default:
          break;
        }
    }

  switch (macro)
    {
    case 'u':
    case 'f':
    case 'd':
    case 'n':
      if (uris)
        {
          uri = uris->data;
          expanded = expand_macro_uri (macro, uri,
                                       force_file_uri, force_file_uri_macro);
          if (expanded)
            {
              g_string_append (exec, expanded);
              g_free (expanded);
            }
          uris = uris->next;
        }

      break;

    case 'U':
    case 'F':
    case 'D':
    case 'N':
      while (uris)
        {
          uri = uris->data;
          expanded = expand_macro_uri (macro, uri,
                                       force_file_uri, force_file_uri_macro);
          if (expanded)
            {
              g_string_append (exec, expanded);
              g_free (expanded);
            }

          uris = uris->next;

          if (uris != NULL && expanded)
            g_string_append_c (exec, ' ');
        }

      break;

    case 'i':
      if (info->icon_name)
        {
          g_string_append (exec, "--icon ");
          expanded = g_shell_quote (info->icon_name);
          g_string_append (exec, expanded);
          g_free (expanded);
        }
      break;

    case 'c':
      if (info->name)
        {
          expanded = g_shell_quote (info->name);
          g_string_append (exec, expanded);
          g_free (expanded);
        }
      break;

    case 'k':
      if (info->filename)
        {
          expanded = g_shell_quote (info->filename);
          g_string_append (exec, expanded);
          g_free (expanded);
        }
      break;

    case 'm': /* deprecated */
      break;

    case '%':
      g_string_append_c (exec, '%');
      break;
    }

  *uri_list = uris;
}

static gboolean
expand_application_parameters (GDesktopAppInfo   *info,
                               const gchar       *exec_line,
                               GList            **uris,
                               int               *argc,
                               char            ***argv,
                               GError           **error)
{
  GList *uri_list = *uris;
  const char *p = exec_line;
  GString *expanded_exec;
  gboolean res;

  if (exec_line == NULL)
    {
      g_set_error_literal (error, G_IO_ERROR, G_IO_ERROR_FAILED,
                           _("Desktop file didn’t specify Exec field"));
      return FALSE;
    }

  expanded_exec = g_string_new (NULL);

  while (*p)
    {
      if (p[0] == '%' && p[1] != '\0')
        {
          expand_macro (p[1], expanded_exec, info, uris);
          p++;
        }
      else
        g_string_append_c (expanded_exec, *p);

      p++;
    }

  /* No file substitutions */
  if (uri_list == *uris && uri_list != NULL)
    {
      /* If there is no macro default to %f. This is also what KDE does */
      g_string_append_c (expanded_exec, ' ');
      expand_macro ('f', expanded_exec, info, uris);
    }

  res = g_shell_parse_argv (expanded_exec->str, argc, argv, error);
  g_string_free (expanded_exec, TRUE);
  return res;
}

static gboolean
prepend_terminal_to_vector (int    *argc,
                            char ***argv)
{
#ifndef G_OS_WIN32
  char **real_argv;
  int real_argc;
  int i, j;
  char **term_argv = NULL;
  int term_argc = 0;
  char *check;
  char **the_argv;

  g_return_val_if_fail (argc != NULL, FALSE);
  g_return_val_if_fail (argv != NULL, FALSE);

  /* sanity */
  if(*argv == NULL)
    *argc = 0;

  the_argv = *argv;

  /* compute size if not given */
  if (*argc < 0)
    {
      for (i = 0; the_argv[i] != NULL; i++)
        ;
      *argc = i;
    }

  term_argc = 2;
  term_argv = g_new0 (char *, 3);

  check = g_find_program_in_path ("gnome-terminal");
  if (check != NULL)
    {
      term_argv[0] = check;
      /* Since 2017, gnome-terminal has preferred `--` over `-x` or `-e`. */
      term_argv[1] = g_strdup ("--");
    }
  else
    {
      if (check == NULL)
        check = g_find_program_in_path ("mate-terminal");
      if (check == NULL)
        check = g_find_program_in_path ("xfce4-terminal");
      if (check != NULL)
        {
          term_argv[0] = check;
          /* Note that gnome-terminal takes -x and
           * as -e in gnome-terminal is broken we use that. */
          term_argv[1] = g_strdup ("-x");
        }
      else
        {
          if (check == NULL)
            check = g_find_program_in_path ("tilix");
          if (check == NULL)
            check = g_find_program_in_path ("konsole");
          if (check == NULL)
            check = g_find_program_in_path ("nxterm");
          if (check == NULL)
            check = g_find_program_in_path ("color-xterm");
          if (check == NULL)
            check = g_find_program_in_path ("rxvt");
          if (check == NULL)
            check = g_find_program_in_path ("dtterm");
          if (check == NULL)
            check = g_find_program_in_path ("xterm");
          if (check == NULL)
            {
              g_debug ("Couldn’t find a known terminal");
              g_free (term_argv);
              return FALSE;
            }
          term_argv[0] = check;
          term_argv[1] = g_strdup ("-e");
        }
    }

  real_argc = term_argc + *argc;
  real_argv = g_new (char *, real_argc + 1);

  for (i = 0; i < term_argc; i++)
    real_argv[i] = term_argv[i];

  for (j = 0; j < *argc; j++, i++)
    real_argv[i] = (char *)the_argv[j];

  real_argv[i] = NULL;

  g_free (*argv);
  *argv = real_argv;
  *argc = real_argc;

  /* we use g_free here as we sucked all the inner strings
   * out from it into real_argv */
  g_free (term_argv);
  return TRUE;
#else
  return FALSE;
#endif /* G_OS_WIN32 */
}

static GList *
create_files_for_uris (GList *uris)
{
  GList *res;
  GList *iter;

  res = NULL;

  for (iter = uris; iter; iter = iter->next)
    {
      GFile *file = g_file_new_for_uri ((char *)iter->data);
      res = g_list_prepend (res, file);
    }

  return g_list_reverse (res);
}

static void
notify_desktop_launch (GDBusConnection  *session_bus,
                       GDesktopAppInfo  *info,
                       long              pid,
                       const char       *display,
                       const char       *sn_id,
                       GList            *uris)
{
  GDBusMessage *msg;
  GVariantBuilder uri_variant;
  GVariantBuilder extras_variant;
  GList *iter;
  const char *desktop_file_id;
  const char *gio_desktop_file;

  if (session_bus == NULL)
    return;

  g_variant_builder_init (&uri_variant, G_VARIANT_TYPE ("as"));
  for (iter = uris; iter; iter = iter->next)
    g_variant_builder_add (&uri_variant, "s", iter->data);

  g_variant_builder_init (&extras_variant, G_VARIANT_TYPE ("a{sv}"));
  if (sn_id != NULL && g_utf8_validate (sn_id, -1, NULL))
    g_variant_builder_add (&extras_variant, "{sv}",
                           "startup-id",
                           g_variant_new ("s",
                                          sn_id));
  gio_desktop_file = g_getenv ("GIO_LAUNCHED_DESKTOP_FILE");
  if (gio_desktop_file != NULL)
    g_variant_builder_add (&extras_variant, "{sv}",
                           "origin-desktop-file",
                           g_variant_new_bytestring (gio_desktop_file));
  if (g_get_prgname () != NULL)
    g_variant_builder_add (&extras_variant, "{sv}",
                           "origin-prgname",
                           g_variant_new_bytestring (g_get_prgname ()));
  g_variant_builder_add (&extras_variant, "{sv}",
                         "origin-pid",
                         g_variant_new ("x",
                                        (gint64)getpid ()));

  if (info->filename)
    desktop_file_id = info->filename;
  else if (info->desktop_id)
    desktop_file_id = info->desktop_id;
  else
    desktop_file_id = "";

  msg = g_dbus_message_new_signal ("/org/gtk/gio/DesktopAppInfo",
                                   "org.gtk.gio.DesktopAppInfo",
                                   "Launched");
  g_dbus_message_set_body (msg, g_variant_new ("(@aysxasa{sv})",
                                               g_variant_new_bytestring (desktop_file_id),
                                               display ? display : "",
                                               (gint64)pid,
                                               &uri_variant,
                                               &extras_variant));
  g_dbus_connection_send_message (session_bus,
                                  msg, 0,
                                  NULL,
                                  NULL);
  g_object_unref (msg);
}

static void
emit_launch_started (GAppLaunchContext *context,
                     GDesktopAppInfo   *info,
                     const gchar       *startup_id)
{
  GVariantBuilder builder;
  GVariant *platform_data = NULL;

  if (startup_id)
    {
      g_variant_builder_init (&builder, G_VARIANT_TYPE_ARRAY);
      g_variant_builder_add (&builder, "{sv}",
                             "startup-notification-id",
                             g_variant_new_string (startup_id));
      platform_data = g_variant_ref_sink (g_variant_builder_end (&builder));
    }
  g_signal_emit_by_name (context, "launch-started", info, platform_data);
  g_clear_pointer (&platform_data, g_variant_unref);
}

#define _SPAWN_FLAGS_DEFAULT (G_SPAWN_SEARCH_PATH)

static gboolean
g_desktop_app_info_launch_uris_with_spawn (GDesktopAppInfo            *info,
                                           GDBusConnection            *session_bus,
                                           const gchar                *exec_line,
                                           GList                      *uris,
                                           GAppLaunchContext          *launch_context,
                                           GSpawnFlags                 spawn_flags,
                                           GSpawnChildSetupFunc        user_setup,
                                           gpointer                    user_setup_data,
                                           GDesktopAppLaunchCallback   pid_callback,
                                           gpointer                    pid_callback_data,
                                           gint                        stdin_fd,
                                           gint                        stdout_fd,
                                           gint                        stderr_fd,
                                           GError                    **error)
{
  gboolean completed = FALSE;
  GList *old_uris;
  GList *dup_uris;

  char **argv, **envp;
  int argc;

  g_return_val_if_fail (info != NULL, FALSE);

  argv = NULL;

  if (launch_context)
    envp = g_app_launch_context_get_environment (launch_context);
  else
    envp = g_get_environ ();

  /* The GList* passed to expand_application_parameters() will be modified
   * internally by expand_macro(), so we need to pass a copy of it instead,
   * and also use that copy to control the exit condition of the loop below.
   */
  dup_uris = uris;
  do
    {
      GPid pid;
      GList *launched_uris;
      GList *iter;
      char *sn_id = NULL;
      char **wrapped_argv;
      int i;
      gsize j;
      const gchar * const wrapper_argv[] =
        {
          "/bin/sh",
          "-e",
          "-u",
          "-c", "export GIO_LAUNCHED_DESKTOP_FILE_PID=$$; exec \"$@\"",
          "sh",  /* argv[0] for sh */
        };

      old_uris = dup_uris;
      if (!expand_application_parameters (info, exec_line, &dup_uris, &argc, &argv, error))
        goto out;

      /* Get the subset of URIs we're launching with this process */
      launched_uris = NULL;
      for (iter = old_uris; iter != NULL && iter != dup_uris; iter = iter->next)
        launched_uris = g_list_prepend (launched_uris, iter->data);
      launched_uris = g_list_reverse (launched_uris);

      if (info->terminal && !prepend_terminal_to_vector (&argc, &argv))
        {
          g_set_error_literal (error, G_IO_ERROR, G_IO_ERROR_FAILED,
                               _("Unable to find terminal required for application"));
          goto out;
        }

      if (info->filename)
        envp = g_environ_setenv (envp,
                                 "GIO_LAUNCHED_DESKTOP_FILE",
                                 info->filename,
                                 TRUE);

      sn_id = NULL;
      if (launch_context)
        {
          GList *launched_files = create_files_for_uris (launched_uris);

          if (info->startup_notify)
            {
              sn_id = g_app_launch_context_get_startup_notify_id (launch_context,
                                                                  G_APP_INFO (info),
                                                                  launched_files);
              if (sn_id)
                envp = g_environ_setenv (envp, "DESKTOP_STARTUP_ID", sn_id, TRUE);
            }

          g_list_free_full (launched_files, g_object_unref);

          emit_launch_started (launch_context, info, sn_id);
        }

      /* Wrap the @argv in a command which will set the
       * `GIO_LAUNCHED_DESKTOP_FILE_PID` environment variable. We can’t set this
       * in @envp along with `GIO_LAUNCHED_DESKTOP_FILE` because we need to know
       * the PID of the new forked process. We can’t use setenv() between fork()
       * and exec() because we’d rather use posix_spawn() for speed.
       *
       * `sh` should be available on all the platforms that `GDesktopAppInfo`
       * currently supports (since they are all POSIX). If additional platforms
       * need to be supported in future, it will probably have to be replaced
       * with a wrapper program (grep the GLib git history for
       * `gio-launch-desktop` for an example of this which could be
       * resurrected). */
      wrapped_argv = g_new (char *, argc + G_N_ELEMENTS (wrapper_argv) + 1);

      for (j = 0; j < G_N_ELEMENTS (wrapper_argv); j++)
        wrapped_argv[j] = g_strdup (wrapper_argv[j]);
      for (i = 0; i < argc; i++)
        wrapped_argv[i + G_N_ELEMENTS (wrapper_argv)] = g_steal_pointer (&argv[i]);

      wrapped_argv[i + G_N_ELEMENTS (wrapper_argv)] = NULL;
      g_free (argv);
      argv = NULL;

      if (!g_spawn_async_with_fds (info->path,
                                   wrapped_argv,
                                   envp,
                                   spawn_flags,
                                   user_setup,
                                   user_setup_data,
                                   &pid,
                                   stdin_fd,
                                   stdout_fd,
                                   stderr_fd,
                                   error))
        {
          if (sn_id)
            g_app_launch_context_launch_failed (launch_context, sn_id);

          g_free (sn_id);
          g_list_free (launched_uris);

          goto out;
        }

      if (pid_callback != NULL)
        pid_callback (info, pid, pid_callback_data);

      if (launch_context != NULL)
        {
          GVariantBuilder builder;
          GVariant *platform_data;

          g_variant_builder_init (&builder, G_VARIANT_TYPE_ARRAY);
          g_variant_builder_add (&builder, "{sv}", "pid", g_variant_new_int32 (pid));
          if (sn_id)
            g_variant_builder_add (&builder, "{sv}", "startup-notification-id", g_variant_new_string (sn_id));
          platform_data = g_variant_ref_sink (g_variant_builder_end (&builder));
          g_signal_emit_by_name (launch_context, "launched", info, platform_data);
          g_variant_unref (platform_data);
        }

      notify_desktop_launch (session_bus,
                             info,
                             pid,
                             NULL,
                             sn_id,
                             launched_uris);

      g_free (sn_id);
      g_list_free (launched_uris);

      g_strfreev (wrapped_argv);
      wrapped_argv = NULL;
    }
  while (dup_uris != NULL);

  completed = TRUE;

 out:
  g_strfreev (argv);
  g_strfreev (envp);

  return completed;
}

static gchar *
object_path_from_appid (const gchar *appid)
{
  gchar *appid_path, *iter;

  appid_path = g_strconcat ("/", appid, NULL);
  for (iter = appid_path; *iter; iter++)
    {
      if (*iter == '.')
        *iter = '/';

      if (*iter == '-')
        *iter = '_';
    }

  return appid_path;
}

static GVariant *
g_desktop_app_info_make_platform_data (GDesktopAppInfo   *info,
                                       GList             *uris,
                                       GAppLaunchContext *launch_context)
{
  GVariantBuilder builder;

  g_variant_builder_init (&builder, G_VARIANT_TYPE_VARDICT);

  if (launch_context)
    {
      GList *launched_files = create_files_for_uris (uris);

      if (info->startup_notify)
        {
          gchar *sn_id;

          sn_id = g_app_launch_context_get_startup_notify_id (launch_context, G_APP_INFO (info), launched_files);
          if (sn_id)
            g_variant_builder_add (&builder, "{sv}", "desktop-startup-id", g_variant_new_take_string (sn_id));
        }

      g_list_free_full (launched_files, g_object_unref);
    }

  return g_variant_builder_end (&builder);
}

typedef struct
{
  GDesktopAppInfo     *info; /* (owned) */
  GAppLaunchContext   *launch_context; /* (owned) (nullable) */
  GAsyncReadyCallback  callback;
  gchar               *startup_id; /* (owned) */
  gpointer             user_data;
} LaunchUrisWithDBusData;

static void
launch_uris_with_dbus_data_free (LaunchUrisWithDBusData *data)
{
  g_clear_object (&data->info);
  g_clear_object (&data->launch_context);
  g_free (data->startup_id);

  g_free (data);
}

static void
launch_uris_with_dbus_signal_cb (GObject      *object,
                                 GAsyncResult *result,
                                 gpointer      user_data)
{
  LaunchUrisWithDBusData *data = user_data;
  GVariantBuilder builder;

  if (data->launch_context)
    {
      if (g_task_had_error (G_TASK (result)))
        g_app_launch_context_launch_failed (data->launch_context, data->startup_id);
      else
        {
          GVariant *platform_data;

          g_variant_builder_init (&builder, G_VARIANT_TYPE_ARRAY);
          /* the docs guarantee `pid` will be set, but we can’t
           * easily know it for a D-Bus process, so set it to zero */
          g_variant_builder_add (&builder, "{sv}", "pid", g_variant_new_int32 (0));
          if (data->startup_id)
            g_variant_builder_add (&builder, "{sv}",
                                   "startup-notification-id",
                                   g_variant_new_string (data->startup_id));
          platform_data = g_variant_ref_sink (g_variant_builder_end (&builder));
          g_signal_emit_by_name (data->launch_context,
                                 "launched",
                                 data->info,
                                 platform_data);
          g_variant_unref (platform_data);
        }
    }

  if (data->callback)
    data->callback (object, result, data->user_data);

  launch_uris_with_dbus_data_free (data);
}

static void
launch_uris_with_dbus (GDesktopAppInfo    *info,
                       GDBusConnection    *session_bus,
                       GList              *uris,
                       GAppLaunchContext  *launch_context,
                       GCancellable       *cancellable,
                       GAsyncReadyCallback callback,
                       gpointer            user_data)
{
  GVariant *platform_data;
  GVariantBuilder builder;
  GVariantDict dict;
  gchar *object_path;
  LaunchUrisWithDBusData *data;

  g_variant_builder_init (&builder, G_VARIANT_TYPE_TUPLE);

  if (uris)
    {
      GList *iter;

      g_variant_builder_open (&builder, G_VARIANT_TYPE_STRING_ARRAY);
      for (iter = uris; iter; iter = iter->next)
        g_variant_builder_add (&builder, "s", iter->data);
      g_variant_builder_close (&builder);
    }

  platform_data = g_desktop_app_info_make_platform_data (info, uris, launch_context);

  g_variant_builder_add_value (&builder, platform_data);
  object_path = object_path_from_appid (info->app_id);

  data = g_new0 (LaunchUrisWithDBusData, 1);
  data->info = g_object_ref (info);
  data->callback = callback;
  data->user_data = user_data;
  data->launch_context = launch_context ? g_object_ref (launch_context) : NULL;
  g_variant_dict_init (&dict, platform_data);
  g_variant_dict_lookup (&dict, "desktop-startup-id", "s", &data->startup_id);

  if (launch_context)
    emit_launch_started (launch_context, info, data->startup_id);

  g_dbus_connection_call (session_bus, info->app_id, object_path, "org.freedesktop.Application",
                          uris ? "Open" : "Activate", g_variant_builder_end (&builder),
                          NULL, G_DBUS_CALL_FLAGS_NONE, -1,
                          cancellable, launch_uris_with_dbus_signal_cb, g_steal_pointer (&data));
  g_free (object_path);

  g_variant_dict_clear (&dict);
}

static gboolean
g_desktop_app_info_launch_uris_with_dbus (GDesktopAppInfo    *info,
                                          GDBusConnection    *session_bus,
                                          GList              *uris,
                                          GAppLaunchContext  *launch_context,
                                          GCancellable       *cancellable,
                                          GAsyncReadyCallback callback,
                                          gpointer            user_data)
{
  GList *ruris = uris;
  char *app_id = NULL;

  g_return_val_if_fail (info != NULL, FALSE);

#ifdef G_OS_UNIX
  app_id = g_desktop_app_info_get_string (info, "X-Flatpak");
  if (app_id && *app_id)
    {
      ruris = g_document_portal_add_documents (uris, app_id, NULL);
      if (ruris == NULL)
        ruris = uris;
    }
#endif

  launch_uris_with_dbus (info, session_bus, ruris, launch_context,
                         cancellable, callback, user_data);

  if (ruris != uris)
    g_list_free_full (ruris, g_free);

  g_free (app_id);

  return TRUE;
}

static gboolean
g_desktop_app_info_launch_uris_internal (GAppInfo                   *appinfo,
                                         GList                      *uris,
                                         GAppLaunchContext          *launch_context,
                                         GSpawnFlags                 spawn_flags,
                                         GSpawnChildSetupFunc        user_setup,
                                         gpointer                    user_setup_data,
                                         GDesktopAppLaunchCallback   pid_callback,
                                         gpointer                    pid_callback_data,
                                         gint                        stdin_fd,
                                         gint                        stdout_fd,
                                         gint                        stderr_fd,
                                         GError                     **error)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);
  GDBusConnection *session_bus;
  gboolean success = TRUE;

  session_bus = g_bus_get_sync (G_BUS_TYPE_SESSION, NULL, NULL);

  if (session_bus && info->app_id)
    /* This is non-blocking API. Similar to launching via fork()/exec()
     * we don't wait around to see if the program crashed during startup.
     * This is what startup-notification's job is...
     */
    g_desktop_app_info_launch_uris_with_dbus (info, session_bus, uris, launch_context,
                                              NULL, NULL, NULL);
  else
    success = g_desktop_app_info_launch_uris_with_spawn (info, session_bus, info->exec, uris, launch_context,
                                                         spawn_flags, user_setup, user_setup_data,
                                                         pid_callback, pid_callback_data,
                                                         stdin_fd, stdout_fd, stderr_fd, error);

  if (session_bus != NULL)
    {
      /* This asynchronous flush holds a reference until it completes,
       * which ensures that the following unref won't immediately kill
       * the connection if we were the initial owner.
       */
      g_dbus_connection_flush (session_bus, NULL, NULL, NULL);
      g_object_unref (session_bus);
    }

  return success;
}

static gboolean
g_desktop_app_info_launch_uris (GAppInfo           *appinfo,
                                GList              *uris,
                                GAppLaunchContext  *launch_context,
                                GError            **error)
{
  return g_desktop_app_info_launch_uris_internal (appinfo, uris,
                                                  launch_context,
                                                  _SPAWN_FLAGS_DEFAULT,
                                                  NULL, NULL, NULL, NULL,
                                                  -1, -1, -1,
                                                  error);
}

typedef struct
{
  GAppInfo *appinfo;
  GList *uris;
  GAppLaunchContext *context;
} LaunchUrisData;

static void
launch_uris_data_free (LaunchUrisData *data)
{
  g_clear_object (&data->context);
  g_list_free_full (data->uris, g_free);
  g_free (data);
}

static void
launch_uris_with_dbus_cb (GObject      *object,
                          GAsyncResult *result,
                          gpointer      user_data)
{
  GTask *task = G_TASK (user_data);
  GError *error = NULL;

  g_dbus_connection_call_finish (G_DBUS_CONNECTION (object), result, &error);
  if (error != NULL)
    {
      g_dbus_error_strip_remote_error (error);
      g_task_return_error (task, g_steal_pointer (&error));
    }
  else
    g_task_return_boolean (task, TRUE);

  g_object_unref (task);
}

static void
launch_uris_flush_cb (GObject      *object,
                      GAsyncResult *result,
                      gpointer      user_data)
{
  GTask *task = G_TASK (user_data);

  g_dbus_connection_flush_finish (G_DBUS_CONNECTION (object), result, NULL);
  g_task_return_boolean (task, TRUE);
  g_object_unref (task);
}

static void
launch_uris_bus_get_cb (GObject      *object,
                        GAsyncResult *result,
                        gpointer      user_data)
{
  GTask *task = G_TASK (user_data);
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (g_task_get_source_object (task));
  LaunchUrisData *data = g_task_get_task_data (task);
  GCancellable *cancellable = g_task_get_cancellable (task);
  GDBusConnection *session_bus;
  GError *error = NULL;

  session_bus = g_bus_get_finish (result, NULL);

  if (session_bus && info->app_id)
    {
      /* FIXME: The g_document_portal_add_documents() function, which is called
       * from the g_desktop_app_info_launch_uris_with_dbus() function, still
       * uses blocking calls.
       */
      g_desktop_app_info_launch_uris_with_dbus (info, session_bus,
                                                data->uris, data->context,
                                                cancellable,
                                                launch_uris_with_dbus_cb,
                                                g_steal_pointer (&task));
    }
  else
    {
      /* FIXME: The D-Bus message from the notify_desktop_launch() function
       * can be still lost even if flush is called later. See:
       * https://gitlab.freedesktop.org/dbus/dbus/issues/72
       */
      g_desktop_app_info_launch_uris_with_spawn (info, session_bus, info->exec,
                                                 data->uris, data->context,
                                                 _SPAWN_FLAGS_DEFAULT, NULL,
                                                 NULL, NULL, NULL, -1, -1, -1,
                                                 &error);
      if (error != NULL)
        {
          g_task_return_error (task, g_steal_pointer (&error));
          g_object_unref (task);
        }
      else
        g_dbus_connection_flush (session_bus,
                                 cancellable,
                                 launch_uris_flush_cb,
                                 g_steal_pointer (&task));
    }

  g_clear_object (&session_bus);
}

static void
g_desktop_app_info_launch_uris_async (GAppInfo           *appinfo,
                                      GList              *uris,
                                      GAppLaunchContext  *context,
                                      GCancellable       *cancellable,
                                      GAsyncReadyCallback callback,
                                      gpointer            user_data)
{
  GTask *task;
  LaunchUrisData *data;

  task = g_task_new (appinfo, cancellable, callback, user_data);
  g_task_set_source_tag (task, g_desktop_app_info_launch_uris_async);

  data = g_new0 (LaunchUrisData, 1);
  data->uris = g_list_copy_deep (uris, (GCopyFunc) g_strdup, NULL);
  data->context = (context != NULL) ? g_object_ref (context) : NULL;
  g_task_set_task_data (task, g_steal_pointer (&data), (GDestroyNotify) launch_uris_data_free);

  g_bus_get (G_BUS_TYPE_SESSION, cancellable, launch_uris_bus_get_cb, task);
}

static gboolean
g_desktop_app_info_launch_uris_finish (GAppInfo     *appinfo,
                                       GAsyncResult *result,
                                       GError      **error)
{
  g_return_val_if_fail (g_task_is_valid (result, appinfo), FALSE);

  return g_task_propagate_boolean (G_TASK (result), error);
}

static gboolean
g_desktop_app_info_supports_uris (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->exec &&
    ((strstr (info->exec, "%u") != NULL) ||
     (strstr (info->exec, "%U") != NULL));
}

static gboolean
g_desktop_app_info_supports_files (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return info->exec &&
    ((strstr (info->exec, "%f") != NULL) ||
     (strstr (info->exec, "%F") != NULL));
}

static gboolean
g_desktop_app_info_launch (GAppInfo           *appinfo,
                           GList              *files,
                           GAppLaunchContext  *launch_context,
                           GError            **error)
{
  GList *uris;
  char *uri;
  gboolean res;

  uris = NULL;
  while (files)
    {
      uri = g_file_get_uri (files->data);
      uris = g_list_prepend (uris, uri);
      files = files->next;
    }

  uris = g_list_reverse (uris);

  res = g_desktop_app_info_launch_uris (appinfo, uris, launch_context, error);

  g_list_free_full (uris, g_free);

  return res;
}

/**
 * g_desktop_app_info_launch_uris_as_manager_with_fds:
 * @appinfo: a #GDesktopAppInfo
 * @uris: (element-type utf8): List of URIs
 * @launch_context: (nullable): a #GAppLaunchContext
 * @spawn_flags: #GSpawnFlags, used for each process
 * @user_setup: (scope async) (nullable): a #GSpawnChildSetupFunc, used once
 *     for each process.
 * @user_setup_data: (closure user_setup) (nullable): User data for @user_setup
 * @pid_callback: (scope call) (nullable): Callback for child processes
 * @pid_callback_data: (closure pid_callback) (nullable): User data for @callback
 * @stdin_fd: file descriptor to use for child's stdin, or -1
 * @stdout_fd: file descriptor to use for child's stdout, or -1
 * @stderr_fd: file descriptor to use for child's stderr, or -1
 * @error: return location for a #GError, or %NULL
 *
 * Equivalent to g_desktop_app_info_launch_uris_as_manager() but allows
 * you to pass in file descriptors for the stdin, stdout and stderr streams
 * of the launched process.
 *
 * If application launching occurs via some non-spawn mechanism (e.g. D-Bus
 * activation) then @stdin_fd, @stdout_fd and @stderr_fd are ignored.
 *
 * Returns: %TRUE on successful launch, %FALSE otherwise.
 *
 * Since: 2.58
 */
gboolean
g_desktop_app_info_launch_uris_as_manager_with_fds (GDesktopAppInfo            *appinfo,
                                                    GList                      *uris,
                                                    GAppLaunchContext          *launch_context,
                                                    GSpawnFlags                 spawn_flags,
                                                    GSpawnChildSetupFunc        user_setup,
                                                    gpointer                    user_setup_data,
                                                    GDesktopAppLaunchCallback   pid_callback,
                                                    gpointer                    pid_callback_data,
                                                    gint                        stdin_fd,
                                                    gint                        stdout_fd,
                                                    gint                        stderr_fd,
                                                    GError                    **error)
{
  return g_desktop_app_info_launch_uris_internal ((GAppInfo*)appinfo,
                                                  uris,
                                                  launch_context,
                                                  spawn_flags,
                                                  user_setup,
                                                  user_setup_data,
                                                  pid_callback,
                                                  pid_callback_data,
                                                  stdin_fd,
                                                  stdout_fd,
                                                  stderr_fd,
                                                  error);
}

/**
 * g_desktop_app_info_launch_uris_as_manager:
 * @appinfo: a #GDesktopAppInfo
 * @uris: (element-type utf8): List of URIs
 * @launch_context: (nullable): a #GAppLaunchContext
 * @spawn_flags: #GSpawnFlags, used for each process
 * @user_setup: (scope async) (nullable): a #GSpawnChildSetupFunc, used once
 *     for each process.
 * @user_setup_data: (closure user_setup) (nullable): User data for @user_setup
 * @pid_callback: (scope call) (nullable): Callback for child processes
 * @pid_callback_data: (closure pid_callback) (nullable): User data for @callback
 * @error: return location for a #GError, or %NULL
 *
 * This function performs the equivalent of g_app_info_launch_uris(),
 * but is intended primarily for operating system components that
 * launch applications.  Ordinary applications should use
 * g_app_info_launch_uris().
 *
 * If the application is launched via GSpawn, then @spawn_flags, @user_setup
 * and @user_setup_data are used for the call to g_spawn_async().
 * Additionally, @pid_callback (with @pid_callback_data) will be called to
 * inform about the PID of the created process. See g_spawn_async_with_pipes()
 * for information on certain parameter conditions that can enable an
 * optimized posix_spawn() codepath to be used.
 *
 * If application launching occurs via some other mechanism (eg: D-Bus
 * activation) then @spawn_flags, @user_setup, @user_setup_data,
 * @pid_callback and @pid_callback_data are ignored.
 *
 * Returns: %TRUE on successful launch, %FALSE otherwise.
 */
gboolean
g_desktop_app_info_launch_uris_as_manager (GDesktopAppInfo            *appinfo,
                                           GList                      *uris,
                                           GAppLaunchContext          *launch_context,
                                           GSpawnFlags                 spawn_flags,
                                           GSpawnChildSetupFunc        user_setup,
                                           gpointer                    user_setup_data,
                                           GDesktopAppLaunchCallback   pid_callback,
                                           gpointer                    pid_callback_data,
                                           GError                    **error)
{
  return g_desktop_app_info_launch_uris_as_manager_with_fds (appinfo,
                                                             uris,
                                                             launch_context,
                                                             spawn_flags,
                                                             user_setup,
                                                             user_setup_data,
                                                             pid_callback,
                                                             pid_callback_data,
                                                             -1, -1, -1,
                                                             error);
}

/* OnlyShowIn API support {{{2 */

/**
 * g_desktop_app_info_set_desktop_env:
 * @desktop_env: a string specifying what desktop this is
 *
 * Sets the name of the desktop that the application is running in.
 * This is used by g_app_info_should_show() and
 * g_desktop_app_info_get_show_in() to evaluate the
 * `OnlyShowIn` and `NotShowIn`
 * desktop entry fields.
 *
 * Should be called only once; subsequent calls are ignored.
 *
 * Deprecated:2.42:do not use this API.  Since 2.42 the value of the
 * `XDG_CURRENT_DESKTOP` environment variable will be used.
 */
void
g_desktop_app_info_set_desktop_env (const gchar *desktop_env)
{
  get_current_desktops (desktop_env);
}

static gboolean
g_desktop_app_info_should_show (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (info->nodisplay)
    return FALSE;

  return g_desktop_app_info_get_show_in (info, NULL);
}

/* mime types/default apps support {{{2 */

typedef enum {
  CONF_DIR,
  APP_DIR,
  MIMETYPE_DIR
} DirType;

static char *
ensure_dir (DirType   type,
            GError  **error)
{
  char *path, *display_name;
  int errsv;

  switch (type)
    {
    case CONF_DIR:
      path = g_build_filename (g_get_user_config_dir (), NULL);
      break;

    case APP_DIR:
      path = g_build_filename (g_get_user_data_dir (), "applications", NULL);
      break;

    case MIMETYPE_DIR:
      path = g_build_filename (g_get_user_data_dir (), "mime", "packages", NULL);
      break;

    default:
      g_assert_not_reached ();
    }

  g_debug ("%s: Ensuring %s", G_STRFUNC, path);

  errno = 0;
  if (g_mkdir_with_parents (path, 0700) == 0)
    return path;

  errsv = errno;
  display_name = g_filename_display_name (path);
  if (type == APP_DIR)
    g_set_error (error, G_IO_ERROR, g_io_error_from_errno (errsv),
                 _("Can’t create user application configuration folder %s: %s"),
                 display_name, g_strerror (errsv));
  else
    g_set_error (error, G_IO_ERROR, g_io_error_from_errno (errsv),
                 _("Can’t create user MIME configuration folder %s: %s"),
                 display_name, g_strerror (errsv));

  g_free (display_name);
  g_free (path);

  return NULL;
}

static gboolean
update_mimeapps_list (const char  *desktop_id,
                      const char  *content_type,
                      UpdateMimeFlags flags,
                      GError     **error)
{
  char *dirname, *filename, *string;
  GKeyFile *key_file;
  gboolean load_succeeded, res;
  char **old_list, **list;
  gsize length, data_size;
  char *data;
  int i, j, k;
  char **content_types;

  /* Don't add both at start and end */
  g_assert (!((flags & UPDATE_MIME_SET_DEFAULT) &&
              (flags & UPDATE_MIME_SET_NON_DEFAULT)));

  dirname = ensure_dir (CONF_DIR, error);
  if (!dirname)
    return FALSE;

  filename = g_build_filename (dirname, "mimeapps.list", NULL);
  g_free (dirname);

  key_file = g_key_file_new ();
  load_succeeded = g_key_file_load_from_file (key_file, filename, G_KEY_FILE_NONE, NULL);
  if (!load_succeeded ||
      (!g_key_file_has_group (key_file, ADDED_ASSOCIATIONS_GROUP) &&
       !g_key_file_has_group (key_file, REMOVED_ASSOCIATIONS_GROUP) &&
       !g_key_file_has_group (key_file, DEFAULT_APPLICATIONS_GROUP)))
    {
      g_key_file_free (key_file);
      key_file = g_key_file_new ();
    }

  if (content_type)
    {
      content_types = g_new (char *, 2);
      content_types[0] = g_strdup (content_type);
      content_types[1] = NULL;
    }
  else
    {
      content_types = g_key_file_get_keys (key_file, DEFAULT_APPLICATIONS_GROUP, NULL, NULL);
    }

  for (k = 0; content_types && content_types[k]; k++)
    {
      /* set as default, if requested so */
      string = g_key_file_get_string (key_file,
                                      DEFAULT_APPLICATIONS_GROUP,
                                      content_types[k],
                                      NULL);

      if (g_strcmp0 (string, desktop_id) != 0 &&
          (flags & UPDATE_MIME_SET_DEFAULT))
        {
          g_free (string);
          string = g_strdup (desktop_id);

          /* add in the non-default list too, if it's not already there */
          flags |= UPDATE_MIME_SET_NON_DEFAULT;
        }

      if (string == NULL || desktop_id == NULL)
        g_key_file_remove_key (key_file,
                               DEFAULT_APPLICATIONS_GROUP,
                               content_types[k],
                               NULL);
      else
        g_key_file_set_string (key_file,
                               DEFAULT_APPLICATIONS_GROUP,
                               content_types[k],
                               string);

      g_free (string);
    }

  if (content_type)
    {
      /* reuse the list from above */
    }
  else
    {
      g_strfreev (content_types);
      content_types = g_key_file_get_keys (key_file, ADDED_ASSOCIATIONS_GROUP, NULL, NULL);
    }

  for (k = 0; content_types && content_types[k]; k++)
    {
      /* Add to the right place in the list */

      length = 0;
      old_list = g_key_file_get_string_list (key_file, ADDED_ASSOCIATIONS_GROUP,
                                             content_types[k], &length, NULL);

      list = g_new (char *, 1 + length + 1);

      i = 0;

      /* if we're adding a last-used hint, just put the application in front of the list */
      if (flags & UPDATE_MIME_SET_LAST_USED)
        {
          /* avoid adding this again as non-default later */
          if (flags & UPDATE_MIME_SET_NON_DEFAULT)
            flags ^= UPDATE_MIME_SET_NON_DEFAULT;

          list[i++] = g_strdup (desktop_id);
        }

      if (old_list)
        {
          for (j = 0; old_list[j] != NULL; j++)
            {
              if (g_strcmp0 (old_list[j], desktop_id) != 0)
                {
                  /* rewrite other entries if they're different from the new one */
                  list[i++] = g_strdup (old_list[j]);
                }
              else if (flags & UPDATE_MIME_SET_NON_DEFAULT)
                {
                  /* we encountered an old entry which is equal to the one we're adding as non-default,
                   * don't change its position in the list.
                   */
                  flags ^= UPDATE_MIME_SET_NON_DEFAULT;
                  list[i++] = g_strdup (old_list[j]);
                }
            }
        }

      /* add it at the end of the list */
      if (flags & UPDATE_MIME_SET_NON_DEFAULT)
        list[i++] = g_strdup (desktop_id);

      list[i] = NULL;

      g_strfreev (old_list);

      if (list[0] == NULL || desktop_id == NULL)
        g_key_file_remove_key (key_file,
                               ADDED_ASSOCIATIONS_GROUP,
                               content_types[k],
                               NULL);
      else
        g_key_file_set_string_list (key_file,
                                    ADDED_ASSOCIATIONS_GROUP,
                                    content_types[k],
                                    (const char * const *)list, i);

      g_strfreev (list);
    }

  if (content_type)
    {
      /* reuse the list from above */
    }
  else
    {
      g_strfreev (content_types);
      content_types = g_key_file_get_keys (key_file, REMOVED_ASSOCIATIONS_GROUP, NULL, NULL);
    }

  for (k = 0; content_types && content_types[k]; k++)
    {
      /* Remove from removed associations group (unless remove) */

      length = 0;
      old_list = g_key_file_get_string_list (key_file, REMOVED_ASSOCIATIONS_GROUP,
                                             content_types[k], &length, NULL);

      list = g_new (char *, 1 + length + 1);

      i = 0;
      if (flags & UPDATE_MIME_REMOVE)
        list[i++] = g_strdup (desktop_id);
      if (old_list)
        {
          for (j = 0; old_list[j] != NULL; j++)
            {
              if (g_strcmp0 (old_list[j], desktop_id) != 0)
                list[i++] = g_strdup (old_list[j]);
            }
        }
      list[i] = NULL;

      g_strfreev (old_list);

      if (list[0] == NULL || desktop_id == NULL)
        g_key_file_remove_key (key_file,
                               REMOVED_ASSOCIATIONS_GROUP,
                               content_types[k],
                               NULL);
      else
        g_key_file_set_string_list (key_file,
                                    REMOVED_ASSOCIATIONS_GROUP,
                                    content_types[k],
                                    (const char * const *)list, i);

      g_strfreev (list);
    }

  g_strfreev (content_types);

  data = g_key_file_to_data (key_file, &data_size, error);
  g_key_file_free (key_file);

  res = g_file_set_contents_full (filename, data, data_size,
                                  G_FILE_SET_CONTENTS_CONSISTENT | G_FILE_SET_CONTENTS_ONLY_EXISTING,
                                  0600, error);

  desktop_file_dirs_invalidate_user_config ();

  g_free (filename);
  g_free (data);

  return res;
}

static gboolean
g_desktop_app_info_set_as_last_used_for_type (GAppInfo    *appinfo,
                                              const char  *content_type,
                                              GError     **error)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (!g_desktop_app_info_ensure_saved (info, error))
    return FALSE;

  if (!info->desktop_id)
    {
      g_set_error_literal (error, G_IO_ERROR, G_IO_ERROR_FAILED,
                           _("Application information lacks an identifier"));
      return FALSE;
    }

  /* both add support for the content type and set as last used */
  return update_mimeapps_list (info->desktop_id, content_type,
                               UPDATE_MIME_SET_NON_DEFAULT |
                               UPDATE_MIME_SET_LAST_USED,
                               error);
}

static gboolean
g_desktop_app_info_set_as_default_for_type (GAppInfo    *appinfo,
                                            const char  *content_type,
                                            GError     **error)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (!g_desktop_app_info_ensure_saved (info, error))
    return FALSE;

  if (!info->desktop_id)
    {
      g_set_error_literal (error, G_IO_ERROR, G_IO_ERROR_FAILED,
                           _("Application information lacks an identifier"));
      return FALSE;
    }

  return update_mimeapps_list (info->desktop_id, content_type,
                               UPDATE_MIME_SET_DEFAULT,
                               error);
}

static void
update_program_done (GPid     pid,
                     gint     status,
                     gpointer data)
{
  /* Did the application exit correctly */
  if (g_spawn_check_wait_status (status, NULL))
    {
      /* Here we could clean out any caches in use */
    }
}

static void
run_update_command (char *command,
                    char *subdir)
{
        char *argv[3] = {
                NULL,
                NULL,
                NULL,
        };
        GPid pid = 0;
        GError *error = NULL;

        argv[0] = command;
        argv[1] = g_build_filename (g_get_user_data_dir (), subdir, NULL);

        if (g_spawn_async ("/", argv,
                           NULL,       /* envp */
                           G_SPAWN_SEARCH_PATH |
                           G_SPAWN_STDOUT_TO_DEV_NULL |
                           G_SPAWN_STDERR_TO_DEV_NULL |
                           G_SPAWN_DO_NOT_REAP_CHILD,
                           NULL, NULL, /* No setup function */
                           &pid,
                           &error))
          g_child_watch_add (pid, update_program_done, NULL);
        else
          {
            /* If we get an error at this point, it's quite likely the user doesn't
             * have an installed copy of either 'update-mime-database' or
             * 'update-desktop-database'.  I don't think we want to popup an error
             * dialog at this point, so we just do a g_warning to give the user a
             * chance of debugging it.
             */
            g_warning ("%s", error->message);
            g_error_free (error);
          }

        g_free (argv[1]);
}

static gboolean
g_desktop_app_info_set_as_default_for_extension (GAppInfo    *appinfo,
                                                 const char  *extension,
                                                 GError     **error)
{
  char *filename, *basename, *mimetype;
  char *dirname;
  gboolean res;

  if (!g_desktop_app_info_ensure_saved (G_DESKTOP_APP_INFO (appinfo), error))
    return FALSE;

  dirname = ensure_dir (MIMETYPE_DIR, error);
  if (!dirname)
    return FALSE;

  basename = g_strdup_printf ("user-extension-%s.xml", extension);
  filename = g_build_filename (dirname, basename, NULL);
  g_free (basename);
  g_free (dirname);

  mimetype = g_strdup_printf ("application/x-extension-%s", extension);

  if (!g_file_test (filename, G_FILE_TEST_EXISTS))
    {
      char *contents;

      contents =
        g_strdup_printf ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                         "<mime-info xmlns=\"http://www.freedesktop.org/standards/shared-mime-info\">\n"
                         " <mime-type type=\"%s\">\n"
                         "  <comment>%s document</comment>\n"
                         "  <glob pattern=\"*.%s\"/>\n"
                         " </mime-type>\n"
                         "</mime-info>\n", mimetype, extension, extension);

      g_file_set_contents_full (filename, contents, -1,
                                G_FILE_SET_CONTENTS_CONSISTENT | G_FILE_SET_CONTENTS_ONLY_EXISTING,
                                0600, NULL);
      g_free (contents);

      run_update_command ("update-mime-database", "mime");
    }
  g_free (filename);

  res = g_desktop_app_info_set_as_default_for_type (appinfo,
                                                    mimetype,
                                                    error);

  g_free (mimetype);

  return res;
}

static gboolean
g_desktop_app_info_add_supports_type (GAppInfo    *appinfo,
                                      const char  *content_type,
                                      GError     **error)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (!g_desktop_app_info_ensure_saved (G_DESKTOP_APP_INFO (info), error))
    return FALSE;

  return update_mimeapps_list (info->desktop_id, content_type,
                               UPDATE_MIME_SET_NON_DEFAULT,
                               error);
}

static gboolean
g_desktop_app_info_can_remove_supports_type (GAppInfo *appinfo)
{
  return TRUE;
}

static gboolean
g_desktop_app_info_remove_supports_type (GAppInfo    *appinfo,
                                         const char  *content_type,
                                         GError     **error)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (!g_desktop_app_info_ensure_saved (G_DESKTOP_APP_INFO (info), error))
    return FALSE;

  return update_mimeapps_list (info->desktop_id, content_type,
                               UPDATE_MIME_REMOVE,
                               error);
}

static const char **
g_desktop_app_info_get_supported_types (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  return (const char**) info->mime_types;
}

/* Saving and deleting {{{2 */

static gboolean
g_desktop_app_info_ensure_saved (GDesktopAppInfo  *info,
                                 GError          **error)
{
  GKeyFile *key_file;
  char *dirname;
  char *filename;
  char *data, *desktop_id;
  gsize data_size;
  int fd;
  gboolean res;

  if (info->filename != NULL)
    return TRUE;

  /* This is only used for object created with
   * g_app_info_create_from_commandline. All other
   * object should have a filename
   */

  dirname = ensure_dir (APP_DIR, error);
  if (!dirname)
    return FALSE;

  key_file = g_key_file_new ();

  g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                         "Encoding", "UTF-8");
  g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                         G_KEY_FILE_DESKTOP_KEY_VERSION, "1.0");
  g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                         G_KEY_FILE_DESKTOP_KEY_TYPE,
                         G_KEY_FILE_DESKTOP_TYPE_APPLICATION);
  if (info->terminal)
    g_key_file_set_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP,
                            G_KEY_FILE_DESKTOP_KEY_TERMINAL, TRUE);
  if (info->nodisplay)
    g_key_file_set_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP,
                            G_KEY_FILE_DESKTOP_KEY_NO_DISPLAY, TRUE);

  g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                         G_KEY_FILE_DESKTOP_KEY_EXEC, info->exec);

  g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                         G_KEY_FILE_DESKTOP_KEY_NAME, info->name);

  if (info->generic_name != NULL)
    g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                           GENERIC_NAME_KEY, info->generic_name);

  if (info->fullname != NULL)
    g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                           FULL_NAME_KEY, info->fullname);

  g_key_file_set_string (key_file, G_KEY_FILE_DESKTOP_GROUP,
                         G_KEY_FILE_DESKTOP_KEY_COMMENT, info->comment);

  g_key_file_set_boolean (key_file, G_KEY_FILE_DESKTOP_GROUP,
                          G_KEY_FILE_DESKTOP_KEY_NO_DISPLAY, TRUE);

  data = g_key_file_to_data (key_file, &data_size, NULL);
  g_key_file_free (key_file);

  desktop_id = g_strdup_printf ("userapp-%s-XXXXXX.desktop", info->name);
  filename = g_build_filename (dirname, desktop_id, NULL);
  g_free (desktop_id);
  g_free (dirname);

  fd = g_mkstemp (filename);
  if (fd == -1)
    {
      char *display_name;

      display_name = g_filename_display_name (filename);
      g_set_error (error, G_IO_ERROR, G_IO_ERROR_FAILED,
                   _("Can’t create user desktop file %s"), display_name);
      g_free (display_name);
      g_free (filename);
      g_free (data);
      return FALSE;
    }

  desktop_id = g_path_get_basename (filename);

  /* FIXME - actually handle error */
  (void) g_close (fd, NULL);

  res = g_file_set_contents_full (filename, data, data_size,
                                  G_FILE_SET_CONTENTS_CONSISTENT | G_FILE_SET_CONTENTS_ONLY_EXISTING,
                                  0600, error);
  g_free (data);
  if (!res)
    {
      g_free (desktop_id);
      g_free (filename);
      return FALSE;
    }

  info->filename = filename;
  info->desktop_id = desktop_id;

  run_update_command ("update-desktop-database", "applications");

  /* We just dropped a file in the user's desktop file directory.  Save
   * the monitor the bother of having to notice it and invalidate
   * immediately.
   *
   * This means that calls directly following this will be able to see
   * the results immediately.
   */
  desktop_file_dirs_invalidate_user_data ();

  return TRUE;
}

static gboolean
g_desktop_app_info_can_delete (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (info->filename)
    {
      if (strstr (info->filename, "/userapp-"))
        return g_access (info->filename, W_OK) == 0;
    }

  return FALSE;
}

static gboolean
g_desktop_app_info_delete (GAppInfo *appinfo)
{
  GDesktopAppInfo *info = G_DESKTOP_APP_INFO (appinfo);

  if (info->filename)
    {
      if (g_remove (info->filename) == 0)
        {
          update_mimeapps_list (info->desktop_id, NULL,
                                UPDATE_MIME_NONE,
                                NULL);

          g_free (info->filename);
          info->filename = NULL;
          g_free (info->desktop_id);
          info->desktop_id = NULL;

          return TRUE;
        }
    }

  return FALSE;
}

/* Create for commandline {{{2 */
/**
 * g_app_info_create_from_commandline:
 * @commandline: (type filename): the commandline to use
 * @application_name: (nullable): the application name, or %NULL to use @commandline
 * @flags: flags that can specify details of the created #GAppInfo
 * @error: a #GError location to store the error occurring, %NULL to ignore.
 *
 * Creates a new #GAppInfo from the given information.
 *
 * Note that for @commandline, the quoting rules of the Exec key of the
 * [freedesktop.org Desktop Entry Specification](http://freedesktop.org/Standards/desktop-entry-spec)
 * are applied. For example, if the @commandline contains
 * percent-encoded URIs, the percent-character must be doubled in order to prevent it from
 * being swallowed by Exec key unquoting. See the specification for exact quoting rules.
 *
 * Returns: (transfer full): new #GAppInfo for given command.
 **/
GAppInfo *
g_app_info_create_from_commandline (const char           *commandline,
                                    const char           *application_name,
                                    GAppInfoCreateFlags   flags,
                                    GError              **error)
{
  char **split;
  char *basename;
  GDesktopAppInfo *info;

  g_return_val_if_fail (commandline, NULL);

  info = g_object_new (G_TYPE_DESKTOP_APP_INFO, NULL);

  info->filename = NULL;
  info->desktop_id = NULL;

  info->terminal = (flags & G_APP_INFO_CREATE_NEEDS_TERMINAL) != 0;
  info->startup_notify = (flags & G_APP_INFO_CREATE_SUPPORTS_STARTUP_NOTIFICATION) != 0;
  info->hidden = FALSE;
  if ((flags & G_APP_INFO_CREATE_SUPPORTS_URIS) != 0)
    info->exec = g_strconcat (commandline, " %u", NULL);
  else
    info->exec = g_strconcat (commandline, " %f", NULL);
  info->nodisplay = TRUE;
  info->binary = binary_from_exec (info->exec);

  if (application_name)
    info->name = g_strdup (application_name);
  else
    {
      /* FIXME: this should be more robust. Maybe g_shell_parse_argv and use argv[0] */
      split = g_strsplit (commandline, " ", 2);
      basename = split[0] ? g_path_get_basename (split[0]) : NULL;
      g_strfreev (split);
      info->name = basename;
      if (info->name == NULL)
        info->name = g_strdup ("custom");
    }
  info->comment = g_strdup_printf (_("Custom definition for %s"), info->name);

  return G_APP_INFO (info);
}

/* GAppInfo interface init */

static void
g_desktop_app_info_iface_init (GAppInfoIface *iface)
{
  iface->dup = g_desktop_app_info_dup;
  iface->equal = g_desktop_app_info_equal;
  iface->get_id = g_desktop_app_info_get_id;
  iface->get_name = g_desktop_app_info_get_name;
  iface->get_description = g_desktop_app_info_get_description;
  iface->get_executable = g_desktop_app_info_get_executable;
  iface->get_icon = g_desktop_app_info_get_icon;
  iface->launch = g_desktop_app_info_launch;
  iface->supports_uris = g_desktop_app_info_supports_uris;
  iface->supports_files = g_desktop_app_info_supports_files;
  iface->launch_uris = g_desktop_app_info_launch_uris;
  iface->launch_uris_async = g_desktop_app_info_launch_uris_async;
  iface->launch_uris_finish = g_desktop_app_info_launch_uris_finish;
  iface->should_show = g_desktop_app_info_should_show;
  iface->set_as_default_for_type = g_desktop_app_info_set_as_default_for_type;
  iface->set_as_default_for_extension = g_desktop_app_info_set_as_default_for_extension;
  iface->add_supports_type = g_desktop_app_info_add_supports_type;
  iface->can_remove_supports_type = g_desktop_app_info_can_remove_supports_type;
  iface->remove_supports_type = g_desktop_app_info_remove_supports_type;
  iface->can_delete = g_desktop_app_info_can_delete;
  iface->do_delete = g_desktop_app_info_delete;
  iface->get_commandline = g_desktop_app_info_get_commandline;
  iface->get_display_name = g_desktop_app_info_get_display_name;
  iface->set_as_last_used_for_type = g_desktop_app_info_set_as_last_used_for_type;
  iface->get_supported_types = g_desktop_app_info_get_supported_types;
}

/* Recommended applications {{{2 */

/* Converts content_type into a list of itself with all of its parent
 * types (if include_fallback is enabled) or just returns a single-item
 * list with the unaliased content type.
 */
static gchar **
get_list_of_mimetypes (const gchar *content_type,
                       gboolean     include_fallback)
{
  gchar *unaliased;
  GPtrArray *array;

  array = g_ptr_array_new ();
  unaliased = _g_unix_content_type_unalias (content_type);
  g_ptr_array_add (array, unaliased);

  if (include_fallback)
    {
      guint i;

      /* Iterate the array as we grow it, until we have nothing more to add */
      for (i = 0; i < array->len; i++)
        {
          gchar **parents = _g_unix_content_type_get_parents (g_ptr_array_index (array, i));
          gint j;

          for (j = 0; parents[j]; j++)
            /* Don't add duplicates */
            if (!array_contains (array, parents[j]))
              g_ptr_array_add (array, parents[j]);
            else
              g_free (parents[j]);

          /* We already stole or freed each element.  Free the container. */
          g_free (parents);
        }
    }

  g_ptr_array_add (array, NULL);

  return (gchar **) g_ptr_array_free (array, FALSE);
}

static gchar **
g_desktop_app_info_get_desktop_ids_for_content_type (const gchar *content_type,
                                                     gboolean     include_fallback)
{
  GPtrArray *hits, *blocklist;
  gchar **types;
  guint i, j;

  hits = g_ptr_array_new ();
  blocklist = g_ptr_array_new ();

  types = get_list_of_mimetypes (content_type, include_fallback);

  desktop_file_dirs_lock ();

  for (i = 0; types[i]; i++)
    for (j = 0; j < desktop_file_dirs->len; j++)
      desktop_file_dir_mime_lookup (g_ptr_array_index (desktop_file_dirs, j), types[i], hits, blocklist);

  /* We will keep the hits past unlocking, so we must dup them */
  for (i = 0; i < hits->len; i++)
    hits->pdata[i] = g_strdup (hits->pdata[i]);

  desktop_file_dirs_unlock ();

  g_ptr_array_add (hits, NULL);

  g_ptr_array_free (blocklist, TRUE);
  g_strfreev (types);

  return (gchar **) g_ptr_array_free (hits, FALSE);
}

/**
 * g_app_info_get_recommended_for_type:
 * @content_type: the content type to find a #GAppInfo for
 *
 * Gets a list of recommended #GAppInfos for a given content type, i.e.
 * those applications which claim to support the given content type exactly,
 * and not by MIME type subclassing.
 * Note that the first application of the list is the last used one, i.e.
 * the last one for which g_app_info_set_as_last_used_for_type() has been
 * called.
 *
 * Returns: (element-type GAppInfo) (transfer full): #GList of #GAppInfos
 *     for given @content_type or %NULL on error.
 *
 * Since: 2.28
 **/
GList *
g_app_info_get_recommended_for_type (const gchar *content_type)
{
  gchar **desktop_ids;
  GList *infos;
  gint i;

  g_return_val_if_fail (content_type != NULL, NULL);

  desktop_ids = g_desktop_app_info_get_desktop_ids_for_content_type (content_type, FALSE);

  infos = NULL;
  for (i = 0; desktop_ids[i]; i++)
    {
      GDesktopAppInfo *info;

      info = g_desktop_app_info_new (desktop_ids[i]);
      if (info)
        infos = g_list_prepend (infos, info);
    }

  g_strfreev (desktop_ids);

  return g_list_reverse (infos);
}

/**
 * g_app_info_get_fallback_for_type:
 * @content_type: the content type to find a #GAppInfo for
 *
 * Gets a list of fallback #GAppInfos for a given content type, i.e.
 * those applications which claim to support the given content type
 * by MIME type subclassing and not directly.
 *
 * Returns: (element-type GAppInfo) (transfer full): #GList of #GAppInfos
 *     for given @content_type or %NULL on error.
 *
 * Since: 2.28
 **/
GList *
g_app_info_get_fallback_for_type (const gchar *content_type)
{
  gchar **recommended_ids;
  gchar **all_ids;
  GList *infos;
  gint i;

  g_return_val_if_fail (content_type != NULL, NULL);

  recommended_ids = g_desktop_app_info_get_desktop_ids_for_content_type (content_type, FALSE);
  all_ids = g_desktop_app_info_get_desktop_ids_for_content_type (content_type, TRUE);

  infos = NULL;
  for (i = 0; all_ids[i]; i++)
    {
      GDesktopAppInfo *info;
      gint j;

      /* Don't return the ones on the recommended list */
      for (j = 0; recommended_ids[j]; j++)
        if (g_str_equal (all_ids[i], recommended_ids[j]))
          break;

      if (recommended_ids[j])
        continue;

      info = g_desktop_app_info_new (all_ids[i]);

      if (info)
        infos = g_list_prepend (infos, info);
    }

  g_strfreev (recommended_ids);
  g_strfreev (all_ids);

  return g_list_reverse (infos);
}

/**
 * g_app_info_get_all_for_type:
 * @content_type: the content type to find a #GAppInfo for
 *
 * Gets a list of all #GAppInfos for a given content type,
 * including the recommended and fallback #GAppInfos. See
 * g_app_info_get_recommended_for_type() and
 * g_app_info_get_fallback_for_type().
 *
 * Returns: (element-type GAppInfo) (transfer full): #GList of #GAppInfos
 *     for given @content_type or %NULL on error.
 **/
GList *
g_app_info_get_all_for_type (const char *content_type)
{
  gchar **desktop_ids;
  GList *infos;
  gint i;

  g_return_val_if_fail (content_type != NULL, NULL);

  desktop_ids = g_desktop_app_info_get_desktop_ids_for_content_type (content_type, TRUE);

  infos = NULL;
  for (i = 0; desktop_ids[i]; i++)
    {
      GDesktopAppInfo *info;

      info = g_desktop_app_info_new (desktop_ids[i]);
      if (info)
        infos = g_list_prepend (infos, info);
    }

  g_strfreev (desktop_ids);

  return g_list_reverse (infos);
}

/**
 * g_app_info_reset_type_associations:
 * @content_type: a content type
 *
 * Removes all changes to the type associations done by
 * g_app_info_set_as_default_for_type(),
 * g_app_info_set_as_default_for_extension(),
 * g_app_info_add_supports_type() or
 * g_app_info_remove_supports_type().
 *
 * Since: 2.20
 */
void
g_app_info_reset_type_associations (const char *content_type)
{
  update_mimeapps_list (NULL, content_type,
                        UPDATE_MIME_NONE,
                        NULL);
}

/**
 * g_app_info_get_default_for_type:
 * @content_type: the content type to find a #GAppInfo for
 * @must_support_uris: if %TRUE, the #GAppInfo is expected to
 *     support URIs
 *
 * Gets the default #GAppInfo for a given content type.
 *
 * Returns: (transfer full) (nullable): #GAppInfo for given @content_type or
 *     %NULL on error.
 */
GAppInfo *
g_app_info_get_default_for_type (const char *content_type,
                                 gboolean    must_support_uris)
{
  GPtrArray *blocklist;
  GPtrArray *results;
  GAppInfo *info;
  gchar **types;
  guint i, j, k;

  g_return_val_if_fail (content_type != NULL, NULL);

  types = get_list_of_mimetypes (content_type, TRUE);

  blocklist = g_ptr_array_new ();
  results = g_ptr_array_new ();
  info = NULL;

  desktop_file_dirs_lock ();

  for (i = 0; types[i]; i++)
    {
      /* Collect all the default apps for this type */
      for (j = 0; j < desktop_file_dirs->len; j++)
        desktop_file_dir_default_lookup (g_ptr_array_index (desktop_file_dirs, j), types[i], results);

      /* Consider the associations as well... */
      for (j = 0; j < desktop_file_dirs->len; j++)
        desktop_file_dir_mime_lookup (g_ptr_array_index (desktop_file_dirs, j), types[i], results, blocklist);

      /* (If any), see if one of those apps is installed... */
      for (j = 0; j < results->len; j++)
        {
          const gchar *desktop_id = g_ptr_array_index (results, j);

          for (k = 0; k < desktop_file_dirs->len; k++)
            {
              info = (GAppInfo *) desktop_file_dir_get_app (g_ptr_array_index (desktop_file_dirs, k), desktop_id);

              if (info)
                {
                  if (!must_support_uris || g_app_info_supports_uris (info))
                    goto out;

                  g_clear_object (&info);
                }
            }
        }

      /* Reset the list, ready to try again with the next (parent)
       * mimetype, but keep the blocklist in place.
       */
      g_ptr_array_set_size (results, 0);
    }

out:
  desktop_file_dirs_unlock ();

  g_ptr_array_unref (blocklist);
  g_ptr_array_unref (results);
  g_strfreev (types);

  return info;
}

/**
 * g_app_info_get_default_for_uri_scheme:
 * @uri_scheme: a string containing a URI scheme.
 *
 * Gets the default application for handling URIs with
 * the given URI scheme. A URI scheme is the initial part
 * of the URI, up to but not including the ':', e.g. "http",
 * "ftp" or "sip".
 *
 * Returns: (transfer full) (nullable): #GAppInfo for given @uri_scheme or
 *     %NULL on error.
 */
GAppInfo *
g_app_info_get_default_for_uri_scheme (const char *uri_scheme)
{
  GAppInfo *app_info;
  char *content_type, *scheme_down;

  scheme_down = g_ascii_strdown (uri_scheme, -1);
  content_type = g_strdup_printf ("x-scheme-handler/%s", scheme_down);
  g_free (scheme_down);
  app_info = g_app_info_get_default_for_type (content_type, FALSE);
  g_free (content_type);

  return app_info;
}

/* "Get all" API {{{2 */

/**
 * g_desktop_app_info_get_implementations:
 * @interface: the name of the interface
 *
 * Gets all applications that implement @interface.
 *
 * An application implements an interface if that interface is listed in
 * the Implements= line of the desktop file of the application.
 *
 * Returns: (element-type GDesktopAppInfo) (transfer full): a list of #GDesktopAppInfo
 * objects.
 *
 * Since: 2.42
 **/
GList *
g_desktop_app_info_get_implementations (const gchar *interface)
{
  GList *result = NULL;
  GList **ptr;
  guint i;

  desktop_file_dirs_lock ();

  for (i = 0; i < desktop_file_dirs->len; i++)
    desktop_file_dir_get_implementations (g_ptr_array_index (desktop_file_dirs, i), &result, interface);

  desktop_file_dirs_unlock ();

  ptr = &result;
  while (*ptr)
    {
      gchar *name = (*ptr)->data;
      GDesktopAppInfo *app;

      app = g_desktop_app_info_new (name);
      g_free (name);

      if (app)
        {
          (*ptr)->data = app;
          ptr = &(*ptr)->next;
        }
      else
        *ptr = g_list_delete_link (*ptr, *ptr);
    }

  return result;
}

/**
 * g_desktop_app_info_search:
 * @search_string: the search string to use
 *
 * Searches desktop files for ones that match @search_string.
 *
 * The return value is an array of strvs.  Each strv contains a list of
 * applications that matched @search_string with an equal score.  The
 * outer list is sorted by score so that the first strv contains the
 * best-matching applications, and so on.
 * The algorithm for determining matches is undefined and may change at
 * any time.
 *
 * None of the search results are subjected to the normal validation
 * checks performed by g_desktop_app_info_new() (for example, checking that
 * the executable referenced by a result exists), and so it is possible for
 * g_desktop_app_info_new() to return %NULL when passed an app ID returned by
 * this function. It is expected that calling code will do this when
 * subsequently creating a #GDesktopAppInfo for each result.
 *
 * Returns: (array zero-terminated=1) (element-type GStrv) (transfer full): a
 *   list of strvs.  Free each item with g_strfreev() and free the outer
 *   list with g_free().
 */
gchar ***
g_desktop_app_info_search (const gchar *search_string)
{
  gchar **search_tokens;
  gint last_category = -1;
  gchar ***results;
  gint n_categories = 0;
  gint start_of_category;
  gint i, j;
  guint k;

  search_tokens = g_str_tokenize_and_fold (search_string, NULL, NULL);

  desktop_file_dirs_lock ();

  reset_total_search_results ();

  for (k = 0; k < desktop_file_dirs->len; k++)
    {
      for (j = 0; search_tokens[j]; j++)
        {
          desktop_file_dir_search (g_ptr_array_index (desktop_file_dirs, k), search_tokens[j]);
          merge_token_results (j == 0);
        }
      merge_directory_results ();
    }

  sort_total_search_results ();

  /* Count the total number of unique categories */
  for (i = 0; i < static_total_results_size; i++)
    if (static_total_results[i].category != last_category)
      {
        last_category = static_total_results[i].category;
        n_categories++;
      }

  results = g_new (gchar **, n_categories + 1);

  /* Start loading into the results list */
  start_of_category = 0;
  for (i = 0; i < n_categories; i++)
    {
      gint n_items_in_category = 0;
      gint this_category;
      gint j;

      this_category = static_total_results[start_of_category].category;

      while (start_of_category + n_items_in_category < static_total_results_size &&
             static_total_results[start_of_category + n_items_in_category].category == this_category)
        n_items_in_category++;

      results[i] = g_new (gchar *, n_items_in_category + 1);
      for (j = 0; j < n_items_in_category; j++)
        results[i][j] = g_strdup (static_total_results[start_of_category + j].app_name);
      results[i][j] = NULL;

      start_of_category += n_items_in_category;
    }
  results[i] = NULL;

  desktop_file_dirs_unlock ();

  g_strfreev (search_tokens);

  return results;
}

/**
 * g_app_info_get_all:
 *
 * Gets a list of all of the applications currently registered
 * on this system.
 *
 * For desktop files, this includes applications that have
 * `NoDisplay=true` set or are excluded from display by means
 * of `OnlyShowIn` or `NotShowIn`. See g_app_info_should_show().
 * The returned list does not include applications which have
 * the `Hidden` key set.
 *
 * Returns: (element-type GAppInfo) (transfer full): a newly allocated #GList of references to #GAppInfos.
 **/
GList *
g_app_info_get_all (void)
{
  GHashTable *apps;
  GHashTableIter iter;
  gpointer value;
  guint i;
  GList *infos;

  apps = g_hash_table_new_full (g_str_hash, g_str_equal, g_free, NULL);

  desktop_file_dirs_lock ();

  for (i = 0; i < desktop_file_dirs->len; i++)
    desktop_file_dir_get_all (g_ptr_array_index (desktop_file_dirs, i), apps);

  desktop_file_dirs_unlock ();

  infos = NULL;
  g_hash_table_iter_init (&iter, apps);
  while (g_hash_table_iter_next (&iter, NULL, &value))
    {
      if (value)
        infos = g_list_prepend (infos, value);
    }

  g_hash_table_destroy (apps);

  return infos;
}

/* GDesktopAppInfoLookup interface {{{2 */

/**
 * GDesktopAppInfoLookup:
 *
 * #GDesktopAppInfoLookup is an opaque data structure and can only be accessed
 * using the following functions.
 *
 * Deprecated: 2.28: The #GDesktopAppInfoLookup interface is deprecated and
 *    unused by GIO.
 **/

G_GNUC_BEGIN_IGNORE_DEPRECATIONS

typedef GDesktopAppInfoLookupIface GDesktopAppInfoLookupInterface;
G_DEFINE_INTERFACE (GDesktopAppInfoLookup, g_desktop_app_info_lookup, G_TYPE_OBJECT)

static void
g_desktop_app_info_lookup_default_init (GDesktopAppInfoLookupInterface *iface)
{
}

/* "Get for mime type" APIs {{{2 */

/**
 * g_desktop_app_info_lookup_get_default_for_uri_scheme:
 * @lookup: a #GDesktopAppInfoLookup
 * @uri_scheme: a string containing a URI scheme.
 *
 * Gets the default application for launching applications
 * using this URI scheme for a particular #GDesktopAppInfoLookup
 * implementation.
 *
 * The #GDesktopAppInfoLookup interface and this function is used
 * to implement g_app_info_get_default_for_uri_scheme() backends
 * in a GIO module. There is no reason for applications to use it
 * directly. Applications should use g_app_info_get_default_for_uri_scheme().
 *
 * Returns: (transfer full) (nullable): #GAppInfo for given @uri_scheme or
 *    %NULL on error.
 *
 * Deprecated: 2.28: The #GDesktopAppInfoLookup interface is deprecated and
 *    unused by GIO.
 */
GAppInfo *
g_desktop_app_info_lookup_get_default_for_uri_scheme (GDesktopAppInfoLookup *lookup,
                                                      const char            *uri_scheme)
{
  GDesktopAppInfoLookupIface *iface;

  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO_LOOKUP (lookup), NULL);

  iface = G_DESKTOP_APP_INFO_LOOKUP_GET_IFACE (lookup);

  return (* iface->get_default_for_uri_scheme) (lookup, uri_scheme);
}

G_GNUC_END_IGNORE_DEPRECATIONS

/* Misc getter APIs {{{2 */

/**
 * g_desktop_app_info_get_startup_wm_class:
 * @info: a #GDesktopAppInfo that supports startup notify
 *
 * Retrieves the StartupWMClass field from @info. This represents the
 * WM_CLASS property of the main window of the application, if launched
 * through @info.
 *
 * Returns: (nullable) (transfer none): the startup WM class, or %NULL if none is set
 * in the desktop file.
 *
 * Since: 2.34
 */
const char *
g_desktop_app_info_get_startup_wm_class (GDesktopAppInfo *info)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), NULL);

  return info->startup_wm_class;
}

/**
 * g_desktop_app_info_get_string:
 * @info: a #GDesktopAppInfo
 * @key: the key to look up
 *
 * Looks up a string value in the keyfile backing @info.
 *
 * The @key is looked up in the "Desktop Entry" group.
 *
 * Returns: (nullable): a newly allocated string, or %NULL if the key
 *     is not found
 *
 * Since: 2.36
 */
char *
g_desktop_app_info_get_string (GDesktopAppInfo *info,
                               const char      *key)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), NULL);

  return g_key_file_get_string (info->keyfile,
                                G_KEY_FILE_DESKTOP_GROUP, key, NULL);
}

/**
 * g_desktop_app_info_get_locale_string:
 * @info: a #GDesktopAppInfo
 * @key: the key to look up
 *
 * Looks up a localized string value in the keyfile backing @info
 * translated to the current locale.
 *
 * The @key is looked up in the "Desktop Entry" group.
 *
 * Returns: (nullable): a newly allocated string, or %NULL if the key
 *     is not found
 *
 * Since: 2.56
 */
char *
g_desktop_app_info_get_locale_string (GDesktopAppInfo *info,
                                      const char      *key)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), NULL);
  g_return_val_if_fail (key != NULL && *key != '\0', NULL);

  return g_key_file_get_locale_string (info->keyfile,
                                       G_KEY_FILE_DESKTOP_GROUP,
                                       key, NULL, NULL);
}

/**
 * g_desktop_app_info_get_boolean:
 * @info: a #GDesktopAppInfo
 * @key: the key to look up
 *
 * Looks up a boolean value in the keyfile backing @info.
 *
 * The @key is looked up in the "Desktop Entry" group.
 *
 * Returns: the boolean value, or %FALSE if the key
 *     is not found
 *
 * Since: 2.36
 */
gboolean
g_desktop_app_info_get_boolean (GDesktopAppInfo *info,
                                const char      *key)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), FALSE);

  return g_key_file_get_boolean (info->keyfile,
                                 G_KEY_FILE_DESKTOP_GROUP, key, NULL);
}

/**
 * g_desktop_app_info_get_string_list:
 * @info: a #GDesktopAppInfo
 * @key: the key to look up
 * @length: (out) (optional): return location for the number of returned strings, or %NULL
 *
 * Looks up a string list value in the keyfile backing @info.
 *
 * The @key is looked up in the "Desktop Entry" group.
 *
 * Returns: (array zero-terminated=1 length=length) (element-type utf8) (transfer full):
 *  a %NULL-terminated string array or %NULL if the specified
 *  key cannot be found. The array should be freed with g_strfreev().
 *
 * Since: 2.60
 */
gchar **
g_desktop_app_info_get_string_list (GDesktopAppInfo *info,
                                    const char      *key,
                                    gsize           *length)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), NULL);

  return g_key_file_get_string_list (info->keyfile,
                                     G_KEY_FILE_DESKTOP_GROUP, key, length, NULL);
}

/**
 * g_desktop_app_info_has_key:
 * @info: a #GDesktopAppInfo
 * @key: the key to look up
 *
 * Returns whether @key exists in the "Desktop Entry" group
 * of the keyfile backing @info.
 *
 * Returns: %TRUE if the @key exists
 *
 * Since: 2.36
 */
gboolean
g_desktop_app_info_has_key (GDesktopAppInfo *info,
                            const char      *key)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), FALSE);

  return g_key_file_has_key (info->keyfile,
                             G_KEY_FILE_DESKTOP_GROUP, key, NULL);
}

/* Desktop actions support {{{2 */

/**
 * g_desktop_app_info_list_actions:
 * @info: a #GDesktopAppInfo
 *
 * Returns the list of "additional application actions" supported on the
 * desktop file, as per the desktop file specification.
 *
 * As per the specification, this is the list of actions that are
 * explicitly listed in the "Actions" key of the [Desktop Entry] group.
 *
 * Returns: (array zero-terminated=1) (element-type utf8) (transfer none): a list of strings, always non-%NULL
 *
 * Since: 2.38
 **/
const gchar * const *
g_desktop_app_info_list_actions (GDesktopAppInfo *info)
{
  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), NULL);

  return (const gchar **) info->actions;
}

static gboolean
app_info_has_action (GDesktopAppInfo *info,
                     const gchar     *action_name)
{
  gint i;

  for (i = 0; info->actions[i]; i++)
    if (g_str_equal (info->actions[i], action_name))
      return TRUE;

  return FALSE;
}

/**
 * g_desktop_app_info_get_action_name:
 * @info: a #GDesktopAppInfo
 * @action_name: the name of the action as from
 *   g_desktop_app_info_list_actions()
 *
 * Gets the user-visible display name of the "additional application
 * action" specified by @action_name.
 *
 * This corresponds to the "Name" key within the keyfile group for the
 * action.
 *
 * Returns: (transfer full): the locale-specific action name
 *
 * Since: 2.38
 */
gchar *
g_desktop_app_info_get_action_name (GDesktopAppInfo *info,
                                    const gchar     *action_name)
{
  gchar *group_name;
  gchar *result;

  g_return_val_if_fail (G_IS_DESKTOP_APP_INFO (info), NULL);
  g_return_val_if_fail (action_name != NULL, NULL);
  g_return_val_if_fail (app_info_has_action (info, action_name), NULL);

  group_name = g_strdup_printf ("Desktop Action %s", action_name);
  result = g_key_file_get_locale_string (info->keyfile, group_name, "Name", NULL, NULL);
  g_free (group_name);

  /* The spec says that the Name field must be given.
   *
   * If it's not, let's follow the behaviour of our get_name()
   * implementation above and never return %NULL.
   */
  if (result == NULL)
    result = g_strdup (_("Unnamed"));

  return result;
}

/**
 * g_desktop_app_info_launch_action:
 * @info: a #GDesktopAppInfo
 * @action_name: the name of the action as from
 *   g_desktop_app_info_list_actions()
 * @launch_context: (nullable): a #GAppLaunchContext
 *
 * Activates the named application action.
 *
 * You may only call this function on action names that were
 * returned from g_desktop_app_info_list_actions().
 *
 * Note that if the main entry of the desktop file indicates that the
 * application supports startup notification, and @launch_context is
 * non-%NULL, then startup notification will be used when activating the
 * action (and as such, invocation of the action on the receiving side
 * must signal the end of startup notification when it is completed).
 * This is the expected behaviour of applications declaring additional
 * actions, as per the desktop file specification.
 *
 * As with g_app_info_launch() there is no way to detect failures that
 * occur while using this function.
 *
 * Since: 2.38
 */
void
g_desktop_app_info_launch_action (GDesktopAppInfo   *info,
                                  const gchar       *action_name,
                                  GAppLaunchContext *launch_context)
{
  GDBusConnection *session_bus;

  g_return_if_fail (G_IS_DESKTOP_APP_INFO (info));
  g_return_if_fail (action_name != NULL);
  g_return_if_fail (app_info_has_action (info, action_name));

  session_bus = g_bus_get_sync (G_BUS_TYPE_SESSION, NULL, NULL);

  if (session_bus && info->app_id)
    {
      gchar *object_path;

      object_path = object_path_from_appid (info->app_id);
      g_dbus_connection_call (session_bus, info->app_id, object_path,
                              "org.freedesktop.Application", "ActivateAction",
                              g_variant_new ("(sav@a{sv})", action_name, NULL,
                                             g_desktop_app_info_make_platform_data (info, NULL, launch_context)),
                              NULL, G_DBUS_CALL_FLAGS_NONE, -1, NULL, NULL, NULL);
      g_free (object_path);
    }
  else
    {
      gchar *group_name;
      gchar *exec_line;

      group_name = g_strdup_printf ("Desktop Action %s", action_name);
      exec_line = g_key_file_get_string (info->keyfile, group_name, "Exec", NULL);
      g_free (group_name);

      if (exec_line)
        g_desktop_app_info_launch_uris_with_spawn (info, session_bus, exec_line, NULL, launch_context,
                                                   _SPAWN_FLAGS_DEFAULT, NULL, NULL, NULL, NULL,
                                                   -1, -1, -1, NULL);

      g_free (exec_line);
    }

  if (session_bus != NULL)
    {
      g_dbus_connection_flush (session_bus, NULL, NULL, NULL);
      g_object_unref (session_bus);
    }
}
/* Epilogue {{{1 */

/* vim:set foldmethod=marker: */
