/* -*- mode: C; c-file-style: "linux" -*- */
/* GdkPixbuf library
 * queryloaders.c:
 *
 * Copyright (C) 2002 The Free Software Foundation
 *
 * Author: Matthias Clasen <maclas@gmx.de>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include <glib.h>
#include <glib/gprintf.h>
#include <gmodule.h>

#include <errno.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "gdk-pixbuf/gdk-pixbuf.h"
#include "gdk-pixbuf/gdk-pixbuf-private.h"

#ifdef USE_LA_MODULES
#define SOEXT ".la"
#else
#define SOEXT ("." G_MODULE_SUFFIX)
#endif
#define SOEXT_LEN (strlen (SOEXT))

#ifdef G_OS_WIN32
#include <windows.h>
#endif

static void
print_escaped (GString *contents, const char *str)
{
        gchar *tmp = g_strescape (str, "");
        g_string_append_printf (contents, "\"%s\" ", tmp);
        g_free (tmp);
}

static int
loader_sanity_check (const char *path, GdkPixbufFormat *info, GdkPixbufModule *vtable)
{
        const GdkPixbufModulePattern *pattern;
        const char *error = "";

        for (pattern = info->signature; pattern->prefix; pattern++)
        {
                int prefix_len = strlen (pattern->prefix);
                if (prefix_len == 0)
                {
                        error = "empty pattern";

                        goto error;
                }
                if (pattern->mask)
                {
                        int mask_len = strlen (pattern->mask);
                        if (mask_len != prefix_len)
                        {
                                error = "mask length mismatch";

                                goto error;
                        }
                        if (strspn (pattern->mask, " !xzn*") < mask_len)
                        {
                                error = "bad char in mask";

                                goto error;
                        }
                }
        }

        if (!vtable->load && !vtable->begin_load && !vtable->load_animation)
        {
                error = "no load method implemented";

                goto error;
        }

        if (vtable->begin_load && (!vtable->stop_load || !vtable->load_increment))
        {
                error = "incremental loading support incomplete";

                goto error;
        }

        if ((info->flags & GDK_PIXBUF_FORMAT_WRITABLE) && !(vtable->save || vtable->save_to_callback))
        {
                error = "loader claims to support saving but doesn't implement save";
                goto error;
        }

        return 1;

 error:
        g_fprintf (stderr, "Loader sanity check failed for %s: %s\n",
                   path, error);

        return 0;
}

static void
write_loader_info (GString *contents, const char *path, GdkPixbufFormat *info)
{
        const GdkPixbufModulePattern *pattern;
        char **mime;
        char **ext;

        g_string_append_printf (contents, "\"%s\"\n", path);
        g_string_append_printf (contents, "\"%s\" %u \"%s\" \"%s\" \"%s\"\n",
                  info->name,
                  info->flags,
                  info->domain ? info->domain : GETTEXT_PACKAGE,
                  info->description,
                  info->license ? info->license : "");
        for (mime = info->mime_types; *mime; mime++) {
                g_string_append_printf (contents, "\"%s\" ", *mime);
        }
        g_string_append (contents, "\"\"\n");
        for (ext = info->extensions; *ext; ext++) {
                g_string_append_printf (contents, "\"%s\" ", *ext);
        }
        g_string_append (contents, "\"\"\n");
        for (pattern = info->signature; pattern->prefix; pattern++) {
                print_escaped (contents, pattern->prefix);
                print_escaped (contents, pattern->mask ? (const char *)pattern->mask : "");
                g_string_append_printf (contents, "%d\n", pattern->relevance);
        }
        g_string_append_c (contents, '\n');
}

static void
query_module (GString *contents, const char *dir, const char *file)
{
        char *path;
        GModule *module;
        void                    (*fill_info)     (GdkPixbufFormat *info);
        void                    (*fill_vtable)   (GdkPixbufModule *module);
        gpointer fill_info_ptr;
        gpointer fill_vtable_ptr;

        if (g_path_is_absolute (file))
                path = g_strdup (file);
        else
                path = g_build_filename (dir, file, NULL);

        module = g_module_open (path, 0);
        if (module &&
            g_module_symbol (module, "fill_info", &fill_info_ptr) &&
            g_module_symbol (module, "fill_vtable", &fill_vtable_ptr)) {
                GdkPixbufFormat *info;
                GdkPixbufModule *vtable;

#ifdef G_OS_WIN32
                /* Replace backslashes in path with forward slashes, so that
                 * it reads in without problems.
                 */
                {
                        char *p = path;
                        while (*p) {
                                if (*p == '\\')
                                        *p = '/';
                                p++;
                        }
                }
#endif
                info = g_new0 (GdkPixbufFormat, 1);
                vtable = g_new0 (GdkPixbufModule, 1);

                vtable->module = module;

                fill_info = fill_info_ptr;
                fill_vtable = fill_vtable_ptr;

                (*fill_info) (info);
                (*fill_vtable) (vtable);

                if (loader_sanity_check (path, info, vtable))
                        write_loader_info (contents, path, info);

                g_free (info);
                g_free (vtable);
        }
        else {
                if (module == NULL)
                        g_fprintf (stderr, "g_module_open() failed for %s: %s\n", path,
                                   g_module_error());
                else
                        g_fprintf (stderr, "Cannot load loader %s\n", path);
        }
        if (module)
                g_module_close (module);
        g_free (path);
}

#ifdef G_OS_WIN32

static char *
get_toplevel (void)
{
  static char *toplevel = NULL;

  if (toplevel == NULL)
          toplevel = g_win32_get_package_installation_directory_of_module (NULL);

  return toplevel;
}

static char *
get_libdir (void)
{
  static char *libdir = NULL;

  if (libdir == NULL)
          libdir = g_build_filename (get_toplevel (), "lib", NULL);

  return libdir;
}

#undef GDK_PIXBUF_LIBDIR
#define GDK_PIXBUF_LIBDIR get_libdir()

#endif

static gchar *
gdk_pixbuf_get_module_file (void)
{
        gchar *result = g_strdup (g_getenv ("GDK_PIXBUF_MODULE_FILE"));

        if (!result)
                result = g_build_filename (GDK_PIXBUF_LIBDIR, "gdk-pixbuf-2.0", GDK_PIXBUF_BINARY_VERSION, "loaders.cache", NULL);

        return result;
}

int main (int argc, char **argv)
{
        gint i;
        const gchar *prgname;
        GString *contents;
        gchar *cache_file = NULL;
        gint first_file = 1;

#ifdef G_OS_WIN32
        gchar *libdir;
        gchar *runtime_prefix;
        gchar *slash;

        if (g_ascii_strncasecmp (PIXBUF_LIBDIR, GDK_PIXBUF_PREFIX, strlen (GDK_PIXBUF_PREFIX)) == 0 &&
            G_IS_DIR_SEPARATOR (PIXBUF_LIBDIR[strlen (GDK_PIXBUF_PREFIX)])) {
                /* GDK_PIXBUF_PREFIX is a prefix of PIXBUF_LIBDIR, as it
                 * normally is. Replace that prefix in PIXBUF_LIBDIR
                 * with the installation directory on this machine.
                 * We assume this invokation of
                 * gdk-pixbuf-query-loaders is run from either a "bin"
                 * subdirectory of the installation directory, or in
                 * the installation directory itself.
                 */
                wchar_t fn[1000];
                GetModuleFileNameW (NULL, fn, G_N_ELEMENTS (fn));
                runtime_prefix = g_utf16_to_utf8 (fn, -1, NULL, NULL, NULL);
                slash = strrchr (runtime_prefix, '\\');
                *slash = '\0';
                slash = strrchr (runtime_prefix, '\\');
                /* If running from some weird location, or from the
                 * build directory (either in the .libs folder where
                 * libtool places the real executable when using a
                 * wrapper, or directly from the gdk-pixbuf folder),
                 * use the compile-time libdir.
                 */
                if (slash == NULL ||
                    g_ascii_strcasecmp (slash + 1, ".libs") == 0 ||
                    g_ascii_strcasecmp (slash + 1, "gdk-pixbuf") == 0) {
                        libdir = PIXBUF_LIBDIR;
                }
                else {
                        if (slash != NULL && g_ascii_strcasecmp (slash + 1, "bin") == 0) {
                                *slash = '\0';
                        }

                        libdir = g_strconcat (runtime_prefix,
                                              "/",
                                              PIXBUF_LIBDIR + strlen (GDK_PIXBUF_PREFIX) + 1,
                                              NULL);
                }
        }
        else {
                libdir = PIXBUF_LIBDIR;
        }

#undef PIXBUF_LIBDIR
#define PIXBUF_LIBDIR libdir

#endif

	/* This call is necessary to ensure we actually link against libgobject;
	 * otherwise it may be stripped if -Wl,--as-needed is in use.
	 * 
	 * The reason we need to link against libgobject is because it now has
	 * a global constructor.  If the dynamically loaded modules happen
	 * to dlclose() libgobject, then reopen it again, we're in for trouble.
	 *
	 * See: https://bugzilla.gnome.org/show_bug.cgi?id=686822
	 */
	g_type_ensure (G_TYPE_OBJECT);

        if (argc > 1 && strcmp (argv[1], "--update-cache") == 0) {
                cache_file = gdk_pixbuf_get_module_file ();
                first_file = 2;
        }

        contents = g_string_new ("");

        prgname = g_get_prgname ();
        g_string_append_printf (contents,
                                "# GdkPixbuf Image Loader Modules file\n"
                                "# Automatically generated file, do not edit\n"
                                "# Created by %s from gdk-pixbuf-%s\n"
                                "#\n",
                                (prgname ? prgname : "gdk-pixbuf-query-loaders"),
                                GDK_PIXBUF_VERSION);

        if (argc == first_file) {
#ifdef USE_GMODULE
                const char *path;
                GDir *dir;

                path = g_getenv ("GDK_PIXBUF_MODULEDIR");
#ifdef G_OS_WIN32
                if (path != NULL && *path != '\0')
                        path = g_locale_to_utf8 (path, -1, NULL, NULL, NULL);
#endif
                if (path == NULL || *path == '\0')
                        path = PIXBUF_LIBDIR;

                g_string_append_printf (contents, "# LoaderDir = %s\n#\n", path);

                dir = g_dir_open (path, 0, NULL);
                if (dir) {
                        const char *dent;

                        while ((dent = g_dir_read_name (dir))) {
                                gint len = strlen (dent);
                                if (len > SOEXT_LEN &&
                                    strcmp (dent + len - SOEXT_LEN, SOEXT) == 0) {
                                        query_module (contents, path, dent);
                                }
                        }
                        g_dir_close (dir);
                }
#else
                g_string_append_printf (contents, "# dynamic loading of modules not supported\n");
#endif
        }
        else {
                char *cwd = g_get_current_dir ();

                for (i = first_file; i < argc; i++) {
                        char *infilename = argv[i];
#ifdef G_OS_WIN32
                        infilename = g_locale_to_utf8 (infilename,
                                                       -1, NULL, NULL, NULL);
#endif
                        query_module (contents, cwd, infilename);
                }
                g_free (cwd);
        }

        if (cache_file) {
                GError *err;

                err = NULL;
                if (!g_file_set_contents (cache_file, contents->str, -1, &err)) {
                        g_fprintf (stderr, "%s\n", err->message);
                }
        }
        else
                g_print ("%s\n", contents->str);

        return 0;
}
