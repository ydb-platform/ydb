/* Gdk-Pixbuf-Pixdata - GdkPixbuf to GdkPixdata
 * Copyright (C) 1999, 2001 Tim Janik
 * Copyright (C) 2012 Red Hat, Inc
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
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */
#include <contrib/restricted/glib/config.h>

#include "gdk-pixbuf.h"
#include "gdk-pixdata.h"
#include <glib/gprintf.h>
#include <stdlib.h>
#include <string.h>


/* --- defines --- */
#undef	G_LOG_DOMAIN
#define	G_LOG_DOMAIN	"Gdk-Pixbuf-Pixdata"
#define PRG_NAME        "gdk-pixbuf-pixdata-3.0"
#define PKG_NAME        "gdk-pixbuf"
#define PKG_HTTP_HOME   "http://www.gtk.org"

static gboolean use_rle = FALSE;

/* --- prototypes --- */
static void	parse_args	(gint    *argc_p,
				 gchar ***argv_p);
static void	print_blurb	(FILE    *bout,
				 gboolean print_help);


int
main (int   argc,
      char *argv[])
{
  GdkPixbuf *pixbuf;
  GError *error = NULL;
  gchar *infilename;
  gchar *outfilename;
  gpointer free_me;
  GdkPixdata pixdata;
  guint8 *data;
  guint data_len;

  /* parse args and do fast exits */
  parse_args (&argc, &argv);

  if (argc != 3)
    {
      print_blurb (stderr, TRUE);
      return 1;
    }

#ifdef G_OS_WIN32
  infilename = g_locale_to_utf8 (argv[1], -1, NULL, NULL, NULL);
#else
  infilename = argv[1];
#endif

#ifdef G_OS_WIN32
  outfilename = g_locale_to_utf8 (argv[2], -1, NULL, NULL, NULL);
#else
  outfilename = argv[2];
#endif

  pixbuf = gdk_pixbuf_new_from_file (infilename, &error);
  if (!pixbuf)
    {
      g_printerr ("failed to load \"%s\": %s\n",
		  argv[1],
		  error->message);
      g_error_free (error);
      return 1;
    }

G_GNUC_BEGIN_IGNORE_DEPRECATIONS
  free_me = gdk_pixdata_from_pixbuf (&pixdata, pixbuf, use_rle);
  data = gdk_pixdata_serialize (&pixdata, &data_len);
G_GNUC_END_IGNORE_DEPRECATIONS

  if (!g_file_set_contents (outfilename, (char *)data, data_len, &error))
    {
      g_printerr ("failed to load \"%s\": %s\n",
		  argv[1],
		  error->message);
      g_error_free (error);
      return 1;
    }

  g_free (data);
  g_free (free_me);
  g_object_unref (pixbuf);

  return 0;
}

static void
parse_args (gint    *argc_p,
	    gchar ***argv_p)
{
  guint argc = *argc_p;
  gchar **argv = *argv_p;
  guint i, e;

  for (i = 1; i < argc; i++)
    {
      if (strcmp ("--rle", argv[i]) == 0)
	{
	  use_rle = TRUE;
	  argv[i] = NULL;
	}
      else if (strcmp ("-h", argv[i]) == 0 ||
	  strcmp ("--help", argv[i]) == 0)
	{
	  print_blurb (stderr, TRUE);
	  argv[i] = NULL;
	  exit (0);
	}
      else if (strcmp ("-v", argv[i]) == 0 ||
	       strcmp ("--version", argv[i]) == 0)
	{
	  print_blurb (stderr, FALSE);
	  argv[i] = NULL;
	  exit (0);
	}
      else if (strcmp (argv[i], "--g-fatal-warnings") == 0)
	{
	  GLogLevelFlags fatal_mask;

	  fatal_mask = g_log_set_always_fatal (G_LOG_FATAL_MASK);
	  fatal_mask |= G_LOG_LEVEL_WARNING | G_LOG_LEVEL_CRITICAL;
	  g_log_set_always_fatal (fatal_mask);

	  argv[i] = NULL;
	}
    }

  e = 0;
  for (i = 1; i < argc; i++)
    {
      if (e)
	{
	  if (argv[i])
	    {
	      argv[e++] = argv[i];
	      argv[i] = NULL;
	    }
	}
      else if (!argv[i])
	e = i;
    }
  if (e)
    *argc_p = e;
}

static void
print_blurb (FILE    *bout,
	     gboolean print_help)
{
  if (!print_help)
    {
      g_fprintf (bout, "%s version ", PRG_NAME);
      g_fprintf (bout, "%s", GDK_PIXBUF_VERSION);
      g_fprintf (bout, "\n");
      g_fprintf (bout, "%s comes with ABSOLUTELY NO WARRANTY.\n", PRG_NAME);
      g_fprintf (bout, "You may redistribute copies of %s under the terms of\n", PRG_NAME);
      g_fprintf (bout, "the GNU Lesser General Public License which can be found in the\n");
      g_fprintf (bout, "%s source package. Sources, examples and contact\n", PKG_NAME);
      g_fprintf (bout, "information are available at %s\n", PKG_HTTP_HOME);
    }
  else
    {
      g_fprintf (bout, "Usage: %s [options] [input-file] [output-file]\n", PRG_NAME);
      g_fprintf (bout, "  -h, --help                 show this help message\n");
      g_fprintf (bout, "  -v, --version              print version informations\n");
      g_fprintf (bout, "  --g-fatal-warnings         make warnings fatal (abort)\n");
    }
}
