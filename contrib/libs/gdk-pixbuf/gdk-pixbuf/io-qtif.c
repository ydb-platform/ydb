/* -*- mode: C; c-file-style: "linux" -*- */
/* GdkPixbuf library - QTIF image loader
 *
 * This module extracts image data from QTIF format and uses
 * other GDK pixbuf modules to decode the image data.
 *
 * Copyright (C) 2008 Kevin Peng
 *
 * Authors: Kevin Peng <kevin@zycomtech.com>
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
#include <errno.h>
#include <libintl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <setjmp.h>
#include "gdk-pixbuf.h"
#include "gdk-pixbuf-private.h"

/***
 * Definitions
 */
/* Read buffer size */
#define READ_BUFFER_SIZE    8192

/* Only allow atom of size up to 10MB. */
#define ATOM_SIZE_MAX       100000000

/* Aborts after going to through this many atoms. */
#define QTIF_ATOM_COUNT_MAX 10u

/* QTIF static image data tag "idat". */
#define QTIF_TAG_IDATA      0x69646174u


/***
 * Types
 */
/* QTIF State */
typedef enum {
    STATE_READY,
    STATE_DATA,
    STATE_OTHER
} QTIFState;

/* QTIF Atom Header */
typedef struct {
    guint32 length;
    guint32 tag;
} QtHeader;

/* QTIF loader context */
typedef struct {
    GdkPixbufLoader *loader;
    gpointer        user_data;
    QTIFState       state;
    guint32         run_length;
    gint            atom_count;

    guchar          header_buffer[sizeof(QtHeader)];

    GdkPixbufModuleSizeFunc     size_func;
    GdkPixbufModulePreparedFunc prepare_func;
    GdkPixbufModuleUpdatedFunc  update_func;
    gint            cb_prepare_count;
    gint            cb_update_count;
} QTIFContext;

/***
 * Local function prototypes
 */
static GdkPixbuf *gdk_pixbuf__qtif_image_load (FILE *f, GError **error);
static gpointer gdk_pixbuf__qtif_image_begin_load (GdkPixbufModuleSizeFunc size_func,
                                                   GdkPixbufModulePreparedFunc prepare_func,
                                                   GdkPixbufModuleUpdatedFunc update_func,
                                                   gpointer user_data,
                                                   GError **error);
static gboolean gdk_pixbuf__qtif_image_stop_load (gpointer context, GError **error);
static gboolean gdk_pixbuf__qtif_image_load_increment(gpointer context,
                                                      const guchar *buf, guint size,
                                                      GError **error);
static gboolean gdk_pixbuf__qtif_image_create_loader (QTIFContext *context, GError **error);
static gboolean gdk_pixbuf__qtif_image_free_loader (QTIFContext *context, GError **error);

static void gdk_pixbuf__qtif_cb_size_prepared(GdkPixbufLoader *loader,
                                              gint width,
                                              gint height,
                                              gpointer user_data);
static void gdk_pixbuf__qtif_cb_area_prepared(GdkPixbufLoader *loader, gpointer user_data);
static void gdk_pixbuf__qtif_cb_area_updated(GdkPixbufLoader *loader,
                                             gint x,
                                             gint y,
                                             gint width,
                                             gint height,
                                             gpointer user_data);

/***
 * Function definitions.
 */

/* Load QTIF from a file handler. */
static GdkPixbuf *gdk_pixbuf__qtif_image_load (FILE *f, GError **error)
{
    guint count;

    if(f == NULL)
    {
        g_set_error_literal (error, GDK_PIXBUF_ERROR,
                             GDK_PIXBUF_ERROR_BAD_OPTION,
                             _("Input file descriptor is NULL."));
        return NULL;
    }

    for(count = QTIF_ATOM_COUNT_MAX; count != 0u; count--)
    {
        QtHeader hdr;
        size_t rd;

        /* Read QtHeader. */
        rd = fread(&hdr, 1, sizeof(QtHeader), f);
        if(rd != sizeof(QtHeader))
        {
            g_set_error_literal(error, GDK_PIXBUF_ERROR,
                                GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                _("Failed to read QTIF header"));
            return NULL;
        }

        hdr.length = GUINT32_FROM_BE(hdr.length) - sizeof(QtHeader);
        if(hdr.length > ATOM_SIZE_MAX)
        {
            g_set_error(error, GDK_PIXBUF_ERROR,
                        GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                        ngettext (  "QTIF atom size too large (%d byte)",
                                    "QTIF atom size too large (%d bytes)",
                                    hdr.length),
                        hdr.length);
            return NULL;
        }

        switch(GUINT32_FROM_BE(hdr.tag))
        {
        case QTIF_TAG_IDATA: /* "idat" data atom. */
            {
                /* Load image using GdkPixbufLoader. */
                guchar *buf;
                GdkPixbufLoader *loader;
                GdkPixbuf *pixbuf = NULL;
                GError *tmp = NULL;

                /* Allocate read buffer. */
                buf = g_try_malloc(READ_BUFFER_SIZE);
                if(buf == NULL)
                {
                    g_set_error(error, GDK_PIXBUF_ERROR,
                                GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                ngettext ( "Failed to allocate %d byte for file read buffer",
                                           "Failed to allocate %d bytes for file read buffer",
                                           READ_BUFFER_SIZE
                                ),
                                READ_BUFFER_SIZE);
                    return NULL;
                }

                /* Create GdkPixbufLoader. */
                loader = gdk_pixbuf_loader_new();
                if(loader == NULL)
                {
                    g_set_error(error, GDK_PIXBUF_ERROR,
                                GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                ngettext (  "QTIF atom size too large (%d byte)",
                                            "QTIF atom size too large (%d bytes)",
                                            hdr.length),
                                hdr.length);
                    goto clean_up;
                }

                /* Read atom data. */
                while(hdr.length != 0u)
                {
                    if(fread(buf, 1, rd, f) != rd)
                    {
                        g_set_error(error, GDK_PIXBUF_ERROR,
                                    GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                    _("File error when reading QTIF atom: %s"), g_strerror(errno));
                        break;
                    }

                    if(!gdk_pixbuf_loader_write(loader, buf, rd, &tmp))
                    {
                        g_propagate_error (error, tmp);
                        break;
                    }
                    hdr.length -= rd;
                }

clean_up:
                /* Release loader */
                if(loader != NULL)
                {
                    gdk_pixbuf_loader_close(loader, NULL);
                    pixbuf = gdk_pixbuf_loader_get_pixbuf(loader);
                    if(pixbuf != NULL)
                    {
                        g_object_ref(pixbuf);
                    }
                    g_object_unref(loader);
                }
                if(buf != NULL)
                {
                    g_free(buf);
                }
                return pixbuf;
            }

        default:
            /* Skip any other types of atom. */
            if(!fseek(f, hdr.length, SEEK_CUR))
            {
                g_set_error(error, GDK_PIXBUF_ERROR,
                            GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                            ngettext (  "Failed to skip the next %d byte with seek().",
                                        "Failed to skip the next %d bytes with seek().",
                                        hdr.length),
                            hdr.length);
                return NULL;
            }
            break;
        }
    }
    return NULL;
}

/* Incremental load begin. */
static gpointer gdk_pixbuf__qtif_image_begin_load (GdkPixbufModuleSizeFunc size_func,
                                                   GdkPixbufModulePreparedFunc prepare_func,
                                                   GdkPixbufModuleUpdatedFunc update_func,
                                                   gpointer user_data,
                                                   GError **error)
{
    QTIFContext *context;

    /* Create context struct. */
    context = g_new0(QTIFContext, 1);
    if(context == NULL)
    {
        g_set_error_literal (error, GDK_PIXBUF_ERROR,
                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                             _("Failed to allocate QTIF context structure."));
        return NULL;
    }

    /* Fill context parameters. */
    context->loader = NULL;
    context->user_data = user_data;
    context->state = STATE_READY;
    context->run_length = 0u;
    context->atom_count = QTIF_ATOM_COUNT_MAX;
    context->size_func = size_func;
    context->prepare_func = prepare_func;
    context->update_func = update_func;

    return context;
}

/* Incremental load clean up. */
static gboolean gdk_pixbuf__qtif_image_stop_load (gpointer data, GError **error)
{
    QTIFContext *context = (QTIFContext *)data;
    gboolean ret = TRUE;

    if(context->loader != NULL)
    {
        GError *tmp = NULL;

        ret = gdk_pixbuf__qtif_image_free_loader(context, &tmp);
        if(!ret)
        {
            g_propagate_error (error, tmp);
        }
    }
    g_free(context);

    return ret;
}

/* Create a new GdkPixbufLoader and connect to its signals. */
static gboolean gdk_pixbuf__qtif_image_create_loader (QTIFContext *context, GError **error)
{
    GError *tmp = NULL;

    if(context == NULL)
    {
        return FALSE;
    }

    /* Free existing loader. */
    if(context->loader != NULL)
    {
        gdk_pixbuf__qtif_image_free_loader(context, &tmp);
    }

    /* Create GdkPixbufLoader object. */
    context->loader = gdk_pixbuf_loader_new();
    if(context->loader == NULL)
    {
        g_set_error_literal (error, GDK_PIXBUF_ERROR,
                             GDK_PIXBUF_ERROR_FAILED,
                             _("Failed to create GdkPixbufLoader object."));
        return FALSE;
    }

    /* Connect signals. */
    context->cb_prepare_count = 0;
    context->cb_update_count = 0;
    if(context->size_func != NULL)
    {
        g_signal_connect(context->loader, "size-prepared",
                         G_CALLBACK(gdk_pixbuf__qtif_cb_size_prepared),
                         context);
    }
    if(context->prepare_func != NULL)
    {
        g_signal_connect(context->loader, "area-prepared",
                         G_CALLBACK(gdk_pixbuf__qtif_cb_area_prepared),
                         context);
    }
    if(context->update_func != NULL)
    {
        g_signal_connect(context->loader, "area-updated",
                         G_CALLBACK(gdk_pixbuf__qtif_cb_area_updated),
                         context);
    }
    return TRUE;
}

/* Free the GdkPixbufLoader and perform callback if haven't done so. */
static gboolean gdk_pixbuf__qtif_image_free_loader (QTIFContext *context, GError **error)
{
    GdkPixbuf *pixbuf;
    GError *tmp = NULL;
    gboolean ret;

    if((context == NULL) || (context->loader == NULL))
    {
        return FALSE;
    }

    /* Close GdkPixbufLoader. */
    ret = gdk_pixbuf_loader_close(context->loader, &tmp);
    if(!ret)
    {
        g_propagate_error (error, tmp);
    }


    /* Get GdkPixbuf from GdkPixbufLoader. */
    pixbuf = gdk_pixbuf_loader_get_pixbuf(context->loader);
    if(pixbuf != NULL)
    {
        g_object_ref(pixbuf);
    }

    /* Free GdkPixbufLoader. */
    g_object_ref(context->loader);
    context->loader = NULL;

    if(pixbuf != NULL)
    {
        /* Callback functions should be called for at least once. */
        if((context->prepare_func != NULL) && (context->cb_prepare_count == 0))
        {
            (context->prepare_func)(pixbuf, NULL, context->user_data);
        }

        if((context->update_func != NULL) && (context->cb_update_count == 0))
        {
            gint width;
            gint height;

            width = gdk_pixbuf_get_width(pixbuf);
            height = gdk_pixbuf_get_height(pixbuf);
            (context->update_func)(pixbuf, 0, 0, width, height, context->user_data);
        }

        /* Free GdkPixbuf (callback function should ref it). */
        g_object_ref(pixbuf);
    }

    return ret;
}


/* Incrementally load the next chunk of data. */
static gboolean gdk_pixbuf__qtif_image_load_increment (gpointer data,
                                                       const guchar *buf, guint size,
                                                       GError **error)
{
    QTIFContext *context = (QTIFContext *)data;
    GError *tmp = NULL;
    gboolean ret = TRUE; /* Return TRUE for insufficient data. */

    while(ret && (size != 0u))
    {
        switch(context->state)
        {
        case STATE_READY:
            /* Abort if we have seen too mant atoms. */
            if(context->atom_count == 0u)
            {
                g_set_error_literal (error, GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Failed to find an image data atom."));
                return FALSE;
            }
            context->atom_count--;

            /* Copy to header buffer in context, in case supplied data is not enough. */
            while(context->run_length < sizeof(QtHeader))
            {
                context->header_buffer[context->run_length] = *buf;
                context->run_length++;
                buf++;
                size--;
            }

            /* Parse buffer as QT header. */
            if(context->run_length == sizeof(QtHeader))
            {
                QtHeader *hdr = (QtHeader *)context->header_buffer;
                context->run_length = GUINT32_FROM_BE(hdr->length) - sizeof(QtHeader);

                /* Atom max size check. */
                if(context->run_length > ATOM_SIZE_MAX)
                {
                    g_set_error(error, GDK_PIXBUF_ERROR,
                                       GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                       ngettext (  "QTIF atom size too large (%d byte)",
                                                   "QTIF atom size too large (%d bytes)",
                                                    hdr->length),
                                       hdr->length);
                    return FALSE;
                }

                /* Set state according to atom type. */
                if(GUINT32_FROM_BE(hdr->tag) == QTIF_TAG_IDATA)
                {
                    GError *tmp = NULL;

                    context->state = STATE_DATA;

                    /* Create GdkPixbufLoader for this image data. */
                    ret = gdk_pixbuf__qtif_image_create_loader(context, &tmp);
                    if(!ret)
                    {
                        g_propagate_error (error, tmp);
                    }
                }
                else
                {
                    context->state = STATE_OTHER;
                }
            }
            break;

        default: /* Both STATE_DATA and STATE_OTHER will come here. */
            /* Check for atom boundary. */
            if(context->run_length > size)
            {
                /* Supply image data to GdkPixbufLoader if in STATE_DATA. */
                if(context->state == STATE_DATA)
                {
                    tmp = NULL;
                    ret = gdk_pixbuf_loader_write(context->loader, buf, size, &tmp);
                    if(!ret && (error != NULL) && (*error == NULL))
                    {
                        g_propagate_error (error, tmp);
                    }
                }
                context->run_length -= size;
                size = 0u;
            }
            else
            {
                /* Supply image data to GdkPixbufLoader if in STATE_DATA. */
                if(context->state == STATE_DATA)
                {
                    gboolean r;

                    /* Here we should have concluded a complete image atom. */
                    tmp = NULL;
                    ret = gdk_pixbuf_loader_write(context->loader, buf, context->run_length, &tmp);
                    if(!ret && (error != NULL) && (*error == NULL))
                    {
                        g_propagate_error (error, tmp);
                    }

                    /* Free GdkPixbufLoader and handle callback. */
                    tmp = NULL;
                    r = gdk_pixbuf__qtif_image_free_loader(context, &tmp);
                    if(!r)
                    {
                        if((error != NULL) && (*error == NULL))
                        {
                            g_propagate_error (error, tmp);
                        }
                        ret = FALSE;
                    }
                }
                buf = &buf[context->run_length];
                size -= context->run_length;
                context->run_length = 0u;
                context->state = STATE_READY;
            }
            break;
        }
    }

    return ret;
}

/* Event handlers */
static void gdk_pixbuf__qtif_cb_size_prepared(GdkPixbufLoader *loader,
                                              gint width,
                                              gint height,
                                              gpointer user_data)
{
    QTIFContext *context = (QTIFContext *)user_data;
    if((context != NULL) && (context->size_func != NULL))
    {
        (context->size_func)(&width, &height, context->user_data);
        context->cb_prepare_count++;
    }
}

static void gdk_pixbuf__qtif_cb_area_prepared(GdkPixbufLoader *loader, gpointer user_data)
{
    QTIFContext *context = (QTIFContext *)user_data;
    if((loader != NULL) && (context != NULL) && (context->prepare_func != NULL))
    {
        GdkPixbuf *pixbuf = gdk_pixbuf_loader_get_pixbuf(context->loader);
        (context->prepare_func)(pixbuf, NULL, context->user_data);
        context->cb_update_count++;
    }
}

static void gdk_pixbuf__qtif_cb_area_updated(GdkPixbufLoader *loader,
                                             gint x,
                                             gint y,
                                             gint width,
                                             gint height,
                                             gpointer user_data)
{
    QTIFContext *context = (QTIFContext *)user_data;
    if((loader != NULL) && (context != NULL) && (context->update_func != NULL))
    {
        GdkPixbuf *pixbuf = gdk_pixbuf_loader_get_pixbuf(context->loader);
        (context->update_func)(pixbuf, x, y, width, height, context->user_data);
    }
}


#ifndef INCLUDE_qtif
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__qtif_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
    module->load = gdk_pixbuf__qtif_image_load;
    module->begin_load = gdk_pixbuf__qtif_image_begin_load;
    module->stop_load = gdk_pixbuf__qtif_image_stop_load;
    module->load_increment = gdk_pixbuf__qtif_image_load_increment;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
    static const GdkPixbufModulePattern signature[] = {
        { "abcdidsc", "xxxx    ", 100 },
        { "abcdidat", "xxxx    ", 100 },
        { NULL, NULL, 0 }
    };
    static const gchar *mime_types[] = {
        "image/x-quicktime",
        "image/qtif",
        NULL
    };
    static const gchar *extensions[] = {
        "qtif",
        "qif",
        NULL
    };

    info->name = "qtif";
    info->signature = (GdkPixbufModulePattern *) signature;
    info->description = NC_("image format", "QuickTime");
    info->mime_types = (gchar **) mime_types;
    info->extensions = (gchar **) extensions;
    info->flags = GDK_PIXBUF_FORMAT_THREADSAFE;
    info->license = "LGPL";
}

