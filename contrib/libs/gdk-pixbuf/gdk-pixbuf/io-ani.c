/* -*- mode: C; c-file-style: "linux" -*- */
/* GdkPixbuf library - ANI image loader
 *
 * Copyright (C) 2002 The Free Software Foundation
 *
 * Authors: Matthias Clasen <maclas@gmx.de>
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

#undef DEBUG_ANI

#include <contrib/restricted/glib/config.h>
#include <stdlib.h>
#include <string.h>
#include "gdk-pixbuf-private.h"
#include "gdk-pixbuf-loader.h"
#include "io-ani-animation.h"

static int
lsb_32 (guchar *src)
{
	return src[0] | (src[1] << 8) | (src[2] << 16) | (src[3] << 24);
}

#define MAKE_TAG(a,b,c,d) ( (guint32)d << 24 | \
			    (guint32)c << 16 | \
			    (guint32)b <<  8 | \
			    (guint32)a )

#define TAG_RIFF MAKE_TAG('R','I','F','F')
#define TAG_ACON MAKE_TAG('A','C','O','N')
#define TAG_LIST MAKE_TAG('L','I','S','T')
#define TAG_INAM MAKE_TAG('I','N','A','M')
#define TAG_IART MAKE_TAG('I','A','R','T')
#define TAG_anih MAKE_TAG('a','n','i','h')
#define TAG_seq  MAKE_TAG('s','e','q',' ')
#define TAG_rate MAKE_TAG('r','a','t','e')
#define TAG_icon MAKE_TAG('i','c','o','n')

typedef struct _AniLoaderContext 
{
        guint32 cp;
        
        guchar *buffer;
        guchar *byte;
        guint   n_bytes;
        guint   buffer_size;
        
        GdkPixbufModulePreparedFunc prepared_func;
        GdkPixbufModuleUpdatedFunc updated_func;
        gpointer user_data;
        
        guint32 data_size;
        
        guint32 HeaderSize;
        guint32 NumFrames; 
        guint32 NumSteps; 
        guint32 Width;   
        guint32 Height; 
        guint32 BitCount; 
        guint32 NumPlanes; 
        guint32 DisplayRate; 
        guint32 Flags;
        
        guint32 chunk_id;
        guint32 chunk_size;
        
        gchar  *title;
        gchar  *author;

        GdkPixbufAniAnim *animation;
	GdkPixbufLoader *loader;

        int     pos;
} AniLoaderContext;


#define BYTES_LEFT(context) \
        ((context)->n_bytes - ((context)->byte - (context)->buffer))

static void
read_int8 (AniLoaderContext *context,
           guchar     *data,
           int        count)
{
        int total = MIN (count, BYTES_LEFT (context));
        memcpy (data, context->byte, total);
        context->byte += total;
        context->cp += total;
}


static guint32
read_int32 (AniLoaderContext *context)
{
        guint32 result;

        read_int8 (context, (guchar*) &result, 4);
        return lsb_32 ((guchar *) &result);
}

static void
context_free (AniLoaderContext *context)
{
        if (!context)
                return;

	if (context->loader) 
	{
		gdk_pixbuf_loader_close (context->loader, NULL);
		g_object_unref (context->loader);
	}
        if (context->animation) 
		g_object_unref (context->animation);
        g_free (context->buffer);
        g_free (context->title);
        g_free (context->author);
        
        g_free (context);
}

static void
prepared_callback (GdkPixbufLoader *loader,
                   gpointer data)
{
	AniLoaderContext *context = (AniLoaderContext*)data;

#ifdef DEBUG_ANI
	g_print ("%d pixbuf prepared\n", context->pos);
#endif

	GdkPixbuf *pixbuf = gdk_pixbuf_loader_get_pixbuf (loader);
        if (!pixbuf)
		return;

	if (gdk_pixbuf_get_width (pixbuf) > context->animation->width) 
		context->animation->width = gdk_pixbuf_get_width (pixbuf);
	
	if (gdk_pixbuf_get_height (pixbuf) > context->animation->height) 
		context->animation->height = gdk_pixbuf_get_height (pixbuf);
	
	if (context->title != NULL) 
		gdk_pixbuf_set_option (pixbuf, "Title", context->title);
	
	if (context->author != NULL) 
		gdk_pixbuf_set_option (pixbuf, "Author", context->author);

	g_object_ref (pixbuf);
	context->animation->pixbufs[context->pos] = pixbuf;

	if (context->pos == 0) 
	{
		if (context->prepared_func)
			(* context->prepared_func) (pixbuf, 
						    GDK_PIXBUF_ANIMATION (context->animation), 
						    context->user_data);
	}
	else {
		/* FIXME - this is necessary for nice display of loading 
		   animations because GtkImage ignores 
		   gdk_pixbuf_animation_iter_on_currently_loading_frame()
		   and always exposes the full frame */
		GdkPixbuf *last = context->animation->pixbufs[context->pos - 1];
		gint width = MIN (gdk_pixbuf_get_width (last), gdk_pixbuf_get_width (pixbuf));
		gint height = MIN (gdk_pixbuf_get_height (last), gdk_pixbuf_get_height (pixbuf));
		gdk_pixbuf_copy_area (last, 0, 0, width, height, pixbuf, 0, 0);
	}

	context->pos++;
}

static void
updated_callback (GdkPixbufLoader* loader,
		  gint x, gint y, gint width, gint height,
		  gpointer data)
{
	AniLoaderContext *context = (AniLoaderContext*)data;

	GdkPixbuf *pixbuf = gdk_pixbuf_loader_get_pixbuf (loader);
	
	if (context->updated_func)
		(* context->updated_func) (pixbuf, 
					   x, y, width, height,
					   context->user_data);
}

static gboolean
ani_load_chunk (AniLoaderContext *context, GError **error)
{
        int i;
        
        if (context->chunk_id == 0x0) {
                if (BYTES_LEFT (context) < 8)
                        return FALSE;
                context->chunk_id = read_int32 (context);
                context->chunk_size = read_int32 (context);
		/* Pad it up to word length */
		if (context->chunk_size % 2)
			context->chunk_size += (2 - (context->chunk_size % 2));
        
        }
        
        while (context->chunk_id == TAG_LIST)
	{
		if (BYTES_LEFT (context) < 12)
			return FALSE;
                        
		read_int32 (context);
		context->chunk_id = read_int32 (context);
		context->chunk_size = read_int32 (context);
		/* Pad it up to word length */
		if (context->chunk_size % 2)
			context->chunk_size += (2 - (context->chunk_size % 2));
        
	}
        
	if (context->chunk_id == TAG_icon) 
	{
		GError *loader_error = NULL;
		guchar *data;
		guint32 towrite;

		if (context->loader == NULL) 
		{
			if (context->pos >= context->NumFrames) 
			{
				g_set_error_literal (error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                     _("Unexpected icon chunk in animation"));
				return FALSE; 
			}

#ifdef DEBUG_ANI
			g_print ("opening loader\n");
#endif
			context->loader = gdk_pixbuf_loader_new_with_type ("ico", &loader_error);
			if (loader_error) 
			{
				g_propagate_error (error, loader_error);
				return FALSE;
			}
			g_signal_connect (context->loader, "area_prepared",
					  G_CALLBACK (prepared_callback), context);
			g_signal_connect (context->loader, "area_updated",
					  G_CALLBACK (updated_callback), context);
		}

		towrite = MIN (context->chunk_size, BYTES_LEFT (context));
		data = context->byte;
		context->byte += towrite;
		context->cp += towrite;
#ifdef DEBUG_ANI
		g_print ("miss %d, get %d, leftover %d\n", context->chunk_size, towrite, BYTES_LEFT (context));
#endif
		context->chunk_size -= towrite;
		if (!gdk_pixbuf_loader_write (context->loader, data, towrite, &loader_error)) 
		{
			g_propagate_error (error, loader_error);
			gdk_pixbuf_loader_close (context->loader, NULL);
			g_object_unref (context->loader);
			context->loader = NULL;
			return FALSE; 
		}
		if (context->chunk_size == 0) 
		{
#ifdef DEBUG_ANI
			g_print ("closing loader\n");
#endif
			if (!gdk_pixbuf_loader_close (context->loader, &loader_error)) 
			{
				g_propagate_error (error, loader_error);
				g_object_unref (context->loader);
				context->loader = NULL;
				return FALSE;
			}
			g_object_unref (context->loader);
			context->loader = NULL;
			context->chunk_id = 0x0;
		}
		return BYTES_LEFT (context) > 0;
	}

        if (BYTES_LEFT (context) < context->chunk_size)
                return FALSE;
        
        if (context->chunk_id == TAG_anih) 
	{
		context->HeaderSize = read_int32 (context);
		context->NumFrames = read_int32 (context);
		context->NumSteps = read_int32 (context);
		context->Width = read_int32 (context);
		context->Height = read_int32 (context);
		context->BitCount = read_int32 (context);
		context->NumPlanes = read_int32 (context);
		context->DisplayRate = read_int32 (context);
		context->Flags = read_int32 (context);
                        
#ifdef DEBUG_ANI	  
		g_print ("HeaderSize \t%" G_GUINT32_FORMAT
			 "\nNumFrames \t%" G_GUINT32_FORMAT
			 "\nNumSteps \t%" G_GUINT32_FORMAT
			 "\nWidth    \t%" G_GUINT32_FORMAT
			 "\nHeight   \t%" G_GUINT32_FORMAT
			 "\nBitCount \t%" G_GUINT32_FORMAT
			 "\nNumPlanes \t%" G_GUINT32_FORMAT
			 "\nDisplayRate \t%" G_GUINT32_FORMAT
			 "\nSequenceFlag \t%d"
			 "\nIconFlag \t%d"
			 "\n",
			 context->HeaderSize, context->NumFrames, 
			 context->NumSteps, context->Width, 
			 context->Height, context->BitCount, 
			 context->NumPlanes, context->DisplayRate, 
			 (context->Flags & 0x2) != 0, 
			 (context->Flags & 0x1) != 0);
#endif
		if (!(context->Flags & 0x2))
			context->NumSteps = context->NumFrames;
		if (context->NumFrames == 0 || 
		    context->NumFrames >= 1024 || 
		    context->NumSteps == 0 || 
		    context->NumSteps >= 1024) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Invalid header in animation"));
			return FALSE;
		}
      
		context->animation = g_object_new (GDK_TYPE_PIXBUF_ANI_ANIM, NULL);        
		if (!context->animation) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load animation"));
			return FALSE;
		}

		context->animation->n_pixbufs = context->NumFrames;
		context->animation->n_frames = context->NumSteps;

		context->animation->total_time = context->NumSteps * (context->DisplayRate * 1000 / 60);
		context->animation->width = 0;
		context->animation->height = 0;

		context->animation->pixbufs = g_try_new0 (GdkPixbuf*, context->NumFrames);
		context->animation->delay = g_try_new (gint, context->NumSteps);
		context->animation->sequence = g_try_new (gint, context->NumSteps);

		if (!context->animation->pixbufs || 
		    !context->animation->delay || 
		    !context->animation->sequence) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load animation"));
			return FALSE;
		}

		for (i = 0; i < context->NumSteps; i++) 
		{
			/* default values if the corresponding chunks are absent */
			context->animation->delay[i] = context->DisplayRate * 1000 / 60;
			context->animation->sequence[i] = MIN (i, context->NumFrames  - 1);	  
		}
	}
        else if (context->chunk_id == TAG_rate) 
	{
		if (context->chunk_size != 4 * context->NumSteps) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Malformed chunk in animation"));
			return FALSE; 
		}
		if (!context->animation) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Invalid header in animation"));
			return FALSE;
		}

		context->animation->total_time = 0;
		for (i = 0; i < context->NumSteps; i++) 
		{
			context->animation->delay[i] = read_int32 (context) * 1000 / 60;
			context->animation->total_time += context->animation->delay[i];
		}
	}
        else if (context->chunk_id == TAG_seq) 
	{
		if (context->chunk_size != 4 * context->NumSteps) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Malformed chunk in animation"));
			return FALSE; 
		}
		if (!context->animation) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Invalid header in animation"));
			return FALSE;
		}
		for (i = 0; i < context->NumSteps; i++) 
		{
			context->animation->sequence[i] = read_int32 (context);
			if (context->animation->sequence[i] >= context->NumFrames) 
			{
				g_set_error_literal (error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                     _("Malformed chunk in animation"));
				return FALSE; 
			}
		}
	}
        else if (context->chunk_id == TAG_INAM) 
	{
		if (!context->animation) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Invalid header in animation"));
			return FALSE;
		}
		context->title = g_try_malloc (context->chunk_size + 1);
		if (!context->title) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load animation"));
			return FALSE;
		}
		context->title[context->chunk_size] = 0;
		read_int8 (context, (guchar *)context->title, context->chunk_size);
#ifdef DEBUG_ANI
		g_print ("INAM %s\n", context->title);
#endif
		for (i = 0; i < context->pos; i++)
			gdk_pixbuf_set_option (context->animation->pixbufs[i], "Title", context->title);			
	}
        else if (context->chunk_id == TAG_IART) 
	{
		if (!context->animation) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Invalid header in animation"));
			return FALSE;
		}
		context->author = g_try_malloc (context->chunk_size + 1);
		if (!context->author) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load animation"));
			return FALSE;
		}
		context->author[context->chunk_size] = 0;
		read_int8 (context, (guchar *)context->author, context->chunk_size);
#ifdef DEBUG_ANI
		g_print ("IART %s\n", context->author);
#endif
		for (i = 0; i < context->pos; i++)
			gdk_pixbuf_set_option (context->animation->pixbufs[i], "Author", context->author);			
	}

#ifdef DEBUG_ANI
	{
		gint32 dummy = lsb_32 ((guchar *)&context->chunk_id);
        
		g_print ("Loaded chunk with ID '%c%c%c%c' and length %" G_GUINT32_FORMAT "\n",
			 ((char*)&dummy)[0], ((char*)&dummy)[1],
			 ((char*)&dummy)[2], ((char*)&dummy)[3], 
			 context->chunk_size);
	}
#endif 
		
	context->chunk_id = 0x0;
        return TRUE;
}

static gboolean
gdk_pixbuf__ani_image_load_increment (gpointer data,
				      const guchar *buf, guint size,
				      GError **error)
{
        AniLoaderContext *context = (AniLoaderContext *)data;
        
        if (context->n_bytes + size >= context->buffer_size) {
                int drop = context->byte - context->buffer;
                memmove (context->buffer, context->byte, context->n_bytes - drop);
                context->n_bytes -= drop;
                context->byte = context->buffer;
                if (context->n_bytes + size >= context->buffer_size) {
			guchar *tmp;
                        context->buffer_size = MAX (context->n_bytes + size, context->buffer_size + 4096);
#ifdef DEBUG_ANI
                        g_print ("growing buffer to %" G_GUINT32_FORMAT "\n", context->buffer_size);
#endif
                        tmp = g_try_realloc (context->buffer, context->buffer_size);
                        if (!tmp) 
			{
				g_set_error_literal (error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                                     _("Not enough memory to load animation"));
				return FALSE;
			}
                        context->byte = context->buffer = tmp;
                }
        }
        memcpy (context->buffer + context->n_bytes, buf, size);
        context->n_bytes += size;

        if (context->data_size == 0) 
	{
		guint32 riff_id, chunk_id;
                        
		if (BYTES_LEFT (context) < 12)
			return TRUE;
                        
		riff_id = read_int32 (context);
		context->data_size = read_int32 (context);
		chunk_id = read_int32 (context);
                        
		if (riff_id != TAG_RIFF || 
		    context->data_size == 0 || 
		    chunk_id != TAG_ACON) 
		{
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Invalid header in animation"));
			return FALSE; 
		}
	}
        
        if (context->cp < context->data_size + 8) 
	{
		GError *chunk_error = NULL;

		while (ani_load_chunk (context, &chunk_error)) ;
		if (chunk_error) 
		{
			g_propagate_error (error, chunk_error);
			return FALSE;
		}
	}

        return TRUE;
}

static gpointer
gdk_pixbuf__ani_image_begin_load (GdkPixbufModuleSizeFunc size_func, 
                                  GdkPixbufModulePreparedFunc prepared_func, 
				  GdkPixbufModuleUpdatedFunc  updated_func,
				  gpointer user_data,
				  GError **error)
{
        AniLoaderContext *context;
        
        context = g_new0 (AniLoaderContext, 1);
        
        context->prepared_func = prepared_func;
        context->updated_func = updated_func;
        context->user_data = user_data;
        
        context->pos = 0;
        
        context->buffer_size = 4096;
        context->buffer = g_try_malloc (context->buffer_size);
        if (!context->buffer) 
	{
		context_free (context);
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                     _("Not enough memory to load animation"));
		return NULL;
	}
        
        context->byte = context->buffer;
        context->n_bytes = 0;
        
        return (gpointer) context;
}

static gboolean
gdk_pixbuf__ani_image_stop_load (gpointer data,
				 GError **error)
{
        AniLoaderContext *context = (AniLoaderContext *) data;
        gboolean retval;

	g_return_val_if_fail (context != NULL, TRUE);
        if (!context->animation) {
                g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("ANI image was truncated or incomplete."));
                retval = FALSE;
        }
        else {
                retval = TRUE;
        }
        context_free (context);

        return retval;
}

#ifndef INCLUDE_ani
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__ani_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
        module->begin_load = gdk_pixbuf__ani_image_begin_load;
        module->stop_load = gdk_pixbuf__ani_image_stop_load;
        module->load_increment = gdk_pixbuf__ani_image_load_increment;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
	static const GdkPixbufModulePattern signature[] = {
		{ "RIFF    ACON", "    xxxx    ", 100 },
		{ NULL, NULL, 0 }
	};
	static const gchar * mime_types[] = {
		"application/x-navi-animation",
		NULL
	};
	static const gchar * extensions[] = {
		"ani",
		NULL
	};
	
	info->name = "ani";
	info->signature = (GdkPixbufModulePattern *) signature;
	info->description = NC_("image format", "Windows animated cursor");
	info->mime_types = (gchar **) mime_types;
	info->extensions = (gchar **) extensions;
	info->flags = GDK_PIXBUF_FORMAT_THREADSAFE;
	info->license = "LGPL";
}




