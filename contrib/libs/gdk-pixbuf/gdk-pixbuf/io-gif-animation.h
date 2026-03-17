/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - GIF loader declarations
 *
 * Copyright (C) 1999 The Free Software Foundation
 *
 * Authors: Mark Crichton <crichton@gimp.org>
 *          Miguel de Icaza <miguel@gnu.org>
 *          Federico Mena-Quintero <federico@gimp.org>
 *          Havoc Pennington <hp@redhat.com>
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

#ifndef GDK_PIXBUF_GIF_H
#define GDK_PIXBUF_GIF_H

#include "gdk-pixbuf-animation.h"

typedef enum {
        /* Keep this frame and composite next frame over it */
        /* (GIF disposal method 1) */
	GDK_PIXBUF_FRAME_RETAIN,
        /* Revert to background color before compositing next frame */
        /* (GIF disposal method 2) */
	GDK_PIXBUF_FRAME_DISPOSE,
        /* Revert to previously-displayed composite image after
         * displaying this frame
         */
        /* (GIF disposal method 3) */
	GDK_PIXBUF_FRAME_REVERT
} GdkPixbufFrameAction;



typedef struct _GdkPixbufGifAnim GdkPixbufGifAnim;
typedef struct _GdkPixbufGifAnimClass GdkPixbufGifAnimClass;
typedef struct _GdkPixbufFrame GdkPixbufFrame;

#define GDK_TYPE_PIXBUF_GIF_ANIM              (gdk_pixbuf_gif_anim_get_type ())
#define GDK_PIXBUF_GIF_ANIM(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), GDK_TYPE_PIXBUF_GIF_ANIM, GdkPixbufGifAnim))
#define GDK_IS_PIXBUF_GIF_ANIM(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), GDK_TYPE_PIXBUF_GIF_ANIM))

#define GDK_PIXBUF_GIF_ANIM_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), GDK_TYPE_PIXBUF_GIF_ANIM, GdkPixbufGifAnimClass))
#define GDK_IS_PIXBUF_GIF_ANIM_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), GDK_TYPE_PIXBUF_GIF_ANIM))
#define GDK_PIXBUF_GIF_ANIM_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), GDK_TYPE_PIXBUF_GIF_ANIM, GdkPixbufGifAnimClass))

/* Private part of the GdkPixbufGifAnim structure */
struct _GdkPixbufGifAnim {
        GdkPixbufAnimation parent_instance;

        /* Number of frames */
        int n_frames;

        /* Total length of animation */
        int total_time;
        
	/* List of GdkPixbufFrame structures */
        GList *frames;

	/* bounding box size */
	int width, height;

        guchar bg_red;
        guchar bg_green;
        guchar bg_blue;
        
        int loop;
        gboolean loading;
};

struct _GdkPixbufGifAnimClass {
        GdkPixbufAnimationClass parent_class;
        
};

GType gdk_pixbuf_gif_anim_get_type (void) G_GNUC_CONST;



typedef struct _GdkPixbufGifAnimIter GdkPixbufGifAnimIter;
typedef struct _GdkPixbufGifAnimIterClass GdkPixbufGifAnimIterClass;


#define GDK_TYPE_PIXBUF_GIF_ANIM_ITER              (gdk_pixbuf_gif_anim_iter_get_type ())
#define GDK_PIXBUF_GIF_ANIM_ITER(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), GDK_TYPE_PIXBUF_GIF_ANIM_ITER, GdkPixbufGifAnimIter))
#define GDK_IS_PIXBUF_GIF_ANIM_ITER(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), GDK_TYPE_PIXBUF_GIF_ANIM_ITER))

#define GDK_PIXBUF_GIF_ANIM_ITER_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), GDK_TYPE_PIXBUF_GIF_ANIM_ITER, GdkPixbufGifAnimIterClass))
#define GDK_IS_PIXBUF_GIF_ANIM_ITER_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), GDK_TYPE_PIXBUF_GIF_ANIM_ITER))
#define GDK_PIXBUF_GIF_ANIM_ITER_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), GDK_TYPE_PIXBUF_GIF_ANIM_ITER, GdkPixbufGifAnimIterClass))

struct _GdkPixbufGifAnimIter {
        GdkPixbufAnimationIter parent_instance;
        
        GdkPixbufGifAnim   *gif_anim;

        GTimeVal            start_time;
        GTimeVal            current_time;

        /* Time in milliseconds into this run of the animation */
        gint                position;
        
        GList              *current_frame;
        
        gint                first_loop_slowness;
};

struct _GdkPixbufGifAnimIterClass {
        GdkPixbufAnimationIterClass parent_class;

};

GType gdk_pixbuf_gif_anim_iter_get_type (void) G_GNUC_CONST;



struct _GdkPixbufFrame {
	/* The pixbuf with this frame's image data */
	GdkPixbuf *pixbuf;

        /* Offsets for overlaying onto the GIF graphic area */
        int x_offset;
	int y_offset;

	/* Frame duration in ms */
	int delay_time;

        /* Sum of preceding delay times */
        int elapsed;
        
        /* Overlay mode */
	GdkPixbufFrameAction action;

        /* TRUE if the pixbuf has been modified since
         * the last frame composite operation
         */
        gboolean need_recomposite;

        /* TRUE if the background for this frame is transparent */
        gboolean bg_transparent;
        
        /* The below reflects the "use hell of a lot of RAM"
         * philosophy of coding
         */
        
        /* Cached composite image (the image you actually display
         * for this frame)
         */
        GdkPixbuf *composited;

        /* Cached revert image (the contents of the area
         * covered by the frame prior to compositing;
         * same size as pixbuf, not as the composite image; only
         * used for FRAME_REVERT frames)
         */
        GdkPixbuf *revert;
};

void gdk_pixbuf_gif_anim_frame_composite (GdkPixbufGifAnim *gif_anim,
                                          GdkPixbufFrame   *frame);

#endif
