/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - GDIP loader declarations
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

#ifndef GDK_PIXBUF_GDIP_H
#define GDK_PIXBUF_GDIP_H

#include <gdk-pixbuf/gdk-pixbuf-animation.h>

typedef struct _GdkPixbufGdipAnim GdkPixbufGdipAnim;
typedef struct _GdkPixbufGdipAnimClass GdkPixbufGdipAnimClass;
typedef struct _GdkPixbufFrame GdkPixbufFrame;

#define GDK_TYPE_PIXBUF_GDIP_ANIM              (gdk_pixbuf_gdip_anim_get_type ())
#define GDK_PIXBUF_GDIP_ANIM(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), GDK_TYPE_PIXBUF_GDIP_ANIM, GdkPixbufGdipAnim))
#define GDK_IS_PIXBUF_GDIP_ANIM(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), GDK_TYPE_PIXBUF_GDIP_ANIM))

#define GDK_PIXBUF_GDIP_ANIM_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), GDK_TYPE_PIXBUF_GDIP_ANIM, GdkPixbufGdipAnimClass))
#define GDK_IS_PIXBUF_GDIP_ANIM_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), GDK_TYPE_PIXBUF_GDIP_ANIM))
#define GDK_PIXBUF_GDIP_ANIM_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), GDK_TYPE_PIXBUF_GDIP_ANIM, GdkPixbufGdipAnimClass))

/* Private part of the GdkPixbufGdipAnim structure */
struct _GdkPixbufGdipAnim {
        GdkPixbufAnimation parent_instance;

        /* Number of frames */
        int n_frames;

        /* Total length of animation */
        int total_time;
        
	/* List of GdkPixbufFrame structures */
        GList *frames;

	/* bounding box size */
	int width, height;
        
        int loop;
        gboolean loading;
};

struct _GdkPixbufGdipAnimClass {
        GdkPixbufAnimationClass parent_class;
        
};

GType gdk_pixbuf_gdip_anim_get_type (void) G_GNUC_CONST;

typedef struct _GdkPixbufGdipAnimIter GdkPixbufGdipAnimIter;
typedef struct _GdkPixbufGdipAnimIterClass GdkPixbufGdipAnimIterClass;


#define GDK_TYPE_PIXBUF_GDIP_ANIM_ITER              (gdk_pixbuf_gdip_anim_iter_get_type ())
#define GDK_PIXBUF_GDIP_ANIM_ITER(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), GDK_TYPE_PIXBUF_GDIP_ANIM_ITER, GdkPixbufGdipAnimIter))
#define GDK_IS_PIXBUF_GDIP_ANIM_ITER(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), GDK_TYPE_PIXBUF_GDIP_ANIM_ITER))

#define GDK_PIXBUF_GDIP_ANIM_ITER_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), GDK_TYPE_PIXBUF_GDIP_ANIM_ITER, GdkPixbufGdipAnimIterClass))
#define GDK_IS_PIXBUF_GDIP_ANIM_ITER_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), GDK_TYPE_PIXBUF_GDIP_ANIM_ITER))
#define GDK_PIXBUF_GDIP_ANIM_ITER_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), GDK_TYPE_PIXBUF_GDIP_ANIM_ITER, GdkPixbufGdipAnimIterClass))

struct _GdkPixbufGdipAnimIter {
        GdkPixbufAnimationIter parent_instance;
        
        GdkPixbufGdipAnim   *gdip_anim;

        GTimeVal            start_time;
        GTimeVal            current_time;

        /* Time in milliseconds into this run of the animation */
        gint                position;
        
        GList              *current_frame;
        
        gint                first_loop_slowness;
};

struct _GdkPixbufGdipAnimIterClass {
        GdkPixbufAnimationIterClass parent_class;

};

GType gdk_pixbuf_gdip_anim_iter_get_type (void) G_GNUC_CONST;

struct _GdkPixbufFrame {
	/* The pixbuf with this frame's image data */
	GdkPixbuf *pixbuf;

	/* Frame duration in ms */
	int delay_time;

        /* Sum of preceding delay times */
        int elapsed;        
};

#endif
