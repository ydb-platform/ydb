/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - ANI loader declarations
 *
 * Copyright (C) 2002 The Free Software Foundation
 *
 * Author: Matthias Clasen <maclas@gmx.de>
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

#ifndef GDK_PIXBUF_ANI_ANIMATION_H
#define GDK_PIXBUF_ANI_ANIMATION_H

#include "gdk-pixbuf-private.h"
#include "gdk-pixbuf-animation.h"

typedef struct _GdkPixbufAniAnim GdkPixbufAniAnim;
typedef struct _GdkPixbufAniAnimClass GdkPixbufAniAnimClass;

#define GDK_TYPE_PIXBUF_ANI_ANIM              (gdk_pixbuf_ani_anim_get_type ())
#define GDK_PIXBUF_ANI_ANIM(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), GDK_TYPE_PIXBUF_ANI_ANIM, GdkPixbufAniAnim))
#define GDK_IS_PIXBUF_ANI_ANIM(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), GDK_TYPE_PIXBUF_ANI_ANIM))

#define GDK_PIXBUF_ANI_ANIM_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), GDK_TYPE_PIXBUF_ANI_ANIM, GdkPixbufAniAnimClass))
#define GDK_IS_PIXBUF_ANI_ANIM_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), GDK_TYPE_PIXBUF_ANI_ANIM))
#define GDK_PIXBUF_ANI_ANIM_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), GDK_TYPE_PIXBUF_ANI_ANIM, GdkPixbufAniAnimClass))

/* Private part of the GdkPixbufAniAnim structure */
struct _GdkPixbufAniAnim {
        GdkPixbufAnimation parent_instance;

        /* Total length of animation */
        int total_time;
        
        /* Number of frames */
        int n_frames;
        
        /* Number of pixbufs */
        int n_pixbufs;
        
        GdkPixbuf **pixbufs;
        
        /* Maps frame number to pixbuf */
        int *sequence;
        
        /* The duration of each frame, in milliseconds */
	int *delay;
        
        /* bounding box size */
	int width, height;
};

struct _GdkPixbufAniAnimClass {
        GdkPixbufAnimationClass parent_class;
        
};

GType gdk_pixbuf_ani_anim_get_type (void) G_GNUC_CONST;



typedef struct _GdkPixbufAniAnimIter GdkPixbufAniAnimIter;
typedef struct _GdkPixbufAniAnimIterClass GdkPixbufAniAnimIterClass;


#define GDK_TYPE_PIXBUF_ANI_ANIM_ITER              (gdk_pixbuf_ani_anim_iter_get_type ())
#define GDK_PIXBUF_ANI_ANIM_ITER(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), GDK_TYPE_PIXBUF_ANI_ANIM_ITER, GdkPixbufAniAnimIter))
#define GDK_IS_PIXBUF_ANI_ANIM_ITER(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), GDK_TYPE_PIXBUF_ANI_ANIM_ITER))

#define GDK_PIXBUF_ANI_ANIM_ITER_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), GDK_TYPE_PIXBUF_ANI_ANIM_ITER, GdkPixbufAniAnimIterClass))
#define GDK_IS_PIXBUF_ANI_ANIM_ITER_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), GDK_TYPE_PIXBUF_ANI_ANIM_ITER))
#define GDK_PIXBUF_ANI_ANIM_ITER_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), GDK_TYPE_PIXBUF_ANI_ANIM_ITER, GdkPixbufAniAnimIterClass))

struct _GdkPixbufAniAnimIter {
        GdkPixbufAnimationIter parent_instance;
        
        GdkPixbufAniAnim   *ani_anim;

        GTimeVal            start_time;
        GTimeVal            current_time;

        /* Time in milliseconds into this run of the animation */
        gint                position;

        /* Index of the current frame */
        gint                current_frame;

        /* Time in milliseconds from the start of the animation till the
           begin of the current frame */
        gint                elapsed;
};

struct _GdkPixbufAniAnimIterClass {
        GdkPixbufAnimationIterClass parent_class;

};

GType gdk_pixbuf_ani_anim_iter_get_type (void) G_GNUC_CONST;

#endif
