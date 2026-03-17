/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - ani support
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

#include <contrib/restricted/glib/config.h>
#include <errno.h>
#include "gdk-pixbuf-private.h"
#include "io-ani-animation.h"

static void gdk_pixbuf_ani_anim_finalize   (GObject        *object);

static gboolean                gdk_pixbuf_ani_anim_is_static_image  (GdkPixbufAnimation *animation);
static GdkPixbuf*              gdk_pixbuf_ani_anim_get_static_image (GdkPixbufAnimation *animation);
static void                    gdk_pixbuf_ani_anim_get_size (GdkPixbufAnimation *anim,
                                                             int                *width,
                                                             int                *height);
static GdkPixbufAnimationIter* gdk_pixbuf_ani_anim_get_iter (GdkPixbufAnimation *anim,
                                                             const GTimeVal     *start_time);




G_DEFINE_TYPE  (GdkPixbufAniAnim, gdk_pixbuf_ani_anim, GDK_TYPE_PIXBUF_ANIMATION)

static void
gdk_pixbuf_ani_anim_init (GdkPixbufAniAnim *anim)
{
}

static void
gdk_pixbuf_ani_anim_class_init (GdkPixbufAniAnimClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);
        GdkPixbufAnimationClass *anim_class = GDK_PIXBUF_ANIMATION_CLASS (klass);

        object_class->finalize = gdk_pixbuf_ani_anim_finalize;

        anim_class->is_static_image = gdk_pixbuf_ani_anim_is_static_image;
        anim_class->get_static_image = gdk_pixbuf_ani_anim_get_static_image;
        anim_class->get_size = gdk_pixbuf_ani_anim_get_size;
        anim_class->get_iter = gdk_pixbuf_ani_anim_get_iter;
}

static void
gdk_pixbuf_ani_anim_finalize (GObject *object)
{
        GdkPixbufAniAnim *ani_anim = GDK_PIXBUF_ANI_ANIM (object);
        gint i;

        for (i = 0; i < ani_anim->n_pixbufs; i++) {
                if (ani_anim->pixbufs[i])
                        g_object_unref (ani_anim->pixbufs[i]);
        }
        g_free (ani_anim->pixbufs);
        g_free (ani_anim->sequence);
        g_free (ani_anim->delay);

        G_OBJECT_CLASS (gdk_pixbuf_ani_anim_parent_class)->finalize (object);
}

static gboolean
gdk_pixbuf_ani_anim_is_static_image (GdkPixbufAnimation *animation)
{
        GdkPixbufAniAnim *ani_anim;

        ani_anim = GDK_PIXBUF_ANI_ANIM (animation);

        return ani_anim->n_frames == 1;
}

static GdkPixbuf*
gdk_pixbuf_ani_anim_get_static_image (GdkPixbufAnimation *animation)
{
        GdkPixbufAniAnim *ani_anim;

        ani_anim = GDK_PIXBUF_ANI_ANIM (animation);

        if (ani_anim->pixbufs == NULL)
                return NULL;
        else
                return ani_anim->pixbufs[0];
}

static void
gdk_pixbuf_ani_anim_get_size (GdkPixbufAnimation *anim,
                              int                *width,
                              int                *height)
{
        GdkPixbufAniAnim *ani_anim;

        ani_anim = GDK_PIXBUF_ANI_ANIM (anim);

        if (width)
                *width = ani_anim->width;

        if (height)
                *height = ani_anim->height;
}


static void
iter_restart (GdkPixbufAniAnimIter *iter)
{
        iter->current_frame = 0;
        iter->position = 0;
        iter->elapsed = 0;
}

static GdkPixbufAnimationIter*
gdk_pixbuf_ani_anim_get_iter (GdkPixbufAnimation *anim,
                              const GTimeVal     *start_time)
{
        GdkPixbufAniAnimIter *iter;

        iter = g_object_new (GDK_TYPE_PIXBUF_ANI_ANIM_ITER, NULL);

        iter->ani_anim = GDK_PIXBUF_ANI_ANIM (anim);

        g_object_ref (iter->ani_anim);

        iter_restart (iter);

        iter->start_time = *start_time;
        iter->current_time = *start_time;

        return GDK_PIXBUF_ANIMATION_ITER (iter);
}



static void gdk_pixbuf_ani_anim_iter_finalize   (GObject                   *object);

static int        gdk_pixbuf_ani_anim_iter_get_delay_time             (GdkPixbufAnimationIter *iter);
static GdkPixbuf* gdk_pixbuf_ani_anim_iter_get_pixbuf                 (GdkPixbufAnimationIter *iter);
static gboolean   gdk_pixbuf_ani_anim_iter_on_currently_loading_frame (GdkPixbufAnimationIter *iter);
static gboolean   gdk_pixbuf_ani_anim_iter_advance                    (GdkPixbufAnimationIter *iter,
                                                                       const GTimeVal         *current_time);



G_DEFINE_TYPE (GdkPixbufAniAnimIter, gdk_pixbuf_ani_anim_iter, GDK_TYPE_PIXBUF_ANIMATION_ITER);

static void
gdk_pixbuf_ani_anim_iter_init (GdkPixbufAniAnimIter *anim)
{
}

static void
gdk_pixbuf_ani_anim_iter_class_init (GdkPixbufAniAnimIterClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);
        GdkPixbufAnimationIterClass *anim_iter_class =
                GDK_PIXBUF_ANIMATION_ITER_CLASS (klass);

        object_class->finalize = gdk_pixbuf_ani_anim_iter_finalize;

        anim_iter_class->get_delay_time = gdk_pixbuf_ani_anim_iter_get_delay_time;
        anim_iter_class->get_pixbuf = gdk_pixbuf_ani_anim_iter_get_pixbuf;
        anim_iter_class->on_currently_loading_frame = gdk_pixbuf_ani_anim_iter_on_currently_loading_frame;
        anim_iter_class->advance = gdk_pixbuf_ani_anim_iter_advance;
}

static void
gdk_pixbuf_ani_anim_iter_finalize (GObject *object)
{
        GdkPixbufAniAnimIter *iter = GDK_PIXBUF_ANI_ANIM_ITER (object);

        g_object_unref (iter->ani_anim);

        G_OBJECT_CLASS (gdk_pixbuf_ani_anim_iter_parent_class)->finalize (object);
}

static gboolean
gdk_pixbuf_ani_anim_iter_advance (GdkPixbufAnimationIter *anim_iter,
                                  const GTimeVal         *current_time)
{
        GdkPixbufAniAnimIter *iter;
        gint elapsed;
        gint tmp;
        gint old;

        iter = GDK_PIXBUF_ANI_ANIM_ITER (anim_iter);

        iter->current_time = *current_time;

        /* We use milliseconds for all times */
        elapsed =
          (((iter->current_time.tv_sec - iter->start_time.tv_sec) * G_USEC_PER_SEC +
            iter->current_time.tv_usec - iter->start_time.tv_usec)) / 1000;

        if (elapsed < 0) {
                /* Try to compensate; probably the system clock
                 * was set backwards
                 */
                iter->start_time = iter->current_time;
                elapsed = 0;
        }

        g_assert (iter->ani_anim->total_time > 0);

        /* See how many times we've already played the full animation,
         * and subtract time for that.
         */
        elapsed = elapsed % iter->ani_anim->total_time;

        iter->position = elapsed;

        /* Now move to the proper frame */

        iter->elapsed = 0;
        for (tmp = 0; tmp < iter->ani_anim->n_frames; tmp++) {
                if (iter->position >= iter->elapsed &&
                    iter->position < (iter->elapsed + iter->ani_anim->delay[tmp]))
                        break;
                iter->elapsed += iter->ani_anim->delay[tmp];
        }

        old = iter->current_frame;

        iter->current_frame = tmp;

        return iter->current_frame != old;
}

int
gdk_pixbuf_ani_anim_iter_get_delay_time (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufAniAnimIter *iter;

        iter = GDK_PIXBUF_ANI_ANIM_ITER (anim_iter);

        return iter->ani_anim->delay[iter->current_frame] - (iter->position - iter->elapsed);
}

GdkPixbuf*
gdk_pixbuf_ani_anim_iter_get_pixbuf (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufAniAnimIter *iter;
        gint frame;

        iter = GDK_PIXBUF_ANI_ANIM_ITER (anim_iter);

        frame = iter->ani_anim->sequence[iter->current_frame];

        /* this is necessary if the animation is displayed while loading */
        while (frame > 0 && !iter->ani_anim->pixbufs[frame])
                frame--;

        return iter->ani_anim->pixbufs[frame];
}

static gboolean
gdk_pixbuf_ani_anim_iter_on_currently_loading_frame (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufAniAnimIter *iter;
        gint frame;

        iter = GDK_PIXBUF_ANI_ANIM_ITER (anim_iter);

        if (iter->current_frame >= iter->ani_anim->n_frames - 1)
                return TRUE;

        frame = iter->ani_anim->sequence[iter->current_frame + 1];

        if (!iter->ani_anim->pixbufs[frame])
                return TRUE;

        return FALSE;
}










