/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* GdkPixbuf library - animated gdip support
 *
 * Copyright (C) 1999 The Free Software Foundation
 *
 * Authors: Jonathan Blandford <jrb@redhat.com>
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

#include <errno.h>
#include "io-gdip-native.h"
#include "io-gdip-animation.h"

static void gdk_pixbuf_gdip_anim_finalize   (GObject        *object);

static gboolean                gdk_pixbuf_gdip_anim_is_static_image  (GdkPixbufAnimation *animation);
static GdkPixbuf*              gdk_pixbuf_gdip_anim_get_static_image (GdkPixbufAnimation *animation);

static void                    gdk_pixbuf_gdip_anim_get_size (GdkPixbufAnimation *anim,
                                                             int                *width,
                                                             int                *height);
static GdkPixbufAnimationIter* gdk_pixbuf_gdip_anim_get_iter (GdkPixbufAnimation *anim,
                                                             const GTimeVal     *start_time);


G_DEFINE_TYPE (GdkPixbufGdipAnim, gdk_pixbuf_gdip_anim, GDK_TYPE_PIXBUF_ANIMATION);

static void
gdk_pixbuf_gdip_anim_init (GdkPixbufGdipAnim *anim)
{
}

static void
gdk_pixbuf_gdip_anim_class_init (GdkPixbufGdipAnimClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);
        GdkPixbufAnimationClass *anim_class = GDK_PIXBUF_ANIMATION_CLASS (klass);

        object_class->finalize = gdk_pixbuf_gdip_anim_finalize;

        anim_class->is_static_image = gdk_pixbuf_gdip_anim_is_static_image;
        anim_class->get_static_image = gdk_pixbuf_gdip_anim_get_static_image;
        anim_class->get_size = gdk_pixbuf_gdip_anim_get_size;
        anim_class->get_iter = gdk_pixbuf_gdip_anim_get_iter;
}

static void
gdk_pixbuf_gdip_anim_finalize (GObject *object)
{
        GdkPixbufGdipAnim *gdip_anim = GDK_PIXBUF_GDIP_ANIM (object);

        GList *l;
        GdkPixbufFrame *frame;

        for (l = gdip_anim->frames; l; l = l->next) {
                frame = l->data;
                g_object_unref (frame->pixbuf);
                g_free (frame);
        }

        g_list_free (gdip_anim->frames);

        G_OBJECT_CLASS (gdk_pixbuf_gdip_anim_parent_class)->finalize (object);
}

static gboolean
gdk_pixbuf_gdip_anim_is_static_image  (GdkPixbufAnimation *animation)
{
        GdkPixbufGdipAnim *gdip_anim;

        gdip_anim = GDK_PIXBUF_GDIP_ANIM (animation);

        return (gdip_anim->frames != NULL &&
                gdip_anim->frames->next == NULL);
}

static GdkPixbuf*
gdk_pixbuf_gdip_anim_get_static_image (GdkPixbufAnimation *animation)
{
        GdkPixbufGdipAnim *gdip_anim;

        gdip_anim = GDK_PIXBUF_GDIP_ANIM (animation);

        if (gdip_anim->frames == NULL)
                return NULL;
        else
                return GDK_PIXBUF (((GdkPixbufFrame*)gdip_anim->frames->data)->pixbuf);
}

static void
gdk_pixbuf_gdip_anim_get_size (GdkPixbufAnimation *anim,
                              int                *width,
                              int                *height)
{
        GdkPixbufGdipAnim *gdip_anim;

        gdip_anim = GDK_PIXBUF_GDIP_ANIM (anim);

        if (width)
                *width = gdip_anim->width;

        if (height)
                *height = gdip_anim->height;
}


static void
iter_clear (GdkPixbufGdipAnimIter *iter)
{
        iter->current_frame = NULL;
}

static void
iter_restart (GdkPixbufGdipAnimIter *iter)
{
        iter_clear (iter);

        iter->current_frame = iter->gdip_anim->frames;
}

static GdkPixbufAnimationIter*
gdk_pixbuf_gdip_anim_get_iter (GdkPixbufAnimation *anim,
                              const GTimeVal     *start_time)
{
        GdkPixbufGdipAnimIter *iter;

        iter = g_object_new (GDK_TYPE_PIXBUF_GDIP_ANIM_ITER, NULL);

        iter->gdip_anim = GDK_PIXBUF_GDIP_ANIM (anim);

        g_object_ref (iter->gdip_anim);

        iter_restart (iter);

        iter->start_time = *start_time;
        iter->current_time = *start_time;
        iter->first_loop_slowness = 0;

        return GDK_PIXBUF_ANIMATION_ITER (iter);
}

static void gdk_pixbuf_gdip_anim_iter_finalize   (GObject                   *object);

static int        gdk_pixbuf_gdip_anim_iter_get_delay_time             (GdkPixbufAnimationIter *iter);
static GdkPixbuf* gdk_pixbuf_gdip_anim_iter_get_pixbuf                 (GdkPixbufAnimationIter *iter);
static gboolean   gdk_pixbuf_gdip_anim_iter_on_currently_loading_frame (GdkPixbufAnimationIter *iter);
static gboolean   gdk_pixbuf_gdip_anim_iter_advance                    (GdkPixbufAnimationIter *iter,
                                                                       const GTimeVal         *current_time);

G_DEFINE_TYPE (GdkPixbufGdipAnimIter, gdk_pixbuf_gdip_anim_iter, GDK_TYPE_PIXBUF_ANIMATION_ITER);

static void
gdk_pixbuf_gdip_anim_iter_init (GdkPixbufGdipAnimIter *iter)
{
}

static void
gdk_pixbuf_gdip_anim_iter_class_init (GdkPixbufGdipAnimIterClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);
        GdkPixbufAnimationIterClass *anim_iter_class =
                GDK_PIXBUF_ANIMATION_ITER_CLASS (klass);

        object_class->finalize = gdk_pixbuf_gdip_anim_iter_finalize;

        anim_iter_class->get_delay_time = gdk_pixbuf_gdip_anim_iter_get_delay_time;
        anim_iter_class->get_pixbuf = gdk_pixbuf_gdip_anim_iter_get_pixbuf;
        anim_iter_class->on_currently_loading_frame = gdk_pixbuf_gdip_anim_iter_on_currently_loading_frame;
        anim_iter_class->advance = gdk_pixbuf_gdip_anim_iter_advance;
}

static void
gdk_pixbuf_gdip_anim_iter_finalize (GObject *object)
{
        GdkPixbufGdipAnimIter *iter = GDK_PIXBUF_GDIP_ANIM_ITER (object);

        iter_clear (iter);

        g_object_unref (iter->gdip_anim);

        G_OBJECT_CLASS (gdk_pixbuf_gdip_anim_iter_parent_class)->finalize (object);
}

static gboolean
gdk_pixbuf_gdip_anim_iter_advance (GdkPixbufAnimationIter *anim_iter,
                                  const GTimeVal         *current_time)
{
        GdkPixbufGdipAnimIter *iter;
        gint elapsed;
        gint loop;
        GList *tmp;
        GList *old;

        iter = GDK_PIXBUF_GDIP_ANIM_ITER (anim_iter);

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

        g_assert (iter->gdip_anim->total_time > 0);

        /* See how many times we've already played the full animation,
         * and subtract time for that.
         */

        if (iter->gdip_anim->loading)
                loop = 0;
        else {
                /* If current_frame is NULL at this point, we have loaded the
                 * animation from a source which fell behind the speed of the 
                 * display. We remember how much slower the first loop was due
                 * to this and correct the position calculation in order to not
                 * jump in the middle of the second loop.
                 */
                if (iter->current_frame == NULL)
                        iter->first_loop_slowness = MAX(0, elapsed - iter->gdip_anim->total_time);

                loop = (elapsed - iter->first_loop_slowness) / iter->gdip_anim->total_time;
                elapsed = (elapsed - iter->first_loop_slowness) % iter->gdip_anim->total_time;
        }

        iter->position = elapsed;

        /* Now move to the proper frame */
        if (iter->gdip_anim->loop == 0 || loop < iter->gdip_anim->loop)
                tmp = iter->gdip_anim->frames;
        else
                tmp = NULL;
        while (tmp != NULL) {
                GdkPixbufFrame *frame = tmp->data;

                if (iter->position >= frame->elapsed &&
                    iter->position < (frame->elapsed + frame->delay_time))
                        break;

                tmp = tmp->next;
        }

        old = iter->current_frame;

        iter->current_frame = tmp;

        return iter->current_frame != old;
}

int
gdk_pixbuf_gdip_anim_iter_get_delay_time (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufFrame *frame;
        GdkPixbufGdipAnimIter *iter;

        iter = GDK_PIXBUF_GDIP_ANIM_ITER (anim_iter);

        if (iter->current_frame) {
                frame = iter->current_frame->data;

#if 0
                g_print ("frame start: %d pos: %d frame len: %d frame remaining: %d\n",
                         frame->elapsed,
                         iter->position,
                         frame->delay_time,
                         frame->delay_time - (iter->position - frame->elapsed));
#endif

                return frame->delay_time - (iter->position - frame->elapsed);
        } else
                return -1; /* show last frame forever */
}

GdkPixbuf*
gdk_pixbuf_gdip_anim_iter_get_pixbuf (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufGdipAnimIter *iter;
        GdkPixbufFrame *frame;

        iter = GDK_PIXBUF_GDIP_ANIM_ITER (anim_iter);

        frame = iter->current_frame ? iter->current_frame->data : g_list_last (iter->gdip_anim->frames)->data;

#if 0
        if (FALSE && frame)
          g_print ("current frame %d dispose mode %d  %d x %d\n",
                   g_list_index (iter->gdip_anim->frames,
                                 frame),
                   frame->action,
                   gdk_pixbuf_get_width (frame->pixbuf),
                   gdk_pixbuf_get_height (frame->pixbuf));
#endif

        if (frame == NULL)
                return NULL;

        return frame->pixbuf;
}

static gboolean
gdk_pixbuf_gdip_anim_iter_on_currently_loading_frame (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufGdipAnimIter *iter;

        iter = GDK_PIXBUF_GDIP_ANIM_ITER (anim_iter);

        return iter->current_frame == NULL || iter->current_frame->next == NULL;
}
