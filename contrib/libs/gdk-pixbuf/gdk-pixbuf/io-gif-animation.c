/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - animated gif support
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

#include <contrib/restricted/glib/config.h>
#include <errno.h>
#include "gdk-pixbuf-transform.h"
#include "gdk-pixbuf-private.h"
#include "io-gif-animation.h"

static void gdk_pixbuf_gif_anim_finalize   (GObject        *object);

static gboolean                gdk_pixbuf_gif_anim_is_static_image  (GdkPixbufAnimation *animation);
static GdkPixbuf*              gdk_pixbuf_gif_anim_get_static_image (GdkPixbufAnimation *animation);

static void                    gdk_pixbuf_gif_anim_get_size (GdkPixbufAnimation *anim,
                                                             int                *width,
                                                             int                *height);
static GdkPixbufAnimationIter* gdk_pixbuf_gif_anim_get_iter (GdkPixbufAnimation *anim,
                                                             const GTimeVal     *start_time);




G_DEFINE_TYPE (GdkPixbufGifAnim, gdk_pixbuf_gif_anim, GDK_TYPE_PIXBUF_ANIMATION);

static void
gdk_pixbuf_gif_anim_init (GdkPixbufGifAnim *anim)
{
}

static void
gdk_pixbuf_gif_anim_class_init (GdkPixbufGifAnimClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);
        GdkPixbufAnimationClass *anim_class = GDK_PIXBUF_ANIMATION_CLASS (klass);

        object_class->finalize = gdk_pixbuf_gif_anim_finalize;

        anim_class->is_static_image = gdk_pixbuf_gif_anim_is_static_image;
        anim_class->get_static_image = gdk_pixbuf_gif_anim_get_static_image;
        anim_class->get_size = gdk_pixbuf_gif_anim_get_size;
        anim_class->get_iter = gdk_pixbuf_gif_anim_get_iter;
}

static void
gdk_pixbuf_gif_anim_finalize (GObject *object)
{
        GdkPixbufGifAnim *gif_anim = GDK_PIXBUF_GIF_ANIM (object);

        GList *l;
        GdkPixbufFrame *frame;

        for (l = gif_anim->frames; l; l = l->next) {
                frame = l->data;
                g_object_unref (frame->pixbuf);
                if (frame->composited)
                        g_object_unref (frame->composited);
                if (frame->revert)
                        g_object_unref (frame->revert);
                g_free (frame);
        }

        g_list_free (gif_anim->frames);

        G_OBJECT_CLASS (gdk_pixbuf_gif_anim_parent_class)->finalize (object);
}

static gboolean
gdk_pixbuf_gif_anim_is_static_image  (GdkPixbufAnimation *animation)
{
        GdkPixbufGifAnim *gif_anim;

        gif_anim = GDK_PIXBUF_GIF_ANIM (animation);

        return (gif_anim->frames != NULL &&
                gif_anim->frames->next == NULL);
}

static GdkPixbuf*
gdk_pixbuf_gif_anim_get_static_image (GdkPixbufAnimation *animation)
{
        GdkPixbufGifAnim *gif_anim;

        gif_anim = GDK_PIXBUF_GIF_ANIM (animation);

        if (gif_anim->frames == NULL)
                return NULL;
        else
                return GDK_PIXBUF (((GdkPixbufFrame*)gif_anim->frames->data)->pixbuf);
}

static void
gdk_pixbuf_gif_anim_get_size (GdkPixbufAnimation *anim,
                              int                *width,
                              int                *height)
{
        GdkPixbufGifAnim *gif_anim;

        gif_anim = GDK_PIXBUF_GIF_ANIM (anim);

        if (width)
                *width = gif_anim->width;

        if (height)
                *height = gif_anim->height;
}


static void
iter_clear (GdkPixbufGifAnimIter *iter)
{
        iter->current_frame = NULL;
}

static void
iter_restart (GdkPixbufGifAnimIter *iter)
{
        iter_clear (iter);

        iter->current_frame = iter->gif_anim->frames;
}

static GdkPixbufAnimationIter*
gdk_pixbuf_gif_anim_get_iter (GdkPixbufAnimation *anim,
                              const GTimeVal     *start_time)
{
        GdkPixbufGifAnimIter *iter;

        iter = g_object_new (GDK_TYPE_PIXBUF_GIF_ANIM_ITER, NULL);

        iter->gif_anim = GDK_PIXBUF_GIF_ANIM (anim);

        g_object_ref (iter->gif_anim);

        iter_restart (iter);

        iter->start_time = *start_time;
        iter->current_time = *start_time;
        iter->first_loop_slowness = 0;

        return GDK_PIXBUF_ANIMATION_ITER (iter);
}



static void gdk_pixbuf_gif_anim_iter_finalize   (GObject                   *object);

static int        gdk_pixbuf_gif_anim_iter_get_delay_time             (GdkPixbufAnimationIter *iter);
static GdkPixbuf* gdk_pixbuf_gif_anim_iter_get_pixbuf                 (GdkPixbufAnimationIter *iter);
static gboolean   gdk_pixbuf_gif_anim_iter_on_currently_loading_frame (GdkPixbufAnimationIter *iter);
static gboolean   gdk_pixbuf_gif_anim_iter_advance                    (GdkPixbufAnimationIter *iter,
                                                                       const GTimeVal         *current_time);



G_DEFINE_TYPE (GdkPixbufGifAnimIter, gdk_pixbuf_gif_anim_iter, GDK_TYPE_PIXBUF_ANIMATION_ITER);

static void
gdk_pixbuf_gif_anim_iter_init (GdkPixbufGifAnimIter *iter)
{
}

static void
gdk_pixbuf_gif_anim_iter_class_init (GdkPixbufGifAnimIterClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);
        GdkPixbufAnimationIterClass *anim_iter_class =
                GDK_PIXBUF_ANIMATION_ITER_CLASS (klass);

        object_class->finalize = gdk_pixbuf_gif_anim_iter_finalize;

        anim_iter_class->get_delay_time = gdk_pixbuf_gif_anim_iter_get_delay_time;
        anim_iter_class->get_pixbuf = gdk_pixbuf_gif_anim_iter_get_pixbuf;
        anim_iter_class->on_currently_loading_frame = gdk_pixbuf_gif_anim_iter_on_currently_loading_frame;
        anim_iter_class->advance = gdk_pixbuf_gif_anim_iter_advance;
}

static void
gdk_pixbuf_gif_anim_iter_finalize (GObject *object)
{
        GdkPixbufGifAnimIter *iter = GDK_PIXBUF_GIF_ANIM_ITER (object);

        iter_clear (iter);

        g_object_unref (iter->gif_anim);

        G_OBJECT_CLASS (gdk_pixbuf_gif_anim_iter_parent_class)->finalize (object);
}

static void
gtk_pixpuf_gif_reuse_old_composited_buffer (GdkPixbufFrame *old,
                                            GdkPixbufFrame *new)
{
        /* Move old buffer to new frame to reuse memory and
         * minimise footprint */
        new->composited = old->composited;
        old->composited = NULL;
}

static void
gdk_pixbuf_gif_anim_iter_clean_previous (GList *initial)
{
        GdkPixbufFrame *frame;
        GList *tmp;

        /* Cleanup previous frames until first cleaned frame */
        frame = initial->data;

        /* Check the current frame */
        if (!frame->composited || frame->need_recomposite) {
                /* The composite frame doesn't exist,
                 * nothing to cleanup */
                return;
        }

        /* Work with previous frames */
        tmp = initial->prev;
        while (tmp != NULL) {
                frame = tmp->data;
                if (!frame->composited || frame->need_recomposite) {
                        /* Composite for previous frame doesn't exist */
                        break;
                }

                /* delete cached pixbuf */
                g_clear_object (&frame->composited);
                tmp = tmp->prev;
        }
}

static gboolean
gdk_pixbuf_gif_anim_iter_advance (GdkPixbufAnimationIter *anim_iter,
                                  const GTimeVal         *current_time)
{
        GdkPixbufGifAnimIter *iter;
        gint elapsed;
        gint loop;
        GList *tmp;
        GList *old;

        iter = GDK_PIXBUF_GIF_ANIM_ITER (anim_iter);

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

        g_assert (iter->gif_anim->total_time > 0);

        /* See how many times we've already played the full animation,
         * and subtract time for that.
         */

        if (iter->gif_anim->loading)
                loop = 0;
        else {
                /* If current_frame is NULL at this point, we have loaded the
                 * animation from a source which fell behind the speed of the
                 * display. We remember how much slower the first loop was due
                 * to this and correct the position calculation in order to not
                 * jump in the middle of the second loop.
                 */
                if (iter->current_frame == NULL)
                        iter->first_loop_slowness = MAX(0, elapsed - iter->gif_anim->total_time);

                loop = (elapsed - iter->first_loop_slowness) / iter->gif_anim->total_time;
                elapsed = (elapsed - iter->first_loop_slowness) % iter->gif_anim->total_time;
        }

        iter->position = elapsed;

        /* Now move to the proper frame */
        if (iter->gif_anim->loop == 0 || loop < iter->gif_anim->loop)
                tmp = iter->gif_anim->frames;
        else
                tmp = NULL;
        while (tmp != NULL) {
                GdkPixbufFrame *frame = tmp->data;

                if (iter->position >= frame->elapsed &&
                    iter->position < (frame->elapsed + frame->delay_time))
                        break;

                tmp = tmp->next;
                if (tmp)
                        gdk_pixbuf_gif_anim_iter_clean_previous(tmp);
        }

        old = iter->current_frame;

        iter->current_frame = tmp;

        return iter->current_frame != old;
}

int
gdk_pixbuf_gif_anim_iter_get_delay_time (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufFrame *frame;
        GdkPixbufGifAnimIter *iter;

        iter = GDK_PIXBUF_GIF_ANIM_ITER (anim_iter);

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

void
gdk_pixbuf_gif_anim_frame_composite (GdkPixbufGifAnim *gif_anim,
                                     GdkPixbufFrame   *frame)
{
        GList *link;
        GList *tmp;

        link = g_list_find (gif_anim->frames, frame);

        if (frame->need_recomposite || frame->composited == NULL) {
                /* For now, to composite we start with the last
                 * composited frame and composite everything up to
                 * here.
                 */

                /* Rewind to last composited frame. */
                tmp = link;
                while (tmp != NULL) {
                        GdkPixbufFrame *f = tmp->data;

                        if (f->need_recomposite) {
                                if (f->composited) {
                                        g_object_unref (f->composited);
                                        f->composited = NULL;
                                }
                        }

                        if (f->composited != NULL)
                                break;

                        tmp = tmp->prev;
                }

                /* Go forward, compositing all frames up to the current frame */
                if (tmp == NULL)
                        tmp = gif_anim->frames;

                while (tmp != NULL) {
                        GdkPixbufFrame *f = tmp->data;
                        gint clipped_width, clipped_height;

                        if (f->pixbuf == NULL)
                                return;

                        clipped_width = MIN (gif_anim->width - f->x_offset, gdk_pixbuf_get_width (f->pixbuf));
                        clipped_height = MIN (gif_anim->height - f->y_offset, gdk_pixbuf_get_height (f->pixbuf));

                        if (f->need_recomposite) {
                                if (f->composited) {
                                        g_object_unref (f->composited);
                                        f->composited = NULL;
                                }
                        }

                        if (f->composited != NULL)
                                goto next;

                        if (tmp->prev == NULL) {
                                /* First frame may be smaller than the whole image;
                                 * if so, we make the area outside it full alpha if the
                                 * image has alpha, and background color otherwise.
                                 * GIF spec doesn't actually say what to do about this.
                                 */
                                f->composited = gdk_pixbuf_new (GDK_COLORSPACE_RGB,
                                                                TRUE,
                                                                8, gif_anim->width, gif_anim->height);

                                if (f->composited == NULL)
                                        return;

                                /* alpha gets dumped if f->composited has no alpha */

                                gdk_pixbuf_fill (f->composited,
                                                 (gif_anim->bg_red << 24) |
                                                 (gif_anim->bg_green << 16) |
                                                 (gif_anim->bg_blue << 8));

                                if (clipped_width > 0 && clipped_height > 0)
                                        gdk_pixbuf_composite (f->pixbuf,
                                                              f->composited,
                                                              f->x_offset,
                                                              f->y_offset,
                                                              clipped_width,
                                                              clipped_height,
                                                              f->x_offset, f->y_offset,
                                                              1.0, 1.0,
                                                              GDK_INTERP_BILINEAR,
                                                              255);

                                if (f->action == GDK_PIXBUF_FRAME_REVERT)
                                        g_warning ("First frame of GIF has bad dispose mode, GIF loader should not have loaded this image");

                                f->need_recomposite = FALSE;
                        } else {
                                GdkPixbufFrame *prev_frame;
                                gint prev_clipped_width;
                                gint prev_clipped_height;

                                prev_frame = tmp->prev->data;

                                prev_clipped_width = MIN (gif_anim->width - prev_frame->x_offset, gdk_pixbuf_get_width (prev_frame->pixbuf));
                                prev_clipped_height = MIN (gif_anim->height - prev_frame->y_offset, gdk_pixbuf_get_height (prev_frame->pixbuf));

                                /* Init f->composited with what we should have after the previous
                                 * frame
                                 */

                                if (prev_frame->action == GDK_PIXBUF_FRAME_RETAIN) {
                                        gtk_pixpuf_gif_reuse_old_composited_buffer (prev_frame, f);

                                        if (f->composited == NULL)
                                                return;

                                } else if (prev_frame->action == GDK_PIXBUF_FRAME_DISPOSE) {
                                        gtk_pixpuf_gif_reuse_old_composited_buffer (prev_frame, f);

                                        if (f->composited == NULL)
                                                return;

                                        if (prev_clipped_width > 0 && prev_clipped_height > 0) {
                                                /* Clear area of previous frame to background */
                                                GdkPixbuf *area;

                                                area = gdk_pixbuf_new_subpixbuf (f->composited,
                                                                                 prev_frame->x_offset,
                                                                                 prev_frame->y_offset,
                                                                                 prev_clipped_width,
                                                                                 prev_clipped_height);

                                                if (area == NULL)
                                                        return;

                                                gdk_pixbuf_fill (area,
                                                                 (gif_anim->bg_red << 24) |
                                                                 (gif_anim->bg_green << 16) |
                                                                 (gif_anim->bg_blue << 8));

                                                g_object_unref (area);
                                        }
                                } else if (prev_frame->action == GDK_PIXBUF_FRAME_REVERT) {
                                        gtk_pixpuf_gif_reuse_old_composited_buffer (prev_frame, f);

                                        if (f->composited == NULL)
                                                return;

                                        if (prev_frame->revert != NULL &&
                                            prev_clipped_width > 0 && prev_clipped_height > 0) {
                                                /* Copy in the revert frame */
                                                gdk_pixbuf_copy_area (prev_frame->revert,
                                                                      0, 0,
                                                                      gdk_pixbuf_get_width (prev_frame->revert),
                                                                      gdk_pixbuf_get_height (prev_frame->revert),
                                                                      f->composited,
                                                                      prev_frame->x_offset,
                                                                      prev_frame->y_offset);
                                        }
                                } else {
                                        g_warning ("Unknown revert action for GIF frame");
                                }

                                if (f->revert == NULL &&
                                    f->action == GDK_PIXBUF_FRAME_REVERT) {
                                        if (clipped_width > 0 && clipped_height > 0) {
                                                /* We need to save the contents before compositing */
                                                GdkPixbuf *area;

                                                area = gdk_pixbuf_new_subpixbuf (f->composited,
                                                                                 f->x_offset,
                                                                                 f->y_offset,
                                                                                 clipped_width,
                                                                                 clipped_height);

                                                if (area == NULL)
                                                        return;

                                                f->revert = gdk_pixbuf_copy (area);

                                                g_object_unref (area);

                                                if (f->revert == NULL)
                                                        return;
                                        }
                                }

                                if (clipped_width > 0 && clipped_height > 0 &&
                                    f->pixbuf != NULL && f->composited != NULL) {
                                        /* Put current frame onto f->composited */
                                        gdk_pixbuf_composite (f->pixbuf,
                                                              f->composited,
                                                              f->x_offset,
                                                              f->y_offset,
                                                              clipped_width,
                                                              clipped_height,
                                                              f->x_offset, f->y_offset,
                                                              1.0, 1.0,
                                                              GDK_INTERP_NEAREST,
                                                              255);
                                }

                                f->need_recomposite = FALSE;
                        }

                next:
                        if (tmp == link)
                                break;

                        tmp = tmp->next;
                        if (tmp)
                                 gdk_pixbuf_gif_anim_iter_clean_previous(tmp);
                }
        }
}

GdkPixbuf*
gdk_pixbuf_gif_anim_iter_get_pixbuf (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufGifAnimIter *iter;
        GdkPixbufFrame *frame;

        iter = GDK_PIXBUF_GIF_ANIM_ITER (anim_iter);

        frame = iter->current_frame ? iter->current_frame->data : g_list_last (iter->gif_anim->frames)->data;

#if 0
        if (FALSE && frame)
          g_print ("current frame %d dispose mode %d  %d x %d\n",
                   g_list_index (iter->gif_anim->frames,
                                 frame),
                   frame->action,
                   gdk_pixbuf_get_width (frame->pixbuf),
                   gdk_pixbuf_get_height (frame->pixbuf));
#endif

        if (frame == NULL)
                return NULL;

        gdk_pixbuf_gif_anim_frame_composite (iter->gif_anim, frame);

        return frame->composited;
}

static gboolean
gdk_pixbuf_gif_anim_iter_on_currently_loading_frame (GdkPixbufAnimationIter *anim_iter)
{
        GdkPixbufGifAnimIter *iter;

        iter = GDK_PIXBUF_GIF_ANIM_ITER (anim_iter);

        return iter->current_frame == NULL || iter->current_frame->next == NULL;
}
