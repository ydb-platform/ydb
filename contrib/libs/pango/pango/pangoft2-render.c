/* Pango
 * pangoft2-render.c: Rendering routines to FT_Bitmap objects
 *
 * Copyright (C) 2004 Red Hat Software
 * Copyright (C) 2000 Tor Lillqvist
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#include "config.h"
#include <math.h>

#include "pango-font-private.h"
#include "pangoft2-private.h"
#include "pango-impl-utils.h"

/* for compatibility with older freetype versions */
#ifndef FT_LOAD_TARGET_MONO
#define FT_LOAD_TARGET_MONO  FT_LOAD_MONOCHROME
#endif

typedef struct _PangoFT2RendererClass PangoFT2RendererClass;

#define PANGO_FT2_RENDERER_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_FT2_RENDERER, PangoFT2RendererClass))
#define PANGO_IS_FT2_RENDERER_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_FT2_RENDERER))
#define PANGO_FT2_RENDERER_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_FT2_RENDERER, PangoFT2RendererClass))

struct _PangoFT2Renderer
{
  PangoRenderer parent_instance;

  FT_Bitmap *bitmap;
};

struct _PangoFT2RendererClass
{
  PangoRendererClass parent_class;
};

static void pango_ft2_renderer_draw_glyph     (PangoRenderer    *renderer,
					       PangoFont        *font,
					       PangoGlyph        glyph,
					       double            x,
					       double            y);
static void pango_ft2_renderer_draw_trapezoid (PangoRenderer    *renderer,
					       PangoRenderPart   part,
					       double            y1,
					       double            x11,
					       double            x21,
					       double            y2,
					       double            x12,
					       double            x22);

G_DEFINE_TYPE (PangoFT2Renderer, pango_ft2_renderer, PANGO_TYPE_RENDERER)

static void
pango_ft2_renderer_init (PangoFT2Renderer *renderer G_GNUC_UNUSED)
{
}

static void
pango_ft2_renderer_class_init (PangoFT2RendererClass *klass)
{
  PangoRendererClass *renderer_class = PANGO_RENDERER_CLASS (klass);

  renderer_class->draw_glyph = pango_ft2_renderer_draw_glyph;
  renderer_class->draw_trapezoid = pango_ft2_renderer_draw_trapezoid;
}

static void
pango_ft2_renderer_set_bitmap (PangoFT2Renderer *renderer,
			      FT_Bitmap         *bitmap)
{
  renderer->bitmap = bitmap;
}

typedef struct
{
  FT_Bitmap bitmap;
  int bitmap_left;
  int bitmap_top;
} PangoFT2RenderedGlyph;

static void
pango_ft2_free_rendered_glyph (PangoFT2RenderedGlyph *rendered)
{
  g_free (rendered->bitmap.buffer);
  g_slice_free (PangoFT2RenderedGlyph, rendered);
}

static PangoFT2RenderedGlyph *
pango_ft2_font_render_box_glyph (int      width,
				 int      height,
				 int      top,
				 gboolean invalid)
{
  PangoFT2RenderedGlyph *box;
  int i, j, offset1, offset2, line_width;

  line_width = MAX ((height + 43) / 44, 1);
  if (width < 1 || height < 1)
    line_width = 0;

  box = g_slice_new (PangoFT2RenderedGlyph);

  box->bitmap_left = 0;
  box->bitmap_top = top;

  box->bitmap.pixel_mode = ft_pixel_mode_grays;

  box->bitmap.width = width;
  box->bitmap.rows = height;
  box->bitmap.pitch = width;

  box->bitmap.buffer = g_malloc0_n (box->bitmap.rows, box->bitmap.pitch);

  if (G_UNLIKELY (!box->bitmap.buffer)) {
    g_slice_free (PangoFT2RenderedGlyph, box);
    return NULL;
  }

  /* draw the box */
  for (j = 0; j < line_width; j++)
    {
      offset1 = box->bitmap.pitch * (MIN (1 + j, height - 1));
      offset2 = box->bitmap.pitch * (MAX (box->bitmap.rows - 2 - j, 0));
      for (i = 1;
	   i < (int) box->bitmap.width - 1;
	   i++)
	{
	  box->bitmap.buffer[offset1 + i] = 0xff;
	  box->bitmap.buffer[offset2 + i] = 0xff;
	}
    }
  for (j = 0; j < line_width; j++)
    {
      offset1 = MIN (1 + j, width - 1);
      offset2 = MAX ((int) box->bitmap.width - 2 - j, 0);
      for (i = box->bitmap.pitch;
	   i < (int) (box->bitmap.rows - 1) * box->bitmap.pitch;
	   i += box->bitmap.pitch)
	{
	  box->bitmap.buffer[offset1 + i] = 0xff;
	  box->bitmap.buffer[offset2 + i] = 0xff;
	}
    }

  if (invalid)
    {
      /* XXX This may scrabble memory.  Didn't check close enough */
      int inc = PANGO_SCALE * MAX (width - line_width, 0) / (height + 1);
      offset1 = PANGO_SCALE;
      offset2 = PANGO_SCALE * MAX (width - line_width - 1, 0) ;
      for (i = box->bitmap.pitch;
	   i < (int) (box->bitmap.rows - 1) * box->bitmap.pitch;
	   i += box->bitmap.pitch)
        {
	  for (j = 0; j < line_width; j++)
	    {
	      box->bitmap.buffer[PANGO_PIXELS (offset1) + i + j] = 0xff;
	      box->bitmap.buffer[PANGO_PIXELS (offset2) + i + j] = 0xff;
	    }
	  offset1 += inc;
	  offset2 -= inc;
	}

    }

  return box;
}

static PangoFT2RenderedGlyph *
pango_ft2_font_render_glyph (PangoFont *font,
			     PangoGlyph glyph_index)
{
  FT_Face face;
  gboolean invalid_input;

  invalid_input = glyph_index == PANGO_GLYPH_INVALID_INPUT || (glyph_index & ~PANGO_GLYPH_UNKNOWN_FLAG) > 0x10FFFF;

  if (glyph_index & PANGO_GLYPH_UNKNOWN_FLAG)
    {
      PangoFT2RenderedGlyph *box;
      PangoFontMetrics *metrics;

      if (!font)
	goto generic_box;

      metrics = pango_font_get_metrics (font, NULL);
      if (!metrics)
	goto generic_box;

      box = pango_ft2_font_render_box_glyph (PANGO_PIXELS (metrics->approximate_char_width),
					     PANGO_PIXELS (metrics->ascent + metrics->descent),
					     PANGO_PIXELS (metrics->ascent),
					     invalid_input);
      pango_font_metrics_unref (metrics);

      return box;
    }

  face = pango_ft2_font_get_face (font);

  if (face)
    {
      PangoFT2RenderedGlyph *rendered;
      PangoFT2Font *ft2font = (PangoFT2Font *) font;

      rendered = g_slice_new (PangoFT2RenderedGlyph);

      /* Draw glyph */
      FT_Load_Glyph (face, glyph_index, ft2font->load_flags);
      FT_Render_Glyph (face->glyph,
		       (ft2font->load_flags & FT_LOAD_TARGET_MONO ?
			ft_render_mode_mono : ft_render_mode_normal));

      rendered->bitmap = face->glyph->bitmap;
      rendered->bitmap.buffer = g_memdup2 (face->glyph->bitmap.buffer,
                                           face->glyph->bitmap.rows * face->glyph->bitmap.pitch);
      rendered->bitmap_left = face->glyph->bitmap_left;
      rendered->bitmap_top = face->glyph->bitmap_top;

      if (G_UNLIKELY (!rendered->bitmap.buffer)) {
        g_slice_free (PangoFT2RenderedGlyph, rendered);
	return NULL;
      }

      return rendered;
    }
  else
    {
generic_box:
      return  pango_ft2_font_render_box_glyph (PANGO_UNKNOWN_GLYPH_WIDTH,
					       PANGO_UNKNOWN_GLYPH_HEIGHT,
					       PANGO_UNKNOWN_GLYPH_HEIGHT,
					       invalid_input);
    }
}

static void
pango_ft2_renderer_draw_glyph (PangoRenderer *renderer,
			       PangoFont     *font,
			       PangoGlyph     glyph,
			       double         x,
			       double         y)
{
  FT_Bitmap *bitmap = PANGO_FT2_RENDERER (renderer)->bitmap;
  PangoFT2RenderedGlyph *rendered_glyph;
  gboolean add_glyph_to_cache;
  guchar *src, *dest;

  int x_start, x_limit;
  int y_start, y_limit;
  int ixoff = floor (x + 0.5);
  int iyoff = floor (y + 0.5);
  int ix, iy;

  if (glyph & PANGO_GLYPH_UNKNOWN_FLAG)
    {
      /* Since we don't draw hexbox for FT2 renderer,
       * unifiy the rendered bitmap in the cache by converting
       * all missing glyphs to either INVALID_INPUT or UNKNOWN_FLAG.
       */

      gunichar wc = glyph & (~PANGO_GLYPH_UNKNOWN_FLAG);

      if (G_UNLIKELY (glyph == PANGO_GLYPH_INVALID_INPUT || wc > 0x10FFFF))
	glyph = PANGO_GLYPH_INVALID_INPUT;
      else
	glyph = PANGO_GLYPH_UNKNOWN_FLAG;
    }

  rendered_glyph = _pango_ft2_font_get_cache_glyph_data (font, glyph);
  add_glyph_to_cache = FALSE;
  if (rendered_glyph == NULL)
    {
      rendered_glyph = pango_ft2_font_render_glyph (font, glyph);
      if (rendered_glyph == NULL)
        return;
      add_glyph_to_cache = TRUE;
    }

  x_start = MAX (0, - (ixoff + rendered_glyph->bitmap_left));
  x_limit = MIN ((int) rendered_glyph->bitmap.width,
		 (int) (bitmap->width - (ixoff + rendered_glyph->bitmap_left)));

  y_start = MAX (0,  - (iyoff - rendered_glyph->bitmap_top));
  y_limit = MIN ((int) rendered_glyph->bitmap.rows,
		 (int) (bitmap->rows - (iyoff - rendered_glyph->bitmap_top)));

  src = rendered_glyph->bitmap.buffer +
    y_start * rendered_glyph->bitmap.pitch;

  dest = bitmap->buffer +
    (y_start + iyoff - rendered_glyph->bitmap_top) * bitmap->pitch +
    x_start + ixoff + rendered_glyph->bitmap_left;

  switch (rendered_glyph->bitmap.pixel_mode)
    {
    case ft_pixel_mode_grays:
      src += x_start;
      for (iy = y_start; iy < y_limit; iy++)
	{
	  guchar *s = src;
	  guchar *d = dest;

	  for (ix = x_start; ix < x_limit; ix++)
	    {
	      switch (*s)
		{
		case 0:
		  break;
		case 0xff:
		  *d = 0xff;
                  break;
		default:
		  *d = MIN ((gushort) *d + (gushort) *s, 0xff);
		  break;
		}

	      s++;
	      d++;
	    }

	  dest += bitmap->pitch;
	  src  += rendered_glyph->bitmap.pitch;
	}
      break;

    case ft_pixel_mode_mono:
      src += x_start / 8;
      for (iy = y_start; iy < y_limit; iy++)
	{
	  guchar *s = src;
	  guchar *d = dest;

	  for (ix = x_start; ix < x_limit; ix++)
	    {
	      if ((*s) & (1 << (7 - (ix % 8))))
		*d |= 0xff;

	      if ((ix % 8) == 7)
		s++;
	      d++;
	    }

	  dest += bitmap->pitch;
	  src  += rendered_glyph->bitmap.pitch;
	}
      break;

    default:
      g_warning ("pango_ft2_render: "
		 "Unrecognized glyph bitmap pixel mode %d\n",
		 rendered_glyph->bitmap.pixel_mode);
      break;
    }

  if (add_glyph_to_cache)
    {
      _pango_ft2_font_set_glyph_cache_destroy (font,
					       (GDestroyNotify) pango_ft2_free_rendered_glyph);
      _pango_ft2_font_set_cache_glyph_data (font,
					    glyph, rendered_glyph);
    }
}

typedef struct {
  double y;
  double x1;
  double x2;
} Position;

static void
draw_simple_trap (PangoRenderer *renderer,
		  Position      *t,
		  Position      *b)
{
  FT_Bitmap *bitmap = PANGO_FT2_RENDERER (renderer)->bitmap;
  int iy = floor (t->y);
  int x1, x2, x;
  double dy = b->y - t->y;
  guchar *dest;

  if (iy < 0 || iy >= (int) bitmap->rows)
    return;
  dest = bitmap->buffer + iy * bitmap->pitch;

  if (t->x1 < b->x1)
    x1 = floor (t->x1);
  else
    x1 = floor (b->x1);

  if (t->x2 > b->x2)
    x2 = ceil (t->x2);
  else
    x2 = ceil (b->x2);

  x1 = CLAMP (x1, 0, (int) bitmap->width);
  x2 = CLAMP (x2, 0, (int) bitmap->width);

  for (x = x1; x < x2; x++)
    {
      double top_left = MAX (t->x1, x);
      double top_right = MIN (t->x2, x + 1);
      double bottom_left = MAX (b->x1, x);
      double bottom_right = MIN (b->x2, x + 1);
      double c = 0.5 * dy * ((top_right - top_left) + (bottom_right - bottom_left));

      /* When converting to [0,255], we round up. This is intended
       * to prevent the problem of pixels that get divided into
       * multiple slices not being fully black.
       */
      int ic = c * 256;

      dest[x] = MIN (dest[x] + ic, 255);
    }
}

static void
interpolate_position (Position *result,
		      Position *top,
		      Position *bottom,
		      double    val,
		      double    val1,
		      double    val2)
{
  result->y  = (top->y *  (val2 - val) + bottom->y *  (val - val1)) / (val2 - val1);
  result->x1 = (top->x1 * (val2 - val) + bottom->x1 * (val - val1)) / (val2 - val1);
  result->x2 = (top->x2 * (val2 - val) + bottom->x2 * (val - val1)) / (val2 - val1);
}

/* This draws a trapezoid with the parallel sides aligned with
 * the X axis. We do this by subdividing the trapezoid vertically
 * into thin slices (themselves trapezoids) where two edge sides are each
 * contained within a single pixel and then rasterizing each
 * slice. There are frequently multiple slices within a single
 * line so we have to accumulate to get the final result.
 */
static void
pango_ft2_renderer_draw_trapezoid (PangoRenderer   *renderer,
				   PangoRenderPart  part G_GNUC_UNUSED,
				   double           y1,
				   double           x11,
				   double           x21,
				   double           y2,
				   double           x12,
				   double           x22)
{
  Position pos;
  Position t;
  Position b;
  gboolean done = FALSE;

  if (y1 == y2)
    return;

  pos.y = t.y = y1;
  pos.x1 = t.x1 = x11;
  pos.x2 = t.x2 = x21;
  b.y = y2;
  b.x1 = x12;
  b.x2 = x22;

  while (!done)
    {
      Position pos_next;
      double y_next, x1_next, x2_next;
      double ix1, ix2;

      /* The algorithm here is written to emphasize simplicity and
       * numerical stability as opposed to speed.
       *
       * While the end result is slicing up the polygon vertically,
       * conceptually we aren't walking in the X direction, rather we
       * are walking along the edges. When we compute crossing of
       * horizontal pixel boundaries, we use the X coordinate as the
       * interpolating variable, when we compute crossing for vertical
       * pixel boundaries, we use the Y coordinate.
       *
       * This allows us to handle almost exactly horizontal edges without
       * running into difficulties. (Almost exactly horizontal edges
       * come up frequently due to inexactness in computing, say,
       * a 90 degree rotation transformation)
       */

      pos_next = b;
      done = TRUE;

      /* Check for crossing vertical pixel boundaries */
      y_next = floor (pos.y) + 1;
      if (y_next < pos_next.y)
	{
	  interpolate_position (&pos_next, &t, &b,
				y_next, t.y, b.y);
	  pos_next.y = y_next;
	  done = FALSE;
	}

      /* Check left side for crossing horizontal pixel boundaries */
      ix1 = floor (pos.x1);

      if (b.x1 < t.x1)
	{
	  if (ix1 == pos.x1)
	    x1_next = ix1 - 1;
	  else
	    x1_next = ix1;

	  if (x1_next > pos_next.x1)
	    {
	      interpolate_position (&pos_next, &t, &b,
				    x1_next, t.x1, b.x1);
	      pos_next.x1 = x1_next;
	      done = FALSE;
	    }
	}
      else if (b.x1 > t.x1)
	{
	  x1_next = ix1 + 1;

	  if (x1_next < pos_next.x1)
	    {
	      interpolate_position (&pos_next, &t, &b,
				    x1_next, t.x1, b.x1);
	      pos_next.x1 = x1_next;
	      done = FALSE;
	    }
	}

      /* Check right side for crossing horizontal pixel boundaries */
      ix2 = floor (pos.x2);

      if (b.x2 < t.x2)
	{
	  if (ix2 == pos.x2)
	    x2_next = ix2 - 1;
	  else
	    x2_next = ix2;

	  if (x2_next > pos_next.x2)
	    {
	      interpolate_position (&pos_next, &t, &b,
				    x2_next, t.x2, b.x2);
	      pos_next.x2 = x2_next;
	      done = FALSE;
	    }
	}
      else if (x22 > x21)
	{
	  x2_next = ix2 + 1;

	  if (x2_next < pos_next.x2)
	    {
	      interpolate_position (&pos_next, &t, &b,
				    x2_next, t.x2, b.x2);
	      pos_next.x2 = x2_next;
	      done = FALSE;
	    }
	}

      draw_simple_trap (renderer, &pos, &pos_next);
      pos = pos_next;
    }
}

/**
 * pango_ft2_render_layout_subpixel:
 * @bitmap: a FT_Bitmap to render the layout onto
 * @layout: a `PangoLayout`
 * @x: the X position of the left of the layout (in Pango units)
 * @y: the Y position of the top of the layout (in Pango units)
 *
 * Render a `PangoLayout` onto a FreeType2 bitmap, with he
 * location specified in fixed-point Pango units rather than
 * pixels.
 *
 * (Using this will avoid extra inaccuracies from rounding
 * to integer pixels multiple times, even if the final glyph
 * positions are integers.)
 *
 * Since: 1.6
 */
void
pango_ft2_render_layout_subpixel (FT_Bitmap   *bitmap,
				  PangoLayout *layout,
				  int          x,
				  int          y)
{
  PangoContext *context;
  PangoFontMap *fontmap;
  PangoRenderer *renderer;

  g_return_if_fail (bitmap != NULL);
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  context = pango_layout_get_context (layout);
  fontmap = pango_context_get_font_map (context);
  renderer = _pango_ft2_font_map_get_renderer (PANGO_FT2_FONT_MAP (fontmap));

  pango_ft2_renderer_set_bitmap (PANGO_FT2_RENDERER (renderer), bitmap);

  pango_renderer_draw_layout (renderer, layout, x, y);
}

/**
 * pango_ft2_render_layout:
 * @bitmap: a FT_Bitmap to render the layout onto
 * @layout: a `PangoLayout`
 * @x: the X position of the left of the layout (in pixels)
 * @y: the Y position of the top of the layout (in pixels)
 *
 * Render a `PangoLayout` onto a FreeType2 bitmap
 */
void
pango_ft2_render_layout (FT_Bitmap   *bitmap,
			 PangoLayout *layout,
			 int          x,
			 int          y)
{
  pango_ft2_render_layout_subpixel (bitmap, layout, x * PANGO_SCALE, y * PANGO_SCALE);
}

/**
 * pango_ft2_render_layout_line_subpixel:
 * @bitmap: a FT_Bitmap to render the line onto
 * @line: a `PangoLayoutLine`
 * @x: the x position of start of string (in Pango units)
 * @y: the y position of baseline (in Pango units)
 *
 * Render a `PangoLayoutLine` onto a FreeType2 bitmap, with he
 * location specified in fixed-point Pango units rather than
 * pixels.
 *
 * (Using this will avoid extra inaccuracies from rounding
 * to integer pixels multiple times, even if the final glyph
 * positions are integers.)
 *
 * Since: 1.6
 */
void
pango_ft2_render_layout_line_subpixel (FT_Bitmap       *bitmap,
				       PangoLayoutLine *line,
				       int              x,
				       int              y)
{
  PangoContext *context;
  PangoFontMap *fontmap;
  PangoRenderer *renderer;

  g_return_if_fail (bitmap != NULL);
  g_return_if_fail (line != NULL);

  context = pango_layout_get_context (line->layout);
  fontmap = pango_context_get_font_map (context);
  renderer = _pango_ft2_font_map_get_renderer (PANGO_FT2_FONT_MAP (fontmap));

  pango_ft2_renderer_set_bitmap (PANGO_FT2_RENDERER (renderer), bitmap);

  pango_renderer_draw_layout_line (renderer, line, x, y);
}

/**
 * pango_ft2_render_layout_line:
 * @bitmap: a FT_Bitmap to render the line onto
 * @line: a `PangoLayoutLine`
 * @x: the x position of start of string (in pixels)
 * @y: the y position of baseline (in pixels)
 *
 * Render a `PangoLayoutLine` onto a FreeType2 bitmap
 */
void
pango_ft2_render_layout_line (FT_Bitmap       *bitmap,
			      PangoLayoutLine *line,
			      int              x,
			      int              y)
{
  pango_ft2_render_layout_line_subpixel (bitmap, line, x * PANGO_SCALE, y * PANGO_SCALE);
}

/**
 * pango_ft2_render_transformed:
 * @bitmap: the FreeType2 bitmap onto which to draw the string
 * @font: the font in which to draw the string
 * @matrix: (nullable): a `PangoMatrix`
 * @glyphs: the glyph string to draw
 * @x: the x position of the start of the string (in Pango
 *   units in user space coordinates)
 * @y: the y position of the baseline (in Pango units
 *   in user space coordinates)
 *
 * Renders a `PangoGlyphString` onto a FreeType2 bitmap, possibly
 * transforming the layed-out coordinates through a transformation
 * matrix.
 *
 * Note that the transformation matrix for @font is not
 * changed, so to produce correct rendering results, the @font
 * must have been loaded using a `PangoContext` with an identical
 * transformation matrix to that passed in to this function.
 *
 * Since: 1.6
 */
void
pango_ft2_render_transformed (FT_Bitmap         *bitmap,
			      const PangoMatrix *matrix,
			      PangoFont         *font,
			      PangoGlyphString  *glyphs,
			      int                x,
			      int                y)
{
  PangoFontMap *fontmap;
  PangoRenderer *renderer;

  g_return_if_fail (bitmap != NULL);
  g_return_if_fail (glyphs != NULL);
  g_return_if_fail (PANGO_FT2_IS_FONT (font));

  fontmap = PANGO_FC_FONT (font)->fontmap;
  renderer = _pango_ft2_font_map_get_renderer (PANGO_FT2_FONT_MAP (fontmap));

  pango_ft2_renderer_set_bitmap (PANGO_FT2_RENDERER (renderer), bitmap);
  pango_renderer_set_matrix (renderer, matrix);

  pango_renderer_draw_glyphs (renderer, font, glyphs, x, y);
}

/**
 * pango_ft2_render:
 * @bitmap: the FreeType2 bitmap onto which to draw the string
 * @font: the font in which to draw the string
 * @glyphs: the glyph string to draw
 * @x: the x position of the start of the string (in pixels)
 * @y: the y position of the baseline (in pixels)
 *
 * Renders a `PangoGlyphString` onto a FreeType2 bitmap.
 */
void
pango_ft2_render (FT_Bitmap        *bitmap,
		  PangoFont        *font,
		  PangoGlyphString *glyphs,
		  int               x,
		  int               y)
{
  pango_ft2_render_transformed (bitmap, NULL, font, glyphs, x * PANGO_SCALE, y * PANGO_SCALE);
}

