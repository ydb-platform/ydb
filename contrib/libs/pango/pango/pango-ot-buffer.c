/* Pango
 * pango-ot-buffer.c: Buffer of glyphs for shaping/positioning
 *
 * Copyright (C) 2004 Red Hat Software
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

#include "pango-ot-private.h"

static PangoOTBuffer *
pango_ot_buffer_copy (PangoOTBuffer *src)
{
  PangoOTBuffer *dst = g_slice_new (PangoOTBuffer);

  dst->buffer = hb_buffer_reference (src->buffer);

  return dst;
}

G_DEFINE_BOXED_TYPE (PangoOTBuffer, pango_ot_buffer,
                     pango_ot_buffer_copy,
                     pango_ot_buffer_destroy)

/**
 * pango_ot_buffer_new:
 * @font: a `PangoFcFont`
 *
 * Creates a new `PangoOTBuffer` for the given OpenType font.
 *
 * Return value: the newly allocated `PangoOTBuffer`, which should
 *   be freed with [method@PangoOT.Buffer.destroy].
 *
 * Since: 1.4
 */
PangoOTBuffer *
pango_ot_buffer_new (PangoFcFont *font)
{
  PangoOTBuffer *buffer = g_slice_new (PangoOTBuffer);

  buffer->buffer = hb_buffer_create ();

  return buffer;
}

/**
 * pango_ot_buffer_destroy:
 * @buffer: a `PangoOTBuffer`
 *
 * Destroys a `PangoOTBuffer` and free all associated memory.
 *
 * Since: 1.4
 */
void
pango_ot_buffer_destroy (PangoOTBuffer *buffer)
{
  hb_buffer_destroy (buffer->buffer);
  g_slice_free (PangoOTBuffer, buffer);
}

/**
 * pango_ot_buffer_clear:
 * @buffer: a `PangoOTBuffer`
 *
 * Empties a `PangoOTBuffer`, make it ready to add glyphs to.
 *
 * Since: 1.4
 */
void
pango_ot_buffer_clear (PangoOTBuffer *buffer)
{
  hb_buffer_reset (buffer->buffer);
}

/**
 * pango_ot_buffer_add_glyph:
 * @buffer: a `PangoOTBuffer`
 * @glyph: the glyph index to add, like a `PangoGlyph`
 * @properties: the glyph properties
 * @cluster: the cluster that this glyph belongs to
 *
 * Appends a glyph to a `PangoOTBuffer`, with @properties identifying which
 * features should be applied on this glyph.
 *
 * See [method@PangoOT.Ruleset.add_feature].
 *
 * Since: 1.4
 */
void
pango_ot_buffer_add_glyph (PangoOTBuffer *buffer,
			   guint          glyph,
			   guint          properties,
			   guint          cluster)
{
  hb_buffer_add (buffer->buffer, glyph, cluster);
}

/**
 * pango_ot_buffer_set_rtl:
 * @buffer: a `PangoOTBuffer`
 * @rtl: %TRUE for right-to-left text
 *
 * Sets whether glyphs will be rendered right-to-left.
 *
 * This setting is needed for proper horizontal positioning
 * of right-to-left scripts.
 *
 * Since: 1.4
 */
void
pango_ot_buffer_set_rtl (PangoOTBuffer *buffer,
			 gboolean       rtl)
{
  hb_buffer_set_direction (buffer->buffer, rtl ? HB_DIRECTION_RTL : HB_DIRECTION_LTR);
}

/**
 * pango_ot_buffer_set_zero_width_marks
 * @buffer: a `PangoOTBuffer`
 * @zero_width_marks: %TRUE if characters with a mark class should
 *   be forced to zero width
 *
 * Sets whether characters with a mark class should be forced to zero width.
 *
 * This setting is needed for proper positioning of Arabic accents,
 * but will produce incorrect results with standard OpenType Indic
 * fonts.
 *
 * Since: 1.6
 */
void
pango_ot_buffer_set_zero_width_marks (PangoOTBuffer *buffer,
				      gboolean       zero_width_marks)
{
}

/**
 * pango_ot_buffer_get_glyphs
 * @buffer: a `PangoOTBuffer`
 * @glyphs: (array length=n_glyphs) (out) (optional): location to
 *   store the array of glyphs
 * @n_glyphs: (out) (optional): location to store the number of glyphs
 *
 * Gets the glyph array contained in a `PangoOTBuffer`.
 *
 * The glyphs are owned by the buffer and should not be freed,
 * and are only valid as long as buffer is not modified.
 *
 * Since: 1.4
 */
void
pango_ot_buffer_get_glyphs (const PangoOTBuffer  *buffer,
			    PangoOTGlyph        **glyphs,
			    int                  *n_glyphs)
{
  if (glyphs)
    *glyphs = (PangoOTGlyph *) hb_buffer_get_glyph_infos (buffer->buffer, NULL);

  if (n_glyphs)
    *n_glyphs = hb_buffer_get_length (buffer->buffer);
}

/**
 * pango_ot_buffer_output
 * @buffer: a `PangoOTBuffer`
 * @glyphs: a `PangoGlyphString`
 *
 * Exports the glyphs in a `PangoOTBuffer` into a `PangoGlyphString`.
 *
 * This is typically used after the OpenType layout processing
 * is over, to convert the resulting glyphs into a generic Pango
 * glyph string.
 *
 * Since: 1.4
 */
void
pango_ot_buffer_output (const PangoOTBuffer *buffer,
			PangoGlyphString    *glyphs)
{
  unsigned int i;
  int last_cluster;

  unsigned int num_glyphs;
  hb_buffer_t *hb_buffer = buffer->buffer;
  hb_glyph_info_t *hb_glyph;
  hb_glyph_position_t *hb_position;

  if (HB_DIRECTION_IS_BACKWARD (hb_buffer_get_direction (buffer->buffer)))
    hb_buffer_reverse (buffer->buffer);

  /* Copy glyphs into output glyph string */
  num_glyphs = hb_buffer_get_length (hb_buffer);
  hb_glyph = hb_buffer_get_glyph_infos (hb_buffer, NULL);
  hb_position = hb_buffer_get_glyph_positions (hb_buffer, NULL);
  pango_glyph_string_set_size (glyphs, num_glyphs);
  last_cluster = -1;
  for (i = 0; i < num_glyphs; i++)
    {
      glyphs->glyphs[i].glyph = hb_glyph->codepoint;
      glyphs->log_clusters[i] = hb_glyph->cluster;
      glyphs->glyphs[i].attr.is_cluster_start = glyphs->log_clusters[i] != last_cluster;
      last_cluster = glyphs->log_clusters[i];

      glyphs->glyphs[i].geometry.width = hb_position->x_advance;
      glyphs->glyphs[i].geometry.x_offset = hb_position->x_offset;
      glyphs->glyphs[i].geometry.y_offset = hb_position->y_offset;

      hb_glyph++;
      hb_position++;
    }

  if (HB_DIRECTION_IS_BACKWARD (hb_buffer_get_direction (buffer->buffer)))
    hb_buffer_reverse (buffer->buffer);
}
