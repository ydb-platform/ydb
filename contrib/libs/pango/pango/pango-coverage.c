/* Pango
 * pango-coverage.c: Coverage maps for fonts
 *
 * Copyright (C) 2000 Red Hat Software
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
#include <string.h>

#include "pango-coverage-private.h"

G_DEFINE_TYPE (PangoCoverage, pango_coverage, G_TYPE_OBJECT)

static void
pango_coverage_init (PangoCoverage *coverage)
{
}

static void
pango_coverage_finalize (GObject *object)
{
  PangoCoverage *coverage = PANGO_COVERAGE (object);

  if (coverage->chars)
    hb_set_destroy (coverage->chars);

  G_OBJECT_CLASS (pango_coverage_parent_class)->finalize (object);
}

static PangoCoverageLevel
pango_coverage_real_get (PangoCoverage *coverage,
		         int            index)
{
  if (coverage->chars == NULL)
    return PANGO_COVERAGE_NONE;

  if (hb_set_has (coverage->chars, (hb_codepoint_t)index))
    return PANGO_COVERAGE_EXACT;
  else
    return PANGO_COVERAGE_NONE;
}

static void
pango_coverage_real_set (PangoCoverage     *coverage,
		         int                index,
		         PangoCoverageLevel level)
{
  if (coverage->chars == NULL)
    coverage->chars = hb_set_create ();

  if (level != PANGO_COVERAGE_NONE)
    hb_set_add (coverage->chars, (hb_codepoint_t)index);
  else
    hb_set_del (coverage->chars, (hb_codepoint_t)index);
}

static PangoCoverage *
pango_coverage_real_copy (PangoCoverage *coverage)
{
  PangoCoverage *copy;

  g_return_val_if_fail (coverage != NULL, NULL);

  copy = g_object_new (PANGO_TYPE_COVERAGE, NULL);
  if (coverage->chars)
    {
      int i;

      copy->chars = hb_set_create ();
      for (i = hb_set_get_min (coverage->chars); i <= hb_set_get_max (coverage->chars); i++)
        {
          if (hb_set_has (coverage->chars, (hb_codepoint_t)i))
            hb_set_add (copy->chars, (hb_codepoint_t)i);
        }
    }

  return copy;
}

static void
pango_coverage_class_init (PangoCoverageClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);

  object_class->finalize = pango_coverage_finalize;

  class->get = pango_coverage_real_get;
  class->set = pango_coverage_real_set;
  class->copy = pango_coverage_real_copy;
}

/**
 * pango_coverage_new:
 *
 * Create a new `PangoCoverage`
 *
 * Return value: the newly allocated `PangoCoverage`, initialized
 *   to %PANGO_COVERAGE_NONE with a reference count of one, which
 *   should be freed with [method@Pango.Coverage.unref].
 */
PangoCoverage *
pango_coverage_new (void)
{
  return g_object_new (PANGO_TYPE_COVERAGE, NULL);
}

/**
 * pango_coverage_copy:
 * @coverage: a `PangoCoverage`
 *
 * Copy an existing `PangoCoverage`.
 *
 * Return value: (transfer full): the newly allocated `PangoCoverage`,
 *   with a reference count of one, which should be freed with
 *   [method@Pango.Coverage.unref].
 */
PangoCoverage *
pango_coverage_copy (PangoCoverage *coverage)
{
  return PANGO_COVERAGE_GET_CLASS (coverage)->copy (coverage);
}

/**
 * pango_coverage_ref:
 * @coverage: (not nullable): a `PangoCoverage`
 *
 * Increase the reference count on the `PangoCoverage` by one.
 *
 * Return value: (transfer full): @coverage
 *
 * Deprecated: 1.52: Use g_object_ref instead
 */
PangoCoverage *
pango_coverage_ref (PangoCoverage *coverage)
{
  return g_object_ref (coverage);
}

/**
 * pango_coverage_unref:
 * @coverage: (transfer full) (not nullable): a `PangoCoverage`
 *
 * Decrease the reference count on the `PangoCoverage` by one.
 *
 * If the result is zero, free the coverage and all associated memory.
 *
 * Deprecated: 1.52: Use g_object_unref instead
 */
void
pango_coverage_unref (PangoCoverage *coverage)
{
  g_object_unref (coverage);
}

/**
 * pango_coverage_get:
 * @coverage: a `PangoCoverage`
 * @index_: the index to check
 *
 * Determine whether a particular index is covered by @coverage.
 *
 * Return value: the coverage level of @coverage for character @index_.
 */
PangoCoverageLevel
pango_coverage_get (PangoCoverage *coverage,
		    int            index)
{
  return PANGO_COVERAGE_GET_CLASS (coverage)->get (coverage, index);
}

/**
 * pango_coverage_set:
 * @coverage: a `PangoCoverage`
 * @index_: the index to modify
 * @level: the new level for @index_
 *
 * Modify a particular index within @coverage
 */
void
pango_coverage_set (PangoCoverage     *coverage,
		    int                index,
		    PangoCoverageLevel level)
{
  PANGO_COVERAGE_GET_CLASS (coverage)->set (coverage, index, level);
}

/**
 * pango_coverage_max:
 * @coverage: a `PangoCoverage`
 * @other: another `PangoCoverage`
 *
 * Set the coverage for each index in @coverage to be the max (better)
 * value of the current coverage for the index and the coverage for
 * the corresponding index in @other.
 *
 * Deprecated: 1.44: This function does nothing
 */
void
pango_coverage_max (PangoCoverage *coverage,
		    PangoCoverage *other)
{
}

/**
 * pango_coverage_to_bytes:
 * @coverage: a `PangoCoverage`
 * @bytes: (out) (array length=n_bytes) (element-type guint8):
 *   location to store result (must be freed with g_free())
 * @n_bytes: (out): location to store size of result
 *
 * Convert a `PangoCoverage` structure into a flat binary format.
 *
 * Deprecated: 1.44: This returns %NULL
 */
void
pango_coverage_to_bytes (PangoCoverage  *coverage,
			 guchar        **bytes,
			 int            *n_bytes)
{
  *bytes = NULL;
  *n_bytes = 0;
}

/**
 * pango_coverage_from_bytes:
 * @bytes: (array length=n_bytes) (element-type guint8): binary data
 *   representing a `PangoCoverage`
 * @n_bytes: the size of @bytes in bytes
 *
 * Convert data generated from [method@Pango.Coverage.to_bytes]
 * back to a `PangoCoverage`.
 *
 * Return value: (transfer full) (nullable): a newly allocated `PangoCoverage`
 *
 * Deprecated: 1.44: This returns %NULL
 */
PangoCoverage *
pango_coverage_from_bytes (guchar *bytes,
			   int     n_bytes)
{
  return NULL;
}
