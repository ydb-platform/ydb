/* Pango
 * pango-coverage-private.h: Coverage sets for fonts
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

#ifndef __PANGO_COVERAGE_PRIVATE_H__
#define __PANGO_COVERAGE_PRIVATE_H__

#include <glib-object.h>
#include <pango-coverage.h>

G_BEGIN_DECLS

#define PANGO_TYPE_COVERAGE              (pango_coverage_get_type ())
#define PANGO_COVERAGE(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_TYPE_COVERAGE, PangoCoverage))
#define PANGO_IS_COVERAGE(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_TYPE_COVERAGE))
#define PANGO_COVERAGE_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_COVERAGE, PangoCoverageClass))
#define PANGO_IS_COVERAGE_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_COVERAGE))
#define PANGO_COVERAGE_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_COVERAGE, PangoCoverageClass))

typedef struct _PangoCoverageClass   PangoCoverageClass;
typedef struct _PangoCoveragePrivate PangoCoveragePrivate;

struct _PangoCoverage
{
  GObject parent_instance;

  hb_set_t *chars;
};

struct _PangoCoverageClass
{
  GObjectClass parent_class;

  PangoCoverageLevel (* get)  (PangoCoverage      *coverage,
                               int                 index);
  void               (* set)  (PangoCoverage      *coverage,
                               int                 index,
                               PangoCoverageLevel  level);
  PangoCoverage *    (* copy) (PangoCoverage      *coverage);
};

G_END_DECLS

#endif /* __PANGO_COVERAGE_PRIVATE_H__ */
