/* Pango
 * pango-ot-private.h: Implementation details for Pango OpenType code
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

#ifndef __PANGO_OT_PRIVATE_H__
#define __PANGO_OT_PRIVATE_H__

#include <glib.h>
#include <glib-object.h>

#include <pango/pango-ot.h>
#include <hb-ot.h>
#include <hb-ft.h>

#include "pangofc-private.h"

G_BEGIN_DECLS

typedef struct _PangoOTInfoClass PangoOTInfoClass;

/**
 * PangoOTInfo:
 *
 * The `PangoOTInfo` struct contains the various
 * tables associated with an OpenType font. It contains only private fields and
 * should only be accessed via the pango_ot_info_… functions
 * which are documented below. To obtain a `PangoOTInfo`,
 * use pango_ot_info_get().
 */
struct _PangoOTInfo
{
  GObject parent_instance;

  FT_Face face;
  hb_face_t *hb_face;
};

struct _PangoOTInfoClass
{
  GObjectClass parent_class;
};


typedef struct _PangoOTRulesetClass PangoOTRulesetClass;

struct _PangoOTRuleset
{
  GObject parent_instance;
};

struct _PangoOTRulesetClass
{
  GObjectClass parent_class;
};

/**
 * PangoOTBuffer:
 *
 * The `PangoOTBuffer` structure is used to store strings of glyphs associated
 * with a `PangoFcFont`, suitable for OpenType layout processing.  It contains
 * only private fields and should only be accessed via the
 * pango_ot_buffer_… functions which are documented below.
 * To obtain a `PangoOTBuffer`, use pango_ot_buffer_new().
 */
struct _PangoOTBuffer
{
  hb_buffer_t *buffer;
};

G_END_DECLS

#endif /* __PANGO_OT_PRIVATE_H__ */
