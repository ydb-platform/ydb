/* Pango
 * pango-attributes-private.h: Internal structures of PangoLayout
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

#ifndef __PANGO_ATTRIBUTES_PRIVATE_H__
#define __PANGO_ATTRIBUTES_PRIVATE_H__

struct _PangoAttrIterator
{
  GPtrArray *attrs; /* From the list */
  guint n_attrs; /* Copied from the list */

  GPtrArray *attribute_stack;

  guint attr_index;
  guint start_index;
  guint end_index;
};

struct _PangoAttrList
{
  guint ref_count;
  GPtrArray *attributes;
};

void     _pango_attr_list_init         (PangoAttrList     *list);
void     _pango_attr_list_destroy      (PangoAttrList     *list);
gboolean _pango_attr_list_has_attributes (const PangoAttrList *list);

void     _pango_attr_list_get_iterator (PangoAttrList     *list,
                                        PangoAttrIterator *iterator);

void     _pango_attr_iterator_destroy  (PangoAttrIterator *iterator);
gboolean  pango_attr_iterator_advance  (PangoAttrIterator *iterator,
                                        int                index);


#endif
