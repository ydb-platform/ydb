/* Pango
 * pango-attributes.c: Attributed text
 *
 * Copyright (C) 2000-2002 Red Hat Software
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#include "config.h"
#include <string.h>

#include "pango-attributes.h"
#include "pango-attributes-private.h"
#include "pango-impl-utils.h"


/* {{{ Generic attribute code */

G_LOCK_DEFINE_STATIC (attr_type);
static GHashTable *name_map = NULL; /* MT-safe */

/**
 * pango_attr_type_register:
 * @name: an identifier for the type
 *
 * Allocate a new attribute type ID.
 *
 * The attribute type name can be accessed later
 * by using [func@Pango.AttrType.get_name].
 *
 * Return value: the new type ID.
 */
PangoAttrType
pango_attr_type_register (const gchar *name)
{
  static guint current_type = 0x1000000; /* MT-safe */
  guint type;

  G_LOCK (attr_type);

  type = current_type++;

  if (name)
    {
      if (G_UNLIKELY (!name_map))
        name_map = g_hash_table_new (NULL, NULL);

      g_hash_table_insert (name_map, GUINT_TO_POINTER (type), (gpointer) g_intern_string (name));
    }

  G_UNLOCK (attr_type);

  return type;
}

/**
 * pango_attr_type_get_name:
 * @type: an attribute type ID to fetch the name for
 *
 * Fetches the attribute type name.
 *
 * The attribute type name is the string passed in
 * when registering the type using
 * [func@Pango.AttrType.register].
 *
 * The returned value is an interned string (see
 * g_intern_string() for what that means) that should
 * not be modified or freed.
 *
 * Return value: (nullable): the type ID name (which
 *   may be %NULL), or %NULL if @type is a built-in Pango
 *   attribute type or invalid.
 *
 * Since: 1.22
 */
const char *
pango_attr_type_get_name (PangoAttrType type)
{
  const char *result = NULL;

  G_LOCK (attr_type);

  if (name_map)
    result = g_hash_table_lookup (name_map, GUINT_TO_POINTER ((guint) type));

  G_UNLOCK (attr_type);

  return result;
}

/**
 * pango_attribute_init:
 * @attr: a `PangoAttribute`
 * @klass: a `PangoAttrClass`
 *
 * Initializes @attr's klass to @klass, it's start_index to
 * %PANGO_ATTR_INDEX_FROM_TEXT_BEGINNING and end_index to
 * %PANGO_ATTR_INDEX_TO_TEXT_END such that the attribute applies
 * to the entire text by default.
 *
 * Since: 1.20
 */
void
pango_attribute_init (PangoAttribute       *attr,
                      const PangoAttrClass *klass)
{
  g_return_if_fail (attr != NULL);
  g_return_if_fail (klass != NULL);

  attr->klass = klass;
  attr->start_index = PANGO_ATTR_INDEX_FROM_TEXT_BEGINNING;
  attr->end_index   = PANGO_ATTR_INDEX_TO_TEXT_END;
}

/**
 * pango_attribute_copy:
 * @attr: a `PangoAttribute`
 *
 * Make a copy of an attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy].
 */
PangoAttribute *
pango_attribute_copy (const PangoAttribute *attr)
{
  PangoAttribute *result;

  g_return_val_if_fail (attr != NULL, NULL);

  result = attr->klass->copy (attr);
  result->start_index = attr->start_index;
  result->end_index = attr->end_index;

  return result;
}

/**
 * pango_attribute_destroy:
 * @attr: a `PangoAttribute`.
 *
 * Destroy a `PangoAttribute` and free all associated memory.
 */
void
pango_attribute_destroy (PangoAttribute *attr)
{
  g_return_if_fail (attr != NULL);

  attr->klass->destroy (attr);
}

G_DEFINE_BOXED_TYPE (PangoAttribute, pango_attribute,
                     pango_attribute_copy,
                     pango_attribute_destroy);

/**
 * pango_attribute_equal:
 * @attr1: a `PangoAttribute`
 * @attr2: another `PangoAttribute`
 *
 * Compare two attributes for equality.
 *
 * This compares only the actual value of the two
 * attributes and not the ranges that the attributes
 * apply to.
 *
 * Return value: %TRUE if the two attributes have the same value
 */
gboolean
pango_attribute_equal (const PangoAttribute *attr1,
                       const PangoAttribute *attr2)
{
  g_return_val_if_fail (attr1 != NULL, FALSE);
  g_return_val_if_fail (attr2 != NULL, FALSE);

  if (attr1->klass->type != attr2->klass->type)
    return FALSE;

  return attr1->klass->equal (attr1, attr2);
}

/* }}} */
/* {{{ Attribute types */
/* {{{ String attribute */
static PangoAttribute *pango_attr_string_new (const PangoAttrClass *klass,
                                              const char           *str);

static PangoAttribute *
pango_attr_string_copy (const PangoAttribute *attr)
{
  return pango_attr_string_new (attr->klass, ((PangoAttrString *)attr)->value);
}

static void
pango_attr_string_destroy (PangoAttribute *attr)
{
  PangoAttrString *sattr = (PangoAttrString *)attr;

  g_free (sattr->value);
  g_slice_free (PangoAttrString, sattr);
}

static gboolean
pango_attr_string_equal (const PangoAttribute *attr1,
                         const PangoAttribute *attr2)
{
  return strcmp (((PangoAttrString *)attr1)->value, ((PangoAttrString *)attr2)->value) == 0;
}

static PangoAttribute *
pango_attr_string_new (const PangoAttrClass *klass,
                       const char           *str)
{
  PangoAttrString *result = g_slice_new (PangoAttrString);
  pango_attribute_init (&result->attr, klass);
  result->value = g_strdup (str);

  return (PangoAttribute *)result;
}
 /* }}} */
/* {{{ Language attribute */
static PangoAttribute *
pango_attr_language_copy (const PangoAttribute *attr)
{
  return pango_attr_language_new (((PangoAttrLanguage *)attr)->value);
}

static void
pango_attr_language_destroy (PangoAttribute *attr)
{
  PangoAttrLanguage *lattr = (PangoAttrLanguage *)attr;

  g_slice_free (PangoAttrLanguage, lattr);
}

static gboolean
pango_attr_language_equal (const PangoAttribute *attr1,
                           const PangoAttribute *attr2)
{
  return ((PangoAttrLanguage *)attr1)->value == ((PangoAttrLanguage *)attr2)->value;
}
/* }}}} */
/* {{{ Color attribute */
static PangoAttribute *pango_attr_color_new (const PangoAttrClass *klass,
                                             guint16               red,
                                             guint16               green,
                                             guint16               blue);

static PangoAttribute *
pango_attr_color_copy (const PangoAttribute *attr)
{
  const PangoAttrColor *color_attr = (PangoAttrColor *)attr;

  return pango_attr_color_new (attr->klass,
                               color_attr->color.red,
                               color_attr->color.green,
                               color_attr->color.blue);
}

static void
pango_attr_color_destroy (PangoAttribute *attr)
{
  PangoAttrColor *cattr = (PangoAttrColor *)attr;

  g_slice_free (PangoAttrColor, cattr);
}

static gboolean
pango_attr_color_equal (const PangoAttribute *attr1,
                        const PangoAttribute *attr2)
{
  const PangoAttrColor *color_attr1 = (const PangoAttrColor *)attr1;
  const PangoAttrColor *color_attr2 = (const PangoAttrColor *)attr2;

  return (color_attr1->color.red == color_attr2->color.red &&
          color_attr1->color.blue == color_attr2->color.blue &&
          color_attr1->color.green == color_attr2->color.green);
}

static PangoAttribute *
pango_attr_color_new (const PangoAttrClass *klass,
                      guint16               red,
                      guint16               green,
                      guint16               blue)
{
  PangoAttrColor *result = g_slice_new (PangoAttrColor);
  pango_attribute_init (&result->attr, klass);
  result->color.red = red;
  result->color.green = green;
  result->color.blue = blue;

  return (PangoAttribute *)result;
}
/* }}}} */
/* {{{ Integer attribute */
static PangoAttribute *pango_attr_int_new (const PangoAttrClass *klass,
                                           int                   value);

static PangoAttribute *
pango_attr_int_copy (const PangoAttribute *attr)
{
  const PangoAttrInt *int_attr = (PangoAttrInt *)attr;

  return pango_attr_int_new (attr->klass, int_attr->value);
}

static void
pango_attr_int_destroy (PangoAttribute *attr)
{
  PangoAttrInt *iattr = (PangoAttrInt *)attr;

  g_slice_free (PangoAttrInt, iattr);
}

static gboolean
pango_attr_int_equal (const PangoAttribute *attr1,
                      const PangoAttribute *attr2)
{
  const PangoAttrInt *int_attr1 = (const PangoAttrInt *)attr1;
  const PangoAttrInt *int_attr2 = (const PangoAttrInt *)attr2;

  return (int_attr1->value == int_attr2->value);
}

static PangoAttribute *
pango_attr_int_new (const PangoAttrClass *klass,
                    int                   value)
{
  PangoAttrInt *result = g_slice_new (PangoAttrInt);
  pango_attribute_init (&result->attr, klass);
  result->value = value;

  return (PangoAttribute *)result;
}
/* }}} */
/* {{{ Float attribute */
static PangoAttribute *pango_attr_float_new (const PangoAttrClass *klass,
                                             double                value);

static PangoAttribute *
pango_attr_float_copy (const PangoAttribute *attr)
{
  const PangoAttrFloat *float_attr = (PangoAttrFloat *)attr;

  return pango_attr_float_new (attr->klass, float_attr->value);
}

static void
pango_attr_float_destroy (PangoAttribute *attr)
{
  PangoAttrFloat *fattr = (PangoAttrFloat *)attr;

  g_slice_free (PangoAttrFloat, fattr);
}

static gboolean
pango_attr_float_equal (const PangoAttribute *attr1,
                        const PangoAttribute *attr2)
{
  const PangoAttrFloat *float_attr1 = (const PangoAttrFloat *)attr1;
  const PangoAttrFloat *float_attr2 = (const PangoAttrFloat *)attr2;

  return (float_attr1->value == float_attr2->value);
}

static PangoAttribute *
pango_attr_float_new  (const PangoAttrClass *klass,
                       double                value)
{
  PangoAttrFloat *result = g_slice_new (PangoAttrFloat);
  pango_attribute_init (&result->attr, klass);
  result->value = value;

  return (PangoAttribute *)result;
}
/* }}} */
/* {{{ Size attribute */
static PangoAttribute *pango_attr_size_new_internal (int      size,
                                                     gboolean absolute);

static PangoAttribute *
pango_attr_size_copy (const PangoAttribute *attr)
{
  const PangoAttrSize *size_attr = (PangoAttrSize *)attr;

  if (attr->klass->type == PANGO_ATTR_ABSOLUTE_SIZE)
    return pango_attr_size_new_absolute (size_attr->size);
  else
    return pango_attr_size_new (size_attr->size);
}

static void
pango_attr_size_destroy (PangoAttribute *attr)
{
  PangoAttrSize *sattr = (PangoAttrSize *)attr;

  g_slice_free (PangoAttrSize, sattr);
}

static gboolean
pango_attr_size_equal (const PangoAttribute *attr1,
                       const PangoAttribute *attr2)
{
  const PangoAttrSize *size_attr1 = (const PangoAttrSize *)attr1;
  const PangoAttrSize *size_attr2 = (const PangoAttrSize *)attr2;

  return size_attr1->size == size_attr2->size;
}

static PangoAttribute *
pango_attr_size_new_internal (int size,
                              gboolean absolute)
{
  PangoAttrSize *result;

  static const PangoAttrClass klass = {
    PANGO_ATTR_SIZE,
    pango_attr_size_copy,
    pango_attr_size_destroy,
    pango_attr_size_equal
  };
  static const PangoAttrClass absolute_klass = {
    PANGO_ATTR_ABSOLUTE_SIZE,
    pango_attr_size_copy,
    pango_attr_size_destroy,
    pango_attr_size_equal
  };

  result = g_slice_new (PangoAttrSize);
  pango_attribute_init (&result->attr, absolute ? &absolute_klass : &klass);
  result->size = size;
  result->absolute = absolute;

  return (PangoAttribute *)result;
}
/* }}} */
/* {{{ Font description attribute */
static PangoAttribute *
pango_attr_font_desc_copy (const PangoAttribute *attr)
{
  const PangoAttrFontDesc *desc_attr = (const PangoAttrFontDesc *)attr;

  return pango_attr_font_desc_new (desc_attr->desc);
}

static void
pango_attr_font_desc_destroy (PangoAttribute *attr)
{
  PangoAttrFontDesc *desc_attr = (PangoAttrFontDesc *)attr;

  pango_font_description_free (desc_attr->desc);
  g_slice_free (PangoAttrFontDesc, desc_attr);
}

static gboolean
pango_attr_font_desc_equal (const PangoAttribute *attr1,
                            const PangoAttribute *attr2)
{
  const PangoAttrFontDesc *desc_attr1 = (const PangoAttrFontDesc *)attr1;
  const PangoAttrFontDesc *desc_attr2 = (const PangoAttrFontDesc *)attr2;

  return pango_font_description_get_set_fields (desc_attr1->desc) ==
         pango_font_description_get_set_fields (desc_attr2->desc) &&
         pango_font_description_equal (desc_attr1->desc, desc_attr2->desc);
}
/* }}} */
/* {{{ Shape attribute */
static PangoAttribute *
pango_attr_shape_copy (const PangoAttribute *attr)
{
  const PangoAttrShape *shape_attr = (PangoAttrShape *)attr;
  gpointer data;

  if (shape_attr->copy_func)
    data = shape_attr->copy_func (shape_attr->data);
  else
    data = shape_attr->data;

  return pango_attr_shape_new_with_data (&shape_attr->ink_rect, &shape_attr->logical_rect,
                                         data, shape_attr->copy_func, shape_attr->destroy_func);
}

static void
pango_attr_shape_destroy (PangoAttribute *attr)
{
  PangoAttrShape *shape_attr = (PangoAttrShape *)attr;

  if (shape_attr->destroy_func)
    shape_attr->destroy_func (shape_attr->data);

  g_slice_free (PangoAttrShape, shape_attr);
}

static gboolean
pango_attr_shape_equal (const PangoAttribute *attr1,
                        const PangoAttribute *attr2)
{
  const PangoAttrShape *shape_attr1 = (const PangoAttrShape *)attr1;
  const PangoAttrShape *shape_attr2 = (const PangoAttrShape *)attr2;

  return (shape_attr1->logical_rect.x == shape_attr2->logical_rect.x &&
          shape_attr1->logical_rect.y == shape_attr2->logical_rect.y &&
          shape_attr1->logical_rect.width == shape_attr2->logical_rect.width &&
          shape_attr1->logical_rect.height == shape_attr2->logical_rect.height &&
          shape_attr1->ink_rect.x == shape_attr2->ink_rect.x &&
          shape_attr1->ink_rect.y == shape_attr2->ink_rect.y &&
          shape_attr1->ink_rect.width == shape_attr2->ink_rect.width &&
          shape_attr1->ink_rect.height == shape_attr2->ink_rect.height &&
          shape_attr1->data == shape_attr2->data);
}
/* }}} */
/* }}} */
/* {{{ Public API */

/**
 * pango_attr_family_new:
 * @family: the family or comma-separated list of families
 *
 * Create a new font family attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_family_new (const char *family)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FAMILY,
    pango_attr_string_copy,
    pango_attr_string_destroy,
    pango_attr_string_equal
  };

  g_return_val_if_fail (family != NULL, NULL);

  return pango_attr_string_new (&klass, family);
}

/**
 * pango_attr_language_new:
 * @language: language tag
 *
 * Create a new language tag attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_language_new (PangoLanguage *language)
{
  PangoAttrLanguage *result;

  static const PangoAttrClass klass = {
    PANGO_ATTR_LANGUAGE,
    pango_attr_language_copy,
    pango_attr_language_destroy,
    pango_attr_language_equal
  };

  result = g_slice_new (PangoAttrLanguage);
  pango_attribute_init (&result->attr, &klass);
  result->value = language;

  return (PangoAttribute *)result;
}

/**
 * pango_attr_foreground_new:
 * @red: the red value (ranging from 0 to 65535)
 * @green: the green value
 * @blue: the blue value
 *
 * Create a new foreground color attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_foreground_new (guint16 red,
                           guint16 green,
                           guint16 blue)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FOREGROUND,
    pango_attr_color_copy,
    pango_attr_color_destroy,
    pango_attr_color_equal
  };

  return pango_attr_color_new (&klass, red, green, blue);
}

/**
 * pango_attr_background_new:
 * @red: the red value (ranging from 0 to 65535)
 * @green: the green value
 * @blue: the blue value
 *
 * Create a new background color attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_background_new (guint16 red,
                           guint16 green,
                           guint16 blue)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_BACKGROUND,
    pango_attr_color_copy,
    pango_attr_color_destroy,
    pango_attr_color_equal
  };

  return pango_attr_color_new (&klass, red, green, blue);
}

/**
 * pango_attr_size_new:
 * @size: the font size, in %PANGO_SCALE-ths of a point
 *
 * Create a new font-size attribute in fractional points.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_size_new (int size)
{
  return pango_attr_size_new_internal (size, FALSE);
}

/**
 * pango_attr_size_new_absolute:
 * @size: the font size, in %PANGO_SCALE-ths of a device unit
 *
 * Create a new font-size attribute in device units.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.8
 */
PangoAttribute *
pango_attr_size_new_absolute (int size)
{
  return pango_attr_size_new_internal (size, TRUE);
}

/**
 * pango_attr_style_new:
 * @style: the slant style
 *
 * Create a new font slant style attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_style_new (PangoStyle style)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_STYLE,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)style);
}

/**
 * pango_attr_weight_new:
 * @weight: the weight
 *
 * Create a new font weight attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_weight_new (PangoWeight weight)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_WEIGHT,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)weight);
}

/**
 * pango_attr_variant_new:
 * @variant: the variant
 *
 * Create a new font variant attribute (normal or small caps).
 *
 * Return value: (transfer full): the newly allocated `PangoAttribute`,
 *   which should be freed with [method@Pango.Attribute.destroy].
 */
PangoAttribute *
pango_attr_variant_new (PangoVariant variant)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_VARIANT,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)variant);
}

/**
 * pango_attr_stretch_new:
 * @stretch: the stretch
 *
 * Create a new font stretch attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_stretch_new (PangoStretch  stretch)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_STRETCH,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)stretch);
}

/**
 * pango_attr_font_desc_new:
 * @desc: the font description
 *
 * Create a new font description attribute.
 *
 * This attribute allows setting family, style, weight, variant,
 * stretch, and size simultaneously.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_font_desc_new (const PangoFontDescription *desc)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FONT_DESC,
    pango_attr_font_desc_copy,
    pango_attr_font_desc_destroy,
    pango_attr_font_desc_equal
  };

  PangoAttrFontDesc *result = g_slice_new (PangoAttrFontDesc);
  pango_attribute_init (&result->attr, &klass);
  result->desc = pango_font_description_copy (desc);

  return (PangoAttribute *)result;
}

/**
 * pango_attr_underline_new:
 * @underline: the underline style
 *
 * Create a new underline-style attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_underline_new (PangoUnderline underline)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_UNDERLINE,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)underline);
}

/**
 * pango_attr_underline_color_new:
 * @red: the red value (ranging from 0 to 65535)
 * @green: the green value
 * @blue: the blue value
 *
 * Create a new underline color attribute.
 *
 * This attribute modifies the color of underlines.
 * If not set, underlines will use the foreground color.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.8
 */
PangoAttribute *
pango_attr_underline_color_new (guint16 red,
                                guint16 green,
                                guint16 blue)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_UNDERLINE_COLOR,
    pango_attr_color_copy,
    pango_attr_color_destroy,
    pango_attr_color_equal
  };

  return pango_attr_color_new (&klass, red, green, blue);
}

/**
 * pango_attr_strikethrough_new:
 * @strikethrough: %TRUE if the text should be struck-through
 *
 * Create a new strike-through attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_strikethrough_new (gboolean strikethrough)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_STRIKETHROUGH,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)strikethrough);
}

/**
 * pango_attr_strikethrough_color_new:
 * @red: the red value (ranging from 0 to 65535)
 * @green: the green value
 * @blue: the blue value
 *
 * Create a new strikethrough color attribute.
 *
 * This attribute modifies the color of strikethrough lines.
 * If not set, strikethrough lines will use the foreground color.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.8
 */
PangoAttribute *
pango_attr_strikethrough_color_new (guint16 red,
                                    guint16 green,
                                    guint16 blue)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_STRIKETHROUGH_COLOR,
    pango_attr_color_copy,
    pango_attr_color_destroy,
    pango_attr_color_equal
  };

  return pango_attr_color_new (&klass, red, green, blue);
}

/**
 * pango_attr_rise_new:
 * @rise: the amount that the text should be displaced vertically,
 *   in Pango units. Positive values displace the text upwards.
 *
 * Create a new baseline displacement attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_rise_new (int rise)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_RISE,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)rise);
}

/**
 * pango_attr_baseline_shift_new:
 * @shift: either a `PangoBaselineShift` enumeration value or an absolute value (> 1024)
 *   in Pango units, relative to the baseline of the previous run.
 *   Positive values displace the text upwards.
 *
 * Create a new baseline displacement attribute.
 *
 * The effect of this attribute is to shift the baseline of a run,
 * relative to the run of preceding run.
 *
 * <picture>
 *   <source srcset="baseline-shift-dark.png" media="(prefers-color-scheme: dark)">
 *   <img alt="Baseline Shift" src="baseline-shift-light.png">
 * </picture>

 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_baseline_shift_new (int rise)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_BASELINE_SHIFT,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)rise);
}

/**
 * pango_attr_font_scale_new:
 * @scale: a `PangoFontScale` value, which indicates font size change relative
 *   to the size of the previous run.
 *
 *
 * Create a new font scale attribute.
 *
 * The effect of this attribute is to change the font size of a run,
 * relative to the size of preceding run.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_font_scale_new (PangoFontScale scale)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FONT_SCALE,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)scale);
}

/**
 * pango_attr_scale_new:
 * @scale_factor: factor to scale the font
 *
 * Create a new font size scale attribute.
 *
 * The base font for the affected text will have
 * its size multiplied by @scale_factor.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute*
pango_attr_scale_new (double scale_factor)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_SCALE,
    pango_attr_float_copy,
    pango_attr_float_destroy,
    pango_attr_float_equal
  };

  return pango_attr_float_new (&klass, scale_factor);
}

/**
 * pango_attr_fallback_new:
 * @enable_fallback: %TRUE if we should fall back on other fonts
 *   for characters the active font is missing
 *
 * Create a new font fallback attribute.
 *
 * If fallback is disabled, characters will only be
 * used from the closest matching font on the system.
 * No fallback will be done to other fonts on the system
 * that might contain the characters in the text.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.4
 */
PangoAttribute *
pango_attr_fallback_new (gboolean enable_fallback)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FALLBACK,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal,
  };

  return pango_attr_int_new (&klass, (int)enable_fallback);
}

/**
 * pango_attr_letter_spacing_new:
 * @letter_spacing: amount of extra space to add between
 *   graphemes of the text, in Pango units
 *
 * Create a new letter-spacing attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.6
 */
PangoAttribute *
pango_attr_letter_spacing_new (int letter_spacing)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_LETTER_SPACING,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, letter_spacing);
}

/**
 * pango_attr_shape_new_with_data:
 * @ink_rect: ink rectangle to assign to each character
 * @logical_rect: logical rectangle to assign to each character
 * @data: user data pointer
 * @copy_func: (nullable): function to copy @data when the
 *   attribute is copied. If %NULL, @data is simply copied
 *   as a pointer
 * @destroy_func: (nullable): function to free @data when the
 *   attribute is freed
 *
 * Creates a new shape attribute.
 *
 * Like [func@Pango.AttrShape.new], but a user data pointer
 * is also provided; this pointer can be accessed when later
 * rendering the glyph.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.8
 */
PangoAttribute *
pango_attr_shape_new_with_data (const PangoRectangle  *ink_rect,
                                const PangoRectangle  *logical_rect,
                                gpointer               data,
                                PangoAttrDataCopyFunc  copy_func,
                                GDestroyNotify         destroy_func)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_SHAPE,
    pango_attr_shape_copy,
    pango_attr_shape_destroy,
    pango_attr_shape_equal
  };

  PangoAttrShape *result;

  g_return_val_if_fail (ink_rect != NULL, NULL);
  g_return_val_if_fail (logical_rect != NULL, NULL);

  result = g_slice_new (PangoAttrShape);
  pango_attribute_init (&result->attr, &klass);
  result->ink_rect = *ink_rect;
  result->logical_rect = *logical_rect;
  result->data = data;
  result->copy_func = copy_func;
  result->destroy_func =  destroy_func;

  return (PangoAttribute *)result;
}

/**
 * pango_attr_shape_new:
 * @ink_rect: ink rectangle to assign to each character
 * @logical_rect: logical rectangle to assign to each character
 *
 * Create a new shape attribute.
 *
 * A shape is used to impose a particular ink and logical
 * rectangle on the result of shaping a particular glyph.
 * This might be used, for instance, for embedding a picture
 * or a widget inside a `PangoLayout`.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 */
PangoAttribute *
pango_attr_shape_new (const PangoRectangle *ink_rect,
                      const PangoRectangle *logical_rect)
{
  g_return_val_if_fail (ink_rect != NULL, NULL);
  g_return_val_if_fail (logical_rect != NULL, NULL);

  return pango_attr_shape_new_with_data (ink_rect, logical_rect,
                                         NULL, NULL, NULL);
}

/**
 * pango_attr_gravity_new:
 * @gravity: the gravity value; should not be %PANGO_GRAVITY_AUTO
 *
 * Create a new gravity attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.16
 */
PangoAttribute *
pango_attr_gravity_new (PangoGravity gravity)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_GRAVITY,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  g_return_val_if_fail (gravity != PANGO_GRAVITY_AUTO, NULL);

  return pango_attr_int_new (&klass, (int)gravity);
}

/**
 * pango_attr_gravity_hint_new:
 * @hint: the gravity hint value
 *
 * Create a new gravity hint attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.16
 */
PangoAttribute *
pango_attr_gravity_hint_new (PangoGravityHint hint)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_GRAVITY_HINT,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)hint);
}

/**
 * pango_attr_font_features_new:
 * @features: a string with OpenType font features, with the syntax of the [CSS
 * font-feature-settings property](https://www.w3.org/TR/css-fonts-4/#font-rend-desc)
 *
 * Create a new font features tag attribute.
 *
 * You can use this attribute to select OpenType font features like small-caps,
 * alternative glyphs, ligatures, etc. for fonts that support them.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.38
 */
PangoAttribute *
pango_attr_font_features_new (const gchar *features)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FONT_FEATURES,
    pango_attr_string_copy,
    pango_attr_string_destroy,
    pango_attr_string_equal
  };

  g_return_val_if_fail (features != NULL, NULL);

  return pango_attr_string_new (&klass, features);
}

/**
 * pango_attr_foreground_alpha_new:
 * @alpha: the alpha value, between 1 and 65536
 *
 * Create a new foreground alpha attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.38
 */
PangoAttribute *
pango_attr_foreground_alpha_new (guint16 alpha)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_FOREGROUND_ALPHA,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)alpha);
}

/**
 * pango_attr_background_alpha_new:
 * @alpha: the alpha value, between 1 and 65536
 *
 * Create a new background alpha attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.38
 */
PangoAttribute *
pango_attr_background_alpha_new (guint16 alpha)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_BACKGROUND_ALPHA,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)alpha);
}

/**
 * pango_attr_allow_breaks_new:
 * @allow_breaks: %TRUE if we line breaks are allowed
 *
 * Create a new allow-breaks attribute.
 *
 * If breaks are disabled, the range will be kept in a
 * single run, as far as possible.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.44
 */
PangoAttribute *
pango_attr_allow_breaks_new (gboolean allow_breaks)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_ALLOW_BREAKS,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal,
  };

  return pango_attr_int_new (&klass, (int)allow_breaks);
}

/**
 * pango_attr_insert_hyphens_new:
 * @insert_hyphens: %TRUE if hyphens should be inserted
 *
 * Create a new insert-hyphens attribute.
 *
 * Pango will insert hyphens when breaking lines in
 * the middle of a word. This attribute can be used
 * to suppress the hyphen.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.44
 */
PangoAttribute *
pango_attr_insert_hyphens_new (gboolean insert_hyphens)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_INSERT_HYPHENS,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal,
  };

  return pango_attr_int_new (&klass, (int)insert_hyphens);
}

/**
 * pango_attr_show_new:
 * @flags: `PangoShowFlags` to apply
 *
 * Create a new attribute that influences how invisible
 * characters are rendered.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.44
 **/
PangoAttribute *
pango_attr_show_new (PangoShowFlags flags)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_SHOW,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal,
  };

  return pango_attr_int_new (&klass, (int)flags);
}

/**
 * pango_attr_word_new:
 *
 * Marks the range of the attribute as a single word.
 *
 * Note that this may require adjustments to word and
 * sentence classification around the range.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_word_new (void)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_WORD,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal,
  };

  return pango_attr_int_new (&klass, 1);
}

/**
 * pango_attr_sentence_new:
 *
 * Marks the range of the attribute as a single sentence.
 *
 * Note that this may require adjustments to word and
 * sentence classification around the range.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_sentence_new (void)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_SENTENCE,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal,
  };

  return pango_attr_int_new (&klass, 1);
}

/**
 * pango_attr_overline_new:
 * @overline: the overline style
 *
 * Create a new overline-style attribute.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.46
 */
PangoAttribute *
pango_attr_overline_new (PangoOverline overline)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_OVERLINE,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, (int)overline);
}

/**
 * pango_attr_overline_color_new:
 * @red: the red value (ranging from 0 to 65535)
 * @green: the green value
 * @blue: the blue value
 *
 * Create a new overline color attribute.
 *
 * This attribute modifies the color of overlines.
 * If not set, overlines will use the foreground color.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.46
 */
PangoAttribute *
pango_attr_overline_color_new (guint16 red,
                               guint16 green,
                               guint16 blue)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_OVERLINE_COLOR,
    pango_attr_color_copy,
    pango_attr_color_destroy,
    pango_attr_color_equal
  };

  return pango_attr_color_new (&klass, red, green, blue);
}

/**
 * pango_attr_line_height_new:
 * @factor: the scaling factor to apply to the logical height
 *
 * Modify the height of logical line extents by a factor.
 *
 * This affects the values returned by
 * [method@Pango.LayoutLine.get_extents],
 * [method@Pango.LayoutLine.get_pixel_extents] and
 * [method@Pango.LayoutIter.get_line_extents].
 *
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_line_height_new (double factor)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_LINE_HEIGHT,
    pango_attr_float_copy,
    pango_attr_float_destroy,
    pango_attr_float_equal
  };

  return pango_attr_float_new (&klass, factor);
}

/**
 * pango_attr_line_height_new_absolute:
 * @height: the line height, in %PANGO_SCALE-ths of a point
 *
 * Override the height of logical line extents to be @height.
 *
 * This affects the values returned by
 * [method@Pango.LayoutLine.get_extents],
 * [method@Pango.LayoutLine.get_pixel_extents] and
 * [method@Pango.LayoutIter.get_line_extents].
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_line_height_new_absolute (int height)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_ABSOLUTE_LINE_HEIGHT,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, height);
}

/**
 * pango_attr_text_transform_new:
 * @transform: `PangoTextTransform` to apply
 *
 * Create a new attribute that influences how characters
 * are transformed during shaping.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttribute`, which should be freed with
 *   [method@Pango.Attribute.destroy]
 *
 * Since: 1.50
 */
PangoAttribute *
pango_attr_text_transform_new (PangoTextTransform transform)
{
  static const PangoAttrClass klass = {
    PANGO_ATTR_TEXT_TRANSFORM,
    pango_attr_int_copy,
    pango_attr_int_destroy,
    pango_attr_int_equal
  };

  return pango_attr_int_new (&klass, transform);
}
/* }}} */
/* {{{ Binding helpers */

/**
 * pango_attribute_as_int:
 * @attr: A `PangoAttribute` such as weight
 *
 * Returns the attribute cast to `PangoAttrInt`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrInt`,
 *   or %NULL if it's not an integer attribute
 *
 * Since: 1.50
 */
PangoAttrInt *
pango_attribute_as_int (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_STYLE:
    case PANGO_ATTR_WEIGHT:
    case PANGO_ATTR_VARIANT:
    case PANGO_ATTR_STRETCH:
    case PANGO_ATTR_UNDERLINE:
    case PANGO_ATTR_STRIKETHROUGH:
    case PANGO_ATTR_RISE:
    case PANGO_ATTR_FALLBACK:
    case PANGO_ATTR_LETTER_SPACING:
    case PANGO_ATTR_GRAVITY:
    case PANGO_ATTR_GRAVITY_HINT:
    case PANGO_ATTR_FOREGROUND_ALPHA:
    case PANGO_ATTR_BACKGROUND_ALPHA:
    case PANGO_ATTR_ALLOW_BREAKS:
    case PANGO_ATTR_SHOW:
    case PANGO_ATTR_INSERT_HYPHENS:
    case PANGO_ATTR_OVERLINE:
    case PANGO_ATTR_ABSOLUTE_LINE_HEIGHT:
    case PANGO_ATTR_TEXT_TRANSFORM:
    case PANGO_ATTR_WORD:
    case PANGO_ATTR_SENTENCE:
    case PANGO_ATTR_BASELINE_SHIFT:
    case PANGO_ATTR_FONT_SCALE:
      return (PangoAttrInt *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_float:
 * @attr: A `PangoAttribute` such as scale
 *
 * Returns the attribute cast to `PangoAttrFloat`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrFloat`,
 *   or %NULL if it's not a floating point attribute
 *
 * Since: 1.50
 */
PangoAttrFloat *
pango_attribute_as_float (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_SCALE:
    case PANGO_ATTR_LINE_HEIGHT:
      return (PangoAttrFloat *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_string:
 * @attr: A `PangoAttribute` such as family
 *
 * Returns the attribute cast to `PangoAttrString`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrString`,
 *   or %NULL if it's not a string attribute
 *
 * Since: 1.50
 */
PangoAttrString *
pango_attribute_as_string (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_FAMILY:
      return (PangoAttrString *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_size:
 * @attr: A `PangoAttribute` representing a size
 *
 * Returns the attribute cast to `PangoAttrSize`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrSize`,
 *   or NULL if it's not a size attribute
 *
 * Since: 1.50
 */
PangoAttrSize *
pango_attribute_as_size (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_SIZE:
    case PANGO_ATTR_ABSOLUTE_SIZE:
      return (PangoAttrSize *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_color:
 * @attr: A `PangoAttribute` such as foreground
 *
 * Returns the attribute cast to `PangoAttrColor`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrColor`,
 *   or %NULL if it's not a color attribute
 *
 * Since: 1.50
 */
PangoAttrColor *
pango_attribute_as_color (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_FOREGROUND:
    case PANGO_ATTR_BACKGROUND:
    case PANGO_ATTR_UNDERLINE_COLOR:
    case PANGO_ATTR_STRIKETHROUGH_COLOR:
    case PANGO_ATTR_OVERLINE_COLOR:
      return (PangoAttrColor *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_font_desc:
 * @attr: A `PangoAttribute` representing a font description
 *
 * Returns the attribute cast to `PangoAttrFontDesc`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrFontDesc`,
 *   or %NULL if it's not a font description attribute
 *
 * Since: 1.50
 */
PangoAttrFontDesc *
pango_attribute_as_font_desc (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_FONT_DESC:
      return (PangoAttrFontDesc *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_font_features:
 * @attr: A `PangoAttribute` representing font features
 *
 * Returns the attribute cast to `PangoAttrFontFeatures`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrFontFeatures`,
 *   or %NULL if it's not a font features attribute
 *
 * Since: 1.50
 */
PangoAttrFontFeatures *
pango_attribute_as_font_features (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_FONT_FEATURES:
      return (PangoAttrFontFeatures *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_language:
 * @attr: A `PangoAttribute` representing a language
 *
 * Returns the attribute cast to `PangoAttrLanguage`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrLanguage`,
 *   or %NULL if it's not a language attribute
 *
 * Since: 1.50
 */
PangoAttrLanguage *
pango_attribute_as_language (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_LANGUAGE:
      return (PangoAttrLanguage *)attr;

    default:
      return NULL;
    }
}

/**
 * pango_attribute_as_shape:
 * @attr: A `PangoAttribute` representing a shape
 *
 * Returns the attribute cast to `PangoAttrShape`.
 *
 * This is mainly useful for language bindings.
 *
 * Returns: (nullable) (transfer none): The attribute as `PangoAttrShape`,
 *   or %NULL if it's not a shape attribute
 *
 * Since: 1.50
 */
PangoAttrShape *
pango_attribute_as_shape (PangoAttribute *attr)
{
  switch ((int)attr->klass->type)
    {
    case PANGO_ATTR_SHAPE:
      return (PangoAttrShape *)attr;

    default:
      return NULL;
    }
}

/* }}} */
/* {{{ Attribute List */

G_DEFINE_BOXED_TYPE (PangoAttrList, pango_attr_list,
                     pango_attr_list_copy,
                     pango_attr_list_unref);

void
_pango_attr_list_init (PangoAttrList *list)
{
  list->ref_count = 1;
  list->attributes = NULL;
}

/**
 * pango_attr_list_new:
 *
 * Create a new empty attribute list with a reference
 * count of one.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttrList`, which should be freed with
 *   [method@Pango.AttrList.unref]
 */
PangoAttrList *
pango_attr_list_new (void)
{
  PangoAttrList *list = g_slice_new (PangoAttrList);

  _pango_attr_list_init (list);

  return list;
}

/**
 * pango_attr_list_ref:
 * @list: (nullable): a `PangoAttrList`
 *
 * Increase the reference count of the given attribute
 * list by one.
 *
 * Return value: The attribute list passed in
 *
 * Since: 1.10
 */
PangoAttrList *
pango_attr_list_ref (PangoAttrList *list)
{
  if (list == NULL)
    return NULL;

  g_atomic_int_inc ((int *) &list->ref_count);

  return list;
}

void
_pango_attr_list_destroy (PangoAttrList *list)
{
  guint i, p;

  if (!list->attributes)
    return;

  for (i = 0, p = list->attributes->len; i < p; i++)
    {
      PangoAttribute *attr = g_ptr_array_index (list->attributes, i);

      attr->klass->destroy (attr);
    }

  g_ptr_array_free (list->attributes, TRUE);
}

/**
 * pango_attr_list_unref:
 * @list: (nullable): a `PangoAttrList`
 *
 * Decrease the reference count of the given attribute
 * list by one.
 *
 * If the result is zero, free the attribute list
 * and the attributes it contains.
 */
void
pango_attr_list_unref (PangoAttrList *list)
{
  if (list == NULL)
    return;

  g_return_if_fail (list->ref_count > 0);

  if (g_atomic_int_dec_and_test ((int *) &list->ref_count))
    {
      _pango_attr_list_destroy (list);
      g_slice_free (PangoAttrList, list);
    }
}

/**
 * pango_attr_list_copy:
 * @list: (nullable): a `PangoAttrList`
 *
 * Copy @list and return an identical new list.
 *
 * Return value: (nullable): the newly allocated
 *   `PangoAttrList`, with a reference count of one,
 *   which should be freed with [method@Pango.AttrList.unref].
 *   Returns %NULL if @list was %NULL.
 */
PangoAttrList *
pango_attr_list_copy (PangoAttrList *list)
{
  PangoAttrList *new;

  if (list == NULL)
    return NULL;

  new = pango_attr_list_new ();
  if (!list->attributes || list->attributes->len == 0)
    return new;

  new->attributes = g_ptr_array_copy (list->attributes, (GCopyFunc)pango_attribute_copy, NULL);

  return new;
}

static void
pango_attr_list_insert_internal (PangoAttrList  *list,
                                 PangoAttribute *attr,
                                 gboolean        before)
{
  const guint start_index = attr->start_index;
  PangoAttribute *last_attr;

  if (G_UNLIKELY (!list->attributes))
    list->attributes = g_ptr_array_new ();

  if (list->attributes->len == 0)
    {
      g_ptr_array_add (list->attributes, attr);
      return;
    }

  g_assert (list->attributes->len > 0);

  last_attr = g_ptr_array_index (list->attributes, list->attributes->len - 1);

  if (last_attr->start_index < start_index ||
      (!before && last_attr->start_index == start_index))
    {
      g_ptr_array_add (list->attributes, attr);
    }
  else
    {
      guint i, p;

      for (i = 0, p = list->attributes->len; i < p; i++)
        {
          PangoAttribute *cur = g_ptr_array_index (list->attributes, i);

          if (cur->start_index > start_index ||
              (before && cur->start_index == start_index))
            {
              g_ptr_array_insert (list->attributes, i, attr);
              break;
            }
        }
    }
}

/**
 * pango_attr_list_insert:
 * @list: a `PangoAttrList`
 * @attr: (transfer full): the attribute to insert
 *
 * Insert the given attribute into the `PangoAttrList`.
 *
 * It will be inserted after all other attributes with a
 * matching @start_index.
 */
void
pango_attr_list_insert (PangoAttrList  *list,
                        PangoAttribute *attr)
{
  g_return_if_fail (list != NULL);
  g_return_if_fail (attr != NULL);

  pango_attr_list_insert_internal (list, attr, FALSE);
}

/**
 * pango_attr_list_insert_before:
 * @list: a `PangoAttrList`
 * @attr: (transfer full): the attribute to insert
 *
 * Insert the given attribute into the `PangoAttrList`.
 *
 * It will be inserted before all other attributes with a
 * matching @start_index.
 */
void
pango_attr_list_insert_before (PangoAttrList  *list,
                               PangoAttribute *attr)
{
  g_return_if_fail (list != NULL);
  g_return_if_fail (attr != NULL);

  pango_attr_list_insert_internal (list, attr, TRUE);
}

/**
 * pango_attr_list_change:
 * @list: a `PangoAttrList`
 * @attr: (transfer full): the attribute to insert
 *
 * Insert the given attribute into the `PangoAttrList`.
 *
 * It will replace any attributes of the same type
 * on that segment and be merged with any adjoining
 * attributes that are identical.
 *
 * This function is slower than [method@Pango.AttrList.insert]
 * for creating an attribute list in order (potentially
 * much slower for large lists). However,
 * [method@Pango.AttrList.insert] is not suitable for
 * continually changing a set of attributes since it
 * never removes or combines existing attributes.
 */
void
pango_attr_list_change (PangoAttrList  *list,
                        PangoAttribute *attr)
{
  guint i, p;
  guint start_index = attr->start_index;
  guint end_index = attr->end_index;
  gboolean inserted;

  g_return_if_fail (list != NULL);

  if (start_index == end_index) /* empty, nothing to do */
    {
      pango_attribute_destroy (attr);
      return;
    }

  if (!list->attributes || list->attributes->len == 0)
    {
      pango_attr_list_insert (list, attr);
      return;
    }

  inserted = FALSE;
  for (i = 0, p = list->attributes->len; i < p; i++)
    {
      PangoAttribute *tmp_attr = g_ptr_array_index (list->attributes, i);

      if (tmp_attr->start_index > start_index)
        {
          g_ptr_array_insert (list->attributes, i, attr);
          inserted = TRUE;
          break;
        }

      if (tmp_attr->klass->type != attr->klass->type)
        continue;

      if (tmp_attr->end_index < start_index)
        continue; /* This attr does not overlap with the new one */

      g_assert (tmp_attr->start_index <= start_index);
      g_assert (tmp_attr->end_index >= start_index);

      if (pango_attribute_equal (tmp_attr, attr))
        {
          /* We can merge the new attribute with this attribute
           */
          if (tmp_attr->end_index >= end_index)
            {
              /* We are totally overlapping the previous attribute.
               * No action is needed.
               */
              pango_attribute_destroy (attr);
              return;
            }

          tmp_attr->end_index = end_index;
          pango_attribute_destroy (attr);

          attr = tmp_attr;
          inserted = TRUE;
          break;
        }
      else
        {
          /* Split, truncate, or remove the old attribute
           */
          if (tmp_attr->end_index > end_index)
            {
              PangoAttribute *end_attr = pango_attribute_copy (tmp_attr);

              end_attr->start_index = end_index;
              pango_attr_list_insert (list, end_attr);
            }

          if (tmp_attr->start_index == start_index)
            {
              pango_attribute_destroy (tmp_attr);
              g_ptr_array_remove_index (list->attributes, i);
              break;
            }
          else
            {
              tmp_attr->end_index = start_index;
            }
        }
    }

  if (!inserted)
    /* we didn't insert attr yet */
    pango_attr_list_insert (list, attr);

  /* We now have the range inserted into the list one way or the
   * other. Fix up the remainder
   */
  /* Attention: No i = 0 here. */
  for (i = i + 1, p = list->attributes->len; i < p; i++)
    {
      PangoAttribute *tmp_attr = g_ptr_array_index (list->attributes, i);

      if (tmp_attr->start_index > end_index)
        break;

      if (tmp_attr->klass->type != attr->klass->type)
        continue;

      if (tmp_attr == attr)
        continue;

      if (tmp_attr->end_index <= attr->end_index ||
          pango_attribute_equal (tmp_attr, attr))
        {
          /* We can merge the new attribute with this attribute. */
          attr->end_index = MAX (end_index, tmp_attr->end_index);
          pango_attribute_destroy (tmp_attr);
          g_ptr_array_remove_index (list->attributes, i);
          i--;
          p--;
          continue;
        }
      else
        {
          /* Trim the start of this attribute that it begins at the end
           * of the new attribute. This may involve moving it in the list
           * to maintain the required non-decreasing order of start indices.
           */
          int k, m;

          tmp_attr->start_index = attr->end_index;

          for (k = i + 1, m = list->attributes->len; k < m; k++)
            {
              PangoAttribute *tmp_attr2 = g_ptr_array_index (list->attributes, k);

              if (tmp_attr2->start_index >= tmp_attr->start_index)
                break;

              g_ptr_array_index (list->attributes, k - 1) = tmp_attr2;
              g_ptr_array_index (list->attributes, k) = tmp_attr;
            }
        }
    }
}

/**
 * pango_attr_list_update:
 * @list: a `PangoAttrList`
 * @pos: the position of the change
 * @remove: the number of removed bytes
 * @add: the number of added bytes
 *
 * Update indices of attributes in @list for a change in the
 * text they refer to.
 *
 * The change that this function applies is removing @remove
 * bytes at position @pos and inserting @add bytes instead.
 *
 * Attributes that fall entirely in the (@pos, @pos + @remove)
 * range are removed.
 *
 * Attributes that start or end inside the (@pos, @pos + @remove)
 * range are shortened to reflect the removal.
 *
 * Attributes start and end positions are updated if they are
 * behind @pos + @remove.
 *
 * Since: 1.44
 */
void
pango_attr_list_update (PangoAttrList *list,
                        int             pos,
                        int             remove,
                        int             add)
{
  guint i, p;

  g_return_if_fail (pos >= 0);
  g_return_if_fail (remove >= 0);
  g_return_if_fail (add >= 0);

  if (list->attributes)
    for (i = 0, p = list->attributes->len; i < p; i++)
      {
        PangoAttribute *attr = g_ptr_array_index (list->attributes, i);

        if (attr->start_index >= pos &&
          attr->end_index < pos + remove)
          {
            pango_attribute_destroy (attr);
            g_ptr_array_remove_index (list->attributes, i);
            i--; /* Look at this index again */
            p--;
            continue;
          }

        if (attr->start_index != PANGO_ATTR_INDEX_FROM_TEXT_BEGINNING)
          {
            if (attr->start_index >= pos &&
                attr->start_index < pos + remove)
              {
                attr->start_index = pos + add;
              }
            else if (attr->start_index >= pos + remove)
              {
                attr->start_index += add - remove;
              }
          }

        if (attr->end_index != PANGO_ATTR_INDEX_TO_TEXT_END)
          {
            if (attr->end_index >= pos &&
                attr->end_index < pos + remove)
              {
                attr->end_index = pos;
              }
            else if (attr->end_index >= pos + remove)
              {
                if (add > remove &&
                    G_MAXUINT - attr->end_index < add - remove)
                  attr->end_index = G_MAXUINT;
                else
                  attr->end_index += add - remove;
              }
          }
      }
}

/**
 * pango_attr_list_splice:
 * @list: a `PangoAttrList`
 * @other: another `PangoAttrList`
 * @pos: the position in @list at which to insert @other
 * @len: the length of the spliced segment. (Note that this
 *   must be specified since the attributes in @other may only
 *   be present at some subsection of this range)
 *
 * This function opens up a hole in @list, fills it
 * in with attributes from the left, and then merges
 * @other on top of the hole.
 *
 * This operation is equivalent to stretching every attribute
 * that applies at position @pos in @list by an amount @len,
 * and then calling [method@Pango.AttrList.change] with a copy
 * of each attribute in @other in sequence (offset in position
 * by @pos, and limited in length to @len).
 *
 * This operation proves useful for, for instance, inserting
 * a pre-edit string in the middle of an edit buffer.
 *
 * For backwards compatibility, the function behaves differently
 * when @len is 0. In this case, the attributes from @other are
 * not imited to @len, and are just overlayed on top of @list.
 *
 * This mode is useful for merging two lists of attributes together.
 */
void
pango_attr_list_splice (PangoAttrList *list,
                        PangoAttrList *other,
                        gint           pos,
                        gint           len)
{
  guint i, p;
  guint upos, ulen;
  guint end;

  g_return_if_fail (list != NULL);
  g_return_if_fail (other != NULL);
  g_return_if_fail (pos >= 0);
  g_return_if_fail (len >= 0);

  upos = (guint)pos;
  ulen = (guint)len;

/* This definition only works when a and b are unsigned; overflow
 * isn't defined in the C standard for signed integers
 */
#define CLAMP_ADD(a,b) (((a) + (b) < (a)) ? G_MAXUINT : (a) + (b))

  end = CLAMP_ADD (upos, ulen);

  if (list->attributes)
    for (i = 0, p = list->attributes->len; i < p; i++)
      {
        PangoAttribute *attr = g_ptr_array_index (list->attributes, i);;

        if (attr->start_index <= upos)
          {
            if (attr->end_index > upos)
              attr->end_index = CLAMP_ADD (attr->end_index, ulen);
          }
        else
          {
            /* This could result in a zero length attribute if it
             * gets squashed up against G_MAXUINT, but deleting such
             * an element could (in theory) suprise the caller, so
             * we don't delete it.
             */
            attr->start_index = CLAMP_ADD (attr->start_index, ulen);
            attr->end_index = CLAMP_ADD (attr->end_index, ulen);
         }
      }

  if (!other->attributes || other->attributes->len == 0)
    return;

  for (i = 0, p = other->attributes->len; i < p; i++)
    {
      PangoAttribute *attr = pango_attribute_copy (g_ptr_array_index (other->attributes, i));
      if (ulen > 0)
        {
          attr->start_index = MIN (CLAMP_ADD (attr->start_index, upos), end);
          attr->end_index = MIN (CLAMP_ADD (attr->end_index, upos), end);
        }
      else
        {
          attr->start_index = CLAMP_ADD (attr->start_index, upos);
          attr->end_index = CLAMP_ADD (attr->end_index, upos);
        }

      /* Same as above, the attribute could be squashed to zero-length; here
       * pango_attr_list_change() will take care of deleting it.
       */
      pango_attr_list_change (list, attr);
    }
#undef CLAMP_ADD
}

/**
 * pango_attr_list_get_attributes:
 * @list: a `PangoAttrList`
 *
 * Gets a list of all attributes in @list.
 *
 * Return value: (element-type Pango.Attribute) (transfer full):
 *   a list of all attributes in @list. To free this value,
 *   call [method@Pango.Attribute.destroy] on each value and
 *   g_slist_free() on the list.
 *
 * Since: 1.44
 */
GSList *
pango_attr_list_get_attributes (PangoAttrList *list)
{
  GSList *result = NULL;
  guint i, p;

  g_return_val_if_fail (list != NULL, NULL);

  if (!list->attributes || list->attributes->len == 0)
    return NULL;

  for (i = 0, p = list->attributes->len; i < p; i++)
    {
      PangoAttribute *attr = g_ptr_array_index (list->attributes, i);

      result = g_slist_prepend (result, pango_attribute_copy (attr));
    }

  return g_slist_reverse (result);
}

/**
 * pango_attr_list_equal:
 * @list: a `PangoAttrList`
 * @other_list: the other `PangoAttrList`
 *
 * Checks whether @list and @other_list contain the same
 * attributes and whether those attributes apply to the
 * same ranges.
 *
 * Beware that this will return wrong values if any list
 * contains duplicates.
 *
 * Return value: %TRUE if the lists are equal, %FALSE if
 *   they aren't
 *
 * Since: 1.46
 */
gboolean
pango_attr_list_equal (PangoAttrList *list,
                       PangoAttrList *other_list)
{
  GPtrArray *attrs, *other_attrs;
  guint64 skip_bitmask = 0;
  guint i;

  if (list == other_list)
    return TRUE;

  if (list == NULL || other_list == NULL)
    return FALSE;

  if (list->attributes == NULL || other_list->attributes == NULL)
    return list->attributes == other_list->attributes;

  attrs = list->attributes;
  other_attrs = other_list->attributes;

  if (attrs->len != other_attrs->len)
    return FALSE;

  for (i = 0; i < attrs->len; i++)
    {
      PangoAttribute *attr = g_ptr_array_index (attrs, i);
      gboolean attr_equal = FALSE;
      guint other_attr_index;

      for (other_attr_index = 0; other_attr_index < other_attrs->len; other_attr_index++)
        {
          PangoAttribute *other_attr = g_ptr_array_index (other_attrs, other_attr_index);
          guint64 other_attr_bitmask = other_attr_index < 64 ? 1 << other_attr_index : 0;

          if ((skip_bitmask & other_attr_bitmask) != 0)
            continue;

          if (attr->start_index == other_attr->start_index &&
              attr->end_index == other_attr->end_index &&
              pango_attribute_equal (attr, other_attr))
            {
              skip_bitmask |= other_attr_bitmask;
              attr_equal = TRUE;
              break;
            }

        }

      if (!attr_equal)
        return FALSE;
    }

  return TRUE;
}

gboolean
_pango_attr_list_has_attributes (const PangoAttrList *list)
{
  return list && list->attributes != NULL && list->attributes->len > 0;
}

/**
 * pango_attr_list_filter:
 * @list: a `PangoAttrList`
 * @func: (scope call) (closure data): callback function;
 *   returns %TRUE if an attribute should be filtered out
 * @data: (closure): Data to be passed to @func
 *
 * Given a `PangoAttrList` and callback function, removes
 * any elements of @list for which @func returns %TRUE and
 * inserts them into a new list.
 *
 * Return value: (transfer full) (nullable): the new
 *   `PangoAttrList` or %NULL if no attributes of the
 *   given types were found
 *
 * Since: 1.2
 */
PangoAttrList *
pango_attr_list_filter (PangoAttrList       *list,
                        PangoAttrFilterFunc  func,
                        gpointer             data)

{
  PangoAttrList *new = NULL;
  guint i, p;

  g_return_val_if_fail (list != NULL, NULL);

  if (!list->attributes || list->attributes->len == 0)
    return NULL;

  for (i = 0, p = list->attributes->len; i < p; i++)
    {
      PangoAttribute *tmp_attr = g_ptr_array_index (list->attributes, i);

      if ((*func) (tmp_attr, data))
        {
          g_ptr_array_remove_index (list->attributes, i);
          i--; /* Need to look at this index again */
          p--;

          if (G_UNLIKELY (!new))
            {
              new = pango_attr_list_new ();
              new->attributes = g_ptr_array_new ();
            }

          g_ptr_array_add (new->attributes, tmp_attr);
        }
    }

  return new;
}

/* {{{ PangoAttrList serialization */

/* We serialize attribute lists to strings. The format
 * is a comma-separated list of the attributes in the order
 * in which they are in the list, with each attribute having
 * this format:
 *
 * START END NICK VALUE
 *
 * Values that can contain a comma, such as font descriptions
 * are quoted with "".
 */

static const char *
get_attr_type_nick (PangoAttrType attr_type)
{
  GEnumClass *enum_class;
  GEnumValue *enum_value;

  enum_class = g_type_class_ref (pango_attr_type_get_type ());
  enum_value = g_enum_get_value (enum_class, attr_type);
  g_type_class_unref (enum_class);

  return enum_value->value_nick;
}

static GType
get_attr_value_type (PangoAttrType type)
{
  switch ((int)type)
    {
    case PANGO_ATTR_STYLE: return PANGO_TYPE_STYLE;
    case PANGO_ATTR_WEIGHT: return PANGO_TYPE_WEIGHT;
    case PANGO_ATTR_VARIANT: return PANGO_TYPE_VARIANT;
    case PANGO_ATTR_STRETCH: return PANGO_TYPE_STRETCH;
    case PANGO_ATTR_GRAVITY: return PANGO_TYPE_GRAVITY;
    case PANGO_ATTR_GRAVITY_HINT: return PANGO_TYPE_GRAVITY_HINT;
    case PANGO_ATTR_UNDERLINE: return PANGO_TYPE_UNDERLINE;
    case PANGO_ATTR_OVERLINE: return PANGO_TYPE_OVERLINE;
    case PANGO_ATTR_BASELINE_SHIFT: return PANGO_TYPE_BASELINE_SHIFT;
    case PANGO_ATTR_FONT_SCALE: return PANGO_TYPE_FONT_SCALE;
    case PANGO_ATTR_TEXT_TRANSFORM: return PANGO_TYPE_TEXT_TRANSFORM;
    default: return G_TYPE_INVALID;
    }
}

static void
append_enum_value (GString *str,
                   GType    type,
                   int      value)
{
  GEnumClass *enum_class;
  GEnumValue *enum_value;

  enum_class = g_type_class_ref (type);
  enum_value = g_enum_get_value (enum_class, value);
  g_type_class_unref (enum_class);

  if (enum_value)
    g_string_append_printf (str, " %s", enum_value->value_nick);
  else
    g_string_append_printf (str, " %d", value);
}

static void
attr_print (GString        *str,
            PangoAttribute *attr)
{
  PangoAttrString *string;
  PangoAttrLanguage *lang;
  PangoAttrInt *integer;
  PangoAttrFloat *flt;
  PangoAttrFontDesc *font;
  PangoAttrColor *color;
  PangoAttrShape *shape;
  PangoAttrSize *size;
  PangoAttrFontFeatures *features;

  g_string_append_printf (str, "%u %u ", attr->start_index, attr->end_index);

  g_string_append (str, get_attr_type_nick (attr->klass->type));

  if (attr->klass->type == PANGO_ATTR_WEIGHT ||
      attr->klass->type == PANGO_ATTR_STYLE ||
      attr->klass->type == PANGO_ATTR_STRETCH ||
      attr->klass->type == PANGO_ATTR_VARIANT ||
      attr->klass->type == PANGO_ATTR_GRAVITY ||
      attr->klass->type == PANGO_ATTR_GRAVITY_HINT ||
      attr->klass->type == PANGO_ATTR_UNDERLINE ||
      attr->klass->type == PANGO_ATTR_OVERLINE ||
      attr->klass->type == PANGO_ATTR_BASELINE_SHIFT ||
      attr->klass->type == PANGO_ATTR_FONT_SCALE ||
      attr->klass->type == PANGO_ATTR_TEXT_TRANSFORM)
    append_enum_value (str, get_attr_value_type (attr->klass->type), ((PangoAttrInt *)attr)->value);
  else if (attr->klass->type == PANGO_ATTR_STRIKETHROUGH ||
           attr->klass->type == PANGO_ATTR_ALLOW_BREAKS ||
           attr->klass->type == PANGO_ATTR_INSERT_HYPHENS ||
           attr->klass->type == PANGO_ATTR_FALLBACK)
    g_string_append (str, ((PangoAttrInt *)attr)->value ? " true" : " false");
  else if ((string = pango_attribute_as_string (attr)) != NULL)
    {
      char *s = g_strescape (string->value, NULL);
      g_string_append_printf (str, " \"%s\"", s);
      g_free (s);
    }
  else if ((lang = pango_attribute_as_language (attr)) != NULL)
    g_string_append_printf (str, " %s", pango_language_to_string (lang->value));
  else if ((integer = pango_attribute_as_int (attr)) != NULL)
    g_string_append_printf (str, " %d", integer->value);
  else if ((flt = pango_attribute_as_float (attr)) != NULL)
    {
      char buf[20];
      g_ascii_formatd (buf, 20, "%f", flt->value);
      g_string_append_printf (str, " %s", buf);
    }
  else if ((font = pango_attribute_as_font_desc (attr)) != NULL)
    {
      char *s = pango_font_description_to_string (font->desc);
      char *s2 = g_strescape (s, NULL);
      g_string_append_printf (str, " \"%s\"", s2);
      g_free (s2);
      g_free (s);
    }
  else if ((color = pango_attribute_as_color (attr)) != NULL)
    {
      char *s = pango_color_to_string (&color->color);
      g_string_append_printf (str, " %s", s);
      g_free (s);
    }
  else if ((shape = pango_attribute_as_shape (attr)) != NULL)
    g_string_append (str, "shape"); /* FIXME */
  else if ((size = pango_attribute_as_size (attr)) != NULL)
    g_string_append_printf (str, " %d", size->size);
  else if ((features = pango_attribute_as_font_features (attr)) != NULL)
    g_string_append_printf (str, " \"%s\"", features->features);
  else
    g_assert_not_reached ();
}

/**
 * pango_attr_list_to_string:
 * @list: a `PangoAttrList`
 *
 * Serializes a `PangoAttrList` to a string.
 *
 * In the resulting string, serialized attributes are separated by newlines or commas.
 * Individual attributes are serialized to a string of the form
 *
 *   START END TYPE VALUE
 *
 * Where START and END are the indices (with -1 being accepted in place
 * of MAXUINT), TYPE is the nickname of the attribute value type, e.g.
 * _weight_ or _stretch_, and the value is serialized according to its type:
 *
 * - enum values as nick or numeric value
 * - boolean values as _true_ or _false_
 * - integers and floats as numbers
 * - strings as string, optionally quoted
 * - font features as quoted string
 * - PangoLanguage as string
 * - PangoFontDescription as serialized by [method@Pango.FontDescription.to_string], quoted
 * - PangoColor as serialized by [method@Pango.Color.to_string]
 *
 * Examples:
 *
 * ```
 * 0 10 foreground red, 5 15 weight bold, 0 200 font-desc "Sans 10"
 * ```
 *
 * ```
 * 0 -1 weight 700
 * 0 100 family Times
 * ```
 *
 * To parse the returned value, use [func@Pango.AttrList.from_string].
 *
 * Note that shape attributes can not be serialized.
 *
 * Returns: (transfer full): a newly allocated string
 * Since: 1.50
 */
char *
pango_attr_list_to_string (PangoAttrList *list)
{
  GString *s;

  s = g_string_new ("");

  if (list->attributes)
    for (int i = 0; i < list->attributes->len; i++)
      {
        PangoAttribute *attr = g_ptr_array_index (list->attributes, i);

        if (i > 0)
          g_string_append (s, "\n");
        attr_print (s, attr);
      }

  return g_string_free (s, FALSE);
}

static PangoAttrType
get_attr_type_by_nick (const char *nick,
                       int         len)
{
  GEnumClass *enum_class;

  enum_class = g_type_class_ref (pango_attr_type_get_type ());
  for (GEnumValue *ev = enum_class->values; ev->value_name; ev++)
    {
      if (ev->value_nick && strncmp (ev->value_nick, nick, len) == 0)
        {
          g_type_class_unref (enum_class);
          return (PangoAttrType) ev->value;
        }
    }

  g_type_class_unref (enum_class);
  return PANGO_ATTR_INVALID;
}

static int
get_attr_value (PangoAttrType  type,
                const char    *str,
                int            len)
{
  GEnumClass *enum_class;
  char *endp;
  int value;

  enum_class = g_type_class_ref (get_attr_value_type (type));
  for (GEnumValue *ev = enum_class->values; ev->value_name; ev++)
    {
      if (ev->value_nick && strncmp (ev->value_nick, str, len) == 0)
        {
          g_type_class_unref (enum_class);
          return ev->value;
        }
    }
  g_type_class_unref (enum_class);

  value = g_ascii_strtoll (str, &endp, 10);
  if (endp - str == len)
    return value;

  return -1;
}

static gboolean
is_valid_end_char (char c)
{
  return c == ',' || c == '\n' || c == '\0';
}

/**
 * pango_attr_list_from_string:
 * @text: a string
 *
 * Deserializes a `PangoAttrList` from a string.
 *
 * This is the counterpart to [method@Pango.AttrList.to_string].
 * See that functions for details about the format.
 *
 * Returns: (transfer full) (nullable): a new `PangoAttrList`
 * Since: 1.50
 */
PangoAttrList *
pango_attr_list_from_string (const char *text)
{
  PangoAttrList *list;
  const char *p;

  g_return_val_if_fail (text != NULL, NULL);

  list = pango_attr_list_new ();

  if (*text == '\0')
    return list;

  list->attributes = g_ptr_array_new ();

  p = text + strspn (text, " \t\n");
  while (*p)
    {
      char *endp;
      gint64 start_index;
      gint64 end_index;
      char *str;
      PangoAttrType attr_type;
      PangoAttribute *attr;
      PangoLanguage *lang;
      gint64 integer;
      PangoFontDescription *desc;
      PangoColor color;
      double num;
      int len;

      start_index = g_ascii_strtoll (p, &endp, 10);
      if (*endp != ' ')
        goto fail;

      p = endp + strspn (endp, " ");
      if (!*p)
        goto fail;

      end_index = g_ascii_strtoll (p, &endp, 10);
      if (*endp != ' ')
        goto fail;

      p = endp + strspn (endp, " ");

      endp = (char *)p + strcspn (p, " ");
      attr_type = get_attr_type_by_nick (p, endp - p);

      p = endp + strspn (endp, " ");
      if (*p == '\0')
        goto fail;

#define INT_ATTR(name,type) \
          integer = g_ascii_strtoll (p, &endp, 10); \
          if (!is_valid_end_char (*endp)) goto fail; \
          attr = pango_attr_##name##_new ((type)integer);

#define BOOLEAN_ATTR(name,type) \
          if (strncmp (p, "true", strlen ("true")) == 0) \
            { \
              integer = 1; \
              endp = (char *)(p + strlen ("true")); \
            } \
          else if (strncmp (p, "false", strlen ("false")) == 0) \
            { \
              integer = 0; \
              endp = (char *)(p + strlen ("false")); \
            } \
          else \
            integer = g_ascii_strtoll (p, &endp, 10); \
          if (!is_valid_end_char (*endp)) goto fail; \
          attr = pango_attr_##name##_new ((type)integer);

#define ENUM_ATTR(name, type, min, max) \
          endp = (char *)p + strcspn (p, ",\n"); \
          len = endp - p; \
          while (len > 0 && p[len - 1] == ' ') \
            len--; \
          integer = get_attr_value (attr_type, p, len); \
          attr = pango_attr_##name##_new ((type) CLAMP (integer, min, max));

#define FLOAT_ATTR(name) \
          num = g_ascii_strtod (p, &endp); \
          if (!is_valid_end_char (*endp)) goto fail; \
          attr = pango_attr_##name##_new ((float)num);

#define COLOR_ATTR(name) \
          endp = (char *)p + strcspn (p, ",\n"); \
          if (!is_valid_end_char (*endp)) goto fail; \
          str = g_strndup (p, endp - p); \
          if (!pango_color_parse (&color, str)) \
            { \
              g_free (str); \
              goto fail; \
            } \
          attr = pango_attr_##name##_new (color.red, color.green, color.blue); \
          g_free (str);

      switch (attr_type)
        {
        case PANGO_ATTR_INVALID:
          pango_attr_list_unref (list);
          return NULL;

        case PANGO_ATTR_LANGUAGE:
          endp = (char *)p + strcspn (p, ",\n");
          if (!is_valid_end_char (*endp)) goto fail;
          str = g_strndup (p, endp - p);
          lang = pango_language_from_string (str);
          attr = pango_attr_language_new (lang);
          g_free (str);
          break;

        case PANGO_ATTR_FAMILY:
          endp = (char *)p + strcspn (p, ",\n");
          if (!is_valid_end_char (*endp)) goto fail;
          if (p[0] == '"')
            {
              char *str2;

              len = endp - p;
              while (len > 0 && p[len - 1] == ' ')
                len--;

              if (p[len - 1] != '"') goto fail;

              str2 = g_strndup (p + 1, len - 2);
              str = g_strcompress (str2);
              g_free (str2);
            }
          else
            str = g_strndup (p, endp - p);
          attr = pango_attr_family_new (str);
          g_free (str);
          break;

        case PANGO_ATTR_STYLE:
          ENUM_ATTR(style, PangoStyle, PANGO_STYLE_NORMAL, PANGO_STYLE_ITALIC);
          break;

        case PANGO_ATTR_WEIGHT:
          ENUM_ATTR(weight, PangoWeight, PANGO_WEIGHT_THIN, PANGO_WEIGHT_ULTRAHEAVY);
          break;

        case PANGO_ATTR_VARIANT:
          ENUM_ATTR(variant, PangoVariant, PANGO_VARIANT_NORMAL, PANGO_VARIANT_TITLE_CAPS);
          break;

        case PANGO_ATTR_STRETCH:
          ENUM_ATTR(stretch, PangoStretch, PANGO_STRETCH_ULTRA_CONDENSED, PANGO_STRETCH_ULTRA_EXPANDED);
          break;

        case PANGO_ATTR_SIZE:
          INT_ATTR(size, int);
          break;

        case PANGO_ATTR_FONT_DESC:
          if (*p != '"') goto fail;
          p++;
          endp = strchr (p, '"');
          if (!endp) goto fail;
          str = g_strndup (p, endp - p);
          desc = pango_font_description_from_string (str);
          attr = pango_attr_font_desc_new (desc);
          pango_font_description_free (desc);
          g_free (str);
          endp++;
          if (!is_valid_end_char (*endp)) goto fail;
          break;

        case PANGO_ATTR_FOREGROUND:
          COLOR_ATTR(foreground);
          break;

        case PANGO_ATTR_BACKGROUND:
          COLOR_ATTR(background);
          break;

        case PANGO_ATTR_UNDERLINE:
          ENUM_ATTR(underline, PangoUnderline, PANGO_UNDERLINE_NONE, PANGO_UNDERLINE_ERROR_LINE);
          break;

        case PANGO_ATTR_STRIKETHROUGH:
          BOOLEAN_ATTR(strikethrough, gboolean);
          break;

        case PANGO_ATTR_RISE:
          INT_ATTR(rise, int);
          break;

        case PANGO_ATTR_SHAPE:
          endp = (char *)p + strcspn (p, ",\n");
          continue; /* FIXME */

        case PANGO_ATTR_SCALE:
          FLOAT_ATTR(scale);
          break;

        case PANGO_ATTR_FALLBACK:
          BOOLEAN_ATTR(fallback, gboolean);
          break;

        case PANGO_ATTR_LETTER_SPACING:
          INT_ATTR(letter_spacing, int);
          break;

        case PANGO_ATTR_UNDERLINE_COLOR:
          COLOR_ATTR(underline_color);
          break;

        case PANGO_ATTR_STRIKETHROUGH_COLOR:
          COLOR_ATTR(strikethrough_color);
          break;

        case PANGO_ATTR_ABSOLUTE_SIZE:
          integer = g_ascii_strtoll (p, &endp, 10);
          if (!is_valid_end_char (*endp)) goto fail;
          attr = pango_attr_size_new_absolute (integer);
          break;

        case PANGO_ATTR_GRAVITY:
          ENUM_ATTR(gravity, PangoGravity, PANGO_GRAVITY_SOUTH, PANGO_GRAVITY_WEST);
          break;

        case PANGO_ATTR_FONT_FEATURES:
          p++;
          endp = strchr (p, '"');
          if (!endp) goto fail;
          str = g_strndup (p, endp - p);
          attr = pango_attr_font_features_new (str);
          g_free (str);
          endp++;
          if (!is_valid_end_char (*endp)) goto fail;
          break;

        case PANGO_ATTR_GRAVITY_HINT:
          ENUM_ATTR(gravity_hint, PangoGravityHint, PANGO_GRAVITY_HINT_NATURAL, PANGO_GRAVITY_HINT_LINE);
          break;

        case PANGO_ATTR_FOREGROUND_ALPHA:
          INT_ATTR(foreground_alpha, int);
          break;

        case PANGO_ATTR_BACKGROUND_ALPHA:
          INT_ATTR(background_alpha, int);
          break;

        case PANGO_ATTR_ALLOW_BREAKS:
          BOOLEAN_ATTR(allow_breaks, gboolean);
          break;

        case PANGO_ATTR_SHOW:
          INT_ATTR(show, PangoShowFlags);
          break;

        case PANGO_ATTR_INSERT_HYPHENS:
          BOOLEAN_ATTR(insert_hyphens, gboolean);
          break;

        case PANGO_ATTR_OVERLINE:
          ENUM_ATTR(overline, PangoOverline, PANGO_OVERLINE_NONE, PANGO_OVERLINE_SINGLE);
          break;

        case PANGO_ATTR_OVERLINE_COLOR:
          COLOR_ATTR(overline_color);
          break;

        case PANGO_ATTR_LINE_HEIGHT:
          FLOAT_ATTR(line_height);
          break;

        case PANGO_ATTR_ABSOLUTE_LINE_HEIGHT:
          integer = g_ascii_strtoll (p, &endp, 10);
          if (!is_valid_end_char (*endp)) goto fail;
          attr = pango_attr_line_height_new_absolute (integer);
          break;

        case PANGO_ATTR_TEXT_TRANSFORM:
          ENUM_ATTR(text_transform, PangoTextTransform, PANGO_TEXT_TRANSFORM_NONE, PANGO_TEXT_TRANSFORM_CAPITALIZE);
          break;

        case PANGO_ATTR_WORD:
          integer = g_ascii_strtoll (p, &endp, 10);
          if (!is_valid_end_char (*endp)) goto fail;
          attr = pango_attr_word_new ();
          break;

        case PANGO_ATTR_SENTENCE:
          integer = g_ascii_strtoll (p, &endp, 10);
          if (!is_valid_end_char (*endp)) goto fail;
          attr = pango_attr_sentence_new ();
          break;

        case PANGO_ATTR_BASELINE_SHIFT:
          ENUM_ATTR(baseline_shift, PangoBaselineShift, 0, G_MAXINT);
          break;

        case PANGO_ATTR_FONT_SCALE:
          ENUM_ATTR(font_scale, PangoFontScale, PANGO_FONT_SCALE_NONE, PANGO_FONT_SCALE_SMALL_CAPS);
          break;

        default:
          g_assert_not_reached ();
        }

      attr->start_index = (guint)start_index;
      attr->end_index = (guint)end_index;
      g_ptr_array_add (list->attributes, attr);

      p = endp;
      if (*p)
        {
          if (*p == ',')
            p++;
          p += strspn (p, " \n");
        }
    }

  goto success;

fail:
  pango_attr_list_unref (list);
  list = NULL;

success:
  return list;
}

/* }}} */
/* {{{ Attribute Iterator */

G_DEFINE_BOXED_TYPE (PangoAttrIterator,
                     pango_attr_iterator,
                     pango_attr_iterator_copy,
                     pango_attr_iterator_destroy)

void
_pango_attr_list_get_iterator (PangoAttrList     *list,
                               PangoAttrIterator *iterator)
{
  iterator->attribute_stack = NULL;
  iterator->attrs = list->attributes;
  iterator->n_attrs = iterator->attrs ? iterator->attrs->len : 0;

  iterator->attr_index = 0;
  iterator->start_index = 0;
  iterator->end_index = 0;

  if (!pango_attr_iterator_next (iterator))
    iterator->end_index = G_MAXUINT;
}

/**
 * pango_attr_list_get_iterator:
 * @list: a `PangoAttrList`
 *
 * Create a iterator initialized to the beginning of the list.
 *
 * @list must not be modified until this iterator is freed.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttrIterator`, which should be freed with
 *   [method@Pango.AttrIterator.destroy]
 */
PangoAttrIterator *
pango_attr_list_get_iterator (PangoAttrList  *list)
{
  PangoAttrIterator *iterator;

  g_return_val_if_fail (list != NULL, NULL);

  iterator = g_slice_new (PangoAttrIterator);
  _pango_attr_list_get_iterator (list, iterator);

  return iterator;
}

/**
 * pango_attr_iterator_range:
 * @iterator: a PangoAttrIterator
 * @start: (out): location to store the start of the range
 * @end: (out): location to store the end of the range
 *
 * Get the range of the current segment.
 *
 * Note that the stored return values are signed, not unsigned
 * like the values in `PangoAttribute`. To deal with this API
 * oversight, stored return values that wouldn't fit into
 * a signed integer are clamped to %G_MAXINT.
 */
void
pango_attr_iterator_range (PangoAttrIterator *iterator,
                           gint              *start,
                           gint              *end)
{
  g_return_if_fail (iterator != NULL);

  if (start)
    *start = MIN (iterator->start_index, G_MAXINT);
  if (end)
    *end = MIN (iterator->end_index, G_MAXINT);
}

/**
 * pango_attr_iterator_next:
 * @iterator: a `PangoAttrIterator`
 *
 * Advance the iterator until the next change of style.
 *
 * Return value: %FALSE if the iterator is at the end
 *   of the list, otherwise %TRUE
 */
gboolean
pango_attr_iterator_next (PangoAttrIterator *iterator)
{
  int i;

  g_return_val_if_fail (iterator != NULL, FALSE);

  if (iterator->attr_index >= iterator->n_attrs &&
      (!iterator->attribute_stack || iterator->attribute_stack->len == 0))
    return FALSE;

  iterator->start_index = iterator->end_index;
  iterator->end_index = G_MAXUINT;

  if (iterator->attribute_stack)
    {
      for (i = iterator->attribute_stack->len - 1; i>= 0; i--)
        {
          const PangoAttribute *attr = g_ptr_array_index (iterator->attribute_stack, i);

          if (attr->end_index == iterator->start_index)
            g_ptr_array_remove_index (iterator->attribute_stack, i); /* Can't use index_fast :( */
          else
            iterator->end_index = MIN (iterator->end_index, attr->end_index);
        }
    }

  while (1)
    {
      PangoAttribute *attr;

      if (iterator->attr_index >= iterator->n_attrs)
        break;

      attr = g_ptr_array_index (iterator->attrs, iterator->attr_index);

      if (attr->start_index != iterator->start_index)
        break;

      if (attr->end_index > iterator->start_index)
        {
          if (G_UNLIKELY (!iterator->attribute_stack))
            iterator->attribute_stack = g_ptr_array_new ();

          g_ptr_array_add (iterator->attribute_stack, attr);

          iterator->end_index = MIN (iterator->end_index, attr->end_index);
        }

      iterator->attr_index++; /* NEXT! */
    }

  if (iterator->attr_index < iterator->n_attrs)
      {
      PangoAttribute *attr = g_ptr_array_index (iterator->attrs, iterator->attr_index);

      iterator->end_index = MIN (iterator->end_index, attr->start_index);
    }

  return TRUE;
}

/**
 * pango_attr_iterator_copy:
 * @iterator: a `PangoAttrIterator`
 *
 * Copy a `PangoAttrIterator`.
 *
 * Return value: (transfer full): the newly allocated
 *   `PangoAttrIterator`, which should be freed with
 *   [method@Pango.AttrIterator.destroy]
 */
PangoAttrIterator *
pango_attr_iterator_copy (PangoAttrIterator *iterator)
{
  PangoAttrIterator *copy;

  g_return_val_if_fail (iterator != NULL, NULL);

  copy = g_slice_new (PangoAttrIterator);

  *copy = *iterator;

  if (iterator->attribute_stack)
    copy->attribute_stack = g_ptr_array_copy (iterator->attribute_stack, NULL, NULL);
  else
    copy->attribute_stack = NULL;

  return copy;
}

void
_pango_attr_iterator_destroy (PangoAttrIterator *iterator)
{
  if (iterator->attribute_stack)
    g_ptr_array_free (iterator->attribute_stack, TRUE);
}

/**
 * pango_attr_iterator_destroy:
 * @iterator: a `PangoAttrIterator`
 *
 * Destroy a `PangoAttrIterator` and free all associated memory.
 */
void
pango_attr_iterator_destroy (PangoAttrIterator *iterator)
{
  g_return_if_fail (iterator != NULL);

  _pango_attr_iterator_destroy (iterator);
  g_slice_free (PangoAttrIterator, iterator);
}

/**
 * pango_attr_iterator_get:
 * @iterator: a `PangoAttrIterator`
 * @type: the type of attribute to find
 *
 * Find the current attribute of a particular type
 * at the iterator location.
 *
 * When multiple attributes of the same type overlap,
 * the attribute whose range starts closest to the
 * current location is used.
 *
 * Return value: (nullable) (transfer none): the current
 *   attribute of the given type, or %NULL if no attribute
 *   of that type applies to the current location.
 */
PangoAttribute *
pango_attr_iterator_get (PangoAttrIterator *iterator,
                         PangoAttrType      type)
{
  int i;

  g_return_val_if_fail (iterator != NULL, NULL);

  if (!iterator->attribute_stack)
    return NULL;

  for (i = iterator->attribute_stack->len - 1; i>= 0; i--)
    {
      PangoAttribute *attr = g_ptr_array_index (iterator->attribute_stack, i);

      if (attr->klass->type == type)
        return attr;
    }

  return NULL;
}

/**
 * pango_attr_iterator_get_font:
 * @iterator: a `PangoAttrIterator`
 * @desc: a `PangoFontDescription` to fill in with the current
 *   values. The family name in this structure will be set using
 *   [method@Pango.FontDescription.set_family_static] using
 *   values from an attribute in the `PangoAttrList` associated
 *   with the iterator, so if you plan to keep it around, you
 *   must call:
 *   `pango_font_description_set_family (desc, pango_font_description_get_family (desc))`.
 * @language: (out) (optional): location to store language tag
 *   for item, or %NULL if none is found.
 * @extra_attrs: (out) (optional) (element-type Pango.Attribute) (transfer full):
 *   location in which to store a list of non-font attributes
 *   at the the current position; only the highest priority
 *   value of each attribute will be added to this list. In
 *   order to free this value, you must call
 *   [method@Pango.Attribute.destroy] on each member.
 *
 * Get the font and other attributes at the current
 * iterator position.
 */
void
pango_attr_iterator_get_font (PangoAttrIterator     *iterator,
                              PangoFontDescription  *desc,
                              PangoLanguage        **language,
                              GSList               **extra_attrs)
{
  PangoFontMask mask = 0;
  gboolean have_language = FALSE;
  gdouble scale = 0;
  gboolean have_scale = FALSE;
  int i;

  g_return_if_fail (iterator != NULL);
  g_return_if_fail (desc != NULL);

  if (language)
    *language = NULL;

  if (extra_attrs)
    *extra_attrs = NULL;

  if (!iterator->attribute_stack)
    return;

  for (i = iterator->attribute_stack->len - 1; i >= 0; i--)
    {
      const PangoAttribute *attr = g_ptr_array_index (iterator->attribute_stack, i);

      switch ((int) attr->klass->type)
        {
        case PANGO_ATTR_FONT_DESC:
          {
            PangoFontMask new_mask = pango_font_description_get_set_fields (((PangoAttrFontDesc *)attr)->desc) & ~mask;
            mask |= new_mask;
            pango_font_description_unset_fields (desc, new_mask);
            pango_font_description_merge_static (desc, ((PangoAttrFontDesc *)attr)->desc, FALSE);

            break;
          }
        case PANGO_ATTR_FAMILY:
          if (!(mask & PANGO_FONT_MASK_FAMILY))
            {
              mask |= PANGO_FONT_MASK_FAMILY;
              pango_font_description_set_family (desc, ((PangoAttrString *)attr)->value);
            }
          break;
        case PANGO_ATTR_STYLE:
          if (!(mask & PANGO_FONT_MASK_STYLE))
            {
              mask |= PANGO_FONT_MASK_STYLE;
              pango_font_description_set_style (desc, ((PangoAttrInt *)attr)->value);
            }
          break;
        case PANGO_ATTR_VARIANT:
          if (!(mask & PANGO_FONT_MASK_VARIANT))
            {
              mask |= PANGO_FONT_MASK_VARIANT;
              pango_font_description_set_variant (desc, ((PangoAttrInt *)attr)->value);
            }
          break;
        case PANGO_ATTR_WEIGHT:
          if (!(mask & PANGO_FONT_MASK_WEIGHT))
            {
              mask |= PANGO_FONT_MASK_WEIGHT;
              pango_font_description_set_weight (desc, ((PangoAttrInt *)attr)->value);
            }
          break;
        case PANGO_ATTR_STRETCH:
          if (!(mask & PANGO_FONT_MASK_STRETCH))
            {
              mask |= PANGO_FONT_MASK_STRETCH;
              pango_font_description_set_stretch (desc, ((PangoAttrInt *)attr)->value);
            }
          break;
        case PANGO_ATTR_SIZE:
          if (!(mask & PANGO_FONT_MASK_SIZE))
            {
              mask |= PANGO_FONT_MASK_SIZE;
              pango_font_description_set_size (desc, ((PangoAttrSize *)attr)->size);
            }
          break;
        case PANGO_ATTR_ABSOLUTE_SIZE:
          if (!(mask & PANGO_FONT_MASK_SIZE))
            {
              mask |= PANGO_FONT_MASK_SIZE;
              pango_font_description_set_absolute_size (desc, ((PangoAttrSize *)attr)->size);
            }
          break;
        case PANGO_ATTR_SCALE:
          if (!have_scale)
            {
              have_scale = TRUE;
              scale = ((PangoAttrFloat *)attr)->value;
            }
          break;
        case PANGO_ATTR_LANGUAGE:
          if (language)
            {
              if (!have_language)
                {
                  have_language = TRUE;
                  *language = ((PangoAttrLanguage *)attr)->value;
                }
            }
          break;
        default:
          if (extra_attrs)
            {
              gboolean found = FALSE;

              /* Hack: special-case FONT_FEATURES, BASELINE_SHIFT and FONT_SCALE.
               * We don't want these to accumulate, not override each other,
               * so we never merge them.
               * This needs to be handled more systematically.
               */
              if (attr->klass->type != PANGO_ATTR_FONT_FEATURES &&
                  attr->klass->type != PANGO_ATTR_BASELINE_SHIFT &&
                  attr->klass->type != PANGO_ATTR_FONT_SCALE)
                {
                  GSList *tmp_list = *extra_attrs;
                  while (tmp_list)
                    {
                      PangoAttribute *old_attr = tmp_list->data;
                      if (attr->klass->type == old_attr->klass->type)
                        {
                          found = TRUE;
                          break;
                        }

                      tmp_list = tmp_list->next;
                    }
                }

              if (!found)
                *extra_attrs = g_slist_prepend (*extra_attrs, pango_attribute_copy (attr));
            }
        }
    }

  if (have_scale)
    {
      /* We need to use a local variable to ensure that the compiler won't
       * implicitly cast it to integer while the result is kept in registers,
       * leading to a wrong approximation in i386 (with 387 FPU)
       */
      volatile double size = scale * pango_font_description_get_size (desc);

      if (pango_font_description_get_size_is_absolute (desc))
        pango_font_description_set_absolute_size (desc, size);
      else
        pango_font_description_set_size (desc, size);
    }
}

/**
 * pango_attr_iterator_get_attrs:
 * @iterator: a `PangoAttrIterator`
 *
 * Gets a list of all attributes at the current position of the
 * iterator.
 *
 * Return value: (element-type Pango.Attribute) (transfer full):
 *   a list of all attributes for the current range. To free
 *   this value, call [method@Pango.Attribute.destroy] on each
 *   value and g_slist_free() on the list.
 *
 * Since: 1.2
 */
GSList *
pango_attr_iterator_get_attrs (PangoAttrIterator *iterator)
{
  GSList *attrs = NULL;
  int i;

  if (!iterator->attribute_stack ||
      iterator->attribute_stack->len == 0)
    return NULL;

  for (i = iterator->attribute_stack->len - 1; i >= 0; i--)
    {
      PangoAttribute *attr = g_ptr_array_index (iterator->attribute_stack, i);
      GSList *tmp_list2;
      gboolean found = FALSE;

      if (attr->klass->type != PANGO_ATTR_FONT_DESC &&
          attr->klass->type != PANGO_ATTR_BASELINE_SHIFT &&
          attr->klass->type != PANGO_ATTR_FONT_SCALE)
        for (tmp_list2 = attrs; tmp_list2; tmp_list2 = tmp_list2->next)
          {
            PangoAttribute *old_attr = tmp_list2->data;
            if (attr->klass->type == old_attr->klass->type)
              {
                found = TRUE;
                break;
              }
           }

      if (!found)
        attrs = g_slist_prepend (attrs, pango_attribute_copy (attr));
    }

  return attrs;
}

gboolean
pango_attr_iterator_advance (PangoAttrIterator *iterator,
                             int                index)
{
  int start_range, end_range;

  pango_attr_iterator_range (iterator, &start_range, &end_range);

  while (index >= end_range)
    {
      if (!pango_attr_iterator_next (iterator))
        return FALSE;
      pango_attr_iterator_range (iterator, &start_range, &end_range);
    }

  if (start_range > index)
    g_warning ("pango_attr_iterator_advance(): iterator had already "
               "moved beyond the index");

  return TRUE;
}
/* }}} */

/* vim:set foldmethod=marker expandtab: */
