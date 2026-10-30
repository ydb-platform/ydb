/* GObject - GLib Type, Object, Parameter and Signal Library
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include <glib-object.h>
#include "gmarshal-internal.h"

#ifdef G_ENABLE_DEBUG
#define g_marshal_value_peek_boolean(v)  g_value_get_boolean (v)
#define g_marshal_value_peek_char(v)     g_value_get_schar (v)
#define g_marshal_value_peek_uchar(v)    g_value_get_uchar (v)
#define g_marshal_value_peek_int(v)      g_value_get_int (v)
#define g_marshal_value_peek_uint(v)     g_value_get_uint (v)
#define g_marshal_value_peek_long(v)     g_value_get_long (v)
#define g_marshal_value_peek_ulong(v)    g_value_get_ulong (v)
#define g_marshal_value_peek_int64(v)    g_value_get_int64 (v)
#define g_marshal_value_peek_uint64(v)   g_value_get_uint64 (v)
#define g_marshal_value_peek_enum(v)     g_value_get_enum (v)
#define g_marshal_value_peek_flags(v)    g_value_get_flags (v)
#define g_marshal_value_peek_float(v)    g_value_get_float (v)
#define g_marshal_value_peek_double(v)   g_value_get_double (v)
#define g_marshal_value_peek_string(v)   (char*) g_value_get_string (v)
#define g_marshal_value_peek_param(v)    g_value_get_param (v)
#define g_marshal_value_peek_boxed(v)    g_value_get_boxed (v)
#define g_marshal_value_peek_pointer(v)  g_value_get_pointer (v)
#define g_marshal_value_peek_object(v)   g_value_get_object (v)
#define g_marshal_value_peek_variant(v)  g_value_get_variant (v)
#else /* !G_ENABLE_DEBUG */
/* WARNING: This code accesses GValues directly, which is UNSUPPORTED API.
 *          Do not access GValues directly in your code. Instead, use the
 *          g_value_get_*() functions
 */
#define g_marshal_value_peek_boolean(v)  (v)->data[0].v_int
#define g_marshal_value_peek_char(v)     (v)->data[0].v_int
#define g_marshal_value_peek_uchar(v)    (v)->data[0].v_uint
#define g_marshal_value_peek_int(v)      (v)->data[0].v_int
#define g_marshal_value_peek_uint(v)     (v)->data[0].v_uint
#define g_marshal_value_peek_long(v)     (v)->data[0].v_long
#define g_marshal_value_peek_ulong(v)    (v)->data[0].v_ulong
#define g_marshal_value_peek_int64(v)    (v)->data[0].v_int64
#define g_marshal_value_peek_uint64(v)   (v)->data[0].v_uint64
#define g_marshal_value_peek_enum(v)     (v)->data[0].v_long
#define g_marshal_value_peek_flags(v)    (v)->data[0].v_ulong
#define g_marshal_value_peek_float(v)    (v)->data[0].v_float
#define g_marshal_value_peek_double(v)   (v)->data[0].v_double
#define g_marshal_value_peek_string(v)   (v)->data[0].v_pointer
#define g_marshal_value_peek_param(v)    (v)->data[0].v_pointer
#define g_marshal_value_peek_boxed(v)    (v)->data[0].v_pointer
#define g_marshal_value_peek_pointer(v)  (v)->data[0].v_pointer
#define g_marshal_value_peek_object(v)   (v)->data[0].v_pointer
#define g_marshal_value_peek_variant(v)  (v)->data[0].v_pointer
#endif /* !G_ENABLE_DEBUG */

/* BOOLEAN:OBJECT */
void
_g_cclosure_marshal_BOOLEAN__OBJECT (GClosure     *closure,
                                     GValue       *return_value,
                                     guint         n_param_values,
                                     const GValue *param_values,
                                     gpointer      invocation_hint G_GNUC_UNUSED,
                                     gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__OBJECT) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__OBJECT callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 2);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__OBJECT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_object (param_values + 1),
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__OBJECTv (GClosure *closure,
                                      GValue   *return_value,
                                      gpointer  instance,
                                      va_list   args,
                                      gpointer  marshal_data,
                                      int       n_params,
                                      GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__OBJECT) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__OBJECT callback;
  gboolean v_return;
  gpointer arg0;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__OBJECT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       data2);
  if (arg0 != NULL)
    g_object_unref (arg0);

  g_value_set_boolean (return_value, v_return);
}

/* BOOLEAN:OBJECT,FLAGS */
void
_g_cclosure_marshal_BOOLEAN__OBJECT_FLAGS (GClosure     *closure,
                                           GValue       *return_value,
                                           guint         n_param_values,
                                           const GValue *param_values,
                                           gpointer      invocation_hint G_GNUC_UNUSED,
                                           gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__OBJECT_FLAGS) (gpointer data1,
                                                          gpointer arg1,
                                                          guint arg2,
                                                          gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__OBJECT_FLAGS callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__OBJECT_FLAGS) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_object (param_values + 1),
                       g_marshal_value_peek_flags (param_values + 2),
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__OBJECT_FLAGSv (GClosure *closure,
                                            GValue   *return_value,
                                            gpointer  instance,
                                            va_list   args,
                                            gpointer  marshal_data,
                                            int       n_params,
                                            GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__OBJECT_FLAGS) (gpointer data1,
                                                          gpointer arg1,
                                                          guint arg2,
                                                          gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__OBJECT_FLAGS callback;
  gboolean v_return;
  gpointer arg0;
  guint arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (guint) va_arg (args_copy, guint);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__OBJECT_FLAGS) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       arg1,
                       data2);
  if (arg0 != NULL)
    g_object_unref (arg0);

  g_value_set_boolean (return_value, v_return);
}

/* BOOLEAN:OBJECT,OBJECT */
void
_g_cclosure_marshal_BOOLEAN__OBJECT_OBJECT (GClosure     *closure,
                                            GValue       *return_value,
                                            guint         n_param_values,
                                            const GValue *param_values,
                                            gpointer      invocation_hint G_GNUC_UNUSED,
                                            gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__OBJECT_OBJECT) (gpointer data1,
                                                           gpointer arg1,
                                                           gpointer arg2,
                                                           gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__OBJECT_OBJECT callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__OBJECT_OBJECT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_object (param_values + 1),
                       g_marshal_value_peek_object (param_values + 2),
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__OBJECT_OBJECTv (GClosure *closure,
                                             GValue   *return_value,
                                             gpointer  instance,
                                             va_list   args,
                                             gpointer  marshal_data,
                                             int       n_params,
                                             GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__OBJECT_OBJECT) (gpointer data1,
                                                           gpointer arg1,
                                                           gpointer arg2,
                                                           gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__OBJECT_OBJECT callback;
  gboolean v_return;
  gpointer arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__OBJECT_OBJECT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       arg1,
                       data2);
  if (arg0 != NULL)
    g_object_unref (arg0);
  if (arg1 != NULL)
    g_object_unref (arg1);

  g_value_set_boolean (return_value, v_return);
}

/* BOOLEAN:POINTER,INT */
void
_g_cclosure_marshal_BOOLEAN__POINTER_INT (GClosure     *closure,
                                          GValue       *return_value,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint G_GNUC_UNUSED,
                                          gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__POINTER_INT) (gpointer data1,
                                                         gpointer arg1,
                                                         gint arg2,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__POINTER_INT callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__POINTER_INT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_pointer (param_values + 1),
                       g_marshal_value_peek_int (param_values + 2),
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__POINTER_INTv (GClosure *closure,
                                           GValue   *return_value,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__POINTER_INT) (gpointer data1,
                                                         gpointer arg1,
                                                         gint arg2,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__POINTER_INT callback;
  gboolean v_return;
  gpointer arg0;
  gint arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  arg1 = (gint) va_arg (args_copy, gint);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__POINTER_INT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       arg1,
                       data2);


  g_value_set_boolean (return_value, v_return);
}

/* BOOLEAN:STRING */
void
_g_cclosure_marshal_BOOLEAN__STRING (GClosure     *closure,
                                     GValue       *return_value,
                                     guint         n_param_values,
                                     const GValue *param_values,
                                     gpointer      invocation_hint G_GNUC_UNUSED,
                                     gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__STRING) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__STRING callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 2);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__STRING) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_string (param_values + 1),
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__STRINGv (GClosure *closure,
                                      GValue   *return_value,
                                      gpointer  instance,
                                      va_list   args,
                                      gpointer  marshal_data,
                                      int       n_params,
                                      GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__STRING) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__STRING callback;
  gboolean v_return;
  gpointer arg0;
  va_list args_copy;

  g_return_if_fail (return_value != NULL);

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  va_end (args_copy);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__STRING) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);

  g_value_set_boolean (return_value, v_return);
}

/* BOOLEAN:UINT */
void
_g_cclosure_marshal_BOOLEAN__UINT (GClosure     *closure,
                                   GValue       *return_value,
                                   guint         n_param_values,
                                   const GValue *param_values,
                                   gpointer      invocation_hint G_GNUC_UNUSED,
                                   gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__UINT) (gpointer data1,
                                                  guint arg1,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__UINT callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 2);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__UINT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_uint (param_values + 1),
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__UINTv (GClosure *closure,
                                    GValue   *return_value,
                                    gpointer  instance,
                                    va_list   args,
                                    gpointer  marshal_data,
                                    int       n_params,
                                    GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__UINT) (gpointer data1,
                                                  guint arg1,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__UINT callback;
  gboolean v_return;
  guint arg0;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (guint) va_arg (args_copy, guint);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__UINT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       data2);


  g_value_set_boolean (return_value, v_return);
}

/* BOOLEAN:VOID */
void
_g_cclosure_marshal_BOOLEAN__VOID (GClosure     *closure,
                                   GValue       *return_value,
                                   guint         n_param_values,
                                   const GValue *param_values,
                                   gpointer      invocation_hint G_GNUC_UNUSED,
                                   gpointer      marshal_data)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__VOID) (gpointer data1,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__VOID callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 1);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__VOID) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       data2);

  g_value_set_boolean (return_value, v_return);
}

void
_g_cclosure_marshal_BOOLEAN__VOIDv (GClosure *closure,
                                    GValue   *return_value,
                                    gpointer  instance,
                                    va_list   args,
                                    gpointer  marshal_data,
                                    int       n_params,
                                    GType    *param_types)
{
  typedef gboolean (*GMarshalFunc_BOOLEAN__VOID) (gpointer data1,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_BOOLEAN__VOID callback;
  gboolean v_return;

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_BOOLEAN__VOID) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       data2);


  g_value_set_boolean (return_value, v_return);
}

/* INT:BOXED */
void
_g_cclosure_marshal_INT__BOXED (GClosure     *closure,
                                GValue       *return_value,
                                guint         n_param_values,
                                const GValue *param_values,
                                gpointer      invocation_hint G_GNUC_UNUSED,
                                gpointer      marshal_data)
{
  typedef gint (*GMarshalFunc_INT__BOXED) (gpointer data1,
                                           gpointer arg1,
                                           gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_INT__BOXED callback;
  gint v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 2);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_INT__BOXED) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_boxed (param_values + 1),
                       data2);

  g_value_set_int (return_value, v_return);
}

void
_g_cclosure_marshal_INT__BOXEDv (GClosure *closure,
                                 GValue   *return_value,
                                 gpointer  instance,
                                 va_list   args,
                                 gpointer  marshal_data,
                                 int       n_params,
                                 GType    *param_types)
{
  typedef gint (*GMarshalFunc_INT__BOXED) (gpointer data1,
                                           gpointer arg1,
                                           gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_INT__BOXED callback;
  gint v_return;
  gpointer arg0;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_boxed_copy (param_types[0] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg0);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_INT__BOXED) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_boxed_free (param_types[0] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg0);

  g_value_set_int (return_value, v_return);
}

/* INT:OBJECT */
void
_g_cclosure_marshal_INT__OBJECT (GClosure     *closure,
                                 GValue       *return_value,
                                 guint         n_param_values,
                                 const GValue *param_values,
                                 gpointer      invocation_hint G_GNUC_UNUSED,
                                 gpointer      marshal_data)
{
  typedef gint (*GMarshalFunc_INT__OBJECT) (gpointer data1,
                                            gpointer arg1,
                                            gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_INT__OBJECT callback;
  gint v_return;

  g_return_if_fail (return_value != NULL);
  g_return_if_fail (n_param_values == 2);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_INT__OBJECT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       g_marshal_value_peek_object (param_values + 1),
                       data2);

  g_value_set_int (return_value, v_return);
}

void
_g_cclosure_marshal_INT__OBJECTv (GClosure *closure,
                                  GValue   *return_value,
                                  gpointer  instance,
                                  va_list   args,
                                  gpointer  marshal_data,
                                  int       n_params,
                                  GType    *param_types)
{
  typedef gint (*GMarshalFunc_INT__OBJECT) (gpointer data1,
                                            gpointer arg1,
                                            gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_INT__OBJECT callback;
  gint v_return;
  gpointer arg0;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  va_end (args_copy);

  g_return_if_fail (return_value != NULL);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_INT__OBJECT) (marshal_data ? marshal_data : cc->callback);

  v_return = callback (data1,
                       arg0,
                       data2);
  if (arg0 != NULL)
    g_object_unref (arg0);

  g_value_set_int (return_value, v_return);
}

/* VOID:BOOLEAN,BOXED */
void
_g_cclosure_marshal_VOID__BOOLEAN_BOXED (GClosure     *closure,
                                         GValue       *return_value G_GNUC_UNUSED,
                                         guint         n_param_values,
                                         const GValue *param_values,
                                         gpointer      invocation_hint G_GNUC_UNUSED,
                                         gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__BOOLEAN_BOXED) (gpointer data1,
                                                    gboolean arg1,
                                                    gpointer arg2,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__BOOLEAN_BOXED callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__BOOLEAN_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_boolean (param_values + 1),
            g_marshal_value_peek_boxed (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__BOOLEAN_BOXEDv (GClosure *closure,
                                          GValue   *return_value G_GNUC_UNUSED,
                                          gpointer  instance,
                                          va_list   args,
                                          gpointer  marshal_data,
                                          int       n_params,
                                          GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__BOOLEAN_BOXED) (gpointer data1,
                                                    gboolean arg1,
                                                    gpointer arg2,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__BOOLEAN_BOXED callback;
  gboolean arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gboolean) va_arg (args_copy, gboolean);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_boxed_copy (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__BOOLEAN_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_boxed_free (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
}

/* VOID:ENUM,OBJECT */
void
_g_cclosure_marshal_VOID__ENUM_OBJECT (GClosure     *closure,
                                       GValue       *return_value G_GNUC_UNUSED,
                                       guint         n_param_values,
                                       const GValue *param_values,
                                       gpointer      invocation_hint G_GNUC_UNUSED,
                                       gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__ENUM_OBJECT) (gpointer data1,
                                                  gint arg1,
                                                  gpointer arg2,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__ENUM_OBJECT callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__ENUM_OBJECT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_enum (param_values + 1),
            g_marshal_value_peek_object (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__ENUM_OBJECTv (GClosure *closure,
                                        GValue   *return_value G_GNUC_UNUSED,
                                        gpointer  instance,
                                        va_list   args,
                                        gpointer  marshal_data,
                                        int       n_params,
                                        GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__ENUM_OBJECT) (gpointer data1,
                                                  gint arg1,
                                                  gpointer arg2,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__ENUM_OBJECT callback;
  gint arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gint) va_arg (args_copy, gint);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__ENUM_OBJECT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if (arg1 != NULL)
    g_object_unref (arg1);
}

/* VOID:ENUM,OBJECT,OBJECT */
void
_g_cclosure_marshal_VOID__ENUM_OBJECT_OBJECT (GClosure     *closure,
                                              GValue       *return_value G_GNUC_UNUSED,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint G_GNUC_UNUSED,
                                              gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__ENUM_OBJECT_OBJECT) (gpointer data1,
                                                         gint arg1,
                                                         gpointer arg2,
                                                         gpointer arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__ENUM_OBJECT_OBJECT callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__ENUM_OBJECT_OBJECT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_enum (param_values + 1),
            g_marshal_value_peek_object (param_values + 2),
            g_marshal_value_peek_object (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__ENUM_OBJECT_OBJECTv (GClosure *closure,
                                               GValue   *return_value G_GNUC_UNUSED,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__ENUM_OBJECT_OBJECT) (gpointer data1,
                                                         gint arg1,
                                                         gpointer arg2,
                                                         gpointer arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__ENUM_OBJECT_OBJECT callback;
  gint arg0;
  gpointer arg1;
  gpointer arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gint) va_arg (args_copy, gint);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if (arg2 != NULL)
    arg2 = g_object_ref (arg2);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__ENUM_OBJECT_OBJECT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);
  if (arg1 != NULL)
    g_object_unref (arg1);
  if (arg2 != NULL)
    g_object_unref (arg2);
}

/* VOID:INT,INT,INT */
void
_g_cclosure_marshal_VOID__INT_INT_INT (GClosure     *closure,
                                       GValue       *return_value G_GNUC_UNUSED,
                                       guint         n_param_values,
                                       const GValue *param_values,
                                       gpointer      invocation_hint G_GNUC_UNUSED,
                                       gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__INT_INT_INT) (gpointer data1,
                                                  gint arg1,
                                                  gint arg2,
                                                  gint arg3,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__INT_INT_INT callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__INT_INT_INT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_int (param_values + 1),
            g_marshal_value_peek_int (param_values + 2),
            g_marshal_value_peek_int (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__INT_INT_INTv (GClosure *closure,
                                        GValue   *return_value G_GNUC_UNUSED,
                                        gpointer  instance,
                                        va_list   args,
                                        gpointer  marshal_data,
                                        int       n_params,
                                        GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__INT_INT_INT) (gpointer data1,
                                                  gint arg1,
                                                  gint arg2,
                                                  gint arg3,
                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__INT_INT_INT callback;
  gint arg0;
  gint arg1;
  gint arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gint) va_arg (args_copy, gint);
  arg1 = (gint) va_arg (args_copy, gint);
  arg2 = (gint) va_arg (args_copy, gint);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__INT_INT_INT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);

}

/* VOID:OBJECT,OBJECT */
void
_g_cclosure_marshal_VOID__OBJECT_OBJECT (GClosure     *closure,
                                         GValue       *return_value G_GNUC_UNUSED,
                                         guint         n_param_values,
                                         const GValue *param_values,
                                         gpointer      invocation_hint G_GNUC_UNUSED,
                                         gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer arg2,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_object (param_values + 1),
            g_marshal_value_peek_object (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__OBJECT_OBJECTv (GClosure *closure,
                                          GValue   *return_value G_GNUC_UNUSED,
                                          gpointer  instance,
                                          va_list   args,
                                          gpointer  marshal_data,
                                          int       n_params,
                                          GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer arg2,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT callback;
  gpointer arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if (arg0 != NULL)
    g_object_unref (arg0);
  if (arg1 != NULL)
    g_object_unref (arg1);
}

/* VOID:OBJECT,OBJECT,ENUM */
void
_g_cclosure_marshal_VOID__OBJECT_OBJECT_ENUM (GClosure     *closure,
                                              GValue       *return_value G_GNUC_UNUSED,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint G_GNUC_UNUSED,
                                              gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT_ENUM) (gpointer data1,
                                                         gpointer arg1,
                                                         gpointer arg2,
                                                         gint arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT_ENUM callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT_ENUM) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_object (param_values + 1),
            g_marshal_value_peek_object (param_values + 2),
            g_marshal_value_peek_enum (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__OBJECT_OBJECT_ENUMv (GClosure *closure,
                                               GValue   *return_value G_GNUC_UNUSED,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT_ENUM) (gpointer data1,
                                                         gpointer arg1,
                                                         gpointer arg2,
                                                         gint arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT_ENUM callback;
  gpointer arg0;
  gpointer arg1;
  gint arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  arg2 = (gint) va_arg (args_copy, gint);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT_ENUM) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);
  if (arg0 != NULL)
    g_object_unref (arg0);
  if (arg1 != NULL)
    g_object_unref (arg1);
}

/* VOID:OBJECT,OBJECT,STRING,STRING,VARIANT */
void
_g_cclosure_marshal_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT (GClosure     *closure,
                                                               GValue       *return_value G_GNUC_UNUSED,
                                                               guint         n_param_values,
                                                               const GValue *param_values,
                                                               gpointer      invocation_hint G_GNUC_UNUSED,
                                                               gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT) (gpointer data1,
                                                                          gpointer arg1,
                                                                          gpointer arg2,
                                                                          gpointer arg3,
                                                                          gpointer arg4,
                                                                          gpointer arg5,
                                                                          gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT callback;

  g_return_if_fail (n_param_values == 6);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_object (param_values + 1),
            g_marshal_value_peek_object (param_values + 2),
            g_marshal_value_peek_string (param_values + 3),
            g_marshal_value_peek_string (param_values + 4),
            g_marshal_value_peek_variant (param_values + 5),
            data2);
}

void
_g_cclosure_marshal_VOID__OBJECT_OBJECT_STRING_STRING_VARIANTv (GClosure *closure,
                                                                GValue   *return_value G_GNUC_UNUSED,
                                                                gpointer  instance,
                                                                va_list   args,
                                                                gpointer  marshal_data,
                                                                int       n_params,
                                                                GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT) (gpointer data1,
                                                                          gpointer arg1,
                                                                          gpointer arg2,
                                                                          gpointer arg3,
                                                                          gpointer arg4,
                                                                          gpointer arg5,
                                                                          gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT callback;
  gpointer arg0;
  gpointer arg1;
  gpointer arg2;
  gpointer arg3;
  gpointer arg4;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    arg2 = g_strdup (arg2);
  arg3 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[3] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg3 != NULL)
    arg3 = g_strdup (arg3);
  arg4 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[4] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg4 != NULL)
    arg4 = g_variant_ref_sink (arg4);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            arg3,
            arg4,
            data2);
  if (arg0 != NULL)
    g_object_unref (arg0);
  if (arg1 != NULL)
    g_object_unref (arg1);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    g_free (arg2);
  if ((param_types[3] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg3 != NULL)
    g_free (arg3);
  if ((param_types[4] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg4 != NULL)
    g_variant_unref (arg4);
}

/* VOID:OBJECT,OBJECT,VARIANT,BOXED */
void
_g_cclosure_marshal_VOID__OBJECT_OBJECT_VARIANT_BOXED (GClosure     *closure,
                                                       GValue       *return_value G_GNUC_UNUSED,
                                                       guint         n_param_values,
                                                       const GValue *param_values,
                                                       gpointer      invocation_hint G_GNUC_UNUSED,
                                                       gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT_VARIANT_BOXED) (gpointer data1,
                                                                  gpointer arg1,
                                                                  gpointer arg2,
                                                                  gpointer arg3,
                                                                  gpointer arg4,
                                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT_VARIANT_BOXED callback;

  g_return_if_fail (n_param_values == 5);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT_VARIANT_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_object (param_values + 1),
            g_marshal_value_peek_object (param_values + 2),
            g_marshal_value_peek_variant (param_values + 3),
            g_marshal_value_peek_boxed (param_values + 4),
            data2);
}

void
_g_cclosure_marshal_VOID__OBJECT_OBJECT_VARIANT_BOXEDv (GClosure *closure,
                                                        GValue   *return_value G_GNUC_UNUSED,
                                                        gpointer  instance,
                                                        va_list   args,
                                                        gpointer  marshal_data,
                                                        int       n_params,
                                                        GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_OBJECT_VARIANT_BOXED) (gpointer data1,
                                                                  gpointer arg1,
                                                                  gpointer arg2,
                                                                  gpointer arg3,
                                                                  gpointer arg4,
                                                                  gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_OBJECT_VARIANT_BOXED callback;
  gpointer arg0;
  gpointer arg1;
  gpointer arg2;
  gpointer arg3;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if (arg1 != NULL)
    arg1 = g_object_ref (arg1);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    arg2 = g_variant_ref_sink (arg2);
  arg3 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[3] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg3 != NULL)
    arg3 = g_boxed_copy (param_types[3] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg3);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_OBJECT_VARIANT_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            arg3,
            data2);
  if (arg0 != NULL)
    g_object_unref (arg0);
  if (arg1 != NULL)
    g_object_unref (arg1);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    g_variant_unref (arg2);
  if ((param_types[3] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg3 != NULL)
    g_boxed_free (param_types[3] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg3);
}

/* VOID:OBJECT,VARIANT */
void
_g_cclosure_marshal_VOID__OBJECT_VARIANT (GClosure     *closure,
                                          GValue       *return_value G_GNUC_UNUSED,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint G_GNUC_UNUSED,
                                          gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_VARIANT) (gpointer data1,
                                                     gpointer arg1,
                                                     gpointer arg2,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_VARIANT callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_object (param_values + 1),
            g_marshal_value_peek_variant (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__OBJECT_VARIANTv (GClosure *closure,
                                           GValue   *return_value G_GNUC_UNUSED,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__OBJECT_VARIANT) (gpointer data1,
                                                     gpointer arg1,
                                                     gpointer arg2,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__OBJECT_VARIANT callback;
  gpointer arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if (arg0 != NULL)
    arg0 = g_object_ref (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_variant_ref_sink (arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__OBJECT_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if (arg0 != NULL)
    g_object_unref (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_variant_unref (arg1);
}

/* VOID:POINTER,INT,STRING */
void
_g_cclosure_marshal_VOID__POINTER_INT_STRING (GClosure     *closure,
                                              GValue       *return_value G_GNUC_UNUSED,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint G_GNUC_UNUSED,
                                              gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__POINTER_INT_STRING) (gpointer data1,
                                                         gpointer arg1,
                                                         gint arg2,
                                                         gpointer arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__POINTER_INT_STRING callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__POINTER_INT_STRING) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_pointer (param_values + 1),
            g_marshal_value_peek_int (param_values + 2),
            g_marshal_value_peek_string (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__POINTER_INT_STRINGv (GClosure *closure,
                                               GValue   *return_value G_GNUC_UNUSED,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__POINTER_INT_STRING) (gpointer data1,
                                                         gpointer arg1,
                                                         gint arg2,
                                                         gpointer arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__POINTER_INT_STRING callback;
  gpointer arg0;
  gint arg1;
  gpointer arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  arg1 = (gint) va_arg (args_copy, gint);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    arg2 = g_strdup (arg2);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__POINTER_INT_STRING) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    g_free (arg2);
}

/* VOID:STRING,BOOLEAN */
void
_g_cclosure_marshal_VOID__STRING_BOOLEAN (GClosure     *closure,
                                          GValue       *return_value G_GNUC_UNUSED,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint G_GNUC_UNUSED,
                                          gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_BOOLEAN) (gpointer data1,
                                                     gpointer arg1,
                                                     gboolean arg2,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_BOOLEAN callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_BOOLEAN) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_boolean (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_BOOLEANv (GClosure *closure,
                                           GValue   *return_value G_GNUC_UNUSED,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_BOOLEAN) (gpointer data1,
                                                     gpointer arg1,
                                                     gboolean arg2,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_BOOLEAN callback;
  gpointer arg0;
  gboolean arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gboolean) va_arg (args_copy, gboolean);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_BOOLEAN) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
}

/* VOID:STRING,BOXED */
void
_g_cclosure_marshal_VOID__STRING_BOXED (GClosure     *closure,
                                        GValue       *return_value G_GNUC_UNUSED,
                                        guint         n_param_values,
                                        const GValue *param_values,
                                        gpointer      invocation_hint G_GNUC_UNUSED,
                                        gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_BOXED) (gpointer data1,
                                                   gpointer arg1,
                                                   gpointer arg2,
                                                   gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_BOXED callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_boxed (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_BOXEDv (GClosure *closure,
                                         GValue   *return_value G_GNUC_UNUSED,
                                         gpointer  instance,
                                         va_list   args,
                                         gpointer  marshal_data,
                                         int       n_params,
                                         GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_BOXED) (gpointer data1,
                                                   gpointer arg1,
                                                   gpointer arg2,
                                                   gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_BOXED callback;
  gpointer arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_boxed_copy (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_boxed_free (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
}

/* VOID:STRING,BOXED,BOXED */
void
_g_cclosure_marshal_VOID__STRING_BOXED_BOXED (GClosure     *closure,
                                              GValue       *return_value G_GNUC_UNUSED,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint G_GNUC_UNUSED,
                                              gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_BOXED_BOXED) (gpointer data1,
                                                         gpointer arg1,
                                                         gpointer arg2,
                                                         gpointer arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_BOXED_BOXED callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_BOXED_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_boxed (param_values + 2),
            g_marshal_value_peek_boxed (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_BOXED_BOXEDv (GClosure *closure,
                                               GValue   *return_value G_GNUC_UNUSED,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_BOXED_BOXED) (gpointer data1,
                                                         gpointer arg1,
                                                         gpointer arg2,
                                                         gpointer arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_BOXED_BOXED callback;
  gpointer arg0;
  gpointer arg1;
  gpointer arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_boxed_copy (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    arg2 = g_boxed_copy (param_types[2] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg2);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_BOXED_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_boxed_free (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    g_boxed_free (param_types[2] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg2);
}

/* VOID:STRING,INT64,INT64 */
void
_g_cclosure_marshal_VOID__STRING_INT64_INT64 (GClosure     *closure,
                                              GValue       *return_value G_GNUC_UNUSED,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint G_GNUC_UNUSED,
                                              gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_INT64_INT64) (gpointer data1,
                                                         gpointer arg1,
                                                         gint64 arg2,
                                                         gint64 arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_INT64_INT64 callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_INT64_INT64) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_int64 (param_values + 2),
            g_marshal_value_peek_int64 (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_INT64_INT64v (GClosure *closure,
                                               GValue   *return_value G_GNUC_UNUSED,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_INT64_INT64) (gpointer data1,
                                                         gpointer arg1,
                                                         gint64 arg2,
                                                         gint64 arg3,
                                                         gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_INT64_INT64 callback;
  gpointer arg0;
  gint64 arg1;
  gint64 arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gint64) va_arg (args_copy, gint64);
  arg2 = (gint64) va_arg (args_copy, gint64);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_INT64_INT64) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
}

/* VOID:STRING,STRING,STRING,FLAGS */
void
_g_cclosure_marshal_VOID__STRING_STRING_STRING_FLAGS (GClosure     *closure,
                                                      GValue       *return_value G_GNUC_UNUSED,
                                                      guint         n_param_values,
                                                      const GValue *param_values,
                                                      gpointer      invocation_hint G_GNUC_UNUSED,
                                                      gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_STRING_STRING_FLAGS) (gpointer data1,
                                                                 gpointer arg1,
                                                                 gpointer arg2,
                                                                 gpointer arg3,
                                                                 guint arg4,
                                                                 gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_STRING_STRING_FLAGS callback;

  g_return_if_fail (n_param_values == 5);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_STRING_STRING_FLAGS) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_string (param_values + 2),
            g_marshal_value_peek_string (param_values + 3),
            g_marshal_value_peek_flags (param_values + 4),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_STRING_STRING_FLAGSv (GClosure *closure,
                                                       GValue   *return_value G_GNUC_UNUSED,
                                                       gpointer  instance,
                                                       va_list   args,
                                                       gpointer  marshal_data,
                                                       int       n_params,
                                                       GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_STRING_STRING_FLAGS) (gpointer data1,
                                                                 gpointer arg1,
                                                                 gpointer arg2,
                                                                 gpointer arg3,
                                                                 guint arg4,
                                                                 gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_STRING_STRING_FLAGS callback;
  gpointer arg0;
  gpointer arg1;
  gpointer arg2;
  guint arg3;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_strdup (arg1);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    arg2 = g_strdup (arg2);
  arg3 = (guint) va_arg (args_copy, guint);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_STRING_STRING_FLAGS) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            arg3,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_free (arg1);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    g_free (arg2);
}

/* VOID:STRING,STRING,VARIANT */
void
_g_cclosure_marshal_VOID__STRING_STRING_VARIANT (GClosure     *closure,
                                                 GValue       *return_value G_GNUC_UNUSED,
                                                 guint         n_param_values,
                                                 const GValue *param_values,
                                                 gpointer      invocation_hint G_GNUC_UNUSED,
                                                 gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_STRING_VARIANT) (gpointer data1,
                                                            gpointer arg1,
                                                            gpointer arg2,
                                                            gpointer arg3,
                                                            gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_STRING_VARIANT callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_STRING_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_string (param_values + 2),
            g_marshal_value_peek_variant (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_STRING_VARIANTv (GClosure *closure,
                                                  GValue   *return_value G_GNUC_UNUSED,
                                                  gpointer  instance,
                                                  va_list   args,
                                                  gpointer  marshal_data,
                                                  int       n_params,
                                                  GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_STRING_VARIANT) (gpointer data1,
                                                            gpointer arg1,
                                                            gpointer arg2,
                                                            gpointer arg3,
                                                            gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_STRING_VARIANT callback;
  gpointer arg0;
  gpointer arg1;
  gpointer arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_strdup (arg1);
  arg2 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    arg2 = g_variant_ref_sink (arg2);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_STRING_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_free (arg1);
  if ((param_types[2] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg2 != NULL)
    g_variant_unref (arg2);
}

/* VOID:STRING,VARIANT */
void
_g_cclosure_marshal_VOID__STRING_VARIANT (GClosure     *closure,
                                          GValue       *return_value G_GNUC_UNUSED,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint G_GNUC_UNUSED,
                                          gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__STRING_VARIANT) (gpointer data1,
                                                     gpointer arg1,
                                                     gpointer arg2,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_VARIANT callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_string (param_values + 1),
            g_marshal_value_peek_variant (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__STRING_VARIANTv (GClosure *closure,
                                           GValue   *return_value G_GNUC_UNUSED,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__STRING_VARIANT) (gpointer data1,
                                                     gpointer arg1,
                                                     gpointer arg2,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__STRING_VARIANT callback;
  gpointer arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_strdup (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_variant_ref_sink (arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__STRING_VARIANT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_free (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_variant_unref (arg1);
}

/* VOID:UINT,UINT,UINT */
void
_g_cclosure_marshal_VOID__UINT_UINT_UINT (GClosure     *closure,
                                          GValue       *return_value G_GNUC_UNUSED,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint G_GNUC_UNUSED,
                                          gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__UINT_UINT_UINT) (gpointer data1,
                                                     guint arg1,
                                                     guint arg2,
                                                     guint arg3,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__UINT_UINT_UINT callback;

  g_return_if_fail (n_param_values == 4);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__UINT_UINT_UINT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_uint (param_values + 1),
            g_marshal_value_peek_uint (param_values + 2),
            g_marshal_value_peek_uint (param_values + 3),
            data2);
}

void
_g_cclosure_marshal_VOID__UINT_UINT_UINTv (GClosure *closure,
                                           GValue   *return_value G_GNUC_UNUSED,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__UINT_UINT_UINT) (gpointer data1,
                                                     guint arg1,
                                                     guint arg2,
                                                     guint arg3,
                                                     gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__UINT_UINT_UINT callback;
  guint arg0;
  guint arg1;
  guint arg2;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (guint) va_arg (args_copy, guint);
  arg1 = (guint) va_arg (args_copy, guint);
  arg2 = (guint) va_arg (args_copy, guint);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__UINT_UINT_UINT) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            arg2,
            data2);

}

/* VOID:VARIANT,BOXED */
void
_g_cclosure_marshal_VOID__VARIANT_BOXED (GClosure     *closure,
                                         GValue       *return_value G_GNUC_UNUSED,
                                         guint         n_param_values,
                                         const GValue *param_values,
                                         gpointer      invocation_hint G_GNUC_UNUSED,
                                         gpointer      marshal_data)
{
  typedef void (*GMarshalFunc_VOID__VARIANT_BOXED) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer arg2,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__VARIANT_BOXED callback;

  g_return_if_fail (n_param_values == 3);

  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = g_value_peek_pointer (param_values + 0);
    }
  else
    {
      data1 = g_value_peek_pointer (param_values + 0);
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__VARIANT_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            g_marshal_value_peek_variant (param_values + 1),
            g_marshal_value_peek_boxed (param_values + 2),
            data2);
}

void
_g_cclosure_marshal_VOID__VARIANT_BOXEDv (GClosure *closure,
                                          GValue   *return_value G_GNUC_UNUSED,
                                          gpointer  instance,
                                          va_list   args,
                                          gpointer  marshal_data,
                                          int       n_params,
                                          GType    *param_types)
{
  typedef void (*GMarshalFunc_VOID__VARIANT_BOXED) (gpointer data1,
                                                    gpointer arg1,
                                                    gpointer arg2,
                                                    gpointer data2);
  GCClosure *cc = (GCClosure *) closure;
  gpointer data1, data2;
  GMarshalFunc_VOID__VARIANT_BOXED callback;
  gpointer arg0;
  gpointer arg1;
  va_list args_copy;

  G_VA_COPY (args_copy, args);
  arg0 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    arg0 = g_variant_ref_sink (arg0);
  arg1 = (gpointer) va_arg (args_copy, gpointer);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    arg1 = g_boxed_copy (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
  va_end (args_copy);


  if (G_CCLOSURE_SWAP_DATA (closure))
    {
      data1 = closure->data;
      data2 = instance;
    }
  else
    {
      data1 = instance;
      data2 = closure->data;
    }
  callback = (GMarshalFunc_VOID__VARIANT_BOXED) (marshal_data ? marshal_data : cc->callback);

  callback (data1,
            arg0,
            arg1,
            data2);
  if ((param_types[0] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg0 != NULL)
    g_variant_unref (arg0);
  if ((param_types[1] & G_SIGNAL_TYPE_STATIC_SCOPE) == 0 && arg1 != NULL)
    g_boxed_free (param_types[1] & ~G_SIGNAL_TYPE_STATIC_SCOPE, arg1);
}
