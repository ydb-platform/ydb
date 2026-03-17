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

#ifndef ___G_CCLOSURE_MARSHAL_MARSHAL_H__
#define ___G_CCLOSURE_MARSHAL_MARSHAL_H__

#include <glib-object.h>

G_BEGIN_DECLS

/* BOOLEAN:OBJECT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__OBJECT (GClosure     *closure,
                                          GValue       *return_value,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint,
                                          gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__OBJECTv (GClosure *closure,
                                           GValue   *return_value,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types);

/* BOOLEAN:OBJECT,FLAGS */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__OBJECT_FLAGS (GClosure     *closure,
                                                GValue       *return_value,
                                                guint         n_param_values,
                                                const GValue *param_values,
                                                gpointer      invocation_hint,
                                                gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__OBJECT_FLAGSv (GClosure *closure,
                                                 GValue   *return_value,
                                                 gpointer  instance,
                                                 va_list   args,
                                                 gpointer  marshal_data,
                                                 int       n_params,
                                                 GType    *param_types);

/* BOOLEAN:OBJECT,OBJECT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__OBJECT_OBJECT (GClosure     *closure,
                                                 GValue       *return_value,
                                                 guint         n_param_values,
                                                 const GValue *param_values,
                                                 gpointer      invocation_hint,
                                                 gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__OBJECT_OBJECTv (GClosure *closure,
                                                  GValue   *return_value,
                                                  gpointer  instance,
                                                  va_list   args,
                                                  gpointer  marshal_data,
                                                  int       n_params,
                                                  GType    *param_types);

/* BOOLEAN:POINTER,INT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__POINTER_INT (GClosure     *closure,
                                               GValue       *return_value,
                                               guint         n_param_values,
                                               const GValue *param_values,
                                               gpointer      invocation_hint,
                                               gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__POINTER_INTv (GClosure *closure,
                                                GValue   *return_value,
                                                gpointer  instance,
                                                va_list   args,
                                                gpointer  marshal_data,
                                                int       n_params,
                                                GType    *param_types);

/* BOOLEAN:STRING */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__STRING (GClosure     *closure,
                                          GValue       *return_value,
                                          guint         n_param_values,
                                          const GValue *param_values,
                                          gpointer      invocation_hint,
                                          gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__STRINGv (GClosure *closure,
                                           GValue   *return_value,
                                           gpointer  instance,
                                           va_list   args,
                                           gpointer  marshal_data,
                                           int       n_params,
                                           GType    *param_types);

/* BOOLEAN:UINT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__UINT (GClosure     *closure,
                                        GValue       *return_value,
                                        guint         n_param_values,
                                        const GValue *param_values,
                                        gpointer      invocation_hint,
                                        gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__UINTv (GClosure *closure,
                                         GValue   *return_value,
                                         gpointer  instance,
                                         va_list   args,
                                         gpointer  marshal_data,
                                         int       n_params,
                                         GType    *param_types);

/* BOOLEAN:VOID */
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__VOID (GClosure     *closure,
                                        GValue       *return_value,
                                        guint         n_param_values,
                                        const GValue *param_values,
                                        gpointer      invocation_hint,
                                        gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_BOOLEAN__VOIDv (GClosure *closure,
                                         GValue   *return_value,
                                         gpointer  instance,
                                         va_list   args,
                                         gpointer  marshal_data,
                                         int       n_params,
                                         GType    *param_types);

/* INT:BOXED */
G_GNUC_INTERNAL
void _g_cclosure_marshal_INT__BOXED (GClosure     *closure,
                                     GValue       *return_value,
                                     guint         n_param_values,
                                     const GValue *param_values,
                                     gpointer      invocation_hint,
                                     gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_INT__BOXEDv (GClosure *closure,
                                      GValue   *return_value,
                                      gpointer  instance,
                                      va_list   args,
                                      gpointer  marshal_data,
                                      int       n_params,
                                      GType    *param_types);

/* INT:OBJECT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_INT__OBJECT (GClosure     *closure,
                                      GValue       *return_value,
                                      guint         n_param_values,
                                      const GValue *param_values,
                                      gpointer      invocation_hint,
                                      gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_INT__OBJECTv (GClosure *closure,
                                       GValue   *return_value,
                                       gpointer  instance,
                                       va_list   args,
                                       gpointer  marshal_data,
                                       int       n_params,
                                       GType    *param_types);

/* VOID:BOOLEAN,BOXED */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__BOOLEAN_BOXED (GClosure     *closure,
                                              GValue       *return_value,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint,
                                              gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__BOOLEAN_BOXEDv (GClosure *closure,
                                               GValue   *return_value,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types);

/* VOID:ENUM,OBJECT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__ENUM_OBJECT (GClosure     *closure,
                                            GValue       *return_value,
                                            guint         n_param_values,
                                            const GValue *param_values,
                                            gpointer      invocation_hint,
                                            gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__ENUM_OBJECTv (GClosure *closure,
                                             GValue   *return_value,
                                             gpointer  instance,
                                             va_list   args,
                                             gpointer  marshal_data,
                                             int       n_params,
                                             GType    *param_types);

/* VOID:ENUM,OBJECT,OBJECT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__ENUM_OBJECT_OBJECT (GClosure     *closure,
                                                   GValue       *return_value,
                                                   guint         n_param_values,
                                                   const GValue *param_values,
                                                   gpointer      invocation_hint,
                                                   gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__ENUM_OBJECT_OBJECTv (GClosure *closure,
                                                    GValue   *return_value,
                                                    gpointer  instance,
                                                    va_list   args,
                                                    gpointer  marshal_data,
                                                    int       n_params,
                                                    GType    *param_types);

/* VOID:INT,INT,INT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__INT_INT_INT (GClosure     *closure,
                                            GValue       *return_value,
                                            guint         n_param_values,
                                            const GValue *param_values,
                                            gpointer      invocation_hint,
                                            gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__INT_INT_INTv (GClosure *closure,
                                             GValue   *return_value,
                                             gpointer  instance,
                                             va_list   args,
                                             gpointer  marshal_data,
                                             int       n_params,
                                             GType    *param_types);

/* VOID:OBJECT,OBJECT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT (GClosure     *closure,
                                              GValue       *return_value,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint,
                                              gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECTv (GClosure *closure,
                                               GValue   *return_value,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types);

/* VOID:OBJECT,OBJECT,ENUM */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT_ENUM (GClosure     *closure,
                                                   GValue       *return_value,
                                                   guint         n_param_values,
                                                   const GValue *param_values,
                                                   gpointer      invocation_hint,
                                                   gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT_ENUMv (GClosure *closure,
                                                    GValue   *return_value,
                                                    gpointer  instance,
                                                    va_list   args,
                                                    gpointer  marshal_data,
                                                    int       n_params,
                                                    GType    *param_types);

/* VOID:OBJECT,OBJECT,STRING,STRING,VARIANT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT_STRING_STRING_VARIANT (GClosure     *closure,
                                                                    GValue       *return_value,
                                                                    guint         n_param_values,
                                                                    const GValue *param_values,
                                                                    gpointer      invocation_hint,
                                                                    gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT_STRING_STRING_VARIANTv (GClosure *closure,
                                                                     GValue   *return_value,
                                                                     gpointer  instance,
                                                                     va_list   args,
                                                                     gpointer  marshal_data,
                                                                     int       n_params,
                                                                     GType    *param_types);

/* VOID:OBJECT,OBJECT,VARIANT,BOXED */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT_VARIANT_BOXED (GClosure     *closure,
                                                            GValue       *return_value,
                                                            guint         n_param_values,
                                                            const GValue *param_values,
                                                            gpointer      invocation_hint,
                                                            gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_OBJECT_VARIANT_BOXEDv (GClosure *closure,
                                                             GValue   *return_value,
                                                             gpointer  instance,
                                                             va_list   args,
                                                             gpointer  marshal_data,
                                                             int       n_params,
                                                             GType    *param_types);

/* VOID:OBJECT,VARIANT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_VARIANT (GClosure     *closure,
                                               GValue       *return_value,
                                               guint         n_param_values,
                                               const GValue *param_values,
                                               gpointer      invocation_hint,
                                               gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__OBJECT_VARIANTv (GClosure *closure,
                                                GValue   *return_value,
                                                gpointer  instance,
                                                va_list   args,
                                                gpointer  marshal_data,
                                                int       n_params,
                                                GType    *param_types);

/* VOID:POINTER,INT,STRING */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__POINTER_INT_STRING (GClosure     *closure,
                                                   GValue       *return_value,
                                                   guint         n_param_values,
                                                   const GValue *param_values,
                                                   gpointer      invocation_hint,
                                                   gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__POINTER_INT_STRINGv (GClosure *closure,
                                                    GValue   *return_value,
                                                    gpointer  instance,
                                                    va_list   args,
                                                    gpointer  marshal_data,
                                                    int       n_params,
                                                    GType    *param_types);

/* VOID:STRING,BOOLEAN */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_BOOLEAN (GClosure     *closure,
                                               GValue       *return_value,
                                               guint         n_param_values,
                                               const GValue *param_values,
                                               gpointer      invocation_hint,
                                               gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_BOOLEANv (GClosure *closure,
                                                GValue   *return_value,
                                                gpointer  instance,
                                                va_list   args,
                                                gpointer  marshal_data,
                                                int       n_params,
                                                GType    *param_types);

/* VOID:STRING,BOXED */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_BOXED (GClosure     *closure,
                                             GValue       *return_value,
                                             guint         n_param_values,
                                             const GValue *param_values,
                                             gpointer      invocation_hint,
                                             gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_BOXEDv (GClosure *closure,
                                              GValue   *return_value,
                                              gpointer  instance,
                                              va_list   args,
                                              gpointer  marshal_data,
                                              int       n_params,
                                              GType    *param_types);

/* VOID:STRING,BOXED,BOXED */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_BOXED_BOXED (GClosure     *closure,
                                                   GValue       *return_value,
                                                   guint         n_param_values,
                                                   const GValue *param_values,
                                                   gpointer      invocation_hint,
                                                   gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_BOXED_BOXEDv (GClosure *closure,
                                                    GValue   *return_value,
                                                    gpointer  instance,
                                                    va_list   args,
                                                    gpointer  marshal_data,
                                                    int       n_params,
                                                    GType    *param_types);

/* VOID:STRING,INT64,INT64 */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_INT64_INT64 (GClosure     *closure,
                                                   GValue       *return_value,
                                                   guint         n_param_values,
                                                   const GValue *param_values,
                                                   gpointer      invocation_hint,
                                                   gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_INT64_INT64v (GClosure *closure,
                                                    GValue   *return_value,
                                                    gpointer  instance,
                                                    va_list   args,
                                                    gpointer  marshal_data,
                                                    int       n_params,
                                                    GType    *param_types);

/* VOID:STRING,STRING,STRING,FLAGS */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_STRING_STRING_FLAGS (GClosure     *closure,
                                                           GValue       *return_value,
                                                           guint         n_param_values,
                                                           const GValue *param_values,
                                                           gpointer      invocation_hint,
                                                           gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_STRING_STRING_FLAGSv (GClosure *closure,
                                                            GValue   *return_value,
                                                            gpointer  instance,
                                                            va_list   args,
                                                            gpointer  marshal_data,
                                                            int       n_params,
                                                            GType    *param_types);

/* VOID:STRING,STRING,VARIANT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_STRING_VARIANT (GClosure     *closure,
                                                      GValue       *return_value,
                                                      guint         n_param_values,
                                                      const GValue *param_values,
                                                      gpointer      invocation_hint,
                                                      gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_STRING_VARIANTv (GClosure *closure,
                                                       GValue   *return_value,
                                                       gpointer  instance,
                                                       va_list   args,
                                                       gpointer  marshal_data,
                                                       int       n_params,
                                                       GType    *param_types);

/* VOID:STRING,VARIANT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_VARIANT (GClosure     *closure,
                                               GValue       *return_value,
                                               guint         n_param_values,
                                               const GValue *param_values,
                                               gpointer      invocation_hint,
                                               gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__STRING_VARIANTv (GClosure *closure,
                                                GValue   *return_value,
                                                gpointer  instance,
                                                va_list   args,
                                                gpointer  marshal_data,
                                                int       n_params,
                                                GType    *param_types);

/* VOID:UINT,UINT,UINT */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__UINT_UINT_UINT (GClosure     *closure,
                                               GValue       *return_value,
                                               guint         n_param_values,
                                               const GValue *param_values,
                                               gpointer      invocation_hint,
                                               gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__UINT_UINT_UINTv (GClosure *closure,
                                                GValue   *return_value,
                                                gpointer  instance,
                                                va_list   args,
                                                gpointer  marshal_data,
                                                int       n_params,
                                                GType    *param_types);

/* VOID:VARIANT,BOXED */
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__VARIANT_BOXED (GClosure     *closure,
                                              GValue       *return_value,
                                              guint         n_param_values,
                                              const GValue *param_values,
                                              gpointer      invocation_hint,
                                              gpointer      marshal_data);
G_GNUC_INTERNAL
void _g_cclosure_marshal_VOID__VARIANT_BOXEDv (GClosure *closure,
                                               GValue   *return_value,
                                               gpointer  instance,
                                               va_list   args,
                                               gpointer  marshal_data,
                                               int       n_params,
                                               GType    *param_types);


G_END_DECLS

#endif /* ___G_CCLOSURE_MARSHAL_MARSHAL_H__ */
