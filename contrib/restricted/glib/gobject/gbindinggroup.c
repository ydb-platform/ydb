/* GObject - GLib Type, Object, Parameter and Signal Library
 *
 * Copyright (C) 2015-2022 Christian Hergert <christian@hergert.me>
 * Copyright (C) 2015 Garrett Regier <garrettregier@gmail.com>
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
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 */

#include <contrib/restricted/glib/config.h>
#include "glib.h"
#include "glibintl.h"

#include "gbindinggroup.h"
#include "gparamspecs.h"

/**
 * SECTION:gbindinggroup
 * @Title: GBindingGroup
 * @Short_description: Binding multiple properties as a group
 * @include: glib-object.h
 *
 * The #GBindingGroup can be used to bind multiple properties
 * from an object collectively.
 *
 * Use the various methods to bind properties from a single source
 * object to multiple destination objects. Properties can be bound
 * bidirectionally and are connected when the source object is set
 * with g_binding_group_set_source().
 *
 * Since: 2.72
 */

#if 0
# define DEBUG_BINDINGS
#endif

struct _GBindingGroup
{
  GObject    parent_instance;
  GMutex     mutex;
  GObject   *source;         /* (owned weak) */
  GPtrArray *lazy_bindings;  /* (owned) (element-type LazyBinding) */
};

typedef struct _GBindingGroupClass
{
  GObjectClass parent_class;
} GBindingGroupClass;

typedef struct
{
  GBindingGroup      *group;  /* (unowned) */
  const char         *source_property;  /* (interned) */
  const char         *target_property;  /* (interned) */
  GObject            *target;  /* (owned weak) */
  GBinding           *binding;  /* (unowned) */
  gpointer            user_data;
  GDestroyNotify      user_data_destroy;
  gpointer            transform_to;  /* (nullable) (owned) */
  gpointer            transform_from;  /* (nullable) (owned) */
  GBindingFlags       binding_flags;
  guint               using_closures : 1;
} LazyBinding;

G_DEFINE_TYPE (GBindingGroup, g_binding_group, G_TYPE_OBJECT)

typedef enum
{
  PROP_SOURCE = 1,
  N_PROPS
} GBindingGroupProperty;

static void lazy_binding_free (gpointer data);

static GParamSpec *properties[N_PROPS];

static void
g_binding_group_connect (GBindingGroup *self,
                         LazyBinding   *lazy_binding)
{
  GBinding *binding;

  g_assert (G_IS_BINDING_GROUP (self));
  g_assert (self->source != NULL);
  g_assert (lazy_binding != NULL);
  g_assert (lazy_binding->binding == NULL);
  g_assert (lazy_binding->target != NULL);
  g_assert (lazy_binding->target_property != NULL);
  g_assert (lazy_binding->source_property != NULL);

#ifdef DEBUG_BINDINGS
  {
    GFlagsClass *flags_class;
    g_autofree gchar *flags_str = NULL;

    flags_class = g_type_class_ref (G_TYPE_BINDING_FLAGS);
    flags_str = g_flags_to_string (flags_class, lazy_binding->binding_flags);

    g_print ("Binding %s(%p):%s to %s(%p):%s (flags=%s)\n",
             G_OBJECT_TYPE_NAME (self->source),
             self->source,
             lazy_binding->source_property,
             G_OBJECT_TYPE_NAME (lazy_binding->target),
             lazy_binding->target,
             lazy_binding->target_property,
             flags_str);

    g_type_class_unref (flags_class);
  }
#endif

  if (!lazy_binding->using_closures)
    binding = g_object_bind_property_full (self->source,
                                           lazy_binding->source_property,
                                           lazy_binding->target,
                                           lazy_binding->target_property,
                                           lazy_binding->binding_flags,
                                           lazy_binding->transform_to,
                                           lazy_binding->transform_from,
                                           lazy_binding->user_data,
                                           NULL);
  else
    binding = g_object_bind_property_with_closures (self->source,
                                                    lazy_binding->source_property,
                                                    lazy_binding->target,
                                                    lazy_binding->target_property,
                                                    lazy_binding->binding_flags,
                                                    lazy_binding->transform_to,
                                                    lazy_binding->transform_from);

  lazy_binding->binding = binding;
}

static void
g_binding_group_disconnect (LazyBinding *lazy_binding)
{
  g_assert (lazy_binding != NULL);

  if (lazy_binding->binding != NULL)
    {
      g_binding_unbind (lazy_binding->binding);
      lazy_binding->binding = NULL;
    }
}

static void
g_binding_group__source_weak_notify (gpointer  data,
                                     GObject  *where_object_was)
{
  GBindingGroup *self = data;
  guint i;

  g_assert (G_IS_BINDING_GROUP (self));

  g_mutex_lock (&self->mutex);

  self->source = NULL;

  for (i = 0; i < self->lazy_bindings->len; i++)
    {
      LazyBinding *lazy_binding = g_ptr_array_index (self->lazy_bindings, i);

      lazy_binding->binding = NULL;
    }

  g_mutex_unlock (&self->mutex);
}

static void
g_binding_group__target_weak_notify (gpointer  data,
                                     GObject  *where_object_was)
{
  GBindingGroup *self = data;
  LazyBinding *to_free = NULL;
  guint i;

  g_assert (G_IS_BINDING_GROUP (self));

  g_mutex_lock (&self->mutex);

  for (i = 0; i < self->lazy_bindings->len; i++)
    {
      LazyBinding *lazy_binding = g_ptr_array_index (self->lazy_bindings, i);

      if (lazy_binding->target == where_object_was)
        {
          lazy_binding->target = NULL;
          lazy_binding->binding = NULL;

          to_free = g_ptr_array_steal_index_fast (self->lazy_bindings, i);
          break;
        }
    }

  g_mutex_unlock (&self->mutex);

  if (to_free != NULL)
    lazy_binding_free (to_free);
}

static void
lazy_binding_free (gpointer data)
{
  LazyBinding *lazy_binding = data;

  if (lazy_binding->target != NULL)
    {
      g_object_weak_unref (lazy_binding->target,
                           g_binding_group__target_weak_notify,
                           lazy_binding->group);
      lazy_binding->target = NULL;
    }

  g_binding_group_disconnect (lazy_binding);

  lazy_binding->group = NULL;
  lazy_binding->source_property = NULL;
  lazy_binding->target_property = NULL;

  if (lazy_binding->user_data_destroy)
    lazy_binding->user_data_destroy (lazy_binding->user_data);

  if (lazy_binding->using_closures)
    {
      g_clear_pointer (&lazy_binding->transform_to, g_closure_unref);
      g_clear_pointer (&lazy_binding->transform_from, g_closure_unref);
    }

  g_slice_free (LazyBinding, lazy_binding);
}

static void
g_binding_group_dispose (GObject *object)
{
  GBindingGroup *self = (GBindingGroup *)object;
  LazyBinding **lazy_bindings = NULL;
  gsize len = 0;
  gsize i;

  g_assert (G_IS_BINDING_GROUP (self));

  g_mutex_lock (&self->mutex);

  if (self->source != NULL)
    {
      g_object_weak_unref (self->source,
                           g_binding_group__source_weak_notify,
                           self);
      self->source = NULL;
    }

  if (self->lazy_bindings->len > 0)
    lazy_bindings = (LazyBinding **)g_ptr_array_steal (self->lazy_bindings, &len);

  g_mutex_unlock (&self->mutex);

  /* Free bindings without holding self->mutex to avoid re-entrancy
   * from collateral damage through release of binding closure data,
   * GDataList, etc.
   */
  for (i = 0; i < len; i++)
    lazy_binding_free (lazy_bindings[i]);
  g_free (lazy_bindings);

  G_OBJECT_CLASS (g_binding_group_parent_class)->dispose (object);
}

static void
g_binding_group_finalize (GObject *object)
{
  GBindingGroup *self = (GBindingGroup *)object;

  g_assert (self->lazy_bindings != NULL);
  g_assert (self->lazy_bindings->len == 0);

  g_clear_pointer (&self->lazy_bindings, g_ptr_array_unref);
  g_mutex_clear (&self->mutex);

  G_OBJECT_CLASS (g_binding_group_parent_class)->finalize (object);
}

static void
g_binding_group_get_property (GObject    *object,
                              guint       prop_id,
                              GValue     *value,
                              GParamSpec *pspec)
{
  GBindingGroup *self = G_BINDING_GROUP (object);

  switch ((GBindingGroupProperty) prop_id)
    {
    case PROP_SOURCE:
      g_value_take_object (value, g_binding_group_dup_source (self));
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_binding_group_set_property (GObject      *object,
                              guint         prop_id,
                              const GValue *value,
                              GParamSpec   *pspec)
{
  GBindingGroup *self = G_BINDING_GROUP (object);

  switch ((GBindingGroupProperty) prop_id)
    {
    case PROP_SOURCE:
      g_binding_group_set_source (self, g_value_get_object (value));
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_binding_group_class_init (GBindingGroupClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  object_class->dispose = g_binding_group_dispose;
  object_class->finalize = g_binding_group_finalize;
  object_class->get_property = g_binding_group_get_property;
  object_class->set_property = g_binding_group_set_property;

  /**
   * GBindingGroup:source: (nullable)
   *
   * The source object used for binding properties.
   *
   * Since: 2.72
   */
  properties[PROP_SOURCE] =
      g_param_spec_object ("source",
                           "Source",
                           "The source GObject used for binding properties.",
                           G_TYPE_OBJECT,
                           (G_PARAM_READWRITE | G_PARAM_EXPLICIT_NOTIFY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_properties (object_class, N_PROPS, properties);
}

static void
g_binding_group_init (GBindingGroup *self)
{
  g_mutex_init (&self->mutex);
  self->lazy_bindings = g_ptr_array_new_with_free_func (lazy_binding_free);
}

/**
 * g_binding_group_new:
 *
 * Creates a new #GBindingGroup.
 *
 * Returns: (transfer full): a new #GBindingGroup
 *
 * Since: 2.72
 */
GBindingGroup *
g_binding_group_new (void)
{
  return g_object_new (G_TYPE_BINDING_GROUP, NULL);
}

/**
 * g_binding_group_dup_source:
 * @self: the #GBindingGroup
 *
 * Gets the source object used for binding properties.
 *
 * Returns: (transfer none) (nullable) (type GObject): a #GObject or %NULL.
 *
 * Since: 2.72
 */
gpointer
g_binding_group_dup_source (GBindingGroup *self)
{
  GObject *source;

  g_return_val_if_fail (G_IS_BINDING_GROUP (self), NULL);

  g_mutex_lock (&self->mutex);
  source = self->source ? g_object_ref (self->source) : NULL;
  g_mutex_unlock (&self->mutex);

  return source;
}

static gboolean
g_binding_group_check_source (GBindingGroup *self,
                              gpointer       source)
{
  guint i;

  g_assert (G_IS_BINDING_GROUP (self));
  g_assert (!source || G_IS_OBJECT (source));

  for (i = 0; i < self->lazy_bindings->len; i++)
    {
      LazyBinding *lazy_binding = g_ptr_array_index (self->lazy_bindings, i);

      g_return_val_if_fail (g_object_class_find_property (G_OBJECT_GET_CLASS (source),
                                                          lazy_binding->source_property) != NULL,
                            FALSE);
    }

  return TRUE;
}

/**
 * g_binding_group_set_source:
 * @self: the #GBindingGroup
 * @source: (type GObject) (nullable) (transfer none): the source #GObject,
 *   or %NULL to clear it
 *
 * Sets @source as the source object used for creating property
 * bindings. If there is already a source object all bindings from it
 * will be removed.
 *
 * Note that all properties that have been bound must exist on @source.
 *
 * Since: 2.72
 */
void
g_binding_group_set_source (GBindingGroup *self,
                            gpointer       source)
{
  gboolean notify = FALSE;

  g_return_if_fail (G_IS_BINDING_GROUP (self));
  g_return_if_fail (!source || G_IS_OBJECT (source));
  g_return_if_fail (source != (gpointer) self);

  g_mutex_lock (&self->mutex);

  if (source == (gpointer) self->source)
    goto unlock;

  if (self->source != NULL)
    {
      guint i;

      g_object_weak_unref (self->source,
                           g_binding_group__source_weak_notify,
                           self);
      self->source = NULL;

      for (i = 0; i < self->lazy_bindings->len; i++)
        {
          LazyBinding *lazy_binding = g_ptr_array_index (self->lazy_bindings, i);

          g_binding_group_disconnect (lazy_binding);
        }
    }

  if (source != NULL && g_binding_group_check_source (self, source))
    {
      guint i;

      self->source = source;
      g_object_weak_ref (self->source,
                         g_binding_group__source_weak_notify,
                         self);

      for (i = 0; i < self->lazy_bindings->len; i++)
        {
          LazyBinding *lazy_binding;

          lazy_binding = g_ptr_array_index (self->lazy_bindings, i);
          g_binding_group_connect (self, lazy_binding);
        }
    }

  notify = TRUE;

unlock:
  g_mutex_unlock (&self->mutex);

  if (notify)
    g_object_notify_by_pspec (G_OBJECT (self), properties[PROP_SOURCE]);
}

static void
g_binding_group_bind_helper (GBindingGroup  *self,
                             const gchar    *source_property,
                             gpointer        target,
                             const gchar    *target_property,
                             GBindingFlags   flags,
                             gpointer        transform_to,
                             gpointer        transform_from,
                             gpointer        user_data,
                             GDestroyNotify  user_data_destroy,
                             gboolean        using_closures)
{
  LazyBinding *lazy_binding;

  g_return_if_fail (G_IS_BINDING_GROUP (self));
  g_return_if_fail (source_property != NULL);
  g_return_if_fail (self->source == NULL ||
                    g_object_class_find_property (G_OBJECT_GET_CLASS (self->source),
                                                  source_property) != NULL);
  g_return_if_fail (G_IS_OBJECT (target));
  g_return_if_fail (target_property != NULL);
  g_return_if_fail (g_object_class_find_property (G_OBJECT_GET_CLASS (target),
                                                  target_property) != NULL);
  g_return_if_fail (target != (gpointer) self ||
                    strcmp (source_property, target_property) != 0);

  g_mutex_lock (&self->mutex);

  lazy_binding = g_slice_new0 (LazyBinding);
  lazy_binding->group = self;
  lazy_binding->source_property = g_intern_string (source_property);
  lazy_binding->target_property = g_intern_string (target_property);
  lazy_binding->target = target;
  lazy_binding->binding_flags = flags | G_BINDING_SYNC_CREATE;
  lazy_binding->user_data = user_data;
  lazy_binding->user_data_destroy = user_data_destroy;
  lazy_binding->transform_to = transform_to;
  lazy_binding->transform_from = transform_from;

  if (using_closures)
    {
      lazy_binding->using_closures = TRUE;

      if (transform_to != NULL)
        g_closure_sink (g_closure_ref (transform_to));

      if (transform_from != NULL)
        g_closure_sink (g_closure_ref (transform_from));
    }

  g_object_weak_ref (target,
                     g_binding_group__target_weak_notify,
                     self);

  g_ptr_array_add (self->lazy_bindings, lazy_binding);

  if (self->source != NULL)
    g_binding_group_connect (self, lazy_binding);

  g_mutex_unlock (&self->mutex);
}

/**
 * g_binding_group_bind:
 * @self: the #GBindingGroup
 * @source_property: the property on the source to bind
 * @target: (type GObject) (transfer none) (not nullable): the target #GObject
 * @target_property: the property on @target to bind
 * @flags: the flags used to create the #GBinding
 *
 * Creates a binding between @source_property on the source object
 * and @target_property on @target. Whenever the @source_property
 * is changed the @target_property is updated using the same value.
 * The binding flag %G_BINDING_SYNC_CREATE is automatically specified.
 *
 * See g_object_bind_property() for more information.
 *
 * Since: 2.72
 */
void
g_binding_group_bind (GBindingGroup *self,
                      const gchar   *source_property,
                      gpointer       target,
                      const gchar   *target_property,
                      GBindingFlags  flags)
{
  g_binding_group_bind_full (self, source_property,
                             target, target_property,
                             flags,
                             NULL, NULL,
                             NULL, NULL);
}

/**
 * g_binding_group_bind_full:
 * @self: the #GBindingGroup
 * @source_property: the property on the source to bind
 * @target: (type GObject) (transfer none) (not nullable): the target #GObject
 * @target_property: the property on @target to bind
 * @flags: the flags used to create the #GBinding
 * @transform_to: (scope notified) (nullable): the transformation function
 *     from the source object to the @target, or %NULL to use the default
 * @transform_from: (scope notified) (nullable): the transformation function
 *     from the @target to the source object, or %NULL to use the default
 * @user_data: custom data to be passed to the transformation
 *             functions, or %NULL
 * @user_data_destroy: function to be called when disposing the binding,
 *     to free the resources used by the transformation functions
 *
 * Creates a binding between @source_property on the source object and
 * @target_property on @target, allowing you to set the transformation
 * functions to be used by the binding. The binding flag
 * %G_BINDING_SYNC_CREATE is automatically specified.
 *
 * See g_object_bind_property_full() for more information.
 *
 * Since: 2.72
 */
void
g_binding_group_bind_full (GBindingGroup         *self,
                           const gchar           *source_property,
                           gpointer               target,
                           const gchar           *target_property,
                           GBindingFlags          flags,
                           GBindingTransformFunc  transform_to,
                           GBindingTransformFunc  transform_from,
                           gpointer               user_data,
                           GDestroyNotify         user_data_destroy)
{
  g_binding_group_bind_helper (self, source_property,
                               target, target_property,
                               flags,
                               transform_to, transform_from,
                               user_data, user_data_destroy,
                               FALSE);
}

/**
 * g_binding_group_bind_with_closures: (rename-to g_binding_group_bind_full)
 * @self: the #GBindingGroup
 * @source_property: the property on the source to bind
 * @target: (type GObject) (transfer none) (not nullable): the target #GObject
 * @target_property: the property on @target to bind
 * @flags: the flags used to create the #GBinding
 * @transform_to: (nullable) (transfer none): a #GClosure wrapping the
 *     transformation function from the source object to the @target,
 *     or %NULL to use the default
 * @transform_from: (nullable) (transfer none): a #GClosure wrapping the
 *     transformation function from the @target to the source object,
 *     or %NULL to use the default
 *
 * Creates a binding between @source_property on the source object and
 * @target_property on @target, allowing you to set the transformation
 * functions to be used by the binding. The binding flag
 * %G_BINDING_SYNC_CREATE is automatically specified.
 *
 * This function is the language bindings friendly version of
 * g_binding_group_bind_property_full(), using #GClosures
 * instead of function pointers.
 *
 * See g_object_bind_property_with_closures() for more information.
 *
 * Since: 2.72
 */
void
g_binding_group_bind_with_closures (GBindingGroup *self,
                                    const gchar   *source_property,
                                    gpointer       target,
                                    const gchar   *target_property,
                                    GBindingFlags  flags,
                                    GClosure      *transform_to,
                                    GClosure      *transform_from)
{
  g_binding_group_bind_helper (self, source_property,
                               target, target_property,
                               flags,
                               transform_to, transform_from,
                               NULL, NULL,
                               TRUE);
}
