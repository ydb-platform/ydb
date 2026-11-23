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

#include "gparamspecs.h"
#include "gsignalgroup.h"
#include "gvaluetypes.h"

/**
 * SECTION:gsignalgroup
 * @Title: GSignalGroup
 * @Short_description: Manage a collection of signals on a GObject
 *
 * #GSignalGroup manages to simplify the process of connecting
 * many signals to a #GObject as a group. As such there is no API
 * to disconnect a signal from the group.
 *
 * In particular, this allows you to:
 *
 *  - Change the target instance, which automatically causes disconnection
 *    of the signals from the old instance and connecting to the new instance.
 *  - Block and unblock signals as a group
 *  - Ensuring that blocked state transfers across target instances.
 *
 * One place you might want to use such a structure is with #GtkTextView and
 * #GtkTextBuffer. Often times, you'll need to connect to many signals on
 * #GtkTextBuffer from a #GtkTextView subclass. This allows you to create a
 * signal group during instance construction, simply bind the
 * #GtkTextView:buffer property to #GSignalGroup:target and connect
 * all the signals you need. When the #GtkTextView:buffer property changes
 * all of the signals will be transitioned correctly.
 *
 * Since: 2.72
 */

struct _GSignalGroup
{
  GObject     parent_instance;

  GWeakRef    target_ref;
  GRecMutex   mutex;
  GPtrArray  *handlers;
  GType       target_type;
  gssize      block_count;

  guint       has_bound_at_least_once : 1;
};

typedef struct _GSignalGroupClass
{
  GObjectClass parent_class;

  void (*bind) (GSignalGroup *self,
                GObject      *target);
} GSignalGroupClass;

typedef struct
{
  GSignalGroup *group;
  gulong             handler_id;
  GClosure          *closure;
  guint              signal_id;
  GQuark             signal_detail;
  guint              connect_after : 1;
} SignalHandler;

G_DEFINE_TYPE (GSignalGroup, g_signal_group, G_TYPE_OBJECT)

typedef enum
{
  PROP_TARGET = 1,
  PROP_TARGET_TYPE,
  LAST_PROP
} GSignalGroupProperty;

enum
{
  BIND,
  UNBIND,
  LAST_SIGNAL
};

static GParamSpec *properties[LAST_PROP];
static guint signals[LAST_SIGNAL];

static void
g_signal_group_set_target_type (GSignalGroup *self,
                                GType         target_type)
{
  g_assert (G_IS_SIGNAL_GROUP (self));
  g_assert (g_type_is_a (target_type, G_TYPE_OBJECT));

  self->target_type = target_type;

  /* The class must be created at least once for the signals
   * to be registered, otherwise g_signal_parse_name() will fail
   */
  if (G_TYPE_IS_INTERFACE (target_type))
    {
      if (g_type_default_interface_peek (target_type) == NULL)
        g_type_default_interface_unref (g_type_default_interface_ref (target_type));
    }
  else
    {
      if (g_type_class_peek (target_type) == NULL)
        g_type_class_unref (g_type_class_ref (target_type));
    }
}

static void
g_signal_group_gc_handlers (GSignalGroup *self)
{
  guint i;

  g_assert (G_IS_SIGNAL_GROUP (self));

  /*
   * Remove any handlers for which the closures have become invalid. We do
   * this cleanup lazily to avoid situations where we could have disposal
   * active on both the signal group and the peer object.
   */

  for (i = self->handlers->len; i > 0; i--)
    {
      const SignalHandler *handler = g_ptr_array_index (self->handlers, i - 1);

      g_assert (handler != NULL);
      g_assert (handler->closure != NULL);

      if (handler->closure->is_invalid)
        g_ptr_array_remove_index (self->handlers, i - 1);
    }
}

static void
g_signal_group__target_weak_notify (gpointer  data,
                                    GObject  *where_object_was)
{
  GSignalGroup *self = data;
  guint i;

  g_assert (G_IS_SIGNAL_GROUP (self));
  g_assert (where_object_was != NULL);

  g_rec_mutex_lock (&self->mutex);

  g_weak_ref_set (&self->target_ref, NULL);

  for (i = 0; i < self->handlers->len; i++)
    {
      SignalHandler *handler = g_ptr_array_index (self->handlers, i);

      handler->handler_id = 0;
    }

  g_signal_emit (self, signals[UNBIND], 0);
  g_object_notify_by_pspec (G_OBJECT (self), properties[PROP_TARGET]);

  g_rec_mutex_unlock (&self->mutex);
}

static void
g_signal_group_bind_handler (GSignalGroup  *self,
                             SignalHandler *handler,
                             GObject       *target)
{
  gssize i;

  g_assert (self != NULL);
  g_assert (G_IS_OBJECT (target));
  g_assert (handler != NULL);
  g_assert (handler->signal_id != 0);
  g_assert (handler->closure != NULL);
  g_assert (handler->closure->is_invalid == 0);
  g_assert (handler->handler_id == 0);

  handler->handler_id = g_signal_connect_closure_by_id (target,
                                                        handler->signal_id,
                                                        handler->signal_detail,
                                                        handler->closure,
                                                        handler->connect_after);

  g_assert (handler->handler_id != 0);

  for (i = 0; i < self->block_count; i++)
    g_signal_handler_block (target, handler->handler_id);
}

static void
g_signal_group_bind (GSignalGroup *self,
                     GObject      *target)
{
  GObject *hold;
  guint i;

  g_assert (G_IS_SIGNAL_GROUP (self));
  g_assert (!target || G_IS_OBJECT (target));

  if (target == NULL)
    return;

  self->has_bound_at_least_once = TRUE;

  hold = g_object_ref (target);

  g_weak_ref_set (&self->target_ref, hold);
  g_object_weak_ref (hold, g_signal_group__target_weak_notify, self);

  g_signal_group_gc_handlers (self);

  for (i = 0; i < self->handlers->len; i++)
    {
      SignalHandler *handler = g_ptr_array_index (self->handlers, i);

      g_signal_group_bind_handler (self, handler, hold);
    }

  g_signal_emit (self, signals [BIND], 0, hold);

  g_object_unref (hold);
}

static void
g_signal_group_unbind (GSignalGroup *self)
{
  GObject *target;
  guint i;

  g_return_if_fail (G_IS_SIGNAL_GROUP (self));

  target = g_weak_ref_get (&self->target_ref);

  /*
   * Target may be NULL by this point, as we got notified of its destruction.
   * However, if we're early enough, we may get a full reference back and can
   * cleanly disconnect our connections.
   */

  if (target != NULL)
    {
      g_weak_ref_set (&self->target_ref, NULL);

      /*
       * Let go of our weak reference now that we have a full reference
       * for the life of this function.
       */
      g_object_weak_unref (target,
                           g_signal_group__target_weak_notify,
                           self);
    }

  g_signal_group_gc_handlers (self);

  for (i = 0; i < self->handlers->len; i++)
    {
      SignalHandler *handler;
      gulong handler_id;

      handler = g_ptr_array_index (self->handlers, i);

      g_assert (handler != NULL);
      g_assert (handler->signal_id != 0);
      g_assert (handler->closure != NULL);

      handler_id = handler->handler_id;
      handler->handler_id = 0;

      /*
       * If @target is NULL, we lost a race to cleanup the weak
       * instance and the signal connections have already been
       * finalized and therefore nothing to do.
       */

      if (target != NULL && handler_id != 0)
        g_signal_handler_disconnect (target, handler_id);
    }

  g_signal_emit (self, signals [UNBIND], 0);

  g_clear_object (&target);
}

static gboolean
g_signal_group_check_target_type (GSignalGroup *self,
                                  gpointer      target)
{
  if ((target != NULL) &&
      !g_type_is_a (G_OBJECT_TYPE (target), self->target_type))
    {
      g_critical ("Failed to set GSignalGroup of target type %s "
                  "using target %p of type %s",
                  g_type_name (self->target_type),
                  target, G_OBJECT_TYPE_NAME (target));
      return FALSE;
    }

  return TRUE;
}

/**
 * g_signal_group_block:
 * @self: the #GSignalGroup
 *
 * Blocks all signal handlers managed by @self so they will not
 * be called during any signal emissions. Must be unblocked exactly
 * the same number of times it has been blocked to become active again.
 *
 * This blocked state will be kept across changes of the target instance.
 *
 * Since: 2.72
 */
void
g_signal_group_block (GSignalGroup *self)
{
  GObject *target;
  guint i;

  g_return_if_fail (G_IS_SIGNAL_GROUP (self));
  g_return_if_fail (self->block_count >= 0);

  g_rec_mutex_lock (&self->mutex);

  self->block_count++;

  target = g_weak_ref_get (&self->target_ref);

  if (target == NULL)
    goto unlock;

  for (i = 0; i < self->handlers->len; i++)
    {
      const SignalHandler *handler = g_ptr_array_index (self->handlers, i);

      g_assert (handler != NULL);
      g_assert (handler->signal_id != 0);
      g_assert (handler->closure != NULL);
      g_assert (handler->handler_id != 0);

      g_signal_handler_block (target, handler->handler_id);
    }

  g_object_unref (target);

unlock:
  g_rec_mutex_unlock (&self->mutex);
}

/**
 * g_signal_group_unblock:
 * @self: the #GSignalGroup
 *
 * Unblocks all signal handlers managed by @self so they will be
 * called again during any signal emissions unless it is blocked
 * again. Must be unblocked exactly the same number of times it
 * has been blocked to become active again.
 *
 * Since: 2.72
 */
void
g_signal_group_unblock (GSignalGroup *self)
{
  GObject *target;
  guint i;

  g_return_if_fail (G_IS_SIGNAL_GROUP (self));
  g_return_if_fail (self->block_count > 0);

  g_rec_mutex_lock (&self->mutex);

  self->block_count--;

  target = g_weak_ref_get (&self->target_ref);
  if (target == NULL)
    goto unlock;

  for (i = 0; i < self->handlers->len; i++)
    {
      const SignalHandler *handler = g_ptr_array_index (self->handlers, i);

      g_assert (handler != NULL);
      g_assert (handler->signal_id != 0);
      g_assert (handler->closure != NULL);
      g_assert (handler->handler_id != 0);

      g_signal_handler_unblock (target, handler->handler_id);
    }

  g_object_unref (target);

unlock:
  g_rec_mutex_unlock (&self->mutex);
}

/**
 * g_signal_group_dup_target:
 * @self: the #GSignalGroup
 *
 * Gets the target instance used when connecting signals.
 *
 * Returns: (nullable) (transfer full) (type GObject): The target instance
 *
 * Since: 2.72
 */
gpointer
g_signal_group_dup_target (GSignalGroup *self)
{
  GObject *target;

  g_return_val_if_fail (G_IS_SIGNAL_GROUP (self), NULL);

  g_rec_mutex_lock (&self->mutex);
  target = g_weak_ref_get (&self->target_ref);
  g_rec_mutex_unlock (&self->mutex);

  return target;
}

/**
 * g_signal_group_set_target:
 * @self: the #GSignalGroup.
 * @target: (nullable) (type GObject) (transfer none): The target instance used
 *     when connecting signals.
 *
 * Sets the target instance used when connecting signals. Any signal
 * that has been registered with g_signal_group_connect_object() or
 * similar functions will be connected to this object.
 *
 * If the target instance was previously set, signals will be
 * disconnected from that object prior to connecting to @target.
 *
 * Since: 2.72
 */
void
g_signal_group_set_target (GSignalGroup *self,
                           gpointer      target)
{
  GObject *object;

  g_return_if_fail (G_IS_SIGNAL_GROUP (self));

  g_rec_mutex_lock (&self->mutex);

  object = g_weak_ref_get (&self->target_ref);

  if (object == (GObject *)target)
    goto cleanup;

  if (!g_signal_group_check_target_type (self, target))
    goto cleanup;

  /* Only emit unbind if we've ever called bind */
  if (self->has_bound_at_least_once)
    g_signal_group_unbind (self);

  g_signal_group_bind (self, target);

  g_object_notify_by_pspec (G_OBJECT (self), properties[PROP_TARGET]);

cleanup:
  g_clear_object (&object);
  g_rec_mutex_unlock (&self->mutex);
}

static void
signal_handler_free (gpointer data)
{
  SignalHandler *handler = data;

  if (handler->closure != NULL)
    g_closure_invalidate (handler->closure);

  handler->handler_id = 0;
  handler->signal_id = 0;
  handler->signal_detail = 0;
  g_clear_pointer (&handler->closure, g_closure_unref);
  g_slice_free (SignalHandler, handler);
}

static void
g_signal_group_constructed (GObject *object)
{
  GSignalGroup *self = (GSignalGroup *)object;
  GObject *target;

  g_rec_mutex_lock (&self->mutex);

  target = g_weak_ref_get (&self->target_ref);
  if (!g_signal_group_check_target_type (self, target))
    g_signal_group_set_target (self, NULL);

  G_OBJECT_CLASS (g_signal_group_parent_class)->constructed (object);

  g_clear_object (&target);

  g_rec_mutex_unlock (&self->mutex);
}

static void
g_signal_group_dispose (GObject *object)
{
  GSignalGroup *self = (GSignalGroup *)object;

  g_rec_mutex_lock (&self->mutex);

  g_signal_group_gc_handlers (self);

  if (self->has_bound_at_least_once)
    g_signal_group_unbind (self);

  g_clear_pointer (&self->handlers, g_ptr_array_unref);

  g_rec_mutex_unlock (&self->mutex);

  G_OBJECT_CLASS (g_signal_group_parent_class)->dispose (object);
}

static void
g_signal_group_finalize (GObject *object)
{
  GSignalGroup *self = (GSignalGroup *)object;

  g_weak_ref_clear (&self->target_ref);
  g_rec_mutex_clear (&self->mutex);

  G_OBJECT_CLASS (g_signal_group_parent_class)->finalize (object);
}

static void
g_signal_group_get_property (GObject    *object,
                             guint       prop_id,
                             GValue     *value,
                             GParamSpec *pspec)
{
  GSignalGroup *self = G_SIGNAL_GROUP (object);

  switch ((GSignalGroupProperty) prop_id)
    {
    case PROP_TARGET:
      g_value_take_object (value, g_signal_group_dup_target (self));
      break;

    case PROP_TARGET_TYPE:
      g_value_set_gtype (value, self->target_type);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_signal_group_set_property (GObject      *object,
                             guint         prop_id,
                             const GValue *value,
                             GParamSpec   *pspec)
{
  GSignalGroup *self = G_SIGNAL_GROUP (object);

  switch ((GSignalGroupProperty) prop_id)
    {
    case PROP_TARGET:
      g_signal_group_set_target (self, g_value_get_object (value));
      break;

    case PROP_TARGET_TYPE:
      g_signal_group_set_target_type (self, g_value_get_gtype (value));
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
    }
}

static void
g_signal_group_class_init (GSignalGroupClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  object_class->constructed = g_signal_group_constructed;
  object_class->dispose = g_signal_group_dispose;
  object_class->finalize = g_signal_group_finalize;
  object_class->get_property = g_signal_group_get_property;
  object_class->set_property = g_signal_group_set_property;

  /**
   * GSignalGroup:target
   *
   * The target instance used when connecting signals.
   *
   * Since: 2.72
   */
  properties[PROP_TARGET] =
      g_param_spec_object ("target",
                           "Target",
                           "The target instance used when connecting signals.",
                           G_TYPE_OBJECT,
                           (G_PARAM_READWRITE | G_PARAM_EXPLICIT_NOTIFY | G_PARAM_STATIC_STRINGS));

  /**
   * GSignalGroup:target-type
   *
   * The #GType of the target property.
   *
   * Since: 2.72
   */
  properties[PROP_TARGET_TYPE] =
      g_param_spec_gtype ("target-type",
                          "Target Type",
                          "The GType of the target property.",
                          G_TYPE_OBJECT,
                          (G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_properties (object_class, LAST_PROP, properties);

  /**
   * GSignalGroup::bind:
   * @self: the #GSignalGroup
   * @instance: a #GObject containing the new value for #GSignalGroup:target
   *
   * This signal is emitted when #GSignalGroup:target is set to a new value
   * other than %NULL. It is similar to #GObject::notify on `target` except it
   * will not emit when #GSignalGroup:target is %NULL and also allows for
   * receiving the #GObject without a data-race.
   *
   * Since: 2.72
   */
  signals[BIND] =
      g_signal_new ("bind",
                    G_TYPE_FROM_CLASS (klass),
                    G_SIGNAL_RUN_LAST,
                    0,
                    NULL, NULL, NULL,
                    G_TYPE_NONE,
                    1,
                    G_TYPE_OBJECT);

  /**
   * GSignalGroup::unbind:
   * @self: a #GSignalGroup
   *
   * This signal is emitted when the target instance of @self is set to a
   * new #GObject.
   *
   * This signal will only be emitted if the previous target of @self is
   * non-%NULL.
   *
   * Since: 2.72
   */
  signals[UNBIND] =
      g_signal_new ("unbind",
                    G_TYPE_FROM_CLASS (klass),
                    G_SIGNAL_RUN_LAST,
                    0,
                    NULL, NULL, NULL,
                    G_TYPE_NONE,
                    0);
}

static void
g_signal_group_init (GSignalGroup *self)
{
  g_rec_mutex_init (&self->mutex);
  self->handlers = g_ptr_array_new_with_free_func (signal_handler_free);
  self->target_type = G_TYPE_OBJECT;
}

/**
 * g_signal_group_new:
 * @target_type: the #GType of the target instance.
 *
 * Creates a new #GSignalGroup for target instances of @target_type.
 *
 * Returns: (transfer full): a new #GSignalGroup
 *
 * Since: 2.72
 */
GSignalGroup *
g_signal_group_new (GType target_type)
{
  g_return_val_if_fail (g_type_is_a (target_type, G_TYPE_OBJECT), NULL);

  return g_object_new (G_TYPE_SIGNAL_GROUP,
                       "target-type", target_type,
                       NULL);
}

static void
g_signal_group_connect_full (GSignalGroup   *self,
                             const gchar    *detailed_signal,
                             GCallback       c_handler,
                             gpointer        data,
                             GClosureNotify  notify,
                             GConnectFlags   flags,
                             gboolean        is_object)
{
  GObject *target;
  SignalHandler *handler;
  GClosure *closure;
  guint signal_id;
  GQuark signal_detail;

  g_return_if_fail (G_IS_SIGNAL_GROUP (self));
  g_return_if_fail (detailed_signal != NULL);
  g_return_if_fail (g_signal_parse_name (detailed_signal, self->target_type,
                                         &signal_id, &signal_detail, TRUE) != 0);
  g_return_if_fail (c_handler != NULL);
  g_return_if_fail (!is_object || G_IS_OBJECT (data));

  g_rec_mutex_lock (&self->mutex);

  if (self->has_bound_at_least_once)
    {
      g_critical ("Cannot add signals after setting target");
      g_rec_mutex_unlock (&self->mutex);
      return;
    }

  if ((flags & G_CONNECT_SWAPPED) != 0)
    closure = g_cclosure_new_swap (c_handler, data, notify);
  else
    closure = g_cclosure_new (c_handler, data, notify);

  handler = g_slice_new0 (SignalHandler);
  handler->group = self;
  handler->signal_id = signal_id;
  handler->signal_detail = signal_detail;
  handler->closure = g_closure_ref (closure);
  handler->connect_after = ((flags & G_CONNECT_AFTER) != 0);

  g_closure_sink (closure);

  if (is_object)
    {
      /* Set closure->is_invalid when data is disposed. We only track this to avoid
       * reconnecting in the future. However, we do a round of cleanup when ever we
       * connect a new object or the target changes to GC the old handlers.
       */
      g_object_watch_closure (data, closure);
    }

  g_ptr_array_add (self->handlers, handler);

  target = g_weak_ref_get (&self->target_ref);

  if (target != NULL)
    {
      g_signal_group_bind_handler (self, handler, target);
      g_object_unref (target);
    }

  /* Lazily remove any old handlers on connect */
  g_signal_group_gc_handlers (self);

  g_rec_mutex_unlock (&self->mutex);
}

/**
 * g_signal_group_connect_object: (skip)
 * @self: a #GSignalGroup
 * @detailed_signal: a string of the form `signal-name` with optional `::signal-detail`
 * @c_handler: (scope notified): the #GCallback to connect
 * @object: (not nullable) (transfer none): the #GObject to pass as data to @c_handler calls
 * @flags: #GConnectFlags for the signal connection
 *
 * Connects @c_handler to the signal @detailed_signal on #GSignalGroup:target.
 *
 * Ensures that the @object stays alive during the call to @c_handler
 * by temporarily adding a reference count. When the @object is destroyed
 * the signal handler will automatically be removed.
 *
 * You cannot connect a signal handler after #GSignalGroup:target has been set.
 *
 * Since: 2.72
 */
void
g_signal_group_connect_object (GSignalGroup  *self,
                               const gchar   *detailed_signal,
                               GCallback      c_handler,
                               gpointer       object,
                               GConnectFlags  flags)
{
  g_return_if_fail (G_IS_OBJECT (object));

  g_signal_group_connect_full (self, detailed_signal, c_handler, object, NULL,
                               flags, TRUE);
}

/**
 * g_signal_group_connect_data:
 * @self: a #GSignalGroup
 * @detailed_signal: a string of the form "signal-name::detail"
 * @c_handler: (scope notified) (closure data) (destroy notify): the #GCallback to connect
 * @data: the data to pass to @c_handler calls
 * @notify: function to be called when disposing of @self
 * @flags: the flags used to create the signal connection
 *
 * Connects @c_handler to the signal @detailed_signal
 * on the target instance of @self.
 *
 * You cannot connect a signal handler after #GSignalGroup:target has been set.
 *
 * Since: 2.72
 */
void
g_signal_group_connect_data (GSignalGroup   *self,
                             const gchar    *detailed_signal,
                             GCallback       c_handler,
                             gpointer        data,
                             GClosureNotify  notify,
                             GConnectFlags   flags)
{
  g_signal_group_connect_full (self, detailed_signal, c_handler, data, notify,
                               flags, FALSE);
}

/**
 * g_signal_group_connect: (skip)
 * @self: a #GSignalGroup
 * @detailed_signal: a string of the form "signal-name::detail"
 * @c_handler: (scope notified): the #GCallback to connect
 * @data: the data to pass to @c_handler calls
 *
 * Connects @c_handler to the signal @detailed_signal
 * on the target instance of @self.
 *
 * You cannot connect a signal handler after #GSignalGroup:target has been set.
 *
 * Since: 2.72
 */
void
g_signal_group_connect (GSignalGroup *self,
                        const gchar  *detailed_signal,
                        GCallback     c_handler,
                        gpointer      data)
{
  g_signal_group_connect_full (self, detailed_signal, c_handler, data, NULL,
                               0, FALSE);
}

/**
 * g_signal_group_connect_after: (skip)
 * @self: a #GSignalGroup
 * @detailed_signal: a string of the form "signal-name::detail"
 * @c_handler: (scope notified): the #GCallback to connect
 * @data: the data to pass to @c_handler calls
 *
 * Connects @c_handler to the signal @detailed_signal
 * on the target instance of @self.
 *
 * The @c_handler will be called after the default handler of the signal.
 *
 * You cannot connect a signal handler after #GSignalGroup:target has been set.
 *
 * Since: 2.72
 */
void
g_signal_group_connect_after (GSignalGroup *self,
                              const gchar  *detailed_signal,
                              GCallback     c_handler,
                              gpointer      data)
{
  g_signal_group_connect_full (self, detailed_signal, c_handler,
                               data, NULL, G_CONNECT_AFTER, FALSE);
}

/**
 * g_signal_group_connect_swapped:
 * @self: a #GSignalGroup
 * @detailed_signal: a string of the form "signal-name::detail"
 * @c_handler: (scope async): the #GCallback to connect
 * @data: the data to pass to @c_handler calls
 *
 * Connects @c_handler to the signal @detailed_signal
 * on the target instance of @self.
 *
 * The instance on which the signal is emitted and @data
 * will be swapped when calling @c_handler.
 *
 * You cannot connect a signal handler after #GSignalGroup:target has been set.
 *
 * Since: 2.72
 */
void
g_signal_group_connect_swapped (GSignalGroup *self,
                                const gchar  *detailed_signal,
                                GCallback     c_handler,
                                gpointer      data)
{
  g_signal_group_connect_full (self, detailed_signal, c_handler, data, NULL,
                               G_CONNECT_SWAPPED, FALSE);
}
