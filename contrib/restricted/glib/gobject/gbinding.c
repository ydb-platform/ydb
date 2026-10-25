/* gbinding.c: Binding for object properties
 *
 * Copyright (C) 2010  Intel Corp.
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
 * Author: Emmanuele Bassi <ebassi@linux.intel.com>
 */

/**
 * SECTION:gbinding
 * @Title: GBinding
 * @Short_Description: Bind two object properties
 *
 * #GBinding is the representation of a binding between a property on a
 * #GObject instance (or source) and another property on another #GObject
 * instance (or target).
 *
 * Whenever the source property changes, the same value is applied to the
 * target property; for instance, the following binding:
 *
 * |[<!-- language="C" -->
 *   g_object_bind_property (object1, "property-a",
 *                           object2, "property-b",
 *                           G_BINDING_DEFAULT);
 * ]|
 *
 * will cause the property named "property-b" of @object2 to be updated
 * every time g_object_set() or the specific accessor changes the value of
 * the property "property-a" of @object1.
 *
 * It is possible to create a bidirectional binding between two properties
 * of two #GObject instances, so that if either property changes, the
 * other is updated as well, for instance:
 *
 * |[<!-- language="C" --> 
 *   g_object_bind_property (object1, "property-a",
 *                           object2, "property-b",
 *                           G_BINDING_BIDIRECTIONAL);
 * ]|
 *
 * will keep the two properties in sync.
 *
 * It is also possible to set a custom transformation function (in both
 * directions, in case of a bidirectional binding) to apply a custom
 * transformation from the source value to the target value before
 * applying it; for instance, the following binding:
 *
 * |[<!-- language="C" --> 
 *   g_object_bind_property_full (adjustment1, "value",
 *                                adjustment2, "value",
 *                                G_BINDING_BIDIRECTIONAL,
 *                                celsius_to_fahrenheit,
 *                                fahrenheit_to_celsius,
 *                                NULL, NULL);
 * ]|
 *
 * will keep the "value" property of the two adjustments in sync; the
 * @celsius_to_fahrenheit function will be called whenever the "value"
 * property of @adjustment1 changes and will transform the current value
 * of the property before applying it to the "value" property of @adjustment2.
 *
 * Vice versa, the @fahrenheit_to_celsius function will be called whenever
 * the "value" property of @adjustment2 changes, and will transform the
 * current value of the property before applying it to the "value" property
 * of @adjustment1.
 *
 * Note that #GBinding does not resolve cycles by itself; a cycle like
 *
 * |[
 *   object1:propertyA -> object2:propertyB
 *   object2:propertyB -> object3:propertyC
 *   object3:propertyC -> object1:propertyA
 * ]|
 *
 * might lead to an infinite loop. The loop, in this particular case,
 * can be avoided if the objects emit the #GObject::notify signal only
 * if the value has effectively been changed. A binding is implemented
 * using the #GObject::notify signal, so it is susceptible to all the
 * various ways of blocking a signal emission, like g_signal_stop_emission()
 * or g_signal_handler_block().
 *
 * A binding will be severed, and the resources it allocates freed, whenever
 * either one of the #GObject instances it refers to are finalized, or when
 * the #GBinding instance loses its last reference.
 *
 * Bindings for languages with garbage collection can use
 * g_binding_unbind() to explicitly release a binding between the source
 * and target properties, instead of relying on the last reference on the
 * binding, source, and target instances to drop.
 *
 * #GBinding is available since GObject 2.26
 */

#include <contrib/restricted/glib/config.h>

#include <string.h>

#include "gbinding.h"
#include "genums.h"
#include "gmarshal.h"
#include "gobject.h"
#include "gsignal.h"
#include "gparamspecs.h"
#include "gvaluetypes.h"

#include "glibintl.h"


GType
g_binding_flags_get_type (void)
{
  static gsize static_g_define_type_id = 0;

  if (g_once_init_enter (&static_g_define_type_id))
    {
      static const GFlagsValue values[] = {
        { G_BINDING_DEFAULT, "G_BINDING_DEFAULT", "default" },
        { G_BINDING_BIDIRECTIONAL, "G_BINDING_BIDIRECTIONAL", "bidirectional" },
        { G_BINDING_SYNC_CREATE, "G_BINDING_SYNC_CREATE", "sync-create" },
        { G_BINDING_INVERT_BOOLEAN, "G_BINDING_INVERT_BOOLEAN", "invert-boolean" },
        { 0, NULL, NULL }
      };
      GType g_define_type_id =
        g_flags_register_static (g_intern_static_string ("GBindingFlags"), values);
      g_once_init_leave (&static_g_define_type_id, g_define_type_id);
    }

  return static_g_define_type_id;
}

/* Reference counted helper struct that is passed to all callbacks to ensure
 * that they never work with already freed objects without having to store
 * strong references for them.
 *
 * Using strong references anywhere is not possible because of the API
 * requirements of GBinding, specifically that the initial reference of the
 * GBinding is owned by the source/target and the caller and can be released
 * either by the source/target being finalized or calling g_binding_unbind().
 *
 * As such, the only strong reference has to be owned by both weak notifies of
 * the source and target and the first to be called has to release it.
 */
typedef struct {
  GWeakRef binding;
  GWeakRef source;
  GWeakRef target;
  gboolean binding_removed;
} BindingContext;

static BindingContext *
binding_context_ref (BindingContext *context)
{
  return g_atomic_rc_box_acquire (context);
}

static void
binding_context_clear (BindingContext *context)
{
  g_weak_ref_clear (&context->binding);
  g_weak_ref_clear (&context->source);
  g_weak_ref_clear (&context->target);
}

static void
binding_context_unref (BindingContext *context)
{
  g_atomic_rc_box_release_full (context, (GDestroyNotify) binding_context_clear);
}

/* Reference counting for the transform functions to ensure that they're always
 * valid while making use of them in the property notify callbacks.
 *
 * The transform functions are released when unbinding but unbinding can happen
 * while the transform functions are currently in use inside the notify callbacks.
 */
typedef struct {
  GBindingTransformFunc transform_s2t;
  GBindingTransformFunc transform_t2s;

  gpointer transform_data;
  GDestroyNotify destroy_notify;
} TransformFunc;

static TransformFunc *
transform_func_new (GBindingTransformFunc transform_s2t,
                    GBindingTransformFunc transform_t2s,
                    gpointer              transform_data,
                    GDestroyNotify        destroy_notify)
{
  TransformFunc *func = g_atomic_rc_box_new0 (TransformFunc);

  func->transform_s2t = transform_s2t;
  func->transform_t2s = transform_t2s;
  func->transform_data = transform_data;
  func->destroy_notify = destroy_notify;

  return func;
}

static TransformFunc *
transform_func_ref (TransformFunc *func)
{
  return g_atomic_rc_box_acquire (func);
}

static void
transform_func_clear (TransformFunc *func)
{
  if (func->destroy_notify)
    func->destroy_notify (func->transform_data);
}

static void
transform_func_unref (TransformFunc *func)
{
  g_atomic_rc_box_release_full (func, (GDestroyNotify) transform_func_clear);
}

#define G_BINDING_CLASS(klass)          (G_TYPE_CHECK_CLASS_CAST ((klass), G_TYPE_BINDING, GBindingClass))
#define G_IS_BINDING_CLASS(klass)       (G_TYPE_CHECK_CLASS_TYPE ((klass), G_TYPE_BINDING))
#define G_BINDING_GET_CLASS(obj)        (G_TYPE_INSTANCE_GET_CLASS ((obj), G_TYPE_BINDING, GBindingClass))

typedef struct _GBindingClass           GBindingClass;

struct _GBinding
{
  GObject parent_instance;

  /* no reference is held on the objects, to avoid cycles */
  BindingContext *context;

  /* protects transform_func, source, target property notify and
   * target_weak_notify_installed for unbinding */
  GMutex unbind_lock;

  /* transform functions, only NULL after unbinding */
  TransformFunc *transform_func; /* LOCK: unbind_lock */

  /* the property names are interned, so they should not be freed */
  const gchar *source_property;
  const gchar *target_property;

  GParamSpec *source_pspec;
  GParamSpec *target_pspec;

  GBindingFlags flags;

  guint source_notify; /* LOCK: unbind_lock */
  guint target_notify; /* LOCK: unbind_lock */
  gboolean target_weak_notify_installed; /* LOCK: unbind_lock */

  /* a guard, to avoid loops */
  guint is_frozen : 1;
};

struct _GBindingClass
{
  GObjectClass parent_class;
};

enum
{
  PROP_0,

  PROP_SOURCE,
  PROP_TARGET,
  PROP_SOURCE_PROPERTY,
  PROP_TARGET_PROPERTY,
  PROP_FLAGS
};

static guint gobject_notify_signal_id;

G_DEFINE_TYPE (GBinding, g_binding, G_TYPE_OBJECT)

static void weak_unbind (gpointer user_data, GObject *where_the_object_was);

/* Must be called with the unbind lock held, context/binding != NULL and strong
 * references to source/target or NULL.
 * Return TRUE if the binding was actually removed and FALSE if it was already
 * removed before. */
static gboolean
unbind_internal_locked (BindingContext *context, GBinding *binding, GObject *source, GObject *target)
{
  gboolean binding_was_removed = FALSE;

  g_assert (context != NULL);
  g_assert (binding != NULL);

  /* If the target went away we still have a strong reference to the source
   * here and can clear it from the binding. Otherwise if the source went away
   * we can clear the target from the binding. Finalizing an object clears its
   * signal handlers and all weak references pointing to it before calling
   * weak notify callbacks.
   *
   * If both still exist we clean up everything set up by the binding.
   */
  if (source)
    {
      /* We always add/remove the source property notify and the weak notify
       * of the source at the same time, and should only ever do that once. */
      if (binding->source_notify != 0)
        {
          g_signal_handler_disconnect (source, binding->source_notify);

          g_object_weak_unref (source, weak_unbind, context);
          binding_context_unref (context);

          binding->source_notify = 0;
        }
      g_weak_ref_set (&context->source, NULL);
    }

  /* As above, but with the target. If source==target then no weak notify was
   * installed for the target, which is why that is stored as a separate
   * boolean inside the binding. */
  if (target)
    {
      /* There might be a target property notify without a weak notify on the
       * target or the other way around, so these have to be handled
       * independently here unlike for the source. */
      if (binding->target_notify != 0)
        {
          g_signal_handler_disconnect (target, binding->target_notify);

          binding->target_notify = 0;
        }
      g_weak_ref_set (&context->target, NULL);

      /* Remove the weak notify from the target, at most once */
      if (binding->target_weak_notify_installed)
        {
          g_object_weak_unref (target, weak_unbind, context);
          binding_context_unref (context);
          binding->target_weak_notify_installed = FALSE;
        }
    }

  /* Make sure to remove the binding only once and return to the caller that
   * this was the call that actually removed it. */
  if (!context->binding_removed)
    {
      context->binding_removed = TRUE;
      binding_was_removed = TRUE;
    }

  return binding_was_removed;
}

/* the basic assumption is that if either the source or the target
 * goes away then the binding does not exist any more and it should
 * be reaped as well. Each weak notify owns a strong reference to the
 * binding that should be dropped here. */
static void
weak_unbind (gpointer  user_data,
             GObject  *where_the_object_was)
{
  BindingContext *context = user_data;
  GBinding *binding;
  GObject *source, *target;
  gboolean binding_was_removed = FALSE;
  TransformFunc *transform_func;

  binding = g_weak_ref_get (&context->binding);
  if (!binding)
    {
      /* The binding was already destroyed before so there's nothing to do */
      binding_context_unref (context);
      return;
    }

  g_mutex_lock (&binding->unbind_lock);

  transform_func = g_steal_pointer (&binding->transform_func);

  source = g_weak_ref_get (&context->source);
  target = g_weak_ref_get (&context->target);

  /* If this is called then either the source or target or both must be in the
   * process of being disposed. If this happens as part of g_object_unref()
   * then the weak references are actually cleared, otherwise if disposing
   * happens as part of g_object_run_dispose() then they would still point to
   * the disposed object.
   *
   * If the object this is being called for is either the source or the target
   * and we actually got a strong reference to it nonetheless (see above),
   * then signal handlers and weak notifies for it are already disconnected
   * and they must not be disconnected a second time. Instead simply clear the
   * weak reference and be done with it.
   *
   * See https://gitlab.gnome.org/GNOME/glib/-/issues/2266 */

  if (source == where_the_object_was)
    {
      g_weak_ref_set (&context->source, NULL);
      g_clear_object (&source);
    }

  if (target == where_the_object_was)
    {
      g_weak_ref_set (&context->target, NULL);
      g_clear_object (&target);
    }

  binding_was_removed = unbind_internal_locked (context, binding, source, target);

  g_mutex_unlock (&binding->unbind_lock);

  /* Unref source, target and transform_func after the mutex is unlocked as it
   * might release the last reference, which then accesses the mutex again */
  g_clear_object (&target);
  g_clear_object (&source);

  g_clear_pointer (&transform_func, transform_func_unref);

  /* This releases the strong reference we got from the weak ref above */
  g_object_unref (binding);

  /* This will take care of the binding itself. */
  if (binding_was_removed)
    g_object_unref (binding);

  /* Each weak notify owns a reference to the binding context. */
  binding_context_unref (context);
}

static gboolean
default_transform (GBinding     *binding,
                   const GValue *value_a,
                   GValue       *value_b,
                   gpointer      user_data G_GNUC_UNUSED)
{
  /* if it's not the same type, try to convert it using the GValue
   * transformation API; otherwise just copy it
   */
  if (!g_type_is_a (G_VALUE_TYPE (value_a), G_VALUE_TYPE (value_b)))
    {
      /* are these two types compatible (can be directly copied)? */
      if (g_value_type_compatible (G_VALUE_TYPE (value_a),
                                   G_VALUE_TYPE (value_b)))
        {
          g_value_copy (value_a, value_b);
          return TRUE;
        }

      if (g_value_type_transformable (G_VALUE_TYPE (value_a),
                                      G_VALUE_TYPE (value_b)))
        {
          if (g_value_transform (value_a, value_b))
            return TRUE;
        }

      g_warning ("%s: Unable to convert a value of type %s to a "
                 "value of type %s",
                 G_STRLOC,
                 g_type_name (G_VALUE_TYPE (value_a)),
                 g_type_name (G_VALUE_TYPE (value_b)));

      return FALSE;
    }

  g_value_copy (value_a, value_b);
  return TRUE;
}

static gboolean
default_invert_boolean_transform (GBinding     *binding,
                                  const GValue *value_a,
                                  GValue       *value_b,
                                  gpointer      user_data G_GNUC_UNUSED)
{
  gboolean value;

  g_assert (G_VALUE_HOLDS_BOOLEAN (value_a));
  g_assert (G_VALUE_HOLDS_BOOLEAN (value_b));

  value = g_value_get_boolean (value_a);
  value = !value;

  g_value_set_boolean (value_b, value);

  return TRUE;
}

static void
on_source_notify (GObject          *source,
                  GParamSpec       *pspec,
                  BindingContext   *context)
{
  GBinding *binding;
  GObject *target;
  TransformFunc *transform_func;
  GValue from_value = G_VALUE_INIT;
  GValue to_value = G_VALUE_INIT;
  gboolean res;

  binding = g_weak_ref_get (&context->binding);
  if (!binding)
    return;

  if (binding->is_frozen)
    {
      g_object_unref (binding);
      return;
    }

  target = g_weak_ref_get (&context->target);
  if (!target)
    {
      g_object_unref (binding);
      return;
    }

  /* Get the transform function safely */
  g_mutex_lock (&binding->unbind_lock);
  if (!binding->transform_func)
    {
      /* it was released already during unbinding, nothing to do here */
      g_mutex_unlock (&binding->unbind_lock);
      return;
    }
  transform_func = transform_func_ref (binding->transform_func);
  g_mutex_unlock (&binding->unbind_lock);

  g_value_init (&from_value, G_PARAM_SPEC_VALUE_TYPE (binding->source_pspec));
  g_value_init (&to_value, G_PARAM_SPEC_VALUE_TYPE (binding->target_pspec));

  g_object_get_property (source, binding->source_pspec->name, &from_value);

  res = transform_func->transform_s2t (binding,
                                       &from_value,
                                       &to_value,
                                       transform_func->transform_data);

  transform_func_unref (transform_func);

  if (res)
    {
      binding->is_frozen = TRUE;

      g_param_value_validate (binding->target_pspec, &to_value);
      g_object_set_property (target, binding->target_pspec->name, &to_value);

      binding->is_frozen = FALSE;
    }

  g_value_unset (&from_value);
  g_value_unset (&to_value);

  g_object_unref (target);
  g_object_unref (binding);
}

static void
on_target_notify (GObject          *target,
                  GParamSpec       *pspec,
                  BindingContext   *context)
{
  GBinding *binding;
  GObject *source;
  TransformFunc *transform_func;
  GValue from_value = G_VALUE_INIT;
  GValue to_value = G_VALUE_INIT;
  gboolean res;

  binding = g_weak_ref_get (&context->binding);
  if (!binding)
    return;

  if (binding->is_frozen)
    {
      g_object_unref (binding);
      return;
    }

  source = g_weak_ref_get (&context->source);
  if (!source)
    {
      g_object_unref (binding);
      return;
    }

  /* Get the transform function safely */
  g_mutex_lock (&binding->unbind_lock);
  if (!binding->transform_func)
    {
      /* it was released already during unbinding, nothing to do here */
      g_mutex_unlock (&binding->unbind_lock);
      return;
    }
  transform_func = transform_func_ref (binding->transform_func);
  g_mutex_unlock (&binding->unbind_lock);

  g_value_init (&from_value, G_PARAM_SPEC_VALUE_TYPE (binding->target_pspec));
  g_value_init (&to_value, G_PARAM_SPEC_VALUE_TYPE (binding->source_pspec));

  g_object_get_property (target, binding->target_pspec->name, &from_value);

  res = transform_func->transform_t2s (binding,
                                       &from_value,
                                       &to_value,
                                       transform_func->transform_data);
  transform_func_unref (transform_func);

  if (res)
    {
      binding->is_frozen = TRUE;

      g_param_value_validate (binding->source_pspec, &to_value);
      g_object_set_property (source, binding->source_pspec->name, &to_value);

      binding->is_frozen = FALSE;
    }

  g_value_unset (&from_value);
  g_value_unset (&to_value);

  g_object_unref (source);
  g_object_unref (binding);
}

static inline void
g_binding_unbind_internal (GBinding *binding,
                           gboolean  unref_binding)
{
  BindingContext *context = binding->context;
  GObject *source, *target;
  gboolean binding_was_removed = FALSE;
  TransformFunc *transform_func;

  g_mutex_lock (&binding->unbind_lock);

  transform_func = g_steal_pointer (&binding->transform_func);

  source = g_weak_ref_get (&context->source);
  target = g_weak_ref_get (&context->target);

  binding_was_removed = unbind_internal_locked (context, binding, source, target);

  g_mutex_unlock (&binding->unbind_lock);

  /* Unref source, target and transform_func after the mutex is unlocked as it
   * might release the last reference, which then accesses the mutex again */
  g_clear_object (&target);
  g_clear_object (&source);

  g_clear_pointer (&transform_func, transform_func_unref);

  if (binding_was_removed && unref_binding)
    g_object_unref (binding);
}

static void
g_binding_finalize (GObject *gobject)
{
  GBinding *binding = G_BINDING (gobject);

  g_binding_unbind_internal (binding, FALSE);

  binding_context_unref (binding->context);

  g_mutex_clear (&binding->unbind_lock);

  G_OBJECT_CLASS (g_binding_parent_class)->finalize (gobject);
}

/* @key must have already been validated with is_valid()
 * Modifies @key in place. */
static void
canonicalize_key (gchar *key)
{
  gchar *p;

  for (p = key; *p != 0; p++)
    {
      gchar c = *p;

      if (c == '_')
        *p = '-';
    }
}

/* @key must have already been validated with is_valid() */
static gboolean
is_canonical (const gchar *key)
{
  return (strchr (key, '_') == NULL);
}

static gboolean
is_valid_property_name (const gchar *key)
{
  const gchar *p;

  /* First character must be a letter. */
  if ((key[0] < 'A' || key[0] > 'Z') &&
      (key[0] < 'a' || key[0] > 'z'))
    return FALSE;

  for (p = key; *p != 0; p++)
    {
      const gchar c = *p;

      if (c != '-' && c != '_' &&
          (c < '0' || c > '9') &&
          (c < 'A' || c > 'Z') &&
          (c < 'a' || c > 'z'))
        return FALSE;
    }

  return TRUE;
}

static void
g_binding_set_property (GObject      *gobject,
                        guint         prop_id,
                        const GValue *value,
                        GParamSpec   *pspec)
{
  GBinding *binding = G_BINDING (gobject);

  switch (prop_id)
    {
    case PROP_SOURCE:
      g_weak_ref_set (&binding->context->source, g_value_get_object (value));
      break;

    case PROP_TARGET:
      g_weak_ref_set (&binding->context->target, g_value_get_object (value));
      break;

    case PROP_SOURCE_PROPERTY:
    case PROP_TARGET_PROPERTY:
      {
        gchar *name_copy = NULL;
        const gchar *name = g_value_get_string (value);
        const gchar **dest;

        /* Ensure the name we intern is canonical. */
        if (!is_canonical (name))
          {
            name_copy = g_value_dup_string (value);
            canonicalize_key (name_copy);
            name = name_copy;
          }

        if (prop_id == PROP_SOURCE_PROPERTY)
          dest = &binding->source_property;
        else
          dest = &binding->target_property;

        *dest = g_intern_string (name);

        g_free (name_copy);
        break;
      }

    case PROP_FLAGS:
      binding->flags = g_value_get_flags (value);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (gobject, prop_id, pspec);
      break;
    }
}

static void
g_binding_get_property (GObject    *gobject,
                        guint       prop_id,
                        GValue     *value,
                        GParamSpec *pspec)
{
  GBinding *binding = G_BINDING (gobject);

  switch (prop_id)
    {
    case PROP_SOURCE:
      g_value_take_object (value, g_weak_ref_get (&binding->context->source));
      break;

    case PROP_SOURCE_PROPERTY:
      /* @source_property is interned, so we don’t need to take a copy */
      g_value_set_interned_string (value, binding->source_property);
      break;

    case PROP_TARGET:
      g_value_take_object (value, g_weak_ref_get (&binding->context->target));
      break;

    case PROP_TARGET_PROPERTY:
      /* @target_property is interned, so we don’t need to take a copy */
      g_value_set_interned_string (value, binding->target_property);
      break;

    case PROP_FLAGS:
      g_value_set_flags (value, binding->flags);
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (gobject, prop_id, pspec);
      break;
    }
}

static void
g_binding_constructed (GObject *gobject)
{
  GBinding *binding = G_BINDING (gobject);
  GBindingTransformFunc transform_func = default_transform;
  GObject *source, *target;
  GQuark source_property_detail;
  GClosure *source_notify_closure;

  /* assert that we were constructed correctly */
  source = g_weak_ref_get (&binding->context->source);
  target = g_weak_ref_get (&binding->context->target);
  g_assert (source != NULL);
  g_assert (target != NULL);
  g_assert (binding->source_property != NULL);
  g_assert (binding->target_property != NULL);

  /* we assume a check was performed prior to construction - since
   * g_object_bind_property_full() does it; we cannot fail construction
   * anyway, so it would be hard for use to properly warn here
   */
  binding->source_pspec = g_object_class_find_property (G_OBJECT_GET_CLASS (source), binding->source_property);
  binding->target_pspec = g_object_class_find_property (G_OBJECT_GET_CLASS (target), binding->target_property);
  g_assert (binding->source_pspec != NULL);
  g_assert (binding->target_pspec != NULL);

  /* switch to the invert boolean transform if needed */
  if (binding->flags & G_BINDING_INVERT_BOOLEAN)
    transform_func = default_invert_boolean_transform;

  /* set the default transformation functions here */
  binding->transform_func = transform_func_new (transform_func, transform_func, NULL, NULL);

  source_property_detail = g_quark_from_string (binding->source_property);
  source_notify_closure = g_cclosure_new (G_CALLBACK (on_source_notify),
                                          binding_context_ref (binding->context),
                                          (GClosureNotify) binding_context_unref);
  binding->source_notify = g_signal_connect_closure_by_id (source,
                                                           gobject_notify_signal_id,
                                                           source_property_detail,
                                                           source_notify_closure,
                                                           FALSE);

  g_object_weak_ref (source, weak_unbind, binding_context_ref (binding->context));

  if (binding->flags & G_BINDING_BIDIRECTIONAL)
    {
      GQuark target_property_detail;
      GClosure *target_notify_closure;

      target_property_detail = g_quark_from_string (binding->target_property);
      target_notify_closure = g_cclosure_new (G_CALLBACK (on_target_notify),
                                              binding_context_ref (binding->context),
                                              (GClosureNotify) binding_context_unref);
      binding->target_notify = g_signal_connect_closure_by_id (target,
                                                               gobject_notify_signal_id,
                                                               target_property_detail,
                                                               target_notify_closure,
                                                               FALSE);
    }

  if (target != source)
    {
      g_object_weak_ref (target, weak_unbind, binding_context_ref (binding->context));

      /* Need to remember separately if a target weak notify was installed as
       * unlike for the source it can exist independently of the property
       * notification callback */
      binding->target_weak_notify_installed = TRUE;
    }

  g_object_unref (source);
  g_object_unref (target);
}

static void
g_binding_class_init (GBindingClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_notify_signal_id = g_signal_lookup ("notify", G_TYPE_OBJECT);
  g_assert (gobject_notify_signal_id != 0);

  gobject_class->constructed = g_binding_constructed;
  gobject_class->set_property = g_binding_set_property;
  gobject_class->get_property = g_binding_get_property;
  gobject_class->finalize = g_binding_finalize;

  /**
   * GBinding:source:
   *
   * The #GObject that should be used as the source of the binding
   *
   * Since: 2.26
   */
  g_object_class_install_property (gobject_class, PROP_SOURCE,
                                   g_param_spec_object ("source",
                                                        P_("Source"),
                                                        P_("The source of the binding"),
                                                        G_TYPE_OBJECT,
                                                        G_PARAM_CONSTRUCT_ONLY |
                                                        G_PARAM_READWRITE |
                                                        G_PARAM_STATIC_STRINGS));
  /**
   * GBinding:target:
   *
   * The #GObject that should be used as the target of the binding
   *
   * Since: 2.26
   */
  g_object_class_install_property (gobject_class, PROP_TARGET,
                                   g_param_spec_object ("target",
                                                        P_("Target"),
                                                        P_("The target of the binding"),
                                                        G_TYPE_OBJECT,
                                                        G_PARAM_CONSTRUCT_ONLY |
                                                        G_PARAM_READWRITE |
                                                        G_PARAM_STATIC_STRINGS));
  /**
   * GBinding:source-property:
   *
   * The name of the property of #GBinding:source that should be used
   * as the source of the binding.
   *
   * This should be in [canonical form][canonical-parameter-names] to get the
   * best performance.
   *
   * Since: 2.26
   */
  g_object_class_install_property (gobject_class, PROP_SOURCE_PROPERTY,
                                   g_param_spec_string ("source-property",
                                                        P_("Source Property"),
                                                        P_("The property on the source to bind"),
                                                        NULL,
                                                        G_PARAM_CONSTRUCT_ONLY |
                                                        G_PARAM_READWRITE |
                                                        G_PARAM_STATIC_STRINGS));
  /**
   * GBinding:target-property:
   *
   * The name of the property of #GBinding:target that should be used
   * as the target of the binding.
   *
   * This should be in [canonical form][canonical-parameter-names] to get the
   * best performance.
   *
   * Since: 2.26
   */
  g_object_class_install_property (gobject_class, PROP_TARGET_PROPERTY,
                                   g_param_spec_string ("target-property",
                                                        P_("Target Property"),
                                                        P_("The property on the target to bind"),
                                                        NULL,
                                                        G_PARAM_CONSTRUCT_ONLY |
                                                        G_PARAM_READWRITE |
                                                        G_PARAM_STATIC_STRINGS));
  /**
   * GBinding:flags:
   *
   * Flags to be used to control the #GBinding
   *
   * Since: 2.26
   */
  g_object_class_install_property (gobject_class, PROP_FLAGS,
                                   g_param_spec_flags ("flags",
                                                       P_("Flags"),
                                                       P_("The binding flags"),
                                                       G_TYPE_BINDING_FLAGS,
                                                       G_BINDING_DEFAULT,
                                                       G_PARAM_CONSTRUCT_ONLY |
                                                       G_PARAM_READWRITE |
                                                       G_PARAM_STATIC_STRINGS));
}

static void
g_binding_init (GBinding *binding)
{
  g_mutex_init (&binding->unbind_lock);

  binding->context = g_atomic_rc_box_new0 (BindingContext);
  g_weak_ref_init (&binding->context->binding, binding);
  g_weak_ref_init (&binding->context->source, NULL);
  g_weak_ref_init (&binding->context->target, NULL);
}

/**
 * g_binding_get_flags:
 * @binding: a #GBinding
 *
 * Retrieves the flags passed when constructing the #GBinding.
 *
 * Returns: the #GBindingFlags used by the #GBinding
 *
 * Since: 2.26
 */
GBindingFlags
g_binding_get_flags (GBinding *binding)
{
  g_return_val_if_fail (G_IS_BINDING (binding), G_BINDING_DEFAULT);

  return binding->flags;
}

/**
 * g_binding_get_source:
 * @binding: a #GBinding
 *
 * Retrieves the #GObject instance used as the source of the binding.
 *
 * A #GBinding can outlive the source #GObject as the binding does not hold a
 * strong reference to the source. If the source is destroyed before the
 * binding then this function will return %NULL.
 *
 * Use g_binding_dup_source() if the source or binding are used from different
 * threads as otherwise the pointer returned from this function might become
 * invalid if the source is finalized from another thread in the meantime.
 *
 * Returns: (transfer none) (nullable): the source #GObject, or %NULL if the
 *     source does not exist any more.
 *
 * Deprecated: 2.68: Use g_binding_dup_source() for a safer version of this
 * function.
 *
 * Since: 2.26
 */
GObject *
g_binding_get_source (GBinding *binding)
{
  GObject *source;

  g_return_val_if_fail (G_IS_BINDING (binding), NULL);

  source = g_weak_ref_get (&binding->context->source);
  /* Unref here, this API is not thread-safe
   * FIXME: Remove this API when we next break API */
  if (source)
    g_object_unref (source);

  return source;
}

/**
 * g_binding_dup_source:
 * @binding: a #GBinding
 *
 * Retrieves the #GObject instance used as the source of the binding.
 *
 * A #GBinding can outlive the source #GObject as the binding does not hold a
 * strong reference to the source. If the source is destroyed before the
 * binding then this function will return %NULL.
 *
 * Returns: (transfer full) (nullable): the source #GObject, or %NULL if the
 *     source does not exist any more.
 *
 * Since: 2.68
 */
GObject *
g_binding_dup_source (GBinding *binding)
{
  g_return_val_if_fail (G_IS_BINDING (binding), NULL);

  return g_weak_ref_get (&binding->context->source);
}

/**
 * g_binding_get_target:
 * @binding: a #GBinding
 *
 * Retrieves the #GObject instance used as the target of the binding.
 *
 * A #GBinding can outlive the target #GObject as the binding does not hold a
 * strong reference to the target. If the target is destroyed before the
 * binding then this function will return %NULL.
 *
 * Use g_binding_dup_target() if the target or binding are used from different
 * threads as otherwise the pointer returned from this function might become
 * invalid if the target is finalized from another thread in the meantime.
 *
 * Returns: (transfer none) (nullable): the target #GObject, or %NULL if the
 *     target does not exist any more.
 *
 * Deprecated: 2.68: Use g_binding_dup_target() for a safer version of this
 * function.
 *
 * Since: 2.26
 */
GObject *
g_binding_get_target (GBinding *binding)
{
  GObject *target;

  g_return_val_if_fail (G_IS_BINDING (binding), NULL);

  target = g_weak_ref_get (&binding->context->target);
  /* Unref here, this API is not thread-safe
   * FIXME: Remove this API when we next break API */
  if (target)
    g_object_unref (target);

  return target;
}

/**
 * g_binding_dup_target:
 * @binding: a #GBinding
 *
 * Retrieves the #GObject instance used as the target of the binding.
 *
 * A #GBinding can outlive the target #GObject as the binding does not hold a
 * strong reference to the target. If the target is destroyed before the
 * binding then this function will return %NULL.
 *
 * Returns: (transfer full) (nullable): the target #GObject, or %NULL if the
 *     target does not exist any more.
 *
 * Since: 2.68
 */
GObject *
g_binding_dup_target (GBinding *binding)
{
  g_return_val_if_fail (G_IS_BINDING (binding), NULL);

  return g_weak_ref_get (&binding->context->target);
}

/**
 * g_binding_get_source_property:
 * @binding: a #GBinding
 *
 * Retrieves the name of the property of #GBinding:source used as the source
 * of the binding.
 *
 * Returns: the name of the source property
 *
 * Since: 2.26
 */
const gchar *
g_binding_get_source_property (GBinding *binding)
{
  g_return_val_if_fail (G_IS_BINDING (binding), NULL);

  return binding->source_property;
}

/**
 * g_binding_get_target_property:
 * @binding: a #GBinding
 *
 * Retrieves the name of the property of #GBinding:target used as the target
 * of the binding.
 *
 * Returns: the name of the target property
 *
 * Since: 2.26
 */
const gchar *
g_binding_get_target_property (GBinding *binding)
{
  g_return_val_if_fail (G_IS_BINDING (binding), NULL);

  return binding->target_property;
}

/**
 * g_binding_unbind:
 * @binding: a #GBinding
 *
 * Explicitly releases the binding between the source and the target
 * property expressed by @binding.
 *
 * This function will release the reference that is being held on
 * the @binding instance if the binding is still bound; if you want to hold on
 * to the #GBinding instance after calling g_binding_unbind(), you will need
 * to hold a reference to it.
 *
 * Note however that this function does not take ownership of @binding, it
 * only unrefs the reference that was initially created by
 * g_object_bind_property() and is owned by the binding.
 *
 * Since: 2.38
 */
void
g_binding_unbind (GBinding *binding)
{
  g_return_if_fail (G_IS_BINDING (binding));

  g_binding_unbind_internal (binding, TRUE);
}

/**
 * g_object_bind_property_full:
 * @source: (type GObject.Object): the source #GObject
 * @source_property: the property on @source to bind
 * @target: (type GObject.Object): the target #GObject
 * @target_property: the property on @target to bind
 * @flags: flags to pass to #GBinding
 * @transform_to: (scope notified) (nullable): the transformation function
 *     from the @source to the @target, or %NULL to use the default
 * @transform_from: (scope notified) (nullable): the transformation function
 *     from the @target to the @source, or %NULL to use the default
 * @user_data: custom data to be passed to the transformation functions,
 *     or %NULL
 * @notify: (nullable): a function to call when disposing the binding, to free
 *     resources used by the transformation functions, or %NULL if not required
 *
 * Complete version of g_object_bind_property().
 *
 * Creates a binding between @source_property on @source and @target_property
 * on @target, allowing you to set the transformation functions to be used by
 * the binding.
 *
 * If @flags contains %G_BINDING_BIDIRECTIONAL then the binding will be mutual:
 * if @target_property on @target changes then the @source_property on @source
 * will be updated as well. The @transform_from function is only used in case
 * of bidirectional bindings, otherwise it will be ignored
 *
 * The binding will automatically be removed when either the @source or the
 * @target instances are finalized. This will release the reference that is
 * being held on the #GBinding instance; if you want to hold on to the
 * #GBinding instance, you will need to hold a reference to it.
 *
 * To remove the binding, call g_binding_unbind().
 *
 * A #GObject can have multiple bindings.
 *
 * The same @user_data parameter will be used for both @transform_to
 * and @transform_from transformation functions; the @notify function will
 * be called once, when the binding is removed. If you need different data
 * for each transformation function, please use
 * g_object_bind_property_with_closures() instead.
 *
 * Returns: (transfer none): the #GBinding instance representing the
 *     binding between the two #GObject instances. The binding is released
 *     whenever the #GBinding reference count reaches zero.
 *
 * Since: 2.26
 */
GBinding *
g_object_bind_property_full (gpointer               source,
                             const gchar           *source_property,
                             gpointer               target,
                             const gchar           *target_property,
                             GBindingFlags          flags,
                             GBindingTransformFunc  transform_to,
                             GBindingTransformFunc  transform_from,
                             gpointer               user_data,
                             GDestroyNotify         notify)
{
  GParamSpec *pspec;
  GBinding *binding;

  g_return_val_if_fail (G_IS_OBJECT (source), NULL);
  g_return_val_if_fail (source_property != NULL, NULL);
  g_return_val_if_fail (is_valid_property_name (source_property), NULL);
  g_return_val_if_fail (G_IS_OBJECT (target), NULL);
  g_return_val_if_fail (target_property != NULL, NULL);
  g_return_val_if_fail (is_valid_property_name (target_property), NULL);

  if (source == target && g_strcmp0 (source_property, target_property) == 0)
    {
      g_warning ("Unable to bind the same property on the same instance");
      return NULL;
    }

  /* remove the G_BINDING_INVERT_BOOLEAN flag in case we have
   * custom transformation functions
   */
  if ((flags & G_BINDING_INVERT_BOOLEAN) &&
      (transform_to != NULL || transform_from != NULL))
    {
      flags &= ~G_BINDING_INVERT_BOOLEAN;
    }

  pspec = g_object_class_find_property (G_OBJECT_GET_CLASS (source), source_property);
  if (pspec == NULL)
    {
      g_warning ("%s: The source object of type %s has no property called '%s'",
                 G_STRLOC,
                 G_OBJECT_TYPE_NAME (source),
                 source_property);
      return NULL;
    }

  if (!(pspec->flags & G_PARAM_READABLE))
    {
      g_warning ("%s: The source object of type %s has no readable property called '%s'",
                 G_STRLOC,
                 G_OBJECT_TYPE_NAME (source),
                 source_property);
      return NULL;
    }

  if ((flags & G_BINDING_BIDIRECTIONAL) &&
      ((pspec->flags & G_PARAM_CONSTRUCT_ONLY) || !(pspec->flags & G_PARAM_WRITABLE)))
    {
      g_warning ("%s: The source object of type %s has no writable property called '%s'",
                 G_STRLOC,
                 G_OBJECT_TYPE_NAME (source),
                 source_property);
      return NULL;
    }

  if ((flags & G_BINDING_INVERT_BOOLEAN) &&
      !(G_PARAM_SPEC_VALUE_TYPE (pspec) == G_TYPE_BOOLEAN))
    {
      g_warning ("%s: The G_BINDING_INVERT_BOOLEAN flag can only be used "
                 "when binding boolean properties; the source property '%s' "
                 "is of type '%s'",
                 G_STRLOC,
                 source_property,
                 g_type_name (G_PARAM_SPEC_VALUE_TYPE (pspec)));
      return NULL;
    }

  pspec = g_object_class_find_property (G_OBJECT_GET_CLASS (target), target_property);
  if (pspec == NULL)
    {
      g_warning ("%s: The target object of type %s has no property called '%s'",
                 G_STRLOC,
                 G_OBJECT_TYPE_NAME (target),
                 target_property);
      return NULL;
    }

  if ((pspec->flags & G_PARAM_CONSTRUCT_ONLY) || !(pspec->flags & G_PARAM_WRITABLE))
    {
      g_warning ("%s: The target object of type %s has no writable property called '%s'",
                 G_STRLOC,
                 G_OBJECT_TYPE_NAME (target),
                 target_property);
      return NULL;
    }

  if ((flags & G_BINDING_BIDIRECTIONAL) &&
      !(pspec->flags & G_PARAM_READABLE))
    {
      g_warning ("%s: The target object of type %s has no readable property called '%s'",
                 G_STRLOC,
                 G_OBJECT_TYPE_NAME (target),
                 target_property);
      return NULL;
    }

  if ((flags & G_BINDING_INVERT_BOOLEAN) &&
      !(G_PARAM_SPEC_VALUE_TYPE (pspec) == G_TYPE_BOOLEAN))
    {
      g_warning ("%s: The G_BINDING_INVERT_BOOLEAN flag can only be used "
                 "when binding boolean properties; the target property '%s' "
                 "is of type '%s'",
                 G_STRLOC,
                 target_property,
                 g_type_name (G_PARAM_SPEC_VALUE_TYPE (pspec)));
      return NULL;
    }

  binding = g_object_new (G_TYPE_BINDING,
                          "source", source,
                          "source-property", source_property,
                          "target", target,
                          "target-property", target_property,
                          "flags", flags,
                          NULL);

  g_assert (binding->transform_func != NULL);

  /* Use default functions if not provided here */
  if (transform_to == NULL)
    transform_to = binding->transform_func->transform_s2t;

  if (transform_from == NULL)
    transform_from = binding->transform_func->transform_t2s;

  g_clear_pointer (&binding->transform_func, transform_func_unref);
  binding->transform_func = transform_func_new (transform_to, transform_from, user_data, notify);

  /* synchronize the target with the source by faking an emission of
   * the ::notify signal for the source property; this will also take
   * care of the bidirectional binding case because the eventual change
   * will emit a notification on the target
   */
  if (flags & G_BINDING_SYNC_CREATE)
    on_source_notify (source, binding->source_pspec, binding->context);

  return binding;
}

/**
 * g_object_bind_property:
 * @source: (type GObject.Object): the source #GObject
 * @source_property: the property on @source to bind
 * @target: (type GObject.Object): the target #GObject
 * @target_property: the property on @target to bind
 * @flags: flags to pass to #GBinding
 *
 * Creates a binding between @source_property on @source and @target_property
 * on @target.
 *
 * Whenever the @source_property is changed the @target_property is
 * updated using the same value. For instance:
 *
 * |[<!-- language="C" -->
 *   g_object_bind_property (action, "active", widget, "sensitive", 0);
 * ]|
 *
 * Will result in the "sensitive" property of the widget #GObject instance to be
 * updated with the same value of the "active" property of the action #GObject
 * instance.
 *
 * If @flags contains %G_BINDING_BIDIRECTIONAL then the binding will be mutual:
 * if @target_property on @target changes then the @source_property on @source
 * will be updated as well.
 *
 * The binding will automatically be removed when either the @source or the
 * @target instances are finalized. To remove the binding without affecting the
 * @source and the @target you can just call g_object_unref() on the returned
 * #GBinding instance.
 *
 * Removing the binding by calling g_object_unref() on it must only be done if
 * the binding, @source and @target are only used from a single thread and it
 * is clear that both @source and @target outlive the binding. Especially it
 * is not safe to rely on this if the binding, @source or @target can be
 * finalized from different threads. Keep another reference to the binding and
 * use g_binding_unbind() instead to be on the safe side.
 *
 * A #GObject can have multiple bindings.
 *
 * Returns: (transfer none): the #GBinding instance representing the
 *     binding between the two #GObject instances. The binding is released
 *     whenever the #GBinding reference count reaches zero.
 *
 * Since: 2.26
 */
GBinding *
g_object_bind_property (gpointer       source,
                        const gchar   *source_property,
                        gpointer       target,
                        const gchar   *target_property,
                        GBindingFlags  flags)
{
  /* type checking is done in g_object_bind_property_full() */

  return g_object_bind_property_full (source, source_property,
                                      target, target_property,
                                      flags,
                                      NULL,
                                      NULL,
                                      NULL, NULL);
}

typedef struct _TransformData
{
  GClosure *transform_to_closure;
  GClosure *transform_from_closure;
} TransformData;

static gboolean
bind_with_closures_transform_to (GBinding     *binding,
                                 const GValue *source,
                                 GValue       *target,
                                 gpointer      data)
{
  TransformData *t_data = data;
  GValue params[3] = { G_VALUE_INIT, G_VALUE_INIT, G_VALUE_INIT };
  GValue retval = G_VALUE_INIT;
  gboolean res;

  g_value_init (&params[0], G_TYPE_BINDING);
  g_value_set_object (&params[0], binding);

  g_value_init (&params[1], G_TYPE_VALUE);
  g_value_set_boxed (&params[1], source);

  g_value_init (&params[2], G_TYPE_VALUE);
  g_value_set_boxed (&params[2], target);

  g_value_init (&retval, G_TYPE_BOOLEAN);
  g_value_set_boolean (&retval, FALSE);

  g_closure_invoke (t_data->transform_to_closure, &retval, 3, params, NULL);

  res = g_value_get_boolean (&retval);
  if (res)
    {
      const GValue *out_value = g_value_get_boxed (&params[2]);

      g_assert (out_value != NULL);

      g_value_copy (out_value, target);
    }

  g_value_unset (&params[0]);
  g_value_unset (&params[1]);
  g_value_unset (&params[2]);
  g_value_unset (&retval);

  return res;
}

static gboolean
bind_with_closures_transform_from (GBinding     *binding,
                                   const GValue *source,
                                   GValue       *target,
                                   gpointer      data)
{
  TransformData *t_data = data;
  GValue params[3] = { G_VALUE_INIT, G_VALUE_INIT, G_VALUE_INIT };
  GValue retval = G_VALUE_INIT;
  gboolean res;

  g_value_init (&params[0], G_TYPE_BINDING);
  g_value_set_object (&params[0], binding);

  g_value_init (&params[1], G_TYPE_VALUE);
  g_value_set_boxed (&params[1], source);

  g_value_init (&params[2], G_TYPE_VALUE);
  g_value_set_boxed (&params[2], target);

  g_value_init (&retval, G_TYPE_BOOLEAN);
  g_value_set_boolean (&retval, FALSE);

  g_closure_invoke (t_data->transform_from_closure, &retval, 3, params, NULL);

  res = g_value_get_boolean (&retval);
  if (res)
    {
      const GValue *out_value = g_value_get_boxed (&params[2]);

      g_assert (out_value != NULL);

      g_value_copy (out_value, target);
    }

  g_value_unset (&params[0]);
  g_value_unset (&params[1]);
  g_value_unset (&params[2]);
  g_value_unset (&retval);

  return res;
}

static void
bind_with_closures_free_func (gpointer data)
{
  TransformData *t_data = data;

  if (t_data->transform_to_closure != NULL)
    g_closure_unref (t_data->transform_to_closure);

  if (t_data->transform_from_closure != NULL)
    g_closure_unref (t_data->transform_from_closure);

  g_slice_free (TransformData, t_data);
}

/**
 * g_object_bind_property_with_closures: (rename-to g_object_bind_property_full)
 * @source: (type GObject.Object): the source #GObject
 * @source_property: the property on @source to bind
 * @target: (type GObject.Object): the target #GObject
 * @target_property: the property on @target to bind
 * @flags: flags to pass to #GBinding
 * @transform_to: a #GClosure wrapping the transformation function
 *     from the @source to the @target, or %NULL to use the default
 * @transform_from: a #GClosure wrapping the transformation function
 *     from the @target to the @source, or %NULL to use the default
 *
 * Creates a binding between @source_property on @source and @target_property
 * on @target, allowing you to set the transformation functions to be used by
 * the binding.
 *
 * This function is the language bindings friendly version of
 * g_object_bind_property_full(), using #GClosures instead of
 * function pointers.
 *
 * Returns: (transfer none): the #GBinding instance representing the
 *     binding between the two #GObject instances. The binding is released
 *     whenever the #GBinding reference count reaches zero.
 *
 * Since: 2.26
 */
GBinding *
g_object_bind_property_with_closures (gpointer       source,
                                      const gchar   *source_property,
                                      gpointer       target,
                                      const gchar   *target_property,
                                      GBindingFlags  flags,
                                      GClosure      *transform_to,
                                      GClosure      *transform_from)
{
  TransformData *data;

  data = g_slice_new0 (TransformData);

  if (transform_to != NULL)
    {
      if (G_CLOSURE_NEEDS_MARSHAL (transform_to))
        g_closure_set_marshal (transform_to, g_cclosure_marshal_BOOLEAN__BOXED_BOXED);

      data->transform_to_closure = g_closure_ref (transform_to);
      g_closure_sink (data->transform_to_closure);
    }

  if (transform_from != NULL)
    {
      if (G_CLOSURE_NEEDS_MARSHAL (transform_from))
        g_closure_set_marshal (transform_from, g_cclosure_marshal_BOOLEAN__BOXED_BOXED);

      data->transform_from_closure = g_closure_ref (transform_from);
      g_closure_sink (data->transform_from_closure);
    }

  return g_object_bind_property_full (source, source_property,
                                      target, target_property,
                                      flags,
                                      transform_to != NULL ? bind_with_closures_transform_to : NULL,
                                      transform_from != NULL ? bind_with_closures_transform_from : NULL,
                                      data,
                                      bind_with_closures_free_func);
}
