from itertools import chain
from typing import Iterable, Optional

from django.contrib.auth.models import Permission
from django.db.models import Model
from django.db.models.query import QuerySet
from django.utils.encoding import force_str

from guardian.conf import settings as guardian_settings
from guardian.ctypes import get_content_type
from guardian.utils import get_group_obj_perms_model, get_identity, get_user_obj_perms_model


def _get_pks_model_and_ctype(objects):
    """
    Returns the primary keys, model and content type of an iterable of Django model objects.
    Assumes that all objects are of the same content type.
    """

    model = None
    ctype = None
    pks = None

    if isinstance(objects, QuerySet):
        model = objects.model
        pks = [force_str(pk) for pk in objects.values_list("pk", flat=True)]
        ctype = get_content_type(model)
    else:
        pks = []
        for idx, obj in enumerate(objects):
            if not idx:
                model = type(obj)
                ctype = get_content_type(model)
            pks.append(force_str(obj.pk))

    return pks, model, ctype


class ObjectPermissionChecker:
    """Generic object permissions checker class being the heart of `django-guardian`.

    Note:
       Once checked for a single object, permissions are stored, and we don't hit
       the database again if another check is called for this object. This is great
       for templates, views or other request-based checks (assuming we don't
       have hundreds of permissions on a single object as we fetch all
       permissions for checked object).

       if we call `has_perm` for perm1/object1, then we
       change permission state and call `has_perm` again for same
       perm1/object1 on the same instance of ObjectPermissionChecker we won't see a
       difference as permissions are already fetched and stored within the cache
       dictionary.
    """

    def __init__(self, user_or_group: Optional[Model] = None) -> None:
        """Constructor for ObjectPermissionChecker.

        Parameters:
            user_or_group (User, AnonymousUser, Group): The user or group to check permissions for.
        """
        self.user, self.group = get_identity(user_or_group)  # type: ignore[arg-type] # None is not allowed
        self._obj_perms_cache: dict = {}

    def has_perm(self, perm: str, obj: Model) -> bool:
        """Checks if user/group has the specified permission for the given object.

        Parameters:
            perm (str): permission as string, may or may not contain app_label
                prefix (if not prefixed, we grab app_label from `obj`)
            obj (Model): Django model instance for which permission should be checked

        Returns:
            True if user/group has the permission, False otherwise
        """
        if self.user and not self.user.is_active:
            return False
        elif self.user and self.user.is_superuser:
            return True
        if "." in perm:
            _, perm = perm.split(".", 1)
        return perm in self.get_perms(obj)

    def get_group_filters(self, obj):
        ctype = get_content_type(obj)
        model = get_group_obj_perms_model(obj)
        related_name = model.permission.field.related_query_name()

        if self.user:
            group_filters = {f"{related_name}__group__in": self.user.groups.all()}
        else:
            group_filters = {f"{related_name}__group": self.group}

        if model.objects.is_generic():
            group_filters.update(
                {
                    "%s__content_type" % related_name: ctype,
                    "%s__object_pk" % related_name: obj.pk,
                }
            )
        else:
            group_filters["%s__content_object" % related_name] = obj

        return group_filters

    def get_user_filters(self, obj: Model):
        ctype = get_content_type(obj)
        model = get_user_obj_perms_model(obj)
        related_name = model.permission.field.related_query_name()  # type: ignore[attr-defined]

        user_filters = {f"{related_name}__user": self.user}

        if model.objects.is_generic():  # type: ignore[attr-defined]
            user_filters.update(
                {
                    "%s__content_type" % related_name: ctype,
                    "%s__object_pk" % related_name: obj.pk,
                }
            )
        else:
            user_filters["%s__content_object" % related_name] = obj

        return user_filters

    def get_user_perms(self, obj: Model) -> QuerySet[Permission]:
        ctype = get_content_type(obj)

        perms_qs = Permission.objects.filter(content_type=ctype)
        user_filters = self.get_user_filters(obj)
        user_perms_qs = perms_qs.filter(**user_filters)
        user_perms: QuerySet[Permission] = user_perms_qs.values_list("codename", flat=True)

        return user_perms

    def get_group_perms(self, obj: Model) -> QuerySet[Permission]:
        ctype = get_content_type(obj)

        perms_qs = Permission.objects.filter(content_type=ctype)
        group_filters = self.get_group_filters(obj)
        group_perms_qs = perms_qs.filter(**group_filters)
        group_perms: QuerySet[Permission] = group_perms_qs.values_list("codename", flat=True)

        return group_perms

    def get_perms(self, obj: Model) -> list[str]:
        """Get a list of permissions for the given object.

        Parameters:
            obj (Model): Django model instance for which permission should be checked.

        Returns:
            list of codenames for all permissions for given `obj`.
        """
        if self.user and not self.user.is_active:
            return []

        if guardian_settings.AUTO_PREFETCH:
            self._prefetch_cache()

        ctype = get_content_type(obj)
        key = self.get_local_cache_key(obj)
        if key not in self._obj_perms_cache:
            # If auto-prefetching enabled, do not hit the database
            if guardian_settings.AUTO_PREFETCH:
                return []
            if self.user and self.user.is_superuser:
                perms: list[str] = list(
                    Permission.objects.filter(content_type=ctype).values_list("codename", flat=True)
                )
            elif self.user:
                # Query user and group permissions separately and then combine
                # the results to avoid a slow query
                user_perms = self.get_user_perms(obj)
                group_perms = self.get_group_perms(obj)
                perms: list[str] = list(set(chain(user_perms, group_perms)))  # type: ignore[no-redef]
            else:
                perms: list[str] = list(set(self.get_group_perms(obj)))  # type: ignore[no-redef]
            self._obj_perms_cache[key] = perms
        return self._obj_perms_cache[key]

    def get_local_cache_key(self, obj: Model) -> tuple:
        """Returns cache key for `_obj_perms_cache` dict."""
        ctype = get_content_type(obj)
        return ctype.id, force_str(obj.pk)

    def prefetch_perms(self, objects: QuerySet):
        """Prefetches the permissions for objects in `objects` and puts them in the cache.

        Parameters:
            objects (list[Model]): Iterable of Django model objects.
        """
        if self.user and not self.user.is_active:
            return []

        pks, model, ctype = _get_pks_model_and_ctype(objects)

        if self.user and self.user.is_superuser:
            perms: Iterable = list(Permission.objects.filter(content_type=ctype).values_list("codename", flat=True))

            for pk in pks:
                key = (ctype.id, force_str(pk))
                self._obj_perms_cache[key] = perms

            return True

        if self.user:
            group_filters = {"group__in": self.user.groups.all()}
        else:
            group_filters = {"group": self.group}

        group_model = get_group_obj_perms_model(model)
        if group_model.objects.is_generic():  # type: ignore[attr-defined]
            group_filters.update(
                {
                    "content_type": ctype,
                    "object_pk__in": pks,
                }
            )
        else:
            group_filters.update({"content_object_id__in": pks})

        if self.user:
            model = get_user_obj_perms_model(model)
            user_filters = {
                "user": self.user,
            }

            if model.objects.is_generic():
                user_filters.update({"content_type": ctype, "object_pk__in": pks})
            else:
                user_filters.update({"content_object_id__in": pks})

            # Query user and group permissions separately and then combine
            # the results to avoid a slow query
            user_perms_qs = model.objects.filter(**user_filters).select_related("permission")
            group_perms_qs = group_model.objects.filter(**group_filters).select_related("permission")
            perms = chain(user_perms_qs, group_perms_qs)
        else:
            perms = group_model.objects.filter(**group_filters).select_related("permission")

        # initialize entry in '_obj_perms_cache' for all prefetched objects
        for obj in objects:
            key = self.get_local_cache_key(obj)
            if key not in self._obj_perms_cache:
                self._obj_perms_cache[key] = []

        for perm in perms:
            if type(perm).objects.is_generic():
                key = (ctype.id, perm.object_pk)
            else:
                key = (ctype.id, force_str(perm.content_object_id))

            self._obj_perms_cache[key].append(perm.permission.codename)

        return True

    @staticmethod
    def _init_obj_prefetch_cache(obj, *querysets):
        cache = {}
        for qs in querysets:
            perms = qs.select_related("permission__codename").values_list(
                "content_type_id", "object_pk", "permission__codename"
            )
            for p in perms:
                if p[:2] not in cache:
                    cache[p[:2]] = []
                cache[p[:2]] += [
                    p[2],
                ]
        obj._guardian_perms_cache = cache
        return obj, cache

    def _prefetch_cache(self):
        from guardian.utils import get_group_obj_perms_model, get_user_obj_perms_model

        UserObjectPermission = get_user_obj_perms_model()
        GroupObjectPermission = get_group_obj_perms_model()
        if self.user:
            obj = self.user
            querysets = [
                UserObjectPermission.objects.filter(user=obj),
                GroupObjectPermission.objects.filter(group__user=obj),
            ]
        else:
            obj = self.group
            querysets = [
                GroupObjectPermission.objects.filter(group=obj),
            ]

        if not hasattr(obj, "_guardian_perms_cache"):
            obj, cache = self._init_obj_prefetch_cache(obj, *querysets)
        else:
            cache = obj._guardian_perms_cache
        self._obj_perms_cache = cache
