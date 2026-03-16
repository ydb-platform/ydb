from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from guardian.compat import user_model_label
from guardian.ctypes import get_content_type
from guardian.managers import GroupObjectPermissionManager, UserObjectPermissionManager


class BaseObjectPermission(models.Model):
    """Base ObjectPermission model.

    Child classed should additionally define a `content_object` field and either `user` or `group` field.

    See Also:
        `UserObjectPermission` and `GroupObjectPermission`
    """

    permission = models.ForeignKey(Permission, on_delete=models.CASCADE)

    class Meta:
        abstract = True

    def __str__(self) -> str:
        return "{} | {} | {}".format(
            str(self.content_object), str(getattr(self, "user", False) or self.group), str(self.permission.codename)
        )

    def save(self, *args, **kwargs) -> None:
        """Save the current instance.

        Override this if you need to control the saving process.
        The `force_insert` and `force_update` parameters can be used to insist that the “save”
        must be an SQL insert or update statement, respectively (or equivalent for non-SQL backends).
        Normally, they should not be set.

        Other Parameters:
            force_insert (bool): If True, the save will be forced to be an insert.
            force_update (bool): If True, the save will be forced to be an update.
        """
        content_type = get_content_type(self.content_object)
        if content_type != self.permission.content_type:
            raise ValidationError(
                "Cannot persist permission not designed for "
                "this class (permission's type is %r and object's type is %r)"
                % (self.permission.content_type, content_type)
            )
        return super().save(*args, **kwargs)


class BaseGenericObjectPermission(models.Model):
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_pk = models.CharField(_("object ID"), max_length=255)
    content_object = GenericForeignKey(fk_field="object_pk")

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=["content_type", "object_pk"]),
        ]


class UserObjectPermissionBase(BaseObjectPermission):
    """Base class for creating object level permissions for users.

    This class can be used as a base class for creating a custom model to
    manager user object-level permissions.

    Attributes:
        user (ForeignKey): The django user model that has the permission.
        permission (ForeignKey): Foreign key to the permission granted.
        content_object (ForeignKey, GenericForeignKey):
            A foreign Key to the model class that the permission will be granted for.

    Example:
        ```python
        from guardian.models import UserObjectPermissionBase

        class OrgUserObjectPermission(UserObjectPermissionBase):
            \"\"\"Organization Specific permissions.\"\"\"

            class Meta(UserObjectPermissionBase.Meta):
                verbose_name = "Organization Permission"
                verbose_name_plural = "Organization Permissions"

            # Note: class attribute must be named content_object
            content_object = models.ForeignKey("myapp.Org", on_delete=models.CASCADE, db_column="org_object_id")

        ```

    See Also:
        - [Django-Guardian Performance Tuning](https://django-guardian.readthedocs.io/en/stable/userguide/performance.html)
        - [How to override the default UserObjectPermission](https://django-guardian.readthedocs.io/en/stable/configuration.html#guardian-user-obj-perms-model)
    """

    user = models.ForeignKey(user_model_label, on_delete=models.CASCADE)

    objects = UserObjectPermissionManager()

    class Meta:
        abstract = True
        unique_together = ["user", "permission", "content_object"]


class UserObjectPermissionAbstract(UserObjectPermissionBase, BaseGenericObjectPermission):
    class Meta(UserObjectPermissionBase.Meta, BaseGenericObjectPermission.Meta):
        abstract = True
        unique_together = ["user", "permission", "object_pk"]


class UserObjectPermission(UserObjectPermissionAbstract):
    """The default implementation of the UserObjectPermissionAbstract model.

    If `GUARDIAN_USER_OBJ_PERMS_MODEL` is not set at the beginning of the project, this model will be used.
    Uses Django's contenttypes framework to store generic relations.

    See Also:
        - [Django's Documentation on Abstract Base Models](https://docs.djangoproject.com/en/stable/topics/db/models/#abstract-base-classes)
        - [Django-Guardian Performance Tuning](https://django-guardian.readthedocs.io/en/stable/userguide/performance.html)
        - [How to override the default UserObjectPermission](https://django-guardian.readthedocs.io/en/stable/configuration.html#guardian-user-obj-perms-model)
    """

    class Meta(UserObjectPermissionAbstract.Meta):
        abstract = False
        indexes = [
            models.Index(fields=["permission", "user", "content_type", "object_pk"]),
            models.Index(fields=["user", "content_type", "object_pk"]),
        ]


class GroupObjectPermissionBase(BaseObjectPermission):
    """Base class for creating django-guardian groups.

    This class can be used as a base class for creating a groups permission.

    Attributes:
        group (ForeignKey): A foreign key to the django auth group.
        permission (ForeignKey): Foreign key to the permission granted.
        content_object (ForeignKey, GenericForeignKey):
            A foreign Key to the model class that the permission will be granted for.

    Example:
        ```python
        from guardian.models import GroupObjectPermissionBase

        class OrgGroupObjectPermission(GroupObjectPermissionBase):
            \"\"\"Organization Groups.\"\"\"

            class Meta(GroupObjectPermissionBase.Meta):
                verbose_name = "Organization Role"
                verbose_name_plural = "Organization Roles"

            content_object = models.ForeignKey("myapp.Org", on_delete=models.CASCADE, db_column="org_object_id")
        ```

    See Also:
        - [Django-Guardian Performance Tuning](https://django-guardian.readthedocs.io/en/stable/userguide/performance.html)
        - [How to override the default UserObjectPermission](https://django-guardian.readthedocs.io/en/stable/configuration.html#guardian-user-obj-perms-model)
    """

    group = models.ForeignKey(Group, on_delete=models.CASCADE)

    objects = GroupObjectPermissionManager()

    class Meta:
        abstract = True
        unique_together = ["group", "permission", "content_object"]


class GroupObjectPermissionAbstract(GroupObjectPermissionBase, BaseGenericObjectPermission):
    class Meta(GroupObjectPermissionBase.Meta, BaseGenericObjectPermission.Meta):
        abstract = True
        unique_together = ["group", "permission", "object_pk"]


class GroupObjectPermission(GroupObjectPermissionAbstract):
    """The default implementation of the GroupObjectPermissionAbstract model.

    If `GUARDIAN_GROUP_OBJ_PERMS_MODEL` is not set at the beginning of the project, this model will be used.
    Uses Django's contenttypes framework to store generic relations.

    See Also:
        - [Django's Documentation on Abstract Base Models](https://docs.djangoproject.com/en/stable/topics/db/models/#abstract-base-classes)
        - [Django-Guardian Performance Tuning](https://django-guardian.readthedocs.io/en/stable/userguide/performance.html)
        - [How to override the default GroupObjectPermission](https://django-guardian.readthedocs.io/en/stable/configuration.html#guardian-user-obj-perms-model)
    """

    class Meta(GroupObjectPermissionAbstract.Meta):
        abstract = False
        indexes = [
            models.Index(fields=["permission", "group", "content_type", "object_pk"]),
            models.Index(fields=["group", "content_type", "object_pk"]),
        ]
