from importlib import import_module
from types import ModuleType
from typing import Any, Sequence, Type, Union, List, Optional, Tuple, Callable

from django.apps import apps
from django.contrib.auth.models import Permission
from django.core.exceptions import ImproperlyConfigured
from django.template.context import Context
from django.utils.functional import SimpleLazyObject
from django.utils.module_loading import module_has_submodule

from . import settings

if False:  # pragma: nocover
    from .models import TreeItemBase, TreeBase  # noqa


TypePermission = Union[str, int, Permission]

apps_get_model = apps.get_model


def generate_id_for(obj: Any):
    """Generates and returns a unique identifier for the given object."""
    return id(obj)


def tree(alias: str, title: str = '', items: Sequence['TreeItemBase'] = None, **kwargs) -> 'TreeBase':
    """Dynamically creates and returns a sitetree.

    :param alias:
    :param title:
    :param items: dynamic sitetree items objects created by `item` function.
    :param kwargs: Additional arguments to pass to tree item initializer.

    """
    tree_obj = get_tree_model()(alias=alias, title=title, **kwargs)
    tree_obj.id = generate_id_for(tree_obj)
    tree_obj.is_dynamic = True

    if items is not None:
        tree_obj.dynamic_items = []

        def traverse(items):
            for item in items:
                item.tree = tree_obj
                tree_obj.dynamic_items.append(item)
                if hasattr(item, 'dynamic_children'):
                    traverse(item.dynamic_children)

        traverse(items)

    return tree_obj


def clean_permission(permission: TypePermission) -> Union[int, Permission]:
    if isinstance(permission, str):
        # Get permission object from string
        try:
            app, codename = permission.split('.')
        except ValueError:
            raise ValueError(
                f'Wrong permission string format: supplied - `{permission}`; '
                'expected - `<app_name>.<permission_name>`.')
        try:
            return Permission.objects.get(codename=codename, content_type__app_label=app)
        except Permission.DoesNotExist:
            raise ValueError(f'Permission `{app}.{codename}` does not exist.')
    elif not isinstance(permission, (int, Permission)):
        raise ValueError('Permissions must be given as strings, ints, or `Permission` instances.')

    return permission


def clean_permissions(permissions: Union[TypePermission, List[TypePermission]]) -> List[Permission]:
    if permissions is None:
        return []

    # Make permissions a list if currently a single object
    if not isinstance(permissions, list):
        permissions = [permissions]

    return [
        clean_permission(permission)
        for permission in permissions
    ]


def item(
        title: str,
        url: str,
        children: Sequence['TreeItemBase'] = None,
        url_as_pattern: bool = True,
        hint: str = '',
        alias: str = '',
        description: str = '',
        in_menu: bool = True,
        in_breadcrumbs: bool = True,
        in_sitetree: bool = True,
        access_loggedin: bool = False,
        access_guest: bool = False,
        access_by_perms: Union[TypePermission, List[TypePermission]] = None,
        perms_mode_all: bool = True,
        dynamic_attrs: Optional[dict] = None,
        access_check: Optional[Callable[[Context], Optional[bool]]] = None,
        **kwargs
) -> 'TreeItemBase':
    """Dynamically creates and returns a sitetree item object.

    :param title:

    :param url:

    :param children: children for tree item. Children should also be created by `item` function.

    :param url_as_pattern: consider URL as a name of a named URL

    :param hint: hints are usually shown to users

    :param alias: item name to address it from templates

    :param description: additional information on item (usually is not shown to users)

    :param in_menu: show this item in menus

    :param in_breadcrumbs: show this item in breadcrumbs

    :param in_sitetree: show this item in sitetrees

    :param access_loggedin: show item to logged in users only

    :param access_guest: show item to guest users only

    :param access_by_perms: restrict access to users with these permissions.

        This can be set to one or a list of permission names, IDs or Permission instances.

        Permission names are more portable and should be in a form `<app_label>.<perm_codename>`, e.g.:
            my_app.allow_save


    :param perms_mode_all: permissions set interpretation rule:
                True - user should have all the permissions;
                False - user should have any of chosen permissions.

    :param dynamic_attrs: dynamic attributes to be attached to the item runtime

    :param access_check: a callable to perform a custom item access check
            Requires to accept `tree` named parameter (current user is in `tree.current_request.user`).
            Boolean return is considered as an access check result.
            None return instructs sitetree to process with other common access checks.

            .. note:: This callable must support pickling (e.g. be exposed on a module level).

    """
    item_obj = get_tree_item_model()(
        title=title, url=url, urlaspattern=url_as_pattern,
        hint=hint, alias=alias, description=description, inmenu=in_menu,
        insitetree=in_sitetree, inbreadcrumbs=in_breadcrumbs,
        access_loggedin=access_loggedin, access_guest=access_guest,
        **kwargs)

    item_obj.id = generate_id_for(item_obj)
    item_obj.is_dynamic = True
    item_obj.dynamic_children = []

    item_obj.permissions = SimpleLazyObject(lambda: clean_permissions(access_by_perms))
    item_obj.access_perm_type = item_obj.PERM_TYPE_ALL if perms_mode_all else item_obj.PERM_TYPE_ANY

    if access_by_perms:
        item_obj.access_restricted = True

    if access_check:
        item_obj.access_check = access_check

    children = children or []
    for child in children:
        child.parent = item_obj
        item_obj.dynamic_children.append(child)

    dynamic_attrs = dynamic_attrs or {}
    for key, value in dynamic_attrs.items():
        setattr(item_obj, key, value)

    return item_obj


def import_app_sitetree_module(app: str) -> Optional[ModuleType]:
    """Imports sitetree module from a given app.

    :param app: Application name

    """
    module_name = settings.APP_MODULE_NAME
    module = import_module(app)

    try:
        sub_module = import_module(f'{app}.{module_name}')
        return sub_module

    except ImportError:
        if module_has_submodule(module, module_name):
            raise
        return None


def import_project_sitetree_modules() -> List[ModuleType]:
    """Imports sitetrees modules from packages (apps).
    Returns a list of submodules.

    """
    submodules = []
    for app_config in apps.app_configs.values():
        module = import_app_sitetree_module(app_config.name)
        if module is not None:
            submodules.append(module)

    return submodules


def get_app_n_model(settings_entry_name: str) -> Tuple[str, str]:
    """Returns tuple with application and tree[item] model class names.

    :param settings_entry_name:

    """
    try:
        app_name, model_name = getattr(settings, settings_entry_name).split('.')

    except ValueError:
        raise ImproperlyConfigured(
            f'`SITETREE_{settings_entry_name}` must have the following format: `app_name.model_name`.')

    return app_name, model_name


def get_model_class(settings_entry_name: str):
    """Returns a certain sitetree model as defined in the project settings.

    :param settings_entry_name:

    """
    app_name, model_name = get_app_n_model(settings_entry_name)

    try:
        model = apps_get_model(app_name, model_name)

    except (LookupError, ValueError):
        model = None

    if model is None:
        raise ImproperlyConfigured(
            f'`SITETREE_{settings_entry_name}` refers to model `{model_name}` that has not been installed.')

    return model


def get_tree_model() -> Type['TreeBase']:
    """Returns the Tree model, set for the project."""
    return get_model_class('MODEL_TREE')


def get_tree_item_model() -> Type['TreeItemBase']:
    """Returns the TreeItem model, set for the project."""
    return get_model_class('MODEL_TREE_ITEM')
