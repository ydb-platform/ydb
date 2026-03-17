from django.conf import settings


SITETREE_CLS: str = getattr(settings, 'SITETREE_CLS', None)
"""Allows deep tree handling customization. Accepts sitetreeap.SiteTree subclass."""

MODEL_TREE: str = getattr(settings, 'SITETREE_MODEL_TREE', 'sitetree.Tree')
"""Path to a tree model (app.class)."""

MODEL_TREE_ITEM: str = getattr(settings, 'SITETREE_MODEL_TREE_ITEM', 'sitetree.TreeItem')
"""Path to a tree item model (app.class)."""

APP_MODULE_NAME: str = getattr(settings, 'SITETREE_APP_MODULE_NAME', 'sitetrees')
"""Module name where applications store trees shipped with them."""

UNRESOLVED_ITEM_MARKER: str = getattr(settings, 'SITETREE_UNRESOLVED_ITEM_MARKER', u'#unresolved')
"""This string is place instead of item URL if actual URL cannot be resolved."""

RAISE_ITEMS_ERRORS_ON_DEBUG: bool = getattr(settings, 'SITETREE_RAISE_ITEMS_ERRORS_ON_DEBUG', True)
"""Whether to raise exceptions in DEBUG mode if current page item is unresolved."""

DYNAMIC_ONLY: bool = getattr(settings, 'SITETREE_DYNAMIC_ONLY', False)
"""Whether to query DB for static trees items or use dynamic only."""

ITEMS_FIELD_ROOT_ID: str = getattr(settings, 'SITETREE_ITEMS_FIELD_ROOT_ID', '')
"""Item ID to be used for root item in TreeItemChoiceField.
This is adjustable to be able to workaround client-side field validation issues in thirdparties.

"""

CACHE_TIMEOUT: int = getattr(settings, 'SITETREE_CACHE_TIMEOUT', 31536000)
"""Sitetree objects are stored in Django cache for a year (60 * 60 * 24 * 365 = 31536000 sec).
Cache is only invalidated on sitetree or sitetree item change.

"""

CACHE_NAME: str = getattr(settings, 'SITETREE_CACHE_NAME', 'default')
"""Sitetree cache name to use (Defined in django CACHES hash)."""

ADMIN_APP_NAME: str = getattr(settings, 'SITETREE_ADMIN_APP_NAME', 'admin')
"""Admin application name. In cases custom admin application is used."""


# Reserved tree items aliases.
ALIAS_TRUNK = 'trunk'
ALIAS_THIS_CHILDREN = 'this-children'
ALIAS_THIS_SIBLINGS = 'this-siblings'
ALIAS_THIS_ANCESTOR_CHILDREN = 'this-ancestor-children'
ALIAS_THIS_PARENT_SIBLINGS = 'this-parent-siblings'

TREE_ITEMS_ALIASES = [
    ALIAS_TRUNK,
    ALIAS_THIS_CHILDREN,
    ALIAS_THIS_SIBLINGS,
    ALIAS_THIS_ANCESTOR_CHILDREN,
    ALIAS_THIS_PARENT_SIBLINGS
]
