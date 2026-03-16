import warnings
from collections import defaultdict
from copy import deepcopy
from inspect import getfullargspec
from sys import exc_info
from threading import local
from typing import Callable, List, Optional, Dict, Union, Sequence, Any, Tuple

from django.conf import settings
from django.core.cache import caches
from django.db.models import signals, QuerySet
from django.template.base import (
    FilterExpression, Lexer, Parser, Variable, VariableDoesNotExist, VARIABLE_TAG_START)
from django.template.context import Context
from django.template.loader import get_template
from django.urls import reverse, NoReverseMatch
from django.utils import module_loading
from django.utils.encoding import iri_to_uri
from django.utils.translation import get_language

from .compat import TOKEN_TEXT, TOKEN_VAR
from .exceptions import SiteTreeError
from .settings import (
    ALIAS_TRUNK, ALIAS_THIS_CHILDREN, ALIAS_THIS_SIBLINGS, ALIAS_THIS_PARENT_SIBLINGS, ALIAS_THIS_ANCESTOR_CHILDREN,
    UNRESOLVED_ITEM_MARKER, RAISE_ITEMS_ERRORS_ON_DEBUG, CACHE_TIMEOUT, CACHE_NAME, DYNAMIC_ONLY, ADMIN_APP_NAME,
    SITETREE_CLS,
)
from .utils import get_tree_model, get_tree_item_model, import_app_sitetree_module, generate_id_for

if False:  # pragma: nocover
    from django.contrib.auth.models import User  # noqa
    from .models import TreeItemBase, TreeBase

TypeDynamicTrees = Dict[str, Union[Dict[str, List['TreeBase']], List['TreeBase']]]
TypeStrExpr = Union[str, FilterExpression]

MODEL_TREE_CLASS = get_tree_model()
MODEL_TREE_ITEM_CLASS = get_tree_item_model()


_ITEMS_PROCESSOR: Optional[Callable] = None
"""Stores tree items processor callable or None."""

_ITEMS_PROCESSOR_ARGS_LEN: int = 0
"""Number of arguments accepted by items processor."""

_I18N_TREES: List[str] = []
"""Stores aliases of trees supporting internationalization."""

_DYNAMIC_TREES: TypeDynamicTrees = {}
"""Holds trees dynamically loaded from project apps."""

_IDX_ORPHAN_TREES: str = 'orphans'
"""Dictionary index in `_DYNAMIC_TREES` for orphaned trees list."""

_IDX_TPL: str = '%s|:|%s'
"""Name template used as dictionary index in `_DYNAMIC_TREES`."""

_THREAD_LOCAL = local()
_THREAD_SITETREE: str = 'sitetree'
_UNSET = set()  # Sentinel

cache = caches[CACHE_NAME]


def get_sitetree() -> 'SiteTree':
    """Returns SiteTree (thread-singleton) object, implementing utility methods.
    This can return the built-in or a customized (see SITETREE_CLS setting) sitetree handler.

    """
    sitetree = getattr(_THREAD_LOCAL, _THREAD_SITETREE, None)

    if sitetree is None:
        sitetree = _SITETREE_CLS()
        setattr(_THREAD_LOCAL, _THREAD_SITETREE, sitetree)

    return sitetree


def register_items_hook(func: Callable):
    """Registers a hook callable to process tree items right before they are passed to templates.

    .. deprecated:: 1.13.0
       Items hooking with `register_items_hook` is deprecated, please use `SITETREE_CLS`
       setting customization instead and override .apply_hook() method.

    Callable should be able to:

        a) handle ``tree_items`` and ``tree_sender`` key params.
            ``tree_items`` will contain a list of extended TreeItem objects ready to pass to template.
            ``tree_sender`` will contain navigation type identifier
                (e.g.: `menu`, `sitetree`, `breadcrumbs`, `menu.children`, `sitetree.children`)

        b) return a list of extended TreeItems objects to pass to template.


    Example::

        # Put the following code somewhere where it'd be triggered as expected. E.g. in app view.py.

        # First import the register function.
        from sitetree.sitetreeapp import register_items_hook

        # The following function will be used as items processor.
        def my_items_processor(tree_items, tree_sender):
            # Suppose we want to process only menu child items.
            if tree_sender == 'menu.children':
                # Lets add 'Hooked: ' to resolved titles of every item.
                for item in tree_items:
                    item.title_resolved = f'Hooked: {item.title_resolved}'
            # Return items list mutated or not.
            return tree_items

        # And we register items processor.
        register_items_hook(my_items_processor)

    :param func:
    """
    warnings.warn(
        'Items hooking with `register_items_hook` is deprecated, '
        'please use `SITETREE_CLS` settings customization instead.',
        DeprecationWarning, 2)

    global _ITEMS_PROCESSOR
    global _ITEMS_PROCESSOR_ARGS_LEN

    _ITEMS_PROCESSOR = func

    if func:
        args_len = len(getfullargspec(func).args)
        if args_len not in {2, 3}:
            raise SiteTreeError('`register_items_hook()` expects a function with two or three arguments.')
        _ITEMS_PROCESSOR_ARGS_LEN = args_len


def register_i18n_trees(aliases: List[str]):
    """Registers aliases of internationalized sitetrees.
    Internationalized sitetrees are those, which are dubbed by other trees having
    locale identifying suffixes in their aliases.

    Lets suppose ``my_tree`` is the alias of a generic tree. This tree is the one
    that we call by its alias in templates, and it is the one which is used
    if no i18n version of that tree is found.

    Given that ``my_tree_en``, ``my_tree_ru`` and other ``my_tree_{locale-id}``-like
    trees are considered internationalization sitetrees. These are used (if available)
    in accordance with current locale used by project.

    Example::

        # Put the following code somewhere where it'd be triggered as expected. E.g. in main urls.py.

        # First import the register function.
        from sitetree.sitetreeapp import register_i18n_trees

        # At last we register i18n trees.
        register_i18n_trees(['my_tree', 'my_another_tree'])

    :param aliases:

    """
    global _I18N_TREES
    _I18N_TREES = aliases


def register_dynamic_trees(
        trees: Union[List['TreeBase'], Dict[str, List['TreeBase']]],
        *args,
        **kwargs
):
    """Registers dynamic trees to be available for `sitetree` runtime.
    Expects `trees` to be an iterable with structures created with `compose_dynamic_tree()`.

    Example::

        register_dynamic_trees(

            # Get all the trees from `my_app`.
            compose_dynamic_tree('my_app'),

            # Get all the trees from `my_app` and attach them to `main` tree root.
            compose_dynamic_tree('my_app', target_tree_alias='main'),

            # Get all the trees from `my_app` and attach them to `has_dynamic` aliased item in `main` tree.
            compose_dynamic_tree('articles', target_tree_alias='main', parent_tree_item_alias='has_dynamic'),

            # Define a tree right on the registration.
            compose_dynamic_tree((
                tree('dynamic', items=(
                    item('dynamic_1', 'dynamic_1_url', children=(
                        item('dynamic_1_sub_1', 'dynamic_1_sub_1_url'),
                    )),
                    item('dynamic_2', 'dynamic_2_url'),
                )),
            )),
        )

    Accepted kwargs:
        bool reset_cache: Resets tree cache, to introduce all changes made to a tree immediately.

    """
    global _DYNAMIC_TREES

    if _IDX_ORPHAN_TREES not in _DYNAMIC_TREES:
        _DYNAMIC_TREES[_IDX_ORPHAN_TREES] = {}

    if isinstance(trees, dict):  # New `less-brackets` style registration.
        trees = [trees]
        trees.extend(args)

    for tree in trees or []:
        if tree is not None and tree['sitetrees'] is not None:
            if tree['tree'] is None:
                # Register trees as they are defined in app.
                for st in tree['sitetrees']:
                    if st.alias not in _DYNAMIC_TREES[_IDX_ORPHAN_TREES]:
                        _DYNAMIC_TREES[_IDX_ORPHAN_TREES][st.alias] = []
                    _DYNAMIC_TREES[_IDX_ORPHAN_TREES][st.alias].append(st)
            else:
                # Register tree items as parts of existing trees.
                index = _IDX_TPL % (tree['tree'], tree['parent_item'])
                if index not in _DYNAMIC_TREES:
                    _DYNAMIC_TREES[index] = []
                _DYNAMIC_TREES[index].extend(tree['sitetrees'])

    reset_cache = kwargs.get('reset_cache', False)
    if reset_cache:
        cache_ = get_sitetree().cache
        cache_.empty()
        cache_.reset()


def get_dynamic_trees() -> TypeDynamicTrees:
    """Returns a dictionary with currently registered dynamic trees."""
    return _DYNAMIC_TREES


def compose_dynamic_tree(
        src: Union[str, Sequence['TreeBase'], Sequence['TreeItemBase']],
        target_tree_alias: str = None,
        parent_tree_item_alias: str = None,
        include_trees: List[str] = None
) -> dict:
    """Returns a structure describing a dynamic sitetree.utils
    The structure can be built from various sources,

    :param src: If a string is passed to `src`, it'll be treated as the name of an app,
        from where one want to import sitetrees definitions. `src` can be an iterable
        of tree definitions (see `sitetree.toolbox.tree()` and `item()` functions).

    :param target_tree_alias: Static tree alias to attach items from dynamic trees to.

    :param parent_tree_item_alias: Tree item alias from a static tree to attach items from dynamic trees to.

    :param include_trees: Sitetree aliases to filter `src`.

    """
    def result(sitetrees):
        if include_trees is not None:
            sitetrees = [tree for tree in sitetrees if tree.alias in include_trees]

        return {
            'app': src,
            'sitetrees': sitetrees,
            'tree': target_tree_alias,
            'parent_item': parent_tree_item_alias}

    if isinstance(src, str):
        # Considered to be an application name.
        try:
            module = import_app_sitetree_module(src)
            return None if module is None else result(getattr(module, 'sitetrees', None))

        except ImportError as e:
            if settings.DEBUG:
                warnings.warn(f'Unable to register dynamic sitetree(s) for `{src}` application: {e}. ')
            return {}

    return result(src)


class LazyTitle:
    """Lazily resolves any variable found in a title of an item.
    Produces resolved title as unicode representation.

    """
    def __init__(self, title: str):
        self.title = title

    def __str__(self):
        my_lexer = Lexer(self.title)
        my_tokens = my_lexer.tokenize()

        # Deliberately strip off template tokens that are not text or variable.
        for my_token in my_tokens:
            if my_token.token_type not in (TOKEN_TEXT, TOKEN_VAR):
                my_tokens.remove(my_token)

        my_parser = Parser(my_tokens)
        return my_parser.parse().render(get_sitetree().current_page_context)

    def __eq__(self, other):
        return self.__str__() == other


class Cache:
    """Contains cache-related stuff."""

    def __init__(self):
        self.cache: dict = {}

        cache_empty = self.empty
        # Listen for signals from the models.
        signals.post_save.connect(cache_empty, sender=MODEL_TREE_CLASS)
        signals.post_save.connect(cache_empty, sender=MODEL_TREE_ITEM_CLASS)
        signals.post_delete.connect(cache_empty, sender=MODEL_TREE_ITEM_CLASS)
        # Listen to the changes in item permissions table.
        signals.m2m_changed.connect(cache_empty, sender=MODEL_TREE_ITEM_CLASS.access_permissions)

        self.init()

    @classmethod
    def reset(cls):
        """Instructs sitetree to drop and recreate cache.

        Could be used to show up tree changes made in a different process.

        """
        cache.set('sitetrees_reset', True)

    def init(self):
        """Initializes local cache from Django cache."""

        # Drop cache flag set by .reset() method.
        cache.get('sitetrees_reset') and self.empty(init=False)

        self.cache = cache.get(
            'sitetrees', {'sitetrees': {}, 'parents': {}, 'items_by_ids': {}, 'tree_aliases': {}})

    def save(self):
        """Saves sitetree data to Django cache."""
        cache.set('sitetrees', self.cache, CACHE_TIMEOUT)

    def empty(self, **kwargs):
        """Empties cached sitetree data."""
        cache.delete('sitetrees')
        cache.delete('sitetrees_reset')

        kwargs.get('init', True) and self.init()

    def get_entry(self, entry_name: str, key) -> Any:
        """Returns cache entry parameter value by its name.

        :param entry_name:
        :param key:

        """
        return self.cache[entry_name].get(key, False)

    def update_entry_value(self, entry_name: str, key: str, value: Any):
        """Updates cache entry parameter with new data.

        :param entry_name:
        :param key:
        :param value:

        """
        if key not in self.cache[entry_name]:
            self.cache[entry_name][key] = {}

        self.cache[entry_name][key].update(value)

    def set_entry(self, entry_name: str, key: str, value: Any):
        """Replaces entire cache entry parameter data by its name with new data.

        :param entry_name:
        :param key:
        :param value:

        """
        self.cache[entry_name][key] = value


class SiteTree:
    """Main logic handler."""

    cache_cls = Cache  # Allow customizations.

    def __init__(self):
        self.init(context=None)

    def init(self, context: Optional[Context]):
        """Initializes sitetree to handle new request.

        :param context:

        """
        self.cache = self.cache_cls()
        self.current_page_context = context
        self.current_lang = get_language()

        request = context.get('request', None) if context else None

        self.current_request = request

        current_app = (
            getattr(request, 'current_app', None) or
            getattr(getattr(request, 'resolver_match', None), 'namespace', None) or
            (context or {}).get('current_app', '')  # May be set by TreeItemChoiceField.
        )

        self._current_app_is_admin = current_app == ADMIN_APP_NAME
        self._current_app = current_app
        self._current_user_permissions = _UNSET
        self._items_urls = {}  # Resolved urls are cache for a request.
        self._current_items = {}

    def resolve_tree_i18n_alias(self, alias: str) -> str:
        """Resolves internationalized tree alias.
        Verifies whether a separate sitetree is available for currently active language.
        If so, returns i18n alias. If not, returns the initial alias.

        :param alias:

        """
        if alias not in _I18N_TREES:
            return alias

        current_language_code = self.current_lang
        i18n_tree_alias = f'{alias}_{current_language_code}'
        trees_count = self.cache.get_entry('tree_aliases', i18n_tree_alias)

        if trees_count is False:
            trees_count = MODEL_TREE_CLASS.objects.filter(alias=i18n_tree_alias).count()
            self.cache.set_entry('tree_aliases', i18n_tree_alias, trees_count)

        if trees_count:
            alias = i18n_tree_alias

        return alias

    @staticmethod
    def attach_dynamic_tree_items(
            tree_alias: str,
            src_tree_items: Union[Sequence['TreeItemBase'], QuerySet]
    ) -> List['TreeItemBase']:
        """Attaches dynamic sitetrees items registered with `register_dynamic_trees()`
        to an initial (source) items list.

        :param tree_alias:
        :param src_tree_items:

        """
        if not _DYNAMIC_TREES:
            return list(src_tree_items)

        # This guarantees that a dynamic source stays intact,
        # no matter how dynamic sitetrees are attached.
        trees = deepcopy(_DYNAMIC_TREES)

        items = []
        if not src_tree_items:
            if _IDX_ORPHAN_TREES in trees and tree_alias in trees[_IDX_ORPHAN_TREES]:
                for tree in trees[_IDX_ORPHAN_TREES][tree_alias]:
                    items.extend(tree.dynamic_items)
        else:

            # TODO Seems to be underoptimized %)

            # Tree item attachment by alias.
            for static_item in list(src_tree_items):
                items.append(static_item)
                if not static_item.alias:
                    continue

                idx = _IDX_TPL % (tree_alias, static_item.alias)
                if idx not in trees:
                    continue

                for tree in trees[idx]:
                    tree.alias = tree_alias

                    for dyn_item in tree.dynamic_items:  # noqa dynamic attr
                        if dyn_item.parent is None:
                            dyn_item.parent = static_item
                        # Unique IDs are required for the same trees attached
                        # to different parents.
                        dyn_item.id = generate_id_for(dyn_item)
                        items.append(dyn_item)

            # Tree root attachment.
            idx = _IDX_TPL % (tree_alias, None)
            if idx in _DYNAMIC_TREES:
                trees = deepcopy(_DYNAMIC_TREES)
                for tree in trees[idx]:
                    tree.alias = tree_alias
                    items.extend(tree.dynamic_items)  # noqa dynamic attr

        return items

    def current_app_is_admin(self) -> bool:
        """Returns boolean whether current application is Admin contrib."""
        warnings.warn(
            'Accessing .current_app_is_admin() is deprecated.',
            DeprecationWarning, 2)
        return self._current_app_is_admin

    def get_sitetree(self, alias: str) -> Tuple[str, List['TreeItemBase']]:
        """Gets site tree items from the given site tree.
        Caches result to dictionary.
        Returns (tree alias, tree items) tuple.

        :param alias:

        """
        cache_ = self.cache
        get_cache_entry = cache_.get_entry
        set_cache_entry = cache_.set_entry

        caching_required = False

        if not self._current_app_is_admin:
            # We do not need i18n for a tree rendered in Admin dropdown.
            alias = self.resolve_tree_i18n_alias(alias)

        sitetree = get_cache_entry('sitetrees', alias)

        if not sitetree:
            if DYNAMIC_ONLY:
                sitetree = []

            else:
                sitetree = (
                    MODEL_TREE_ITEM_CLASS.objects.
                    select_related('parent', 'tree').
                    prefetch_related('access_permissions__content_type').
                    filter(tree__alias__exact=alias).
                    order_by('parent__sort_order', 'sort_order'))

            sitetree = self.attach_dynamic_tree_items(alias, sitetree)
            set_cache_entry('sitetrees', alias, sitetree)
            caching_required = True

        parents = get_cache_entry('parents', alias)
        if not parents:
            parents = defaultdict(list)
            for item in sitetree:
                parent = getattr(item, 'parent')
                parents[parent].append(item)
            set_cache_entry('parents', alias, parents)

        # Prepare items by ids cache if needed.
        if caching_required:
            # We need this extra pass to avoid future problems on items depth calculation.
            cache_update = cache_.update_entry_value
            for item in sitetree:
                cache_update('items_by_ids', alias, {item.id: item})

        url = self.url
        calculate_item_depth = self.calculate_item_depth

        for item in sitetree:
            if caching_required:
                item.has_children = False

                if not hasattr(item, 'depth'):
                    item.depth = calculate_item_depth(alias, item.id)
                item.depth_range = range(item.depth)

                # Resolve item permissions.
                if item.access_restricted:
                    permissions_src = (
                        item.permissions if getattr(item, 'is_dynamic', False)
                        else item.access_permissions.all())

                    item.perms = set(
                        [f'{perm.content_type.app_label}.{perm.codename}' for perm in permissions_src])

            # Contextual properties.
            item.url_resolved = url(item)
            item.title_resolved = LazyTitle(item.title) if VARIABLE_TAG_START in item.title else item.title
            item.is_current = False
            item.in_current_branch = False

        # Get current item for the given sitetree.
        self.get_tree_current_item(alias)

        # Save sitetree data into cache if needed.
        if caching_required:
            cache_.save()

        return alias, sitetree

    def calculate_item_depth(self, tree_alias: str, item_id: int, depth: int = 0):
        """Calculates depth of the item in the tree.

        :param tree_alias:
        :param item_id:
        :param depth:

        """
        item = self.get_item_by_id(tree_alias, item_id)

        if hasattr(item, 'depth'):
            depth = item.depth + depth

        else:
            if item.parent is not None:
                depth = self.calculate_item_depth(tree_alias, item.parent.id, depth + 1)

        return depth

    def get_item_by_id(self, tree_alias: str, item_id: int) -> 'TreeItemBase':
        """Get the item from the tree by its ID.

        :param tree_alias:
        :param item_id:

        """
        return self.cache.get_entry('items_by_ids', tree_alias)[item_id]

    def get_tree_current_item(self, tree_alias: str) -> Optional['TreeItemBase']:
        """Resolves current tree item of 'tree_alias' tree matching current
        request path against URL of given tree item.

        :param tree_alias:

        """
        current_item = self._current_items.get(tree_alias, _UNSET)

        if current_item is not _UNSET:

            if current_item is not None:
                current_item.is_current = True  # Could be reset by .get_sitetree()

            return current_item  # noqa

        current_item = None

        if self._current_app_is_admin:
            self._current_items[tree_alias] = current_item
            return None

        current_url = self.current_request.path

        if current_url:
            # url quote is an attempt to support non-ascii in url.
            current_url = iri_to_uri(current_url)

        for url_item, url in self._items_urls.items():
            # Iterate each as this dict may contains "current" items for various trees.
            if url != current_url:
                continue

            url_item.is_current = True
            if url_item.tree.alias == tree_alias:
                current_item = url_item

        if current_item is not None:
            self._current_items[tree_alias] = current_item

        return current_item

    def url(self, sitetree_item: Union['TreeItemBase', FilterExpression], context: Context = None) -> str:
        """Resolves item's URL.

        :param sitetree_item: TreeItemBase heir object, 'url' property of which
            is processed as URL pattern or simple URL.

        :param context:

        """
        context = context or self.current_page_context
        resolve_var = self.resolve_var

        if not isinstance(sitetree_item, MODEL_TREE_ITEM_CLASS):
            sitetree_item = resolve_var(sitetree_item, context)

        resolved_url = self._items_urls.get(sitetree_item)
        if resolved_url is not None:
            return resolved_url

        # Resolve only if item's URL is marked as pattern.
        if sitetree_item.urlaspattern:
            url = sitetree_item.url
            view_path = url
            all_arguments = []

            if ' ' in url:
                view_path = url.split(' ')
                # We should try to resolve URL parameters from site tree item.
                for view_argument in view_path[1:]:
                    all_arguments.append(resolve_var(view_argument))

                view_path = view_path[0].strip('"\' ')

            try:
                resolved_url = reverse(
                    view_path,
                    args=all_arguments,
                    current_app=self._current_app
                )

            except NoReverseMatch:
                resolved_url = UNRESOLVED_ITEM_MARKER

        else:
            resolved_url = f'{sitetree_item.url}'

        self._items_urls[sitetree_item] = resolved_url

        return resolved_url

    def init_tree(
            self,
            tree_alias: str,
            context: Context
    ) -> Tuple[Optional[str], Optional[List['TreeItemBase']]]:
        """Initializes sitetree in memory.

        Returns tuple with resolved tree alias and items on success.

        On fail returns (None, None).

        :param tree_alias:
        :param context:

        """
        request = context.get('request', None)

        if request is None:

            if any(exc_info()):
                # Probably we're in a technical
                # exception handling view. So we won't mask
                # the initial exception with the one below.
                return None, None

            raise SiteTreeError(
                'Sitetree requires "django.core.context_processors.request" template context processor to be active. '
                'If it is, check that your view pushes request data into the template.')

        if id(request) != id(self.current_request):
            self.init(context)

        # Resolve tree_alias from the context.
        tree_alias = self.resolve_var(tree_alias)
        tree_alias, sitetree_items = self.get_sitetree(tree_alias)

        if not sitetree_items:
            return None, None

        return tree_alias, sitetree_items

    def get_current_page_title(self, tree_alias: TypeStrExpr, context: Context) -> str:
        """Returns resolved from sitetree title for current page.

        :param tree_alias:
        :param context:

        """
        return self.get_current_page_attr('title_resolved', tree_alias, context)

    def get_current_page_attr(self, attr_name: str, tree_alias: TypeStrExpr, context: Context) -> str:
        """Returns an arbitrary attribute of a sitetree item resolved as current for current page.

        :param attr_name:
        :param tree_alias:
        :param context:

        """
        tree_alias, sitetree_items = self.init_tree(tree_alias, context)
        current_item = self.get_tree_current_item(tree_alias)

        if current_item is None:
            if settings.DEBUG and RAISE_ITEMS_ERRORS_ON_DEBUG:
                raise SiteTreeError(
                    f'Unable to resolve current sitetree item to get a `{attr_name}` for current page. Check whether '
                    'there is an appropriate sitetree item defined for current URL.')

            return ''

        return getattr(current_item, attr_name, '')

    def get_ancestor_level(self, current_item: 'TreeItemBase', depth: int = 1) -> 'TreeItemBase':
        """Returns ancestor of level `deep` recursively

        :param current_item:
        :param depth:

        """
        if current_item.parent is None:
            return current_item

        if depth <= 1:
            return current_item.parent

        return self.get_ancestor_level(current_item.parent, depth=depth-1)

    def menu(self, tree_alias: TypeStrExpr, tree_branches: TypeStrExpr, context: Context) -> List['TreeItemBase']:
        """Builds and returns menu structure for 'sitetree_menu' tag.

        :param tree_alias:
        :param tree_branches:
        :param context:

        """
        tree_alias, sitetree_items = self.init_tree(tree_alias, context)

        if not sitetree_items:
            return []

        tree_branches = self.resolve_var(tree_branches)

        parent_isnull = False
        parent_ids = []
        parent_aliases = []

        current_item = self.get_tree_current_item(tree_alias)
        self.tree_climber(tree_alias, current_item)

        # Support item addressing both through identifiers and aliases.
        for branch_id in tree_branches.split(','):
            branch_id = branch_id.strip()

            if branch_id == ALIAS_TRUNK:
                parent_isnull = True

            elif branch_id == ALIAS_THIS_CHILDREN and current_item is not None:
                branch_id = current_item.id
                parent_ids.append(branch_id)

            elif branch_id == ALIAS_THIS_ANCESTOR_CHILDREN and current_item is not None:
                branch_id = self.get_ancestor_item(tree_alias, current_item).id
                parent_ids.append(branch_id)

            elif branch_id == ALIAS_THIS_SIBLINGS and current_item is not None and current_item.parent is not None:
                branch_id = current_item.parent.id
                parent_ids.append(branch_id)

            elif branch_id == ALIAS_THIS_PARENT_SIBLINGS and current_item is not None:
                branch_id = self.get_ancestor_level(current_item, depth=2).id
                parent_ids.append(branch_id)

            elif branch_id.isdigit():
                parent_ids.append(int(branch_id))

            else:
                parent_aliases.append(branch_id)

        check_access = self.check_access

        menu_items = []
        for item in sitetree_items:
            if not item.hidden and item.inmenu and check_access(item, context):
                if item.parent is None:
                    if parent_isnull:
                        menu_items.append(item)
                else:
                    if item.parent.id in parent_ids or item.parent.alias in parent_aliases:
                        menu_items.append(item)

        menu_items = self.apply_hook(menu_items, 'menu')
        self.update_has_children(tree_alias, menu_items, 'menu')

        return menu_items

    def apply_hook(self, items: List['TreeItemBase'], sender: str) -> List['TreeItemBase']:
        """Applies a custom items processing hook to items supplied, and returns processed list.
        Suitable for items filtering and other manipulations.

        Returns initial items list if no hook is registered.

        .. note:: Use `SITETREE_CLS` setting customization and override this method.

        :param items:
        :param sender: menu, breadcrumbs, sitetree, {type}.children, {type}.has_children

        """
        processor = _ITEMS_PROCESSOR

        if processor is None:
            return items

        if _ITEMS_PROCESSOR_ARGS_LEN == 2:
            return processor(tree_items=items, tree_sender=sender)

        return processor(tree_items=items, tree_sender=sender, context=self.current_page_context)

    def check_access_auth(self, item: 'TreeItemBase', context: Context) -> bool:
        """Performs authentication related checks: whether the current user has an access to a certain item.

        :param item:
        :param context:

        """
        authenticated = self.current_request.user.is_authenticated

        if hasattr(authenticated, '__call__'):
            authenticated = authenticated()

        if item.access_loggedin and not authenticated:
            return False

        if item.access_guest and authenticated:
            return False

        return True

    def check_access_perms(self, item: 'TreeItemBase', context: Context) -> bool:
        """Performs permissions related checks: whether the current user has an access to a certain item.

        :param item:
        :param context:

        """
        if item.access_restricted:
            user_perms = self._current_user_permissions

            if user_perms is _UNSET:
                user_perms = self.get_permissions(self.current_request.user, item)
                self._current_user_permissions = user_perms

            perms = item.perms  # noqa dynamic attr

            if item.access_perm_type == MODEL_TREE_ITEM_CLASS.PERM_TYPE_ALL:
                if len(perms) != len(perms.intersection(user_perms)):
                    return False
            else:
                if not len(perms.intersection(user_perms)):
                    return False

        return True

    def check_access_dyn(self, item: 'TreeItemBase', context: Context) -> Optional[bool]:
        """Performs dynamic item access check.

        :param item: The item is expected to have `access_check` callable attribute implementing the check.
        :param context:

        """
        result = None
        access_check_func = getattr(item, 'access_check', None)

        if access_check_func:
            return access_check_func(tree=self)

        return None

    def check_access(self, item: 'TreeItemBase', context: Context) -> bool:
        """Checks whether a current user has an access to a certain item.

        :param item:
        :param context:

        """
        dyn_check = self.check_access_dyn(item, context)

        if dyn_check is not None:
            return dyn_check

        if not self.check_access_auth(item, context):
            return False

        if not self.check_access_perms(item, context):
            return False

        return True

    def get_permissions(self, user: 'User', item: 'TreeItemBase') -> set:
        """Returns a set of user and group level permissions for a given user.

        :param user:
        :param item:

        """
        return user.get_all_permissions()

    def breadcrumbs(self, tree_alias: TypeStrExpr, context: Context) -> List['TreeItemBase']:
        """Builds and returns breadcrumb trail structure for 'sitetree_breadcrumbs' tag.

        :param tree_alias:
        :param context:

        """
        tree_alias, sitetree_items = self.init_tree(tree_alias, context)

        if not sitetree_items:
            return []

        current_item = self.get_tree_current_item(tree_alias)

        breadcrumbs = []

        if current_item is not None:

            context_ = self.current_page_context
            check_access = self.check_access
            get_item_by_id = self.get_item_by_id

            def climb(base_item):
                """Climbs up the site tree to build breadcrumb path.

                :param TreeItemBase base_item:
                """
                if base_item.inbreadcrumbs and not base_item.hidden and check_access(base_item, context_):
                    breadcrumbs.append(base_item)

                if hasattr(base_item, 'parent') and base_item.parent is not None:
                    climb(get_item_by_id(tree_alias, base_item.parent.id))

            climb(current_item)
            breadcrumbs.reverse()

        items = self.apply_hook(breadcrumbs, 'breadcrumbs')
        self.update_has_children(tree_alias, items, 'breadcrumbs')

        return items

    def tree(self, tree_alias: TypeStrExpr, context:  Context) -> List['TreeItemBase']:
        """Builds and returns tree structure for 'sitetree_tree' tag.

        :param tree_alias:
        :param context:

        """
        tree_alias, sitetree_items = self.init_tree(tree_alias, context)

        if not sitetree_items:
            return []

        tree_items = self.filter_items(self.get_children(tree_alias, None), 'sitetree')
        tree_items = self.apply_hook(tree_items, 'sitetree')
        self.update_has_children(tree_alias, tree_items, 'sitetree')

        return tree_items

    def children(
            self,
            parent_item: Union[str, 'TreeItemBase'],
            navigation_type: str,
            use_template: str,
            context: Context
    ) -> str:
        """Builds and returns site tree item children structure for 'sitetree_children' tag.

        :param parent_item:
        :param navigation_type: menu, sitetree
        :param use_template:
        :param context:

        """
        # Resolve parent item and current tree alias.
        parent_item = self.resolve_var(parent_item, context)
        tree_alias, tree_items = self.get_sitetree(parent_item.tree.alias)

        # Mark path to current item.
        self.tree_climber(tree_alias, self.get_tree_current_item(tree_alias))

        tree_items = self.get_children(tree_alias, parent_item)
        tree_items = self.filter_items(tree_items, navigation_type)
        tree_items = self.apply_hook(tree_items, f'{navigation_type}.children')
        self.update_has_children(tree_alias, tree_items, navigation_type)

        my_template = get_template(use_template)

        context.push()
        context['sitetree_items'] = tree_items
        rendered = my_template.render(context.flatten())
        context.pop()

        return rendered

    def get_children(self, tree_alias: str, item: Optional['TreeItemBase']) -> List['TreeItemBase']:
        """Returns item's children.

        :param tree_alias:
        :param item:

        """
        if not self._current_app_is_admin:
            # We do not need i18n for a tree rendered in Admin dropdown.
            tree_alias = self.resolve_tree_i18n_alias(tree_alias)

        return self.cache.get_entry('parents', tree_alias)[item]

    def update_has_children(self, tree_alias: str, tree_items: List['TreeItemBase'], navigation_type: str):
        """Updates 'has_children' attribute for tree items inplace.

        :param tree_alias:
        :param tree_items:
        :param navigation_type: sitetree, breadcrumbs, menu

        """
        get_children = self.get_children
        filter_items = self.filter_items
        apply_hook = self.apply_hook

        for tree_item in tree_items:
            children = get_children(tree_alias, tree_item)
            children = filter_items(children, navigation_type)
            children = apply_hook(children, f'{navigation_type}.has_children')
            tree_item.has_children = len(children) > 0

    def filter_items(self, items: List['TreeItemBase'], navigation_type: str = None) -> List['TreeItemBase']:
        """Filters sitetree item's children if hidden and by navigation type.

        NB: We do not apply any filters to sitetree in admin app.

        :param items:
        :param navigation_type: sitetree, breadcrumbs, menu

        """
        if self._current_app_is_admin:
            return items

        items_filtered = []

        context = self.current_page_context
        check_access = self.check_access

        for item in items:
            if item.hidden:
                continue

            if not check_access(item, context):
                continue

            if not getattr(item, f'in{navigation_type}', True):  # Hidden for current nav type
                continue

            items_filtered.append(item)

        return items_filtered

    def get_ancestor_item(self, tree_alias: str, base_item: 'TreeItemBase') -> 'TreeItemBase':
        """Climbs up the site tree to resolve root item for chosen one.

        :param tree_alias:
        :param base_item:

        """
        parent = None

        if hasattr(base_item, 'parent') and base_item.parent is not None:
            parent = self.get_ancestor_item(tree_alias, self.get_item_by_id(tree_alias, base_item.parent.id))

        if parent is None:
            return base_item

        return parent

    def tree_climber(self, tree_alias: str, base_item: 'TreeItemBase'):
        """Climbs up the site tree to mark items of current branch.

        :param tree_alias:
        :param base_item:

        """
        if base_item is not None:
            base_item.in_current_branch = True
            if hasattr(base_item, 'parent') and base_item.parent is not None:
                self.tree_climber(tree_alias, self.get_item_by_id(tree_alias, base_item.parent.id))

    def resolve_var(
            self,
            varname: Union[TypeStrExpr, 'TreeItemBase'],
            context: Context = None
    ) -> Any:
        """Resolves name as a variable in a given context.

        If no context specified page context is considered as context.

        :param varname:
        :param context:

        """
        context = context or self.current_page_context

        if isinstance(varname, FilterExpression):
            varname = varname.resolve(context)

        else:
            varname = varname.strip()

            try:
                varname = Variable(varname).resolve(context)
            except VariableDoesNotExist:
                varname = varname

        return varname


_SITETREE_CLS = module_loading.import_string(SITETREE_CLS) if SITETREE_CLS else SiteTree
