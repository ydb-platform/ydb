from django.contrib.auth.models import Permission
from django.db import models
from django.utils.translation import gettext_lazy as _

from .settings import MODEL_TREE, TREE_ITEMS_ALIASES


class CharFieldNullable(models.CharField):
    """We use custom char field to put nulls in SiteTreeItem 'alias' field.
    That allows 'unique_together' directive in Meta to work properly, so
    we don't have two site tree items with the same alias in the same site tree.

    """
    def get_prep_value(self, value):
        if value is not None:
            if value.strip() == '':
                return None
        return self.to_python(value)


class TreeBase(models.Model):

    title = models.CharField(
        _('Title'), max_length=100, help_text=_('Site tree title for presentational purposes.'), blank=True)

    alias = models.CharField(
        _('Alias'), max_length=80,
        help_text=_('Short name to address site tree from templates.<br /><b>Note:</b> change with care.'),
        unique=True, db_index=True)

    class Meta:
        abstract = True
        verbose_name = _('Site Tree')
        verbose_name_plural = _('Site Trees')

    def get_title(self) -> str:
        return self.title or self.alias

    def __str__(self) -> str:
        return self.alias


class TreeItemBase(models.Model):

    PERM_TYPE_ANY = 1
    PERM_TYPE_ALL = 2

    PERM_TYPE_CHOICES = {
        PERM_TYPE_ANY: _('Any'),
        PERM_TYPE_ALL: _('All'),
    }

    title = models.CharField(
        _('Title'), max_length=100,
        help_text=_('Site tree item title. Can contain template variables E.g.: {{ mytitle }}.'))

    hint = models.CharField(
        _('Hint'), max_length=200,
        help_text=_('Some additional information about this item that is used as a hint.'), blank=True, default='')

    url = models.CharField(
        _('URL'), max_length=200,
        help_text=_('Exact URL or URL pattern (see "Additional settings") for this item.'), db_index=True)

    urlaspattern = models.BooleanField(
        _('URL as Pattern'),
        help_text=_('Whether the given URL should be treated as a pattern.<br />'
                    '<b>Note:</b> Refer to Django "URL dispatcher" documentation (e.g. "Naming URL patterns" part).'),
        db_index=True, default=False)

    tree = models.ForeignKey(
        MODEL_TREE, related_name='%(class)s_tree', on_delete=models.CASCADE, verbose_name=_('Site Tree'),
        help_text=_('Site tree this item belongs to.'), db_index=True)

    hidden = models.BooleanField(
        _('Hidden'), help_text=_('Whether to show this item in navigation.'), db_index=True, default=False)

    alias = CharFieldNullable(
        _('Alias'), max_length=80,
        help_text=_(
            'Short name to address site tree item from a template.<br />'
            '<b>Reserved aliases:</b> "%s".' % '", "'.join(TREE_ITEMS_ALIASES)
        ),
        db_index=True, blank=True, null=True)

    description = models.TextField(
        _('Description'),
        help_text=_('Additional comments on this item.'), blank=True, default='')

    inmenu = models.BooleanField(
        _('Show in menu'),
        help_text=_('Whether to show this item in a menu.'), db_index=True, default=True)

    inbreadcrumbs = models.BooleanField(
        _('Show in breadcrumb path'),
        help_text=_('Whether to show this item in a breadcrumb path.'), db_index=True, default=True)

    insitetree = models.BooleanField(
        _('Show in site tree'),
        help_text=_('Whether to show this item in a site tree.'), db_index=True, default=True)

    access_loggedin = models.BooleanField(
        _('Logged in only'),
        help_text=_('Check it to grant access to this item to authenticated users only.'),
        db_index=True, default=False)

    access_guest = models.BooleanField(
        _('Guests only'),
        help_text=_('Check it to grant access to this item to guests only.'), db_index=True, default=False)

    access_restricted = models.BooleanField(
        _('Restrict access to permissions'),
        help_text=_('Check it to restrict user access to this item, using Django permissions system.'),
        db_index=True, default=False)

    access_permissions = models.ManyToManyField(
        Permission, verbose_name=_('Permissions granting access'), blank=True)

    access_perm_type = models.IntegerField(
        _('Permissions interpretation'),
        help_text=_('<b>Any</b> &mdash; user should have any of chosen permissions. '
                    '<b>All</b> &mdash; user should have all chosen permissions.'),
        choices=PERM_TYPE_CHOICES.items(), default=PERM_TYPE_ANY)

    # These two are for 'adjacency list' model.
    # This is the current approach of tree representation for sitetree.
    parent = models.ForeignKey(
        'self', related_name='%(class)s_parent', on_delete=models.CASCADE, verbose_name=_('Parent'),
        help_text=_('Parent site tree item.'), db_index=True, null=True, blank=True)

    sort_order = models.IntegerField(
        _('Sort order'),
        help_text=_('Item position among other site tree items under the same parent.'), db_index=True, default=0)

    def save(self, force_insert=False, force_update=False, **kwargs):
        # Ensure that item is not its own parent, since this breaks
        # the sitetree (and possibly the entire site).
        if self.parent == self:
            self.parent = None
        
        # Set item's sort order to its primary key.
        id_ = self.id
        if id_ and self.sort_order == 0:
            self.sort_order = id_
        
        super().save(force_insert, force_update, **kwargs)

        # Set item's sort order to its primary key if not already set.
        if self.sort_order == 0:
            self.sort_order = self.id
            self.save()

    class Meta:
        abstract = True
        verbose_name = _('Site Tree Item')
        verbose_name_plural = _('Site Tree Items')
        unique_together = ('tree', 'alias')

    def __str__(self) -> str:
        return self.title


class Tree(TreeBase):
    """Built-in tree class. Default functionality."""


class TreeItem(TreeItemBase):
    """Built-in tree item class. Default functionality."""
