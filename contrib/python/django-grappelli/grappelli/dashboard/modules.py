# coding: utf-8

"""
Module where grappelli dashboard modules classes are defined.
"""

from django.apps import apps as django_apps
from django.utils.text import capfirst
from django.utils.translation import gettext_lazy as _

from grappelli.dashboard.utils import AppListElementMixin


class DashboardModule(object):
    """
    Base class for all dashboard modules.
    Dashboard modules have the following properties:

    ``collapsible``
        Boolean that determines whether the module is collapsible, this
        allows users to show/hide module content. Default: ``True``.

    ``column``
        Integer that corresponds to the column.
        Default: None.

    ``title``
        String that contains the module title, make sure you use the django
        gettext functions if your application is multilingual.
        Default value: ''.

    ``title_url``
        String that contains the module title URL. If given the module
        title will be a link to this URL. Default value: ``None``.

    ``css_classes``
        A list of css classes to be added to the module ``div`` class
        attribute. Default value: ``None``.

    ``pre_content``
        Text or HTML content to display above the module content.
        Default value: ``None``.

    ``post_content``
        Text or HTML content to display under the module content.
        Default value: ``None``.

    ``template``
        The template to use to render the module.
        Default value: 'grappelli/dashboard/module.html'.
    """

    template = 'grappelli/dashboard/module.html'
    collapsible = True
    column = None
    show_title = True
    title = ''
    title_url = None
    css_classes = None
    pre_content = None
    post_content = None
    children = None

    def __init__(self, title=None, **kwargs):
        if title is not None:
            self.title = title
        for key in kwargs:
            if hasattr(self.__class__, key):
                setattr(self, key, kwargs[key])
        self.children = self.children or []
        self.css_classes = self.css_classes or []
        # boolean flag to ensure that the module is initialized only once
        self._initialized = False

    def init_with_context(self, context):
        """
        Like for the :class:`~grappelli.dashboard.Dashboard` class, dashboard
        modules have a ``init_with_context`` method that is called with a
        ``django.template.RequestContext`` instance as unique argument.

        This gives you enough flexibility to build complex modules, for
        example, let's build a "history" dashboard module, that will list the
        last ten visited pages::

            from grappelli.dashboard import modules

            class HistoryDashboardModule(modules.LinkList):
                title = 'History'

                def init_with_context(self, context):
                    request = context['request']
                    # we use sessions to store the visited pages stack
                    history = request.session.get('history', [])
                    for item in history:
                        self.children.append(item)
                    # add the current page to the history
                    history.insert(0, {
                        'title': context['title'],
                        'url': request.META['PATH_INFO']
                    })
                    if len(history) > 10:
                        history = history[:10]
                    request.session['history'] = history

        """
        pass

    def is_empty(self):
        """
        Return True if the module has no content and False otherwise.
        """

        return self.pre_content is None and self.post_content is None and len(self.children) == 0

    def render_css_classes(self):
        """
        Return a string containing the css classes for the module.
        """

        ret = ['grp-dashboard-module']
        if self.collapsible:
            ret.append('grp-collapse')
            if "grp-open" not in self.css_classes and "grp-closed" not in self.css_classes:
                ret.append('grp-open')
        ret += self.css_classes
        return ' '.join(ret)


class Group(DashboardModule):
    """
    Represents a group of modules.

    Here's an example of modules group::

        from grappelli.dashboard import modules, Dashboard

        class MyDashboard(Dashboard):
            def __init__(self, **kwargs):
                Dashboard.__init__(self, **kwargs)
                self.children.append(modules.Group(
                    title="My group",
                    children=[
                        modules.AppList(
                            title='Administration',
                            models=('django.contrib.*',)
                        ),
                        modules.AppList(
                            title='Applications',
                            exclude=('django.contrib.*',)
                        )
                    ]
                ))

    """

    template = 'grappelli/dashboard/modules/group.html'

    def init_with_context(self, context):
        if self._initialized:
            return
        for module in self.children:
            module.init_with_context(context)
        self._initialized = True

    def is_empty(self):
        """
        A group of modules is considered empty if it has no children or if
        all its children are empty.
        """

        if super(Group, self).is_empty():
            return True
        for child in self.children:
            if not child.is_empty():
                return False
        return True


class LinkList(DashboardModule):
    """
    A module that displays a list of links.
    """

    title = _('Links')
    template = 'grappelli/dashboard/modules/link_list.html'

    def init_with_context(self, context):
        if self._initialized:
            return
        new_children = []
        for link in self.children:
            if isinstance(link, (tuple, list,)):
                link_dict = {'title': link[0], 'url': link[1]}
                if len(link) >= 3:
                    link_dict['external'] = link[2]
                if len(link) >= 4:
                    link_dict['description'] = link[3]
                if len(link) >= 5:
                    target = link[4]
                    if isinstance(target, bool):
                        target = '_blank' if target else None
                    if target:
                        link_dict['target'] = str(target)
                new_children.append(link_dict)
            else:
                new_children.append(link)
        self.children = new_children
        self._initialized = True


class AppList(DashboardModule, AppListElementMixin):
    """
    Module that lists installed apps and their models.
    """

    title = _('Applications')
    template = 'grappelli/dashboard/modules/app_list.html'
    models = None
    exclude = None

    def __init__(self, title=None, **kwargs):
        self.models = list(kwargs.pop('models', []))
        self.exclude = list(kwargs.pop('exclude', []))
        super(AppList, self).__init__(title, **kwargs)

    def init_with_context(self, context):
        if self._initialized:
            return
        items = self._visible_models(context['request'])
        apps = {}
        for model, perms in items:
            app_label = model._meta.app_label
            if app_label not in apps:
                apps[app_label] = {
                    'name': django_apps.get_app_config(app_label).verbose_name,
                    'title': capfirst(app_label.title()),
                    'url': self._get_admin_app_list_url(model, context),
                    'models': []
                }
            model_dict = {}
            model_dict['title'] = capfirst(model._meta.verbose_name_plural)
            if perms['change'] or perms['view']:
                model_dict['view_only'] = not perms['change']
                model_dict['admin_url'] = self._get_admin_change_url(model, context)
            if perms['add']:
                model_dict['add_url'] = self._get_admin_add_url(model, context)
            apps[app_label]['models'].append(model_dict)

        apps_sorted = list(apps.keys())
        apps_sorted.sort()
        for app in apps_sorted:
            # sort model list alphabetically
            apps[app]['models'].sort(key=lambda i: i['title'])
            self.children.append(apps[app])
        self._initialized = True


class ModelList(DashboardModule, AppListElementMixin):
    """
    Module that lists a set of models.
    """

    template = 'grappelli/dashboard/modules/model_list.html'
    models = None
    exclude = None

    def __init__(self, title=None, models=None, exclude=None, **kwargs):
        self.models = list(models or [])
        self.exclude = list(exclude or [])
        super(ModelList, self).__init__(title, **kwargs)

    def init_with_context(self, context):
        if self._initialized:
            return
        items = self._visible_models(context['request'])
        if not items:
            return
        for model, perms in items:
            model_dict = {}
            model_dict['title'] = capfirst(model._meta.verbose_name_plural)
            if perms['change'] or perms['view']:
                model_dict['view_only'] = not perms['change']
                model_dict['admin_url'] = self._get_admin_change_url(model, context)
            if perms['add']:
                model_dict['add_url'] = self._get_admin_add_url(model, context)
            self.children.append(model_dict)
        self._initialized = True


class RecentActions(DashboardModule):
    """
    Module that lists the recent actions for the current user.
    """

    title = _('Recent actions')
    template = 'grappelli/dashboard/modules/recent_actions.html'
    limit = 10
    include_list = None
    exclude_list = None

    def __init__(self, title=None, limit=10, include_list=None,
                 exclude_list=None, **kwargs):
        self.include_list = include_list or []
        self.exclude_list = exclude_list or []
        kwargs.update({'limit': limit})
        super(RecentActions, self).__init__(title, **kwargs)

    def init_with_context(self, context):
        if self._initialized:
            return
        from django.db.models import Q
        from django.contrib.admin.models import LogEntry

        request = context['request']

        def get_qset(list):
            from django.contrib.contenttypes.models import ContentType
            qset = None
            for contenttype in list:
                if isinstance(contenttype, ContentType):
                    current_qset = Q(content_type__id=contenttype.id)
                else:
                    try:
                        app_label, model = contenttype.split('.')
                    except:
                        raise ValueError('Invalid contenttype: "%s"' % contenttype)
                    current_qset = Q(
                        content_type__app_label=app_label,
                        content_type__model=model
                    )
                if qset is None:
                    qset = current_qset
                else:
                    qset = qset | current_qset
            return qset

        if request.user is None:
            qs = LogEntry.objects.all()
        else:
            qs = LogEntry.objects.filter(user__pk__exact=request.user.pk)

        if self.include_list:
            qs = qs.filter(get_qset(self.include_list))
        if self.exclude_list:
            qs = qs.exclude(get_qset(self.exclude_list))

        self.children = qs.select_related('content_type', 'user')[:self.limit]
        self._initialized = True


class Feed(DashboardModule):
    """
    Class that represents a feed dashboard module.
    """

    title = _('RSS Feed')
    template = 'grappelli/dashboard/modules/feed.html'
    feed_url = None
    limit = None

    def __init__(self, title=None, feed_url=None, limit=None, **kwargs):
        kwargs.update({'feed_url': feed_url, 'limit': limit})
        super(Feed, self).__init__(title, **kwargs)

    def init_with_context(self, context):
        if self._initialized:
            return
        import datetime
        if self.feed_url is None:
            raise ValueError('You must provide a valid feed URL')
        try:
            import feedparser
        except ImportError:
            self.children.append({
                'title': ('You must install the FeedParser python module'),
                'warning': True,
            })
            return

        feed = feedparser.parse(self.feed_url)
        if self.limit is not None:
            entries = feed['entries'][:self.limit]
        else:
            entries = feed['entries']
        for entry in entries:
            entry.url = entry.link
            try:
                entry.date = datetime.date(*entry.updated_parsed[0:3])
            except:
                # no date for certain feeds
                pass
            self.children.append(entry)
        self._initialized = True
