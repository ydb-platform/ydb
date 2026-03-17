# coding: utf-8

"""
Module where grappelli dashboard classes are defined.
"""

from django import forms
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from grappelli.dashboard import modules
from grappelli.dashboard.utils import get_admin_site_name


class Dashboard(metaclass=forms.MediaDefiningClass):
    """
    Base class for dashboards.
    The Dashboard class is a simple python list that has three additional
    properties:

    ``title``
        The dashboard title, by default, it is displayed above the dashboard
        in a ``h2`` tag. Default value: 'Dashboard'.

    ``template``
        The template to use to render the dashboard.
        Default value: 'admin_tools/dashboard/dashboard.html'

    ``columns``
        An integer that represents the number of columns for the dashboard.
        Default value: 2.

    If you want to customize the look of your dashboard and it's modules, you
    can declare css stylesheets and/or javascript files to include when
    rendering the dashboard (these files should be placed in your
    media path), for example::

        from admin_tools.dashboard import Dashboard

        class MyDashboard(Dashboard):
            class Media:
                css = {
                    'all': (
                        'css/mydashboard.css',
                        'css/mystyles.css',
                    ),
                }
                js = (
                    'js/mydashboard.js',
                    'js/myscript.js',
                )

    Here's an example of a custom dashboard::

        from django.urls import reverse
        from django.utils.translation import gettext_lazy as _
        from admin_tools.dashboard import modules, Dashboard

        class MyDashboard(Dashboard):

            # we want a 3 columns layout
            columns = 3

            def __init__(self, **kwargs):
                super(MyDashboard, self).__init__(**kwargs)

                # append an app list module for "Applications"
                self.children.append(modules.AppList(
                    title=_('Applications'),
                    exclude=('django.contrib.*',),
                ))

                # append an app list module for "Administration"
                self.children.append(modules.AppList(
                    title=_('Administration'),
                    models=('django.contrib.*',),
                ))

                # append a recent actions module
                self.children.append(modules.RecentActions(
                    title=_('Recent actions'),
                    limit=5
                ))

    """

    title = _('Dashboard')
    template = 'grappelli/dashboard/dashboard.html'
    columns = 2
    children = None

    def __init__(self, **kwargs):
        for key in kwargs:
            if hasattr(self.__class__, key):
                setattr(self, key, kwargs[key])
        self.children = self.children or []

    def init_with_context(self, context):
        """
        Sometimes you may need to access context or request variables to build
        your dashboard, this is what the ``init_with_context()`` method is for.
        This method is called just before the display with a
        ``django.template.RequestContext`` as unique argument, so you can
        access to all context variables and to the ``django.http.HttpRequest``.
        """
        pass

    def get_id(self):
        """
        Internal method used to distinguish different dashboards in js code.
        """
        return 'grp-dashboard'


class DefaultIndexDashboard(Dashboard):
    """
    The default dashboard displayed on the admin index page.
    To change the default dashboard you'll have to type the following from the
    commandline in your project root directory::

        python manage.py customdashboard

    And then set the `GRAPPELLI_INDEX_DASHBOARD`` settings variable to
    point to your custom index dashboard class.
    """

    def init_with_context(self, context):
        site_name = get_admin_site_name(context)
        # append a link list module for "quick links"
        self.children.append(modules.LinkList(
            _('Quick links'),
            collapsible=False,
            children=[
                [_('Return to site'), '/'],
                [_('Change password'),
                 reverse('%s:password_change' % site_name)],
                [_('Log out'), reverse('%s:logout' % site_name)],
            ]
        ))

        # append an app list module for "Applications"
        self.children.append(modules.AppList(
            _('Applications'),
            exclude=('django.contrib.*',),
        ))

        # append an app list module for "Administration"
        self.children.append(modules.AppList(
            _('Administration'),
            models=('django.contrib.*',),
        ))

        # append a recent actions module
        self.children.append(modules.RecentActions(_('Recent actions'), 5))

        # append a feed module
        self.children.append(modules.Feed(
            _('Latest Django News'),
            feed_url='http://www.djangoproject.com/rss/weblog/',
            limit=5
        ))

        # append another link list module for "support".
        self.children.append(modules.LinkList(
            _('Support'),
            children=[
                {
                    'title': _('Django documentation'),
                    'url': 'http://docs.djangoproject.com/',
                    'external': True,
                    'target': '_blank',
                },
                {
                    'title': _('Django "django-users" mailing list'),
                    'url': 'http://groups.google.com/group/django-users',
                    'external': True,
                    'target': True,  # ~= open in new window: True
                },
                {
                    'title': _('Django irc channel'),
                    'url': 'irc://irc.freenode.net/django',
                    'external': True,
                },
            ]
        ))
