from django.conf import settings
from django.db import models
from django.db.models.fields import FieldDoesNotExist


class HostSiteManager(models.Manager):
    """
    A model manager to limit objects to those associated with a site.

    :param field_name: the name of the related field pointing at the
                       :class:`~django.contrib.sites.models.Site` model,
                       or a series of relations using the
                       ``field1__field2__field3`` notation. Falls back
                       to looking for 'site' and 'sites' fields.
    :param select_related: a boolean specifying whether to use
                           :meth:`~django.db.models.QuerySet.select_related`
                           when querying the database

    Define a manager instance in your model class with one
    of the following notations::

        on_site = HostSiteManager()  # automatically looks for site and sites
        on_site = HostSiteManager("author__site")
        on_site = HostSiteManager("author__blog__site")
        on_site = HostSiteManager("author__blog__site",
                                  select_related=False)

    Then query against it with one of the manager methods::

        def home_page(request):
            posts = BlogPost.on_site.by_request(request).all()
            return render(request, 'home_page.html', {'posts': posts})

    """

    def __init__(self, field_name=None, select_related=True):
        super(HostSiteManager, self).__init__()
        self._field_name = field_name
        self._select_related = select_related
        self._depth = 1
        self._is_validated = False

    def _validate_field_name(self):
        # If a custom name is provided, make sure the field exists on the model
        field = None
        if self._field_name is not None:
            name_parts = self._field_name.split("__", 1)
            rel_depth = len(name_parts)
            if rel_depth > self._depth:
                self._depth = rel_depth
            field_name = name_parts[0]
            try:
                field = self.model._meta.get_field(field_name)
            except FieldDoesNotExist:
                pass
        # Otherwise, see if there is a field called either 'site' or 'sites'
        else:
            for potential_name in ['site', 'sites']:
                try:
                    field = self.model._meta.get_field(potential_name)
                except FieldDoesNotExist:
                    field_name = None
                else:
                    self._field_name = field_name = potential_name
                    self._is_validated = True
                    break
        # Now do a type check on the field (FK or M2M only)
        if field:
            if not isinstance(field, (models.ForeignKey,
                                      models.ManyToManyField)):
                raise TypeError("%s must be a ForeignKey or "
                                "ManyToManyField." % field_name)
        else:
            raise ValueError("%s couldn't find a field named %s in %s." %
                             (self.__class__.__name__, field_name,
                              self.model._meta.object_name))
        self._is_validated = True

    def get_queryset(self, site_id=None):
        if site_id is None:
            site_id = settings.SITE_ID
        if not self._is_validated:
            self._validate_field_name()
        qs = super(HostSiteManager, self).get_queryset()
        return qs.filter(**{'%s__id__exact' % self._field_name: site_id})

    def by_id(self, site_id=None):
        """
        Returns a queryset matching the given site id. If not given
        this falls back to the ``SITE_ID`` setting.

        :param site_id: the ID of the site
        :rtype: :class:`~django.db.models.query.QuerySet`
        """
        return self.get_queryset(site_id)

    def by_request(self, request):
        """
        Returns a queryset matching the given request's site
        attribute.

        :param request: the current request
        :type request: :class:`~django.http.HttpRequest`
        :rtype: :class:`~django.db.models.query.QuerySet`
        """
        if not hasattr(request, "site") or request.site is None:
            return self.none()
        return self.by_site(request.site)

    def by_site(self, site):
        """
        Returns a queryset matching the given site.

        :param site: a site instance
        :type site: :class:`~django.contrib.sites.models.Site`
        :rtype: :class:`~django.db.models.query.QuerySet`
        """
        return self.by_id(site.id)
