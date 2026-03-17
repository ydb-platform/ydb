from django.db import models
from django.contrib.sites.models import Site

from django_hosts.managers import HostSiteManager


class Author(models.Model):
    name = models.TextField()
    site = models.ForeignKey(Site, models.CASCADE)


class BlogPost(models.Model):
    content = models.TextField()
    author = models.ForeignKey(Author, models.CASCADE)

    objects = models.Manager()
    dead_end = HostSiteManager()
    on_site = HostSiteManager('author__site')
    no_select_related = HostSiteManager('author__site',
                                        select_related=False)
    non_existing = HostSiteManager('blabla')
    non_rel = HostSiteManager('content')

    def __unicode__(self):
        return str(self.id)


class WikiPage(models.Model):
    content = models.TextField()
    site = models.ForeignKey(Site, models.CASCADE)

    objects = models.Manager()
    on_site = HostSiteManager()

    def __unicode__(self):
        return str(self.id)
