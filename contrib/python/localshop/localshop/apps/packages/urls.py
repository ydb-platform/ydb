from django.conf.urls import url
from django.views.decorators.cache import cache_page

from localshop.apps.packages import views


app_name = 'packages'

urlpatterns = [
    url(r'^(?P<repo>[-\._\w]+)/?$', views.SimpleIndex.as_view(),
        name='simple_index'),

    url(r'^(?P<repo>[-\._\w]+)/(?P<slug>[-\._\w\s]+)/?$',
        cache_page(60)(views.SimpleDetail.as_view()),
        name='simple_detail'),

    url(r'^(?P<repo>[-\._\w]+)/download/(?P<name>[-\._\w\s]+)/(?P<pk>\d+)/(?P<filename>.*)$',
        views.DownloadReleaseFile.as_view(), name='download'),
]
