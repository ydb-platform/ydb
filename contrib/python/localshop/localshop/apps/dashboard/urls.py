from django.conf.urls import include, url

from localshop.apps.dashboard import views


app_name = 'dashboard'

repository_urls = [
    # Package urls
    url(r'^packages/add/$',
        views.PackageAddView.as_view(),
        name='package_add'),
    url(r'^packages/(?P<name>[-\._\w]+)/', include([
        url(r'^$',
            views.PackageDetailView.as_view(),
            name='package_detail'),
        url(r'^refresh-from-upstream/$',
            views.PackageRefreshView.as_view(),
            name='package_refresh'),
        url(r'^release-mirror-file/$',
            views.PackageMirrorFileView.as_view(),
            name='package_mirror_file'),
    ])),

    # CIDR
    url(r'^settings/cidr/$',
        views.CidrListView.as_view(), name='cidr_index'),
    url(r'^settings/cidr/create$',
        views.CidrCreateView.as_view(), name='cidr_create'),
    url(r'^settings/cidr/(?P<pk>\d+)/edit',
        views.CidrUpdateView.as_view(), name='cidr_edit'),
    url(r'^settings/cidr/(?P<pk>\d+)/delete',
        views.CidrDeleteView.as_view(), name='cidr_delete'),

    # Credentials
    url(r'^settings/credentials/$',
        views.CredentialListView.as_view(),
        name='credential_index'),
    url(r'^settings/credentials/create$',
        views.CredentialCreateView.as_view(),
        name='credential_create'),
    url(r'^settings/credentials/(?P<access_key>[-a-f0-9]+)/secret',
        views.CredentialSecretKeyView.as_view(),
        name='credential_secret'),
    url(r'^settings/credentials/(?P<access_key>[-a-f0-9]+)/edit',
        views.CredentialUpdateView.as_view(),
        name='credential_edit'),
    url(r'^settings/credentials/(?P<access_key>[-a-f0-9]+)/delete',
        views.CredentialDeleteView.as_view(),
        name='credential_delete'),

    url(r'^settings/teams/$', views.TeamAccessView.as_view(), name='team_access'),
]

urlpatterns = [
    url(r'^$', views.IndexView.as_view(), name='index'),
    url(r'^repositories/create$', views.RepositoryCreateView.as_view(), name='repository_create'),

    url(r'^repositories/(?P<slug>[^/]+)/', include([
        url(r'^$', views.RepositoryDetailView.as_view(), name='repository_detail'),
        url(r'^edit$', views.RepositoryUpdateView.as_view(), name='repository_edit'),
        url(r'^delete$', views.RepositoryDeleteView.as_view(), name='repository_delete'),
        url(r'^refresh$', views.RepositoryRefreshView.as_view(), name='repository_refresh'),
    ])),

    url(r'^repositories/(?P<repo>[^/]+)/', include(repository_urls))

]
