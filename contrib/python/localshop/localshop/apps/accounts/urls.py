from django.conf.urls import include, url

from localshop.apps.accounts import views


app_name = 'accounts'

urlpatterns = [
    url(r'^profile/$', views.ProfileView.as_view(), name='profile'),
    url(r'^access-keys/$', views.AccessKeyListView.as_view(), name='access_key_list'),
    url(r'^access-keys/new$', views.AccessKeyCreateView.as_view(), name='access_key_create'),
    url(r'^access-keys/(?P<pk>\d+)/', include([
        url(r'^secret$', views.AccessKeySecretView.as_view(), name='access_key_secret'),
        url(r'^edit$', views.AccessKeyUpdateView.as_view(), name='access_key_edit'),
        url(r'^delete$', views.AccessKeyDeleteView.as_view(), name='access_key_delete'),
    ])),

    url(r'^teams/$', views.TeamListView.as_view(), name='team_list'),
    url(r'^teams/create$', views.TeamCreateView.as_view(), name='team_create'),
    url(r'^teams/(?P<pk>\d+)/', include([
        url(r'^$', views.TeamDetailView.as_view(), name='team_detail'),
        url(r'^edit$', views.TeamUpdateView.as_view(), name='team_edit'),
        url(r'^delete$', views.TeamDeleteView.as_view(), name='team_delete'),
        url(r'^member-add$', views.TeamMemberAddView.as_view(), name='team_member_add'),
        url(r'^member-remove$', views.TeamMemberRemoveView.as_view(), name='team_member_remove'),
    ]))
]
