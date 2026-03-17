from django.conf.urls import url

import views

urlpatterns = [
    url(r'^$', views.index),
    url(r'^traced_with_attrs/', views.traced_func_with_attrs),
    url(r'^traced_with_error/', views.traced_func_with_error),
    url(r'^traced_with_arg/?(?P<arg>\d+)?/?', views.traced_func_with_arg),
    url(r'^traced/', views.traced_func),
    url(r'^traced_scope/', views.traced_scope_func),
    url(r'^untraced/', views.untraced_func)
]
