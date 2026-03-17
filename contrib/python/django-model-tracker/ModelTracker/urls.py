try:
    from django.urls import re_path
except ModuleNotFoundError:
    from django.conf.urls import re_path
from . import views

urlpatterns = [
    re_path('^$',views.main,name="main"),
    re_path('show/',views.showChanges,name="showModelChanges"),
    re_path(r'revert/(\d+)',views.revert,name="revert"),
    re_path('get/',views.getChanges,name="getModelChanges"),
   ]
