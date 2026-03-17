from django.urls import path
from django.contrib.admin.views.decorators import staff_member_required
from .views import AdminExport

view = staff_member_required(AdminExport.as_view())

urlpatterns = [
    path('export/', view, name="export"),
]
