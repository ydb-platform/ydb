from django.urls import path

from dynamic_raw_id.views import label_view

app_name = "dynamic_raw_id"

urlpatterns = [
    path(
        "<slug:app_name>/<slug:model_name>/multiple/",
        label_view,
        {
            "multi": True,
            "template_object_name": "objects",
            "template_name": "dynamic_raw_id/multi_label.html",
        },
        name="dynamic_raw_id_multi_label",
    ),
    path(
        "<slug:app_name>/<slug:model_name>/",
        label_view,
        {"template_name": "dynamic_raw_id/label.html"},
        name="dynamic_raw_id_label",
    ),
]
