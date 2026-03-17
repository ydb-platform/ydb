from __future__ import unicode_literals, absolute_import

from django.contrib import admin
from django.contrib.contenttypes.models import ContentType
from django.views.generic import TemplateView

from . import report, introspection


class AdminExport(TemplateView):
    template_name = 'export_action/export.html'

    def get_queryset(self, model_class):
        if self.request.GET.get("session_key"):
            ids = self.request.session[self.request.GET["session_key"]]
        else:
            ids = self.request.GET['ids'].split(',')
        try:
            model_admin = admin.site._registry[model_class]
        except KeyError:
            raise ValueError("Model %r not registered with admin" % model_class)
        queryset = model_admin.get_queryset(self.request).filter(pk__in=ids)
        return queryset

    def get_model_class(self):
        model_class = ContentType.objects.get(id=self.request.GET['ct']).model_class()
        return model_class

    def get_context_data(self, **kwargs):
        context = super(AdminExport, self).get_context_data(**kwargs)
        field_name = self.request.GET.get('field', '')
        model_class = self.get_model_class()
        queryset = self.get_queryset(model_class)
        path = self.request.GET.get('path', '')
        context['opts'] = model_class._meta
        context['queryset'] = queryset
        context['model_ct'] = self.request.GET['ct']
        context['related_fields'] = introspection.get_relation_fields_from_model(model_class)
        context.update(introspection.get_fields(model_class, field_name, path))
        return context

    def post(self, request, **kwargs):
        context = self.get_context_data(**kwargs)
        fields = []
        for field_name, value in request.POST.items():
            if value == "on":
                fields.append(field_name)
        data_list, message = report.report_to_list(
            context['queryset'],
            fields,
            self.request.user,
        )
        format = request.POST.get("__format")
        if format == "html":
            return report.list_to_html_response(data_list, header=fields)
        elif format == "csv":
            return report.list_to_csv_response(data_list, header=fields)
        else:
            return report.list_to_xlsx_response(data_list, header=fields)

    def get(self, request, *args, **kwargs):
        if request.GET.get("related", request.POST.get("related")):  # Dispatch to the other view
            return AdminExportRelated.as_view()(request=self.request)
        return super(AdminExport, self).get(request, *args, **kwargs)


class AdminExportRelated(TemplateView):
    template_name = 'export_action/fields.html'

    def get(self, request, **kwargs):
        context = self.get_context_data(**kwargs)
        model_class = ContentType.objects.get(id=self.request.GET['model_ct']).model_class()
        field_name = request.GET['field']
        path = request.GET['path']
        field_data = introspection.get_fields(model_class, field_name, path)
        context['related_fields'], model_ct, context['path'] = introspection.get_related_fields(
            model_class, field_name, path
        )
        context['model_ct'] = model_ct.id
        context['field_name'] = field_name
        context['table'] = True
        context.update(field_data)
        return self.render_to_response(context)
