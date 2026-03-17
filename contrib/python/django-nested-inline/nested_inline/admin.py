from django import VERSION, forms
from django.conf import settings
from django.contrib import admin
from django.contrib.admin import helpers
from django.contrib.admin.options import InlineModelAdmin, reverse
from django.contrib.admin.utils import unquote
from django.core.exceptions import FieldDoesNotExist, PermissionDenied
from django.db import models, transaction
from django.forms.formsets import all_valid
from django.http import Http404
from django.templatetags.static import static
from django.utils.decorators import method_decorator
from django.utils.encoding import force_str
from django.utils.html import escape
from django.utils.translation import gettext as _
from django.views.decorators.csrf import csrf_protect

csrf_protect_m = method_decorator(csrf_protect)


class InlineInstancesMixin():
    def get_inline_instances(self, request, obj=None):
        inline_instances = []
        for inline_class in self.inlines:
            inline = inline_class(self.model, self.admin_site)
            if request:
                if VERSION < (2, 1, 0):
                    if not (inline.has_add_permission(request) or
                            inline.has_change_permission(request, obj) or
                            inline.has_delete_permission(request, obj)):
                        continue
                    if not inline.has_add_permission(request):
                        inline.max_num = 0
                else:
                    if not (inline.has_add_permission(request, obj) or
                            inline.has_change_permission(request, obj) or
                            inline.has_delete_permission(request, obj)):
                        continue
                    if not inline.has_add_permission(request, obj):
                        inline.max_num = 0
            inline_instances.append(inline)

        return inline_instances


class NestedModelAdmin(InlineInstancesMixin, admin.ModelAdmin):

    class Media:
        css = {
            "all": ('admin/css/forms-nested.css',)
        }
        js = ('admin/js/inlines-nested%s.js' % ('' if settings.DEBUG else '.min'),)

    def save_formset(self, request, form, formset, change):
        """
        Given an inline formset save it to the database.
        """
        formset.save()

        for form in formset.forms:
            if hasattr(form, 'nested_formsets') and form not in formset.deleted_forms:
                for nested_formset in form.nested_formsets:
                    self.save_formset(request, form, nested_formset, change)

    def save_related(self, request, form, formsets, change):
        """
        Given the ``HttpRequest``, the parent ``ModelForm`` instance, the
        list of inline formsets and a boolean value based on whether the
        parent is being added or changed, save the related objects to the
        database. Note that at this point save_form() and save_model() have
        already been called.
        """
        form.save_m2m()
        for formset in formsets:
            self.save_formset(request, form, formset, change=change)

    def add_nested_inline_formsets(self, request, inline, formset, depth=0):
        if depth > 5:
            raise Exception("Maximum nesting depth reached (5)")
        for form in formset.forms:
            nested_formsets = []
            for nested_inline in inline.get_inline_instances(request):
                InlineFormSet = nested_inline.get_formset(request, form.instance)
                prefix = "%s-%s" % (form.prefix, InlineFormSet.get_default_prefix())

                if request.method == 'POST' and any(s.startswith(prefix) for s in request.POST.keys()):
                    nested_formset = InlineFormSet(request.POST, request.FILES,
                                                   instance=form.instance,
                                                   prefix=prefix, queryset=nested_inline.get_queryset(request))
                else:
                    nested_formset = InlineFormSet(instance=form.instance,
                                                   prefix=prefix, queryset=nested_inline.get_queryset(request))
                nested_formsets.append(nested_formset)
                if nested_inline.inlines:
                    self.add_nested_inline_formsets(request, nested_inline, nested_formset, depth=depth + 1)
            form.nested_formsets = nested_formsets

    def wrap_nested_inline_formsets(self, request, inline, formset):
        media = None

        def get_media(extra_media):
            if media and extra_media:
                return media + extra_media
            elif extra_media:
                return extra_media
            else:
                return media

        for form in formset.forms:
            wrapped_nested_formsets = []
            for nested_inline, nested_formset in zip(inline.get_inline_instances(request), form.nested_formsets):
                if form.instance.pk:
                    instance = form.instance
                else:
                    instance = None
                fieldsets = list(nested_inline.get_fieldsets(request, instance))
                readonly = list(nested_inline.get_readonly_fields(request, instance))
                prepopulated = dict(nested_inline.get_prepopulated_fields(request, instance))
                wrapped_nested_formset = helpers.InlineAdminFormSet(
                    nested_inline, nested_formset,
                    fieldsets, prepopulated, readonly, model_admin=self,
                )
                wrapped_nested_formsets.append(wrapped_nested_formset)
                media = get_media(wrapped_nested_formset.media)
                if nested_inline.inlines:
                    media = get_media(self.wrap_nested_inline_formsets(request, nested_inline, nested_formset))
            form.nested_formsets = wrapped_nested_formsets
        return media

    def formset_has_nested_data(self, formsets):
        for formset in formsets:
            if not formset.is_bound:
                pass
            for form in formset:
                if hasattr(form, 'cleaned_data') and form.cleaned_data:
                    return True
                elif hasattr(form, 'nested_formsets'):
                    if self.formset_has_nested_data(form.nested_formsets):
                        return True

    def all_valid_with_nesting(self, formsets):
        "Recursively validate all nested formsets"
        if not all_valid(formsets):
            return False

        for formset in formsets:
            if not formset.is_bound:
                pass
            for form in formset:
                if hasattr(form, 'nested_formsets'):
                    if not self.all_valid_with_nesting(form.nested_formsets):
                        return False

                    # TODO - find out why this breaks when extra = 1 and just adding new item with no sub items
                    if (not hasattr(form, 'cleaned_data') or not form.cleaned_data) and\
                            self.formset_has_nested_data(form.nested_formsets):

                        form._errors["__all__"] = form.error_class(
                            [u"Parent object must be created when creating nested inlines."]
                        )
                        return False
        return True

    @csrf_protect_m
    @transaction.atomic
    def add_view(self, request, form_url='', extra_context=None):
        "The 'add' admin view for this model."
        model = self.model
        opts = model._meta

        if not self.has_add_permission(request):
            raise PermissionDenied

        ModelForm = self.get_form(request)
        formsets = []
        inline_instances = self.get_inline_instances(request, None)
        if request.method == 'POST':
            form = ModelForm(request.POST, request.FILES)
            if form.is_valid():
                new_object = self.save_form(request, form, change=False)
                form_validated = True
            else:
                form_validated = False
                new_object = self.model()
            prefixes = {}
            for FormSet, inline in self.get_formsets_with_inlines(request):
                prefix = FormSet.get_default_prefix()
                prefixes[prefix] = prefixes.get(prefix, 0) + 1
                if prefixes[prefix] != 1 or not prefix:
                    prefix = "%s-%s" % (prefix, prefixes[prefix])
                formset = FormSet(data=request.POST, files=request.FILES,
                                  instance=new_object,
                                  save_as_new="_saveasnew" in request.POST,
                                  prefix=prefix, queryset=inline.get_queryset(request))
                formsets.append(formset)
                if inline.inlines:
                    self.add_nested_inline_formsets(request, inline, formset)
            if self.all_valid_with_nesting(formsets) and form_validated:
                self.save_model(request, new_object, form, False)
                self.save_related(request, form, formsets, False)
                args = ()
                # Provide `add_message` argument to ModelAdmin.log_addition for
                # Django 1.9 and up.
                if VERSION[:2] >= (1, 9):
                    add_message = self.construct_change_message(
                        request, form, formsets, add=True
                    )
                    args = (request, new_object, add_message)
                else:
                    args = (request, new_object)
                self.log_addition(*args)
                return self.response_add(request, new_object)
        else:
            # Prepare the dict of initial data from the request.
            # We have to special-case M2Ms as a list of comma-separated PKs.
            initial = dict(request.GET.items())
            for k in initial:
                try:
                    f = opts.get_field(k)
                except FieldDoesNotExist:
                    continue
                if isinstance(f, models.ManyToManyField):
                    initial[k] = initial[k].split(",")
            form = ModelForm(initial=initial)
            prefixes = {}
            for FormSet, inline in self.get_formsets_with_inlines(request):
                prefix = FormSet.get_default_prefix()
                prefixes[prefix] = prefixes.get(prefix, 0) + 1
                if prefixes[prefix] != 1 or not prefix:
                    prefix = "%s-%s" % (prefix, prefixes[prefix])
                formset = FormSet(instance=self.model(), prefix=prefix,
                                  queryset=inline.get_queryset(request))
                formsets.append(formset)
                if hasattr(inline, 'inlines') and inline.inlines:
                    self.add_nested_inline_formsets(request, inline, formset)

        adminForm = helpers.AdminForm(form, list(self.get_fieldsets(request)),
                                      self.get_prepopulated_fields(request),
                                      self.get_readonly_fields(request),
                                      model_admin=self)
        media = self.media + adminForm.media

        inline_admin_formsets = []
        for inline, formset in zip(inline_instances, formsets):
            fieldsets = list(inline.get_fieldsets(request))
            readonly = list(inline.get_readonly_fields(request))
            prepopulated = dict(inline.get_prepopulated_fields(request))
            inline_admin_formset = helpers.InlineAdminFormSet(inline, formset,
                                                              fieldsets, prepopulated, readonly, model_admin=self)
            inline_admin_formsets.append(inline_admin_formset)
            media = media + inline_admin_formset.media
            if hasattr(inline, 'inlines') and inline.inlines:
                extra_media = self.wrap_nested_inline_formsets(
                    request, inline, formset)

                if extra_media:
                    media += extra_media

        context = {
            'title': _('Add %s') % force_str(opts.verbose_name),
            'adminform': adminForm,
            'is_popup': "_popup" in request.GET,
            'show_delete': False,
            'media': media,
            'inline_admin_formsets': inline_admin_formsets,
            'errors': helpers.AdminErrorList(form, formsets),
            'app_label': opts.app_label,
        }
        context.update(self.admin_site.each_context(request))
        context.update(extra_context or {})
        return self.render_change_form(request, context, form_url=form_url, add=True)

    @csrf_protect_m
    @transaction.atomic
    def change_view(self, request, object_id, form_url='', extra_context=None):
        "The 'change' admin view for this model."
        model = self.model
        opts = model._meta

        obj = self.get_object(request, unquote(object_id))

        if not self.has_change_permission(request, obj):
            raise PermissionDenied

        if obj is None:
            raise Http404(_('%(name)s object with primary key %(key)r does not exist.') % {
                          'name': force_str(opts.verbose_name), 'key': escape(object_id)})

        if request.method == 'POST' and "_saveasnew" in request.POST:
            return self.add_view(request, form_url=reverse('admin:%s_%s_add' %
                                                           (opts.app_label,
                                                            opts.model_name),
                                                           current_app=self.admin_site.name))

        ModelForm = self.get_form(request, obj)
        formsets = []
        inline_instances = self.get_inline_instances(request, obj)
        if request.method == 'POST':
            form = ModelForm(request.POST, request.FILES, instance=obj)
            if form.is_valid():
                form_validated = True
                new_object = self.save_form(request, form, change=True)
            else:
                form_validated = False
                new_object = obj
            prefixes = {}
            for FormSet, inline in self.get_formsets_with_inlines(request, new_object):
                prefix = FormSet.get_default_prefix()
                prefixes[prefix] = prefixes.get(prefix, 0) + 1
                if prefixes[prefix] != 1 or not prefix:
                    prefix = "%s-%s" % (prefix, prefixes[prefix])
                formset = FormSet(
                    request.POST, request.FILES, instance=new_object,
                    prefix=prefix, queryset=inline.get_queryset(request),
                )
                formsets.append(formset)
                if hasattr(inline, 'inlines') and inline.inlines:
                    self.add_nested_inline_formsets(request, inline, formset)

            if self.all_valid_with_nesting(formsets) and form_validated:
                self.save_model(request, new_object, form, True)
                self.save_related(request, form, formsets, True)
                change_message = self.construct_change_message(request, form, formsets)
                self.log_change(request, new_object, change_message)
                return self.response_change(request, new_object)

        else:
            form = ModelForm(instance=obj)
            prefixes = {}
            for FormSet, inline in self.get_formsets_with_inlines(request, obj):
                prefix = FormSet.get_default_prefix()
                prefixes[prefix] = prefixes.get(prefix, 0) + 1
                if prefixes[prefix] != 1 or not prefix:
                    prefix = "%s-%s" % (prefix, prefixes[prefix])
                formset = FormSet(instance=obj, prefix=prefix, queryset=inline.get_queryset(request))
                formsets.append(formset)
                if hasattr(inline, 'inlines') and inline.inlines:
                    self.add_nested_inline_formsets(request, inline, formset)

        adminForm = helpers.AdminForm(
            form, self.get_fieldsets(request, obj),
            self.get_prepopulated_fields(request, obj),
            self.get_readonly_fields(request, obj),
            model_admin=self,
        )
        media = self.media + adminForm.media

        inline_admin_formsets = []
        for inline, formset in zip(inline_instances, formsets):
            fieldsets = list(inline.get_fieldsets(request, obj))
            readonly = list(inline.get_readonly_fields(request, obj))
            prepopulated = dict(inline.get_prepopulated_fields(request, obj))
            inline_admin_formset = helpers.InlineAdminFormSet(
                inline, formset, fieldsets, prepopulated, readonly, model_admin=self,
            )
            inline_admin_formsets.append(inline_admin_formset)
            media = media + inline_admin_formset.media
            if hasattr(inline, 'inlines') and inline.inlines:
                extra_media = self.wrap_nested_inline_formsets(request, inline, formset)
                if extra_media:
                    media += extra_media

        context = {
            'title': _('Change %s') % force_str(opts.verbose_name),
            'adminform': adminForm,
            'object_id': object_id,
            'original': obj,
            'is_popup': "_popup" in request.GET,
            'media': media,
            'inline_admin_formsets': inline_admin_formsets,
            'errors': helpers.AdminErrorList(form, formsets),
            'app_label': opts.app_label,
        }
        context.update(self.admin_site.each_context(request))
        context.update(extra_context or {})
        return self.render_change_form(request, context, change=True, obj=obj, form_url=form_url)


class NestedInline(InlineInstancesMixin, InlineModelAdmin):
    inlines = []
    new_objects = []

    @property
    def media(self):
        extra = '' if settings.DEBUG else '.min'
        if VERSION[:2] >= (2, 2):
            js = [
                'vendor/select2/select2.full.js',
                'vendor/jquery/jquery%s.js' % extra,
            ]
        elif VERSION[:2] >= (2, 0):
            js = [
                'vendor/jquery/jquery%s.js' % extra,
                'vendor/select2/select2.full.js',
            ]
        elif VERSION[:2] >= (1, 9):
            js = [
                'vendor/jquery/jquery%s.js' % extra,
            ]
        else:
            js = [
                'jquery%s.js' % extra,
            ]
        js.append('jquery.init.js')
        js.append('inlines-nested%s.js' % extra)
        if self.prepopulated_fields:
            js.extend(['urlify.js', 'prepopulate%s.js' % extra])
        if self.filter_vertical or self.filter_horizontal:
            js.extend(['SelectBox.js', 'SelectFilter2.js'])
        return forms.Media(js=[static('admin/js/%s' % url) for url in js])

    def get_formsets_with_inlines(self, request, obj=None):
        for inline in self.get_inline_instances(request):
            yield inline.get_formset(request, obj), inline


class NestedStackedInline(NestedInline):
    template = 'admin/edit_inline/stacked-nested.html'


class NestedTabularInline(NestedInline):
    template = 'admin/edit_inline/tabular-nested.html'
