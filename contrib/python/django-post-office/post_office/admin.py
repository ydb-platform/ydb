import re

from django import forms
from django.conf import settings
from django.contrib import admin, messages
from django.core.exceptions import ValidationError
from django.db import models
from django.forms import BaseInlineFormSet
from django.forms.widgets import TextInput
from django.http.response import HttpResponse, HttpResponseNotFound, HttpResponseRedirect
from django.template import Context, Template
from django.urls import path
from django.urls import re_path, reverse
from django.utils.html import format_html
from django.utils.text import Truncator
from django.utils.translation import gettext_lazy as _

from .fields import CommaSeparatedEmailField
from .models import STATUS, Attachment, Email, EmailTemplate, Log
from .sanitizer import clean_html
from .settings import PRE_DJANGO_6


@admin.display(description='Message')
def get_message_preview(instance):
    return f'{instance.message[:25]}...' if len(instance.message) > 25 else instance.message


class AttachmentInline(admin.StackedInline):
    model = Attachment.emails.through
    extra = 0
    autocomplete_fields = ['attachment']

    def get_formset(self, request, obj=None, **kwargs):
        self.parent_obj = obj
        return super().get_formset(request, obj, **kwargs)

    def get_queryset(self, request):
        """
        Exclude inlined attachments from queryset, because they usually have meaningless names and
        are displayed anyway.
        """
        queryset = super().get_queryset(request)
        if self.parent_obj:
            queryset = queryset.filter(email=self.parent_obj)

        # From https://stackoverflow.com/a/67266338
        return queryset.exclude(
            **{
                'attachment__headers__Content-Disposition__isnull': False,
                'attachment__headers__Content-Disposition__startswith': 'inline',
            }
        ).select_related('attachment')


class LogInline(admin.TabularInline):
    model = Log
    readonly_fields = fields = ['date', 'status', 'exception_type', 'message']
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


class CommaSeparatedEmailWidget(TextInput):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attrs.update({'class': 'vTextField'})

    def format_value(self, value):
        # If the value is a string wrap it in a list so it does not get sliced.
        if not value:
            return ''
        if isinstance(value, str):
            value = [value]
        return ','.join([item for item in value])


@admin.action(description='Requeue selected emails')
def requeue(modeladmin, request, queryset):
    """An admin action to requeue emails."""
    queryset.update(status=STATUS.queued)


@admin.register(Email)
class EmailAdmin(admin.ModelAdmin):
    list_display = [
        'truncated_message_id',
        'to_display',
        'shortened_subject',
        'status',
        'last_updated',
        'scheduled_time',
        'use_template',
    ]
    search_fields = ['to', 'subject']
    readonly_fields = ['message_id', 'render_subject', 'render_plaintext_body', 'render_html_body']
    inlines = [AttachmentInline, LogInline]
    list_filter = ['status', 'template__language', 'template__name']
    formfield_overrides = {CommaSeparatedEmailField: {'widget': CommaSeparatedEmailWidget}}
    actions = [requeue]

    def get_urls(self):
        urls = [
            re_path(
                r'^(?P<pk>\d+)/image/(?P<content_id>[0-9a-f]{32})$',
                self.fetch_email_image,
                name='post_office_email_image',
            ),
            path('<int:pk>/resend/', self.resend, name='resend'),
        ]
        urls.extend(super().get_urls())
        return urls

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('template')

    @admin.display(
        description=_('To'),
        ordering='to',
    )
    def to_display(self, instance):
        return ', '.join(instance.to)

    @admin.display(description='Message-ID')
    def truncated_message_id(self, instance):
        if instance.message_id:
            return Truncator(instance.message_id[1:-1]).chars(10)
        return str(instance.id)

    def has_add_permission(self, request):
        return False

    @admin.display(
        description=_('Subject'),
        ordering='subject',
    )
    def shortened_subject(self, instance):
        if instance.context:
            template_cache_key = '_subject_template_' + str(instance.template_id)
            template = getattr(self, template_cache_key, None)
            if template is None:
                # cache compiled template to speed up rendering of list view
                template = Template(instance.template.subject)
                setattr(self, template_cache_key, template)
            subject = template.render(Context(instance.context))
        else:
            subject = instance.subject
        return Truncator(subject).chars(100)

    @admin.display(
        description=_('Use Template'),
        boolean=True,
    )
    def use_template(self, instance):
        return bool(instance.template_id)

    def _is_text_body(self, part):
        """Check if a message part is a text body (not an attachment)."""
        if PRE_DJANGO_6:
            from django.core.mail.message import SafeMIMEText

            return isinstance(part, SafeMIMEText)
        else:
            # Django 6+: check content type and exclude attachments
            content_type = part.get_content_type()
            if content_type not in ('text/plain', 'text/html'):
                return False
            # Exclude parts marked as attachments
            return part.get_content_disposition() != 'attachment'

    def get_fieldsets(self, request, obj=None):
        fields = ['from_email', 'to', 'cc', 'bcc', 'priority', ('status', 'scheduled_time')]
        if obj.message_id:
            fields.insert(0, 'message_id')
        fieldsets = [(None, {'fields': fields})]
        has_plaintext_content, has_html_content = False, False
        for part in obj.email_message().message().walk():
            if not self._is_text_body(part):
                continue
            content_type = part.get_content_type()
            if content_type == 'text/plain':
                has_plaintext_content = True
            elif content_type == 'text/html':
                has_html_content = True

        if has_html_content:
            fieldsets.append((_('HTML Email'), {'fields': ['render_subject', 'render_html_body']}))
            if has_plaintext_content:
                fieldsets.append((_('Text Email'), {'classes': ['collapse'], 'fields': ['render_plaintext_body']}))
        elif has_plaintext_content:
            fieldsets.append((_('Text Email'), {'fields': ['render_subject', 'render_plaintext_body']}))

        return fieldsets

    @admin.display(description=_('Subject'))
    def render_subject(self, instance):
        message = instance.email_message()
        return message.subject

    @admin.display(description=_('Mail Body'))
    def render_plaintext_body(self, instance):
        for part in instance.email_message().message().walk():
            if self._is_text_body(part) and part.get_content_type() == 'text/plain':
                return format_html('<pre>{}</pre>', part.get_payload())

    @admin.display(description=_('HTML Body'))
    def render_html_body(self, instance):
        pattern = re.compile('cid:([0-9a-f]{32})')
        url = reverse('admin:post_office_email_image', kwargs={'pk': instance.id, 'content_id': 32 * '0'})
        url = url.replace(32 * '0', r'\1')
        for part in instance.email_message().message().walk():
            if self._is_text_body(part) and part.get_content_type() == 'text/html':
                payload = part.get_payload(decode=True).decode('utf-8')
                return clean_html(pattern.sub(url, payload))

    def fetch_email_image(self, request, pk, content_id):
        instance = self.get_object(request, pk)
        for message in instance.email_message().message().walk():
            if message.get_content_maintype() == 'image' and message.get('Content-Id')[1:33] == content_id:
                return HttpResponse(message.get_payload(decode=True), content_type=message.get_content_type())
        return HttpResponseNotFound()

    def resend(self, request, pk):
        instance = self.get_object(request, pk)
        instance.dispatch()
        messages.info(request, 'Email has been sent again')
        return HttpResponseRedirect(reverse('admin:post_office_email_change', args=[instance.pk]))


@admin.register(Log)
class LogAdmin(admin.ModelAdmin):
    list_display = ('date', 'email', 'status', get_message_preview)


class SubjectField(TextInput):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attrs.update({'style': 'width: 610px;'})


class EmailTemplateAdminFormSet(BaseInlineFormSet):
    def clean(self):
        """
        Check that no two Email templates have the same default_template and language.
        """
        super().clean()
        data = set()
        for form in self.forms:
            default_template = form.cleaned_data['default_template']
            language = form.cleaned_data['language']
            if (default_template.id, language) in data:
                msg = _("Duplicate template for language '{language}'.")
                language = dict(form.fields['language'].choices)[language]
                raise ValidationError(msg.format(language=language))
            data.add((default_template.id, language))


class EmailTemplateAdminForm(forms.ModelForm):
    language = forms.ChoiceField(
        choices=settings.LANGUAGES,
        required=False,
        label=_('Language'),
        help_text=_('Render template in alternative language'),
    )

    class Meta:
        model = EmailTemplate
        fields = ['name', 'description', 'subject', 'content', 'html_content', 'language', 'default_template']

    def __init__(self, *args, **kwargs):
        instance = kwargs.get('instance')
        super().__init__(*args, **kwargs)
        if instance and instance.language:
            self.fields['language'].disabled = True


class EmailTemplateInline(admin.StackedInline):
    form = EmailTemplateAdminForm
    formset = EmailTemplateAdminFormSet
    model = EmailTemplate
    extra = 0
    fields = ('language', 'subject', 'content', 'html_content')
    formfield_overrides = {models.CharField: {'widget': SubjectField}}

    def get_max_num(self, request, obj=None, **kwargs):
        return len(settings.LANGUAGES)


@admin.register(EmailTemplate)
class EmailTemplateAdmin(admin.ModelAdmin):
    form = EmailTemplateAdminForm
    list_display = ('name', 'description_shortened', 'subject', 'languages_compact', 'created')
    search_fields = ('name', 'description', 'subject')
    fieldsets = [
        (None, {'fields': ('name', 'description')}),
        (_('Default Content'), {'fields': ('subject', 'content', 'html_content')}),
    ]
    inlines = (EmailTemplateInline,) if settings.USE_I18N else ()
    formfield_overrides = {models.CharField: {'widget': SubjectField}}

    def get_queryset(self, request):
        return self.model.objects.filter(default_template__isnull=True)

    @admin.display(
        description=_('Description'),
        ordering='description',
    )
    def description_shortened(self, instance):
        return Truncator(instance.description.split('\n')[0]).chars(200)

    @admin.display(description=_('Languages'))
    def languages_compact(self, instance):
        languages = [tt.language for tt in instance.translated_templates.order_by('language')]
        return ', '.join(languages)

    def save_model(self, request, obj, form, change):
        obj.save()

        # if the name got changed, also change the translated templates to match again
        if 'name' in form.changed_data:
            obj.translated_templates.update(name=obj.name)


@admin.register(Attachment)
class AttachmentAdmin(admin.ModelAdmin):
    list_display = ['name', 'file']
    filter_horizontal = ['emails']
    search_fields = ['name']
    autocomplete_fields = ['emails']
