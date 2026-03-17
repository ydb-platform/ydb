from django.contrib.auth.models import Group
from django.core.exceptions import ObjectDoesNotExist
from django.db.models.fields import BooleanField
from django.db.models.manager import Manager
from django.template.loader import render_to_string

from .managers import ModerationObjectsManager
from .message_backends import (BaseMessageBackend, BaseMultipleMessageBackend, EmailMessageBackend,
                               EmailMultipleMessageBackend)


class GenericModerator:
    """
    Encapsulates moderation options for a given model.
    """
    manager_names = ['objects']
    moderation_manager_class = ModerationObjectsManager
    bypass_moderation_after_approval = False
    visible_until_rejected = False
    keep_history = False

    fields_exclude = []
    resolve_foreignkeys = True

    visibility_column = None

    auto_approve_for_superusers = True
    auto_approve_for_staff = True
    auto_approve_for_groups = None

    auto_reject_for_anonymous = True
    auto_reject_for_groups = None

    notify_moderator = True
    notify_user = True

    message_backend_class = EmailMessageBackend
    multiple_message_backend_class = EmailMultipleMessageBackend
    subject_template_moderator = \
        'moderation/notification_subject_moderator.txt'
    message_template_moderator = \
        'moderation/notification_message_moderator.txt'
    subject_template_user = 'moderation/notification_subject_user.txt'
    message_template_user = 'moderation/notification_message_user.txt'

    def __init__(self, model_class):
        self.model_class = model_class
        self._validate_options()
        self.base_managers = self._get_base_managers()

        moderated_fields = getattr(model_class, 'moderated_fields', None)
        if moderated_fields:
            for field in model_class._meta.fields:
                if field.name not in moderated_fields:
                    self.fields_exclude.append(field.name)

    def is_auto_approve(self, obj, user):
        '''
        Checks if change on obj by user need to be auto approved
        Returns False if change is not auto approve or reason(Unicode) if
        change need to be auto approved.

        Overwrite this method if you want to provide your custom logic.
        '''
        if self.auto_approve_for_groups and \
           self._check_user_in_groups(user, self.auto_approve_for_groups):
            return self.reason('Auto-approved: User in allowed group')
        if self.auto_approve_for_superusers and user.is_superuser:
            return self.reason('Auto-approved: Superuser')
        if self.auto_approve_for_staff and user.is_staff:
            return self.reason('Auto-approved: Staff')

        return False

    def is_auto_reject(self, obj, user):
        '''
        Checks if change on obj by user need to be auto rejected
        Returns False if change is not auto reject or reason(Unicode) if
        change need to be auto rejected.

        Overwrite this method if you want to provide your custom logic.
        '''
        is_anon = user.is_anonymous
        if callable(is_anon):
            is_anon = is_anon()
        if self.auto_reject_for_anonymous and is_anon:
            return self.reason('Auto-rejected: Anonymous User')
        if self.auto_reject_for_groups and \
           self._check_user_in_groups(user, self.auto_reject_for_groups):
            return self.reason('Auto-rejected: User in disallowed group')

        return False

    def reason(self, reason, user=None, obj=None):
        '''Returns moderation reason for auto moderation.  Optional user
        and object can be passed for a more custom reason.
        '''
        return reason

    def _check_user_in_groups(self, user, groups):
        for group in groups:
            try:
                group = Group.objects.get(name=group)
            except ObjectDoesNotExist:
                return False

            if group in user.groups.all():
                return True

        return False

    def get_message_backend(self):
        if not issubclass(self.message_backend_class, BaseMessageBackend):
            raise TypeError("The message backend used '%s' needs to "
                            "inherit from the BaseMessageBackend "
                            "class" % self.message_backend_class)
        return self.message_backend_class()

    def get_multiple_message_backend(self):
        if not issubclass(self.multiple_message_backend_class, BaseMultipleMessageBackend):
            raise TypeError("The message backend used '{}' needs to "
                            "inherit from the BaseMultipleMessageBackend "
                            "class".format(self.message_backend_class))
        return self.multiple_message_backend_class()

    def send(self, content_object, subject_template, message_template,
             recipient_list, extra_context=None):
        try:
            from django.contrib.sites.models import Site
            site = Site.objects.get_current()
        except RuntimeError:
            site = None

        context = {
            'moderated_object': content_object.moderated_object,
            'content_object': content_object,
            'site': site,
            'content_type': content_object.moderated_object.content_type}

        if extra_context:
            context.update(extra_context)

        message = render_to_string(message_template, context)
        subject = render_to_string(subject_template, context)

        backend = self.get_message_backend()
        backend.send(
            subject=subject,
            message=message,
            recipient_list=recipient_list)

    def send_many(self, queryset, subject_template, message_template,
                  extra_context=None):
        try:
            from django.contrib.sites.models import Site
            site = Site.objects.get_current()
        except RuntimeError:
            site = None

        ctx = extra_context if extra_context else {}

        datatuples = tuple({
            'subject': render_to_string(
                subject_template, ctx.update({
                    'moderated_object': mobj,
                    'content_object': mobj.content_object,
                    'site': site,
                    'content_type': mobj.content_type,
                    'user': mobj.changed_by,
                })),
            'message': render_to_string(
                message_template, ctx.update({
                    'moderated_object': mobj,
                    'content_object': mobj.content_object,
                    'site': site,
                    'content_type': mobj.content_type,
                    'user': mobj.changed_by,
                })),
            # from_email will need to be added
            'recipient_list': [mobj.changed_by.email]
        } for mobj in queryset)

        multiple_backend = self.get_multiple_message_backend()
        multiple_backend.send(datatuples)

    def inform_moderator(self,
                         content_object,
                         extra_context=None):
        '''Send notification to moderator'''
        from .conf.settings import MODERATORS

        if self.notify_moderator:
            self.send(
                content_object=content_object,
                subject_template=self.subject_template_moderator,
                message_template=self.message_template_moderator,
                recipient_list=MODERATORS)

    def inform_user(self, content_object,
                    user,
                    extra_context=None):
        '''
        Send notification to user when object is approved or rejected
        '''
        if extra_context:
            extra_context.update({'user': user})
        else:
            extra_context = {'user': user}
        if self.notify_user:
            self.send(
                content_object=content_object,
                subject_template=self.subject_template_user,
                message_template=self.message_template_user,
                recipient_list=[user.email],
                extra_context=extra_context)

    def inform_users(self, queryset, extra_context=None):
        '''
        Send notifications to users when their objects are approved or rejected
        '''
        if self.notify_user:
            self.send_many(
                queryset=queryset.exclude(changed_by=None)
                                 .select_related('changed_by__email'),
                subject_template=self.subject_template_user,
                message_template=self.message_template_user,
                extra_context=extra_context)

    def _get_base_managers(self):
        base_managers = []

        for manager_name in self.manager_names:
            base_managers.append(
                (
                    manager_name,
                    self._get_base_manager(self.model_class, manager_name)))
        return base_managers

    def _get_base_manager(self, model_class, manager_name):
        """Returns base manager class for given model class """
        if hasattr(model_class, manager_name):
            base_manager = getattr(model_class, manager_name).__class__
        else:
            base_manager = Manager

        return base_manager

    def _validate_options(self):
        if self.visibility_column:
            try:  # Django 1.10+
                field_type = type(self.model_class._meta.get_field(
                    self.visibility_column))
            except AttributeError:
                field_type = type(self.model_class._meta.get_field_by_name(
                    self.visibility_column)[0])

            if field_type != BooleanField:
                msg = "visibility_column field: %s on model %s should " \
                      "be BooleanField type but is %s"
                msg %= (
                    self.moderator.visibility_column,
                    self.changed_object.__class__,
                    field_type)
                raise AttributeError(msg)
