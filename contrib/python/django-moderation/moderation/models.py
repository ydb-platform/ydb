from django.conf import settings
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models, transaction
from django.utils.timezone import now
from django.utils.translation import ugettext_lazy as _
from model_utils import Choices

from . import moderation
from .constants import (MODERATION_DRAFT_STATE, MODERATION_READY_STATE, MODERATION_STATUS_APPROVED,
                        MODERATION_STATUS_PENDING, MODERATION_STATUS_REJECTED)
from .diff import get_changes_between_models
from .fields import SerializedObjectField
from .managers import ModeratedObjectManager
from .signals import post_moderation, pre_moderation

MODERATION_STATES = Choices(
    (MODERATION_READY_STATE, 'ready', _('Ready for moderation')),
    (MODERATION_DRAFT_STATE, 'draft', _('Draft')),
)

STATUS_CHOICES = Choices(
    (MODERATION_STATUS_REJECTED, 'rejected', _('Rejected')),
    (MODERATION_STATUS_APPROVED, 'approved', _('Approved')),
    (MODERATION_STATUS_PENDING, 'pending', _('Pending')),
)


class ModeratedObject(models.Model):
    content_type = models.ForeignKey(ContentType, null=True, blank=True,
                                     on_delete=models.SET_NULL,
                                     editable=False)
    object_pk = models.PositiveIntegerField(null=True, blank=True,
                                            editable=False, db_index=True)
    content_object = GenericForeignKey(ct_field='content_type',
                                       fk_field='object_pk')
    created = models.DateTimeField(auto_now_add=True, editable=False)
    updated = models.DateTimeField(auto_now=True)
    state = models.SmallIntegerField(choices=MODERATION_STATES,
                                     default=MODERATION_DRAFT_STATE,
                                     editable=False)
    status = models.SmallIntegerField(
        choices=STATUS_CHOICES,
        default=MODERATION_STATUS_PENDING,
        editable=False)
    by = models.ForeignKey(
        getattr(settings, 'AUTH_USER_MODEL', 'auth.User'),
        blank=True, null=True, editable=False, on_delete=models.SET_NULL,
        related_name='moderated_objects')
    on = models.DateTimeField(editable=False, blank=True, null=True)
    reason = models.TextField(blank=True, null=True)
    changed_object = SerializedObjectField(serialize_format='json',
                                           editable=False)
    changed_by = models.ForeignKey(
        getattr(settings, 'AUTH_USER_MODEL', 'auth.User'),
        blank=True, null=True, editable=True, on_delete=models.SET_NULL,
        related_name='changed_by_set')

    objects = ModeratedObjectManager()

    content_type.content_type_filter = True

    def __init__(self, *args, **kwargs):
        self.instance = kwargs.get('content_object')
        super().__init__(*args, **kwargs)

    def __str__(self):
        return "%s" % self.changed_object

    def save(self, *args, **kwargs):
        if self.instance:
            self.changed_object = self.instance

        super().save(*args, **kwargs)

    class Meta:
        verbose_name = _('Moderated Object')
        verbose_name_plural = _('Moderated Objects')
        ordering = ['status', 'created']

    def automoderate(self, user=None):
        '''Auto moderate object for given user.
          Returns status of moderation.
        '''
        if user is None:
            user = self.changed_by
        else:
            self.changed_by = user
            # No need to save here, both reject() and approve() will save us.
            # Just save below if the moderation result is PENDING.

        if self.moderator.visible_until_rejected:
            changed_object = self.get_object_for_this_type()
        else:
            changed_object = self.changed_object
        status, reason = self._get_moderation_status_and_reason(
            changed_object,
            user)

        if status == MODERATION_STATUS_REJECTED:
            self.reject(by=self.by, reason=reason)
        elif status == MODERATION_STATUS_APPROVED:
            self.approve(by=self.by, reason=reason)
        else:  # MODERATION_STATUS_PENDING
            self.save()

        return status

    def _get_moderation_status_and_reason(self, obj, user):
        '''
        Returns tuple of moderation status and reason for auto moderation
        '''
        reason = self.moderator.is_auto_reject(obj, user)
        if reason:
            return MODERATION_STATUS_REJECTED, reason
        else:
            reason = self.moderator.is_auto_approve(obj, user)
            if reason:
                return MODERATION_STATUS_APPROVED, reason

        return MODERATION_STATUS_PENDING, None

    def get_object_for_this_type(self):
        pk = self.object_pk
        obj = self.content_type.model_class()._default_unmoderated_manager.get(pk=pk)
        return obj

    def get_absolute_url(self):
        if hasattr(self.changed_object, 'get_absolute_url'):
            return self.changed_object.get_absolute_url()
        return None

    def get_admin_moderate_url(self):
        return "/admin/moderation/moderatedobject/%s/change/" % self.pk

    @property
    def moderator(self):
        model_class = self.content_type.model_class()

        return moderation.get_moderator(model_class)

    def _send_signals_and_moderate(self, new_status, by, reason):
        pre_moderation.send(sender=self.changed_object.__class__,
                            instance=self.changed_object,
                            status=new_status)

        self._moderate(new_status, by, reason)

        post_moderation.send(sender=self.content_type.model_class(),
                             instance=self.content_object,
                             status=new_status)

    def _moderate(self, new_status, by, reason):
        # See register.py pre_save_handler() for the case where the model is
        # reset to its old values, and the new values are stored in the
        # ModeratedObject. In such cases, on approval, we should restore the
        # changes to the base object by saving the one attached to the
        # ModeratedObject.

        if (self.status == MODERATION_STATUS_PENDING and
                new_status == MODERATION_STATUS_APPROVED and
                not self.moderator.visible_until_rejected):
            base_object = self.changed_object
            base_object_force_save = True
        else:
            # The model in the database contains the most recent data already,
            # or we're not ready to approve the changes stored in
            # ModeratedObject.
            obj_class = self.changed_object.__class__
            pk = self.changed_object.pk
            base_object = obj_class._default_unmoderated_manager.get(pk=pk)
            base_object_force_save = False

        if new_status == MODERATION_STATUS_APPROVED:
            # This version is now approved, and will be reverted to if
            # future changes are rejected by a moderator.
            self.state = MODERATION_READY_STATE

        self.status = new_status
        self.on = now()
        self.by = by
        self.reason = reason
        self.save()

        if self.moderator.visibility_column:
            old_visible = getattr(base_object,
                                  self.moderator.visibility_column)

            if new_status == MODERATION_STATUS_APPROVED:
                new_visible = True
            elif new_status == MODERATION_STATUS_REJECTED:
                new_visible = False
            else:  # MODERATION_STATUS_PENDING
                new_visible = self.moderator.visible_until_rejected

            if new_visible != old_visible:
                setattr(base_object, self.moderator.visibility_column,
                        new_visible)
                base_object_force_save = True

        if base_object_force_save:
            # avoid triggering pre/post_save_handler
            with transaction.atomic(using=None, savepoint=False):
                base_object.save_base(raw=True)
                # The _save_parents call is required for models with an
                # inherited visibility_column.
                base_object._save_parents(base_object.__class__, None, None)

        if self.changed_by:
            self.moderator.inform_user(self.content_object, self.changed_by)

    def has_object_been_changed(self, original_obj, only_excluded=False):
        excludes = includes = []
        if only_excluded:
            includes = self.moderator.fields_exclude
        else:
            excludes = self.moderator.fields_exclude

        changes = get_changes_between_models(original_obj,
                                             self.changed_object,
                                             excludes,
                                             includes)

        for change in changes:
            left_change, right_change = changes[change].change
            if left_change != right_change:
                return True

        return False

    def approve(self, by=None, reason=None):
        self._send_signals_and_moderate(MODERATION_STATUS_APPROVED, by, reason)

    def reject(self, by=None, reason=None):
        self._send_signals_and_moderate(MODERATION_STATUS_REJECTED, by, reason)
