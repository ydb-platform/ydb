# -*- coding: utf-8 -*-
import six
from django import VERSION
from django import forms
from django.core.exceptions import ImproperlyConfigured
from django.db.models import fields

from modeltranslation import settings as mt_settings
from modeltranslation.utils import (
    get_language,
    build_localized_fieldname,
    build_localized_verbose_name,
    resolution_order,
)
from modeltranslation.widgets import ClearableWidgetWrapper


SUPPORTED_FIELDS = (
    fields.CharField,
    # Above implies also CommaSeparatedIntegerField, EmailField, FilePathField, SlugField
    # and URLField as they are subclasses of CharField.
    fields.TextField,
    fields.IntegerField,
    # Above implies also BigIntegerField, SmallIntegerField, PositiveIntegerField and
    # PositiveSmallIntegerField, as they are subclasses of IntegerField.
    fields.BooleanField,
    fields.NullBooleanField,
    fields.FloatField,
    fields.DecimalField,
    fields.IPAddressField,
    fields.GenericIPAddressField,
    fields.DateField,
    fields.DateTimeField,
    fields.TimeField,
    fields.files.FileField,
    fields.files.ImageField,
    fields.related.ForeignKey,
    # Above implies also OneToOneField
)

NEW_RELATED_API = VERSION >= (1, 9)


class NONE:
    """
    Used for fallback options when they are not provided (``None`` can be
    given as a fallback or undefined value) or to mark that a nullable value
    is not yet known and needs to be computed (e.g. field default).
    """

    pass


def create_translation_field(model, field_name, lang, empty_value):
    """
    Translation field factory. Returns a ``TranslationField`` based on a
    fieldname and a language.

    The list of supported fields can be extended by defining a tuple of field
    names in the projects settings.py like this::

        MODELTRANSLATION_CUSTOM_FIELDS = ('MyField', 'MyOtherField',)

    If the class is neither a subclass of fields in ``SUPPORTED_FIELDS``, nor
    in ``CUSTOM_FIELDS`` an ``ImproperlyConfigured`` exception will be raised.
    """
    if empty_value not in ('', 'both', None, NONE):
        raise ImproperlyConfigured('%s is not a valid empty_value.' % empty_value)
    field = model._meta.get_field(field_name)
    cls_name = field.__class__.__name__
    if not (isinstance(field, SUPPORTED_FIELDS) or cls_name in mt_settings.CUSTOM_FIELDS):
        raise ImproperlyConfigured('%s is not supported by modeltranslation.' % cls_name)
    translation_class = field_factory(field.__class__)
    return translation_class(translated_field=field, language=lang, empty_value=empty_value)


def field_factory(baseclass):
    class TranslationFieldSpecific(TranslationField, baseclass):
        pass

    # Reflect baseclass name of returned subclass
    TranslationFieldSpecific.__name__ = 'Translation%s' % baseclass.__name__

    return TranslationFieldSpecific


class TranslationField(object):
    """
    The translation field functions as a proxy to the original field which is
    wrapped.

    For every field defined in the model's ``TranslationOptions`` localized
    versions of that field are added to the model depending on the languages
    given in ``settings.LANGUAGES``.

    If for example there is a model ``News`` with a field ``title`` which is
    registered for translation and the ``settings.LANGUAGES`` contains the
    ``de`` and ``en`` languages, the fields ``title_de`` and ``title_en`` will
    be added to the model class. These fields are realized using this
    descriptor.

    The translation field needs to know which language it contains therefore
    that needs to be specified when the field is created.
    """

    def __init__(self, translated_field, language, empty_value, *args, **kwargs):
        from modeltranslation.translator import translator

        # Update the dict of this field with the content of the original one
        # This might be a bit radical?! Seems to work though...
        self.__dict__.update(translated_field.__dict__)

        # Store the originally wrapped field for later
        self.translated_field = translated_field
        self.language = language
        self.empty_value = empty_value
        if empty_value is NONE:
            self.empty_value = None if translated_field.null else ''

        # Default behaviour is that all translations are optional
        if not isinstance(self, fields.BooleanField):
            # TODO: Do we really want to enforce null *at all*? Shouldn't this
            # better honour the null setting of the translated field?
            self.null = True
        self.blank = True

        # Take required_languages translation option into account
        trans_opts = translator.get_options_for_model(self.model)
        if trans_opts.required_languages:
            required_languages = trans_opts.required_languages
            if isinstance(trans_opts.required_languages, (tuple, list)):
                # All fields
                if self.language in required_languages:
                    # self.null = False
                    self.blank = False
            else:
                # Certain fields only
                # Try current language - if not present, try 'default' key
                try:
                    req_fields = required_languages[self.language]
                except KeyError:
                    req_fields = required_languages.get('default', ())
                if self.name in req_fields:
                    # TODO: We might have to handle the whole thing through the
                    # FieldsAggregationMetaClass, as fields can be inherited.
                    # self.null = False
                    self.blank = False

        # Adjust the name of this field to reflect the language
        self.attname = build_localized_fieldname(self.translated_field.name, language)
        self.name = self.attname
        if self.translated_field.db_column:
            self.db_column = build_localized_fieldname(self.translated_field.db_column, language)
            self.column = self.db_column

        # Copy the verbose name and append a language suffix
        # (will show up e.g. in the admin).
        self.verbose_name = build_localized_verbose_name(translated_field.verbose_name, language)

        # ForeignKey support - rewrite related_name
        if not NEW_RELATED_API and self.rel and self.related and not self.rel.is_hidden():
            import copy

            current = self.related.get_accessor_name()
            self.rel = copy.copy(self.rel)  # Since fields cannot share the same rel object.
            # self.related doesn't need to be copied, as it will be recreated in
            # ``RelatedField.do_related_class``

            if self.rel.related_name is None:
                # For implicit related_name use different query field name
                loc_related_query_name = build_localized_fieldname(
                    self.related_query_name(), self.language
                )
                self.related_query_name = lambda: loc_related_query_name
            self.rel.related_name = build_localized_fieldname(current, self.language)
            self.rel.field = self  # Django 1.6
            if hasattr(self.rel.to._meta, '_related_objects_cache'):
                del self.rel.to._meta._related_objects_cache
        elif NEW_RELATED_API and self.remote_field and not self.remote_field.is_hidden():
            import copy

            current = self.remote_field.get_accessor_name()
            # Since fields cannot share the same rel object:
            self.remote_field = copy.copy(self.remote_field)

            if self.remote_field.related_name is None:
                # For implicit related_name use different query field name
                loc_related_query_name = build_localized_fieldname(
                    self.related_query_name(), self.language
                )
                self.related_query_name = lambda: loc_related_query_name
            self.remote_field.related_name = build_localized_fieldname(current, self.language)
            self.remote_field.field = self  # Django 1.6
            if hasattr(self.remote_field.model._meta, '_related_objects_cache'):
                del self.remote_field.model._meta._related_objects_cache

    # Django 1.5 changed definition of __hash__ for fields to be fine with hash requirements.
    # It spoiled our machinery, since TranslationField has the same creation_counter as its
    # original field and fields didn't get added to sets.
    # So here we override __eq__ and __hash__ to fix the issue while retaining fine with
    # http://docs.python.org/2.7/reference/datamodel.html#object.__hash__
    def __eq__(self, other):
        if isinstance(other, fields.Field):
            return self.creation_counter == other.creation_counter and self.language == getattr(
                other, 'language', None
            )
        return super(TranslationField, self).__eq__(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.creation_counter, self.language))

    def formfield(self, *args, **kwargs):
        """
        Returns proper formfield, according to empty_values setting
        (only for ``forms.CharField`` subclasses).

        There are 3 different formfields:
        - CharField that stores all empty values as empty strings;
        - NullCharField that stores all empty values as None (Null);
        - NullableField that can store both None and empty string.

        By default, if no empty_values was specified in model's translation options,
        NullCharField would be used if the original field is nullable, CharField otherwise.

        This can be overridden by setting empty_values to '' or None.

        Setting 'both' will result in NullableField being used.
        Textual widgets (subclassing ``TextInput`` or ``Textarea``) used for
        nullable fields are enriched with a clear checkbox, allowing ``None``
        values to be preserved rather than saved as empty strings.

        The ``forms.CharField`` somewhat surprising behaviour is documented as a
        "won't fix": https://code.djangoproject.com/ticket/9590.
        """
        formfield = super(TranslationField, self).formfield(*args, **kwargs)
        if isinstance(formfield, forms.CharField):
            if self.empty_value is None:
                from modeltranslation.forms import NullCharField

                form_class = formfield.__class__
                kwargs['form_class'] = type(
                    'Null%s' % form_class.__name__, (NullCharField, form_class), {}
                )
                formfield = super(TranslationField, self).formfield(*args, **kwargs)
            elif self.empty_value == 'both':
                from modeltranslation.forms import NullableField

                form_class = formfield.__class__
                kwargs['form_class'] = type(
                    'Nullable%s' % form_class.__name__, (NullableField, form_class), {}
                )
                formfield = super(TranslationField, self).formfield(*args, **kwargs)
                if isinstance(formfield.widget, (forms.TextInput, forms.Textarea)):
                    formfield.widget = ClearableWidgetWrapper(formfield.widget)
        return formfield

    def save_form_data(self, instance, data, check=True):
        # Allow 3rd-party apps forms to be saved using only translated field name.
        # When translated field (e.g. 'name') is specified and translation field (e.g. 'name_en')
        # not, we assume that form was saved without knowledge of modeltranslation and we make
        # things right:
        # Translated field is saved first, settings respective translation field value. Then
        # translation field is being saved without value - and we handle this here (only for
        # active language).
        # Questionable fields are stored in special variable, which is later handled by clean_fields
        # method on the model.
        if check and self.language == get_language() and getattr(instance, self.name) and not data:
            if not hasattr(instance, '_mt_form_pending_clear'):
                instance._mt_form_pending_clear = {}
            instance._mt_form_pending_clear[self.name] = data
        else:
            super(TranslationField, self).save_form_data(instance, data)

    def deconstruct(self):
        name, path, args, kwargs = self.translated_field.deconstruct()
        if self.null is True:
            kwargs.update({'null': True})
        if 'db_column' in kwargs:
            kwargs['db_column'] = self.db_column
        return six.text_type(self.name), path, args, kwargs

    def clone(self):
        from django.utils.module_loading import import_string

        name, path, args, kwargs = self.deconstruct()
        cls = import_string(path)
        return cls(*args, **kwargs)


class TranslationFieldDescriptor(object):
    """
    A descriptor used for the original translated field.
    """

    def __init__(
        self, field, fallback_languages=None, fallback_value=NONE, fallback_undefined=NONE
    ):
        """
        Stores fallback options and the original field, so we know it's name
        and default.
        """
        self.field = field
        self.fallback_languages = fallback_languages
        self.fallback_value = fallback_value
        self.fallback_undefined = fallback_undefined

    def __set__(self, instance, value):
        """
        Updates the translation field for the current language.
        """
        # In order for deferred fields to work, we also need to set the base value
        instance.__dict__[self.field.name] = value
        if isinstance(self.field, fields.related.ForeignKey):
            instance.__dict__[self.field.get_attname()] = None if value is None else value.pk
        if getattr(instance, '_mt_init', False) or getattr(instance, '_mt_disable', False):
            # When assignment takes place in model instance constructor, don't set value.
            # This is essential for only/defer to work, but I think it's sensible anyway.
            # Setting the localized field may also be disabled by setting _mt_disable.
            return
        loc_field_name = build_localized_fieldname(self.field.name, get_language())
        setattr(instance, loc_field_name, value)

    def meaningful_value(self, val, undefined):
        """
        Check if val is considered non-empty.
        """
        if isinstance(val, fields.files.FieldFile):
            return val.name and not (
                isinstance(undefined, fields.files.FieldFile) and val == undefined
            )
        return val is not None and val != undefined

    def __get__(self, instance, owner):
        """
        Returns value from the translation field for the current language, or
        value for some another language according to fallback languages, or the
        custom fallback value, or field's default value.
        """
        if instance is None:
            return self
        default = NONE
        undefined = self.fallback_undefined
        if undefined is NONE:
            default = self.field.get_default()
            undefined = default
        langs = resolution_order(get_language(), self.fallback_languages)
        for lang in langs:
            loc_field_name = build_localized_fieldname(self.field.name, lang)
            val = getattr(instance, loc_field_name, None)
            if self.meaningful_value(val, undefined):
                return val
        if mt_settings.ENABLE_FALLBACKS and self.fallback_value is not NONE:
            return self.fallback_value
        else:
            if default is NONE:
                default = self.field.get_default()
            # Some fields like FileField behave strange, as their get_default() doesn't return
            # instance of attr_class, but rather None or ''.
            # Normally this case is handled in the descriptor, but since we have overridden it, we
            # must mock it up.
            if isinstance(self.field, fields.files.FileField) and not isinstance(
                default, self.field.attr_class
            ):
                return self.field.attr_class(instance, self.field, default)
            return default


class TranslatedRelationIdDescriptor(object):
    """
    A descriptor used for the original '_id' attribute of a translated
    ForeignKey field.
    """

    def __init__(self, field_name, fallback_languages):
        self.field_name = field_name  # The name of the original field (excluding '_id')
        self.fallback_languages = fallback_languages

    def __set__(self, instance, value):
        lang = get_language()
        loc_field_name = build_localized_fieldname(self.field_name, lang)
        # Localized field name with '_id'
        loc_attname = instance._meta.get_field(loc_field_name).get_attname()
        setattr(instance, loc_attname, value)
        base_attname = instance._meta.get_field(self.field_name).get_attname()
        instance.__dict__[base_attname] = value

    def __get__(self, instance, owner):
        if instance is None:
            return self
        langs = resolution_order(get_language(), self.fallback_languages)
        for lang in langs:
            loc_field_name = build_localized_fieldname(self.field_name, lang)
            # Localized field name with '_id'
            loc_attname = instance._meta.get_field(loc_field_name).get_attname()
            val = getattr(instance, loc_attname, None)
            if val is not None:
                return val
        return None


class LanguageCacheSingleObjectDescriptor(object):
    """
    A Mixin for RelatedObjectDescriptors which use current language in cache lookups.
    """

    accessor = None  # needs to be set on instance

    @property
    def cache_name(self):
        """
        Used in django 1.x
        """
        lang = get_language()
        cache = build_localized_fieldname(self.accessor, lang)
        return "_%s_cache" % cache

    def get_cache_name(self):
        """
        Used in django > 2.x
        """
        return build_localized_fieldname(self.accessor, get_language())
