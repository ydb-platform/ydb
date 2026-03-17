import datetime
from dateutil.parser import parse
import decimal
from decimal import Decimal
import importlib

from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import models
try:
    from django.db.models.fields.related import\
        SingleRelatedObjectDescriptor as ReverseOneToOneDescriptor
except ImportError:
    from django.db.models.fields.related_descriptors import\
        ReverseOneToOneDescriptor
from tastypie.compat import datetime_safe

from tastypie.bundle import Bundle
from tastypie.exceptions import ApiFieldError, NotFound
from tastypie.utils import make_aware


class NOT_PROVIDED:
    def __str__(self):
        return 'No default provided.'


# All the ApiField variants.

class ApiField(object):
    "The base implementation of a field used by the resources."
    is_m2m = False
    is_related = False
    dehydrated_type = 'string'
    help_text = ''

    def __init__(self, attribute=None, default=NOT_PROVIDED, null=False, blank=False, readonly=False, unique=False, help_text=None, use_in='all', verbose_name=None):
        """
        Sets up the field. This is generally called when the containing
        ``Resource`` is initialized.

        Optionally accepts an ``attribute``, which should be a string of
        either an instance attribute or callable off the object during the
        ``dehydrate`` or push data onto an object during the ``hydrate``.
        Defaults to ``None``, meaning data will be manually accessed.

        Optionally accepts a ``default``, which provides default data when the
        object being ``dehydrated``/``hydrated`` has no data on the field.
        Defaults to ``NOT_PROVIDED``.

        Optionally accepts a ``null``, which indicated whether or not a
        ``None`` is allowable data on the field. Defaults to ``False``.

        Optionally accepts a ``blank``, which indicated whether or not
        data may be omitted on the field. Defaults to ``False``.

        Optionally accepts a ``readonly``, which indicates whether the field
        is used during the ``hydrate`` or not. Defaults to ``False``.

        Optionally accepts a ``unique``, which indicates if the field is a
        unique identifier for the object.

        Optionally accepts ``help_text``, which lets you provide a
        human-readable description of the field exposed at the schema level.
        Defaults to the per-Field definition.

        Optionally accepts ``use_in``. This may be one of ``list``, ``detail``
        ``all`` or a callable which accepts a ``bundle`` and returns
        ``True`` or ``False``. Indicates wheather this field will be included
        during dehydration of a list of objects or a single object. If ``use_in``
        is a callable, and returns ``True``, the field will be included during
        dehydration.
        Defaults to ``all``.

        Optionally accepts ``verbose_name``, which lets you provide a
        more verbose name of the field exposed at the schema level.
        """
        # Track what the index thinks this field is called.
        self.instance_name = None
        self._resource = None
        self.attribute = attribute
        # Check for `__` in the field for looking through the relation.
        self._attrs = attribute.split('__') if attribute is not None and isinstance(attribute, str) else []
        self._default = default
        self.null = null
        self.blank = blank
        self.readonly = readonly
        self.unique = unique
        self.use_in = 'all'

        if use_in in ['all', 'detail', 'list'] or callable(use_in):
            self.use_in = use_in

        self.verbose_name = verbose_name

        if help_text:
            self.help_text = help_text

    def contribute_to_class(self, cls, name):
        # Do the least we can here so that we don't hate ourselves in the
        # morning.
        self.instance_name = name
        self._resource = cls

    def has_default(self):
        """Returns a boolean of whether this field has a default value."""
        return self._default is not NOT_PROVIDED

    @property
    def default(self):
        """Returns the default value for the field."""
        if callable(self._default):
            return self._default()

        return self._default

    def dehydrate(self, bundle, for_list=True):
        """
        Takes data from the provided object and prepares it for the
        resource.
        """
        if self.attribute is not None:
            current_object = bundle.obj

            for attr in self._attrs:
                previous_object = current_object
                current_object = getattr(current_object, attr, None)

                if current_object is None:
                    if self.null:
                        # Fall out of the loop, given any further attempts at
                        # accesses will fail miserably.
                        break
                    elif self.has_default():
                        current_object = self._default
                        # Fall out of the loop, given any further attempts at
                        # accesses will fail miserably.
                        break
                    else:
                        raise ApiFieldError("The object '%r' has an empty attribute '%s' and doesn't allow a default or null value." % (previous_object, attr))

            if callable(current_object):
                current_object = current_object()

            return self.convert(current_object)

        if self.has_default():
            return self.convert(self.default)
        else:
            return None

    def convert(self, value):
        """
        Handles conversion between the data found and the type of the field.

        Extending classes should override this method and provide correct
        data coercion.
        """
        return value

    def hydrate(self, bundle):
        """
        Takes data stored in the bundle for the field and returns it. Used for
        taking simple data and building a instance object.
        """
        if self.readonly:
            return None
        if self.instance_name not in bundle.data:
            if self.is_related and not self.is_m2m:
                # We've got an FK (or alike field) & a possible parent object.
                # Check for it.
                if bundle.related_obj and bundle.related_name in (self.attribute, self.instance_name):
                    return bundle.related_obj
            if self.blank:
                return None
            if self.attribute:
                try:
                    val = getattr(bundle.obj, self.attribute, None)

                    if val is not None:
                        return val
                except ObjectDoesNotExist:
                    pass
            if self.instance_name:
                try:
                    if hasattr(bundle.obj, self.instance_name):
                        return getattr(bundle.obj, self.instance_name)
                except ObjectDoesNotExist:
                    pass
            if self.has_default():
                if callable(self._default):
                    return self._default()

                return self._default
            if self.null:
                return None

            raise ApiFieldError("The '%s' field has no data and doesn't allow a default or null value." % self.instance_name)

        return bundle.data[self.instance_name]


class CharField(ApiField):
    """
    A text field of arbitrary length.

    Covers both ``models.CharField`` and ``models.TextField``.
    """
    dehydrated_type = 'string'
    help_text = 'Unicode string data. Ex: "Hello World"'

    def convert(self, value):
        if value is None:
            return None

        return str(value)


class FileField(ApiField):
    """
    A file-related field.

    Covers both ``models.FileField`` and ``models.ImageField``.
    """
    dehydrated_type = 'string'
    help_text = 'A file URL as a string. Ex: "http://media.example.com/media/photos/my_photo.jpg"'

    def convert(self, value):
        if value is None:
            return None

        try:
            # Try to return the URL if it's a ``File``, falling back to the string
            # itself if it's been overridden or is a default.
            return getattr(value, 'url', value)
        except ValueError:
            return None


class IntegerField(ApiField):
    """
    An integer field.

    Covers ``models.IntegerField``, ``models.PositiveIntegerField``,
    ``models.PositiveSmallIntegerField`` and ``models.SmallIntegerField``.
    """
    dehydrated_type = 'integer'
    help_text = 'Integer data. Ex: 2673'

    def convert(self, value):
        if value is None:
            return None

        return int(value)


class FloatField(ApiField):
    """
    A floating point field.
    """
    dehydrated_type = 'float'
    help_text = 'Floating point numeric data. Ex: 26.73'

    def convert(self, value):
        if value is None:
            return None

        return float(value)


class DecimalField(ApiField):
    """
    A decimal field.
    """
    dehydrated_type = 'decimal'
    help_text = 'Fixed precision numeric data. Ex: 26.73'

    def convert(self, value):
        if value is None:
            return None

        return Decimal(value)

    def hydrate(self, bundle):
        value = super(DecimalField, self).hydrate(bundle)

        if value and not isinstance(value, Decimal):
            try:
                value = Decimal(value)
            except decimal.InvalidOperation:
                raise ApiFieldError("Invalid decimal string for '%s' field: '%s'" % (self.instance_name, value))

        return value


class BooleanField(ApiField):
    """
    A boolean field.

    Covers both ``models.BooleanField`` and ``models.NullBooleanField``.
    """
    dehydrated_type = 'boolean'
    help_text = 'Boolean data. Ex: True'

    def convert(self, value):
        if value is None:
            return None

        return bool(value)


class ListField(ApiField):
    """
    A list field.
    """
    dehydrated_type = 'list'
    help_text = "A list of data. Ex: ['abc', 26.73, 8]"

    def convert(self, value):
        if value is None:
            return None

        return list(value)


class DictField(ApiField):
    """
    A dictionary field.
    """
    dehydrated_type = 'dict'
    help_text = "A dictionary of data. Ex: {'price': 26.73, 'name': 'Daniel'}"

    def convert(self, value):
        if value is None:
            return None

        return dict(value)


class DateField(ApiField):
    """
    A date field.
    """
    dehydrated_type = 'date'
    help_text = 'A date as a string. Ex: "2010-11-10"'

    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, str):
            try:
                year, month, day = value[:10].split('-')

                return datetime_safe.date(int(year), int(month), int(day))
            except ValueError:
                raise ApiFieldError("Date provided to '%s' field doesn't appear to be a valid date string: '%s'" % (self.instance_name, value))

        return value

    def hydrate(self, bundle):
        value = super(DateField, self).hydrate(bundle)

        if value and not hasattr(value, 'year'):
            try:
                # Try to rip a date/datetime out of it.
                value = make_aware(parse(value))

                if hasattr(value, 'hour'):
                    value = value.date()
            except ValueError:
                pass

        return value


class DateTimeField(ApiField):
    """
    A datetime field.
    """
    dehydrated_type = 'datetime'
    help_text = 'A date & time as a string. Ex: "2010-11-10T03:07:43"'

    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, str):
            try:
                year, month, day = value[:10].split('-')
                hour, minute, second = value[11:19].split(':')

                return make_aware(datetime_safe.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second)))
            except ValueError:
                raise ApiFieldError("Datetime provided to '%s' field doesn't appear to be a valid datetime string: '%s'" % (self.instance_name, value))

        return value

    def hydrate(self, bundle):
        value = super(DateTimeField, self).hydrate(bundle)

        if value and not hasattr(value, 'year'):
            if isinstance(value, str):
                try:
                    # Try to rip a date/datetime out of it.
                    value = make_aware(parse(value))
                except (ValueError, TypeError):
                    raise ApiFieldError("Datetime provided to '%s' field doesn't appear to be a valid datetime string: '%s'" % (self.instance_name, value))

            else:
                raise ApiFieldError("Datetime provided to '%s' field must be a string: %s" % (self.instance_name, value))

        return value


class RelatedField(ApiField):
    """
    Provides access to data that is related within the database.

    The ``RelatedField`` base class is not intended for direct use but provides
    functionality that ``ToOneField`` and ``ToManyField`` build upon.

    The contents of this field actually point to another ``Resource``,
    rather than the related object. This allows the field to represent its data
    in different ways.

    The abstractions based around this are "leaky" in that, unlike the other
    fields provided by ``tastypie``, these fields don't handle arbitrary objects
    very well. The subclasses use Django's ORM layer to make things go, though
    there is no ORM-specific code at this level.
    """
    dehydrated_type = 'related'
    is_related = True
    help_text = 'A related resource. Can be either a URI or set of nested resource data.'

    def __init__(self, to, attribute, related_name=None, default=NOT_PROVIDED, null=False, blank=False, readonly=False, full=False, unique=False, help_text=None, use_in='all', verbose_name=None, full_list=True, full_detail=True):

        """
        Builds the field and prepares it to access to related data.

        The ``to`` argument should point to a ``Resource`` class, NOT
        to a ``Model``. Required.

        The ``attribute`` argument should specify what field/callable points to
        the related data on the instance object. Required.

        Optionally accepts a ``related_name`` argument. Currently unused, as
        unlike Django's ORM layer, reverse relations between ``Resource``
        classes are not automatically created. Defaults to ``None``.

        Optionally accepts a ``null``, which indicated whether or not a
        ``None`` is allowable data on the field. Defaults to ``False``.

        Optionally accepts a ``blank``, which indicated whether or not
        data may be omitted on the field. Defaults to ``False``.

        Optionally accepts a ``readonly``, which indicates whether the field
        is used during the ``hydrate`` or not. Defaults to ``False``.

        Optionally accepts a ``full``, which indicates how the related
        ``Resource`` will appear post-``dehydrate``. If ``False``, the
        related ``Resource`` will appear as a URL to the endpoint of that
        resource. If ``True``, the result of the sub-resource's
        ``dehydrate`` will be included in full.

        Optionally accepts a ``unique``, which indicates if the field is a
        unique identifier for the object.

        Optionally accepts ``help_text``, which lets you provide a
        human-readable description of the field exposed at the schema level.
        Defaults to the per-Field definition.

        Optionally accepts ``use_in``. This may be one of ``list``, ``detail``
        ``all`` or a callable which accepts a ``bundle`` and returns
        ``True`` or ``False``. Indicates wheather this field will be included
        during dehydration of a list of objects or a single object. If ``use_in``
        is a callable, and returns ``True``, the field will be included during
        dehydration.
        Defaults to ``all``.

        Optionally accepts ``verbose_name``, which lets you provide a
        more verbose name of the field exposed at the schema level.

        Optionally accepts a ``full_list``, which indicated whether or not
        data should be fully dehydrated when the request is for a list of
        resources. Accepts ``True``, ``False`` or a callable that accepts
        a bundle and returns ``True`` or ``False``. Depends on ``full``
        being ``True``. Defaults to ``True``.

        Optionally accepts a ``full_detail``, which indicated whether or not
        data should be fully dehydrated when then request is for a single
        resource. Accepts ``True``, ``False`` or a callable that accepts a
        bundle and returns ``True`` or ``False``.Depends on ``full``
        being ``True``. Defaults to ``True``.
        """
        super(RelatedField, self).__init__(attribute=attribute, default=default, null=null, blank=blank, readonly=readonly, unique=unique, help_text=help_text, use_in=use_in, verbose_name=verbose_name)
        self.related_name = related_name
        self.to = to
        self._to_class = None
        self._rel_resources = {}
        self.full = full
        self.full_list = full_list if callable(full_list) else lambda bundle: full_list
        self.full_detail = full_detail if callable(full_detail) else lambda bundle: full_detail

        self.api_name = None
        self.resource_name = None

    def get_related_resource(self, related_instance):
        """
        Instaniates the related resource.
        """
        related_class = type(related_instance)
        if related_class in self._rel_resources:
            return self._rel_resources[related_class]

        related_resource = self.to_class()

        # Fix the ``api_name`` if it's not present.
        if related_resource._meta.api_name is None:
            if self._resource and self._resource._meta.api_name is not None:
                related_resource._meta.api_name = self._resource._meta.api_name

        self._rel_resources[related_class] = related_resource

        return related_resource

    @property
    def to_class(self):
        # We need to be lazy here, because when the metaclass constructs the
        # Resources, other classes may not exist yet.
        # That said, memoize this so we never have to relookup/reimport.
        if self._to_class:
            return self._to_class

        if not isinstance(self.to, str):
            self._to_class = self.to
            return self._to_class

        # Check if we're self-referential and hook it up.
        # We can't do this quite like Django because there's no ``AppCache``
        # here (which I think we should avoid as long as possible).
        if self.to == 'self':
            self._to_class = self._resource
            return self._to_class

        # It's a string. Let's figure it out.
        if '.' in self.to:
            # Try to import.
            module_bits = self.to.split('.')
            module_path, class_name = '.'.join(module_bits[:-1]), module_bits[-1]
            module = importlib.import_module(module_path)
        else:
            # We've got a bare class name here, which won't work (No AppCache
            # to rely on). Try to throw a useful error.
            raise ImportError("Tastypie requires a Python-style path (<module.module.Class>) to lazy load related resources. Only given '%s'." % self.to)

        self._to_class = getattr(module, class_name, None)

        if self._to_class is None:
            raise ImportError("Module '%s' does not appear to have a class called '%s'." % (module_path, class_name))

        return self._to_class

    def dehydrate_related(self, bundle, related_resource, for_list=True):
        """
        Based on the ``full_resource``, returns either the endpoint or the data
        from ``full_dehydrate`` for the related resource.
        """
        should_dehydrate_full_resource = self.should_full_dehydrate(bundle, for_list=for_list)

        if not should_dehydrate_full_resource:
            # Be a good netizen.
            return related_resource.get_resource_uri(bundle)
        else:
            # ZOMG extra data and big payloads.
            bundle = related_resource.build_bundle(
                obj=bundle.obj,
                request=bundle.request,
                objects_saved=bundle.objects_saved
            )
            return related_resource.full_dehydrate(bundle)

    def resource_from_uri(self, fk_resource, uri, request=None, related_obj=None, related_name=None):
        """
        Given a URI is provided, the related resource is attempted to be
        loaded based on the identifiers in the URI.
        """
        err_msg = "Could not find the provided %s object via resource URI '%s'." % (fk_resource._meta.resource_name, uri,)

        if not uri:
            raise ApiFieldError(err_msg)

        try:
            obj = fk_resource.get_via_uri(uri, request=request)
            bundle = fk_resource.build_bundle(
                obj=obj,
                request=request,
                via_uri=True
            )
            return fk_resource.full_dehydrate(bundle)
        except ObjectDoesNotExist:
            raise ApiFieldError(err_msg)

    def resource_from_data(self, fk_resource, data, request=None, related_obj=None, related_name=None):
        """
        Given a dictionary-like structure is provided, a fresh related
        resource is created using that data.
        """
        # Try to hydrate the data provided.
        obj = None
        if getattr(fk_resource._meta, 'include_resource_uri', True) and 'resource_uri' in data:
            uri = data['resource_uri']
            err_msg = "Could not find the provided %s object via resource URI '%s'." % (fk_resource._meta.resource_name, uri,)
            try:
                obj = fk_resource.get_via_uri(uri, request=request)
            except ObjectDoesNotExist:
                raise ApiFieldError(err_msg)

        fk_bundle = fk_resource.build_bundle(
            data=data,
            obj=obj,
            request=request
        )

        if related_obj:
            fk_bundle.related_obj = related_obj
            fk_bundle.related_name = related_name

        unique_keys = {
            k: v
            for k, v in data.items()
            if k == 'pk' or (hasattr(fk_resource, k) and getattr(fk_resource, k).unique)
        }

        # If we have no unique keys, we shouldn't go look for some resource that
        # happens to match other kwargs. In the case of a create, it might be the
        # completely wrong resource.
        # We also need to check to see if updates are allowed on the FK resource.
        if not obj and unique_keys:
            try:
                fk_resource.obj_get(fk_bundle, **data)
            except (ObjectDoesNotExist, NotFound, TypeError):
                try:
                    # Attempt lookup by primary key
                    fk_resource.obj_get(fk_bundle, **unique_keys)
                except (ObjectDoesNotExist, NotFound):
                    pass
            except MultipleObjectsReturned:
                pass

        # If we shouldn't update a resource, or we couldn't find a matching
        # resource we'll just return a populated bundle instead
        # of mistakenly updating something that should be read-only.
        fk_bundle = fk_resource.full_hydrate(fk_bundle)
        fk_resource.is_valid(fk_bundle)
        return fk_bundle

    def resource_from_pk(self, fk_resource, obj, request=None, related_obj=None, related_name=None):
        """
        Given an object with a ``pk`` attribute, the related resource
        is attempted to be loaded via that PK.
        """
        bundle = fk_resource.build_bundle(
            obj=obj,
            request=request
        )
        return fk_resource.full_dehydrate(bundle)

    def build_related_resource(self, value, request=None, related_obj=None, related_name=None):
        """
        Returns a bundle of data built by the related resource, usually via
        ``hydrate`` with the data provided.

        Accepts either a URI, a data dictionary (or dictionary-like structure)
        or an object with a ``pk``.
        """
        fk_resource = self.to_class()
        kwargs = {
            'request': request,
            'related_obj': related_obj,
            'related_name': related_name,
        }

        if isinstance(value, Bundle):
            # Already hydrated, probably nested bundles. Just return.
            return value
        elif isinstance(value, str):
            # We got a URI. Load the object and assign it.
            return self.resource_from_uri(fk_resource, value, **kwargs)
        elif isinstance(value, dict):
            # We've got a data dictionary.
            # Since this leads to creation, this is the only one of these
            # methods that might care about "parent" data.
            return self.resource_from_data(fk_resource, value, **kwargs)
        elif hasattr(value, 'pk'):
            # We've got an object with a primary key.
            return self.resource_from_pk(fk_resource, value, **kwargs)
        else:
            raise ApiFieldError("The '%s' field was given data that was not a URI, not a dictionary-alike and does not have a 'pk' attribute: %s." % (self.instance_name, value))

    def should_full_dehydrate(self, bundle, for_list):
        """
        Based on the ``full``, ``list_full`` and ``detail_full`` returns ``True`` or ``False``
        indicating weather the resource should be fully dehydrated.
        """
        should_dehydrate_full_resource = False
        if self.full:
            is_details_view = not for_list
            if is_details_view:
                if self.full_detail(bundle):
                    should_dehydrate_full_resource = True
            else:
                if self.full_list(bundle):
                    should_dehydrate_full_resource = True

        return should_dehydrate_full_resource


class ToOneField(RelatedField):
    """
    Provides access to related data via foreign key.

    This subclass requires Django's ORM layer to work properly.
    """
    help_text = 'A single related resource. Can be either a URI or set of nested resource data.'

    def __init__(self, to, attribute, related_name=None, default=NOT_PROVIDED,
                 null=False, blank=False, readonly=False, full=False,
                 unique=False, help_text=None, use_in='all', verbose_name=None,
                 full_list=True, full_detail=True):
        super(ToOneField, self).__init__(
            to, attribute, related_name=related_name, default=default,
            null=null, blank=blank, readonly=readonly, full=full,
            unique=unique, help_text=help_text, use_in=use_in,
            verbose_name=verbose_name, full_list=full_list,
            full_detail=full_detail
        )

    def contribute_to_class(self, cls, name):
        super(ToOneField, self).contribute_to_class(cls, name)
        if not self.related_name and isinstance(self.attribute, str):
            related_field = getattr(self._resource._meta.object_class, self.attribute, None)
            if isinstance(related_field, ReverseOneToOneDescriptor):
                # This is the case when we are writing to a reverse one to one field.
                # Enable related name to make this work fantastically.
                # see https://code.djangoproject.com/ticket/18638 (bug; closed; worksforme)
                # and https://github.com/django-tastypie/django-tastypie/issues/566

                # this gets the related_name of the one to one field of our model
                self.related_name = related_field.related.field.name

    def dehydrate(self, bundle, for_list=True):
        foreign_obj = None

        if callable(self.attribute):
            previous_obj = bundle.obj
            foreign_obj = self.attribute(bundle)
        elif isinstance(self.attribute, str):
            foreign_obj = bundle.obj

            for attr in self._attrs:
                previous_obj = foreign_obj
                try:
                    foreign_obj = getattr(foreign_obj, attr, None)
                except ObjectDoesNotExist:
                    foreign_obj = None

        if not foreign_obj:
            if not self.null:
                if callable(self.attribute):
                    raise ApiFieldError("The related resource for resource %s could not be found." % (previous_obj))
                else:
                    raise ApiFieldError("The model '%r' has an empty attribute '%s' and doesn't allow a null value." % (previous_obj, attr))
            return None

        fk_resource = self.get_related_resource(foreign_obj)
        fk_bundle = Bundle(obj=foreign_obj, request=bundle.request)
        return self.dehydrate_related(fk_bundle, fk_resource, for_list=for_list)

    def hydrate(self, bundle):
        value = super(ToOneField, self).hydrate(bundle)

        if value is None:
            return value

        return self.build_related_resource(value, request=bundle.request)


class ForeignKey(ToOneField):
    """
    A convenience subclass for those who prefer to mirror ``django.db.models``.
    """
    pass


class OneToOneField(ToOneField):
    """
    A convenience subclass for those who prefer to mirror ``django.db.models``.
    """
    pass


class ToManyField(RelatedField):
    """
    Provides access to related data via a join table.

    This subclass requires Django's ORM layer to work properly.

    Note that the ``hydrate`` portions of this field are quite different than
    any other field. ``hydrate_m2m`` actually handles the data and relations.
    This is due to the way Django implements M2M relationships.
    """
    is_m2m = True
    help_text = 'Many related resources. Can be either a list of URIs or list of individually nested resource data.'

    def __init__(self, to, attribute, related_name=None, default=NOT_PROVIDED,
                 null=False, blank=False, readonly=False, full=False,
                 unique=False, help_text=None, use_in='all', verbose_name=None,
                 full_list=True, full_detail=True):
        super(ToManyField, self).__init__(
            to, attribute, related_name=related_name, default=default,
            null=null, blank=blank, readonly=readonly, full=full,
            unique=unique, help_text=help_text, use_in=use_in,
            verbose_name=verbose_name, full_list=full_list,
            full_detail=full_detail
        )

    def dehydrate(self, bundle, for_list=True):
        if not bundle.obj or not bundle.obj.pk:
            if not self.null:
                raise ApiFieldError("The model '%r' does not have a primary key and can not be used in a ToMany context." % bundle.obj)

            return []

        the_m2ms = None
        previous_obj = bundle.obj
        attr = self.attribute

        if callable(self.attribute):
            the_m2ms = self.attribute(bundle)
        elif isinstance(self.attribute, str):
            the_m2ms = bundle.obj

            for attr in self._attrs:
                previous_obj = the_m2ms
                try:
                    the_m2ms = getattr(the_m2ms, attr, None)
                except ObjectDoesNotExist:
                    the_m2ms = None

                if the_m2ms is None:
                    break

        if the_m2ms is None:
            if not self.null:
                raise ApiFieldError("The model '%r' has an empty attribute '%s' and doesn't allow a null value." % (previous_obj, attr))
            return []

        if isinstance(the_m2ms, models.Manager):
            the_m2ms = the_m2ms.all()

        m2m_dehydrated = [
            self.dehydrate_related(
                Bundle(obj=m2m, request=bundle.request),
                self.get_related_resource(m2m),
                for_list=for_list
            )
            for m2m in the_m2ms
        ]

        return m2m_dehydrated

    def hydrate(self, bundle):
        pass

    def hydrate_m2m(self, bundle):
        if self.readonly:
            return None

        if bundle.data.get(self.instance_name) is None:
            if self.blank:
                return []
            if self.null:
                return []
            raise ApiFieldError("The '%s' field has no data and doesn't allow a null value." % self.instance_name)

        kwargs = {
            'request': bundle.request,
        }

        if self.related_name:
            kwargs['related_obj'] = bundle.obj
            kwargs['related_name'] = self.related_name

        return [
            self.build_related_resource(value, **kwargs)
            for value in bundle.data.get(self.instance_name)
            if value is not None
        ]


class ManyToManyField(ToManyField):
    """
    A convenience subclass for those who prefer to mirror ``django.db.models``.
    """
    pass


class OneToManyField(ToManyField):
    """
    A convenience subclass for those who prefer to mirror ``django.db.models``.
    """
    pass


class TimeField(ApiField):
    dehydrated_type = 'time'
    help_text = 'A time as string. Ex: "20:05:23"'

    def dehydrate(self, obj, for_list=True):
        return self.convert(super(TimeField, self).dehydrate(obj))

    def convert(self, value):
        if isinstance(value, str):
            return self.to_time(value)
        return value

    def to_time(self, s):
        try:
            dt = parse(s)
        except (ValueError, TypeError) as e:
            raise ApiFieldError(str(e))
        else:
            return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)

    def hydrate(self, bundle):
        value = super(TimeField, self).hydrate(bundle)

        if value and not isinstance(value, datetime.time):
            value = self.to_time(value)

        return value
