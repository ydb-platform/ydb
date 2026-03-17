from django.core.exceptions import ImproperlyConfigured
from django.forms import ModelForm
from django.forms.models import model_to_dict
from django.db.models.fields.related import RelatedField


class Validation(object):
    """
    A basic validation stub that does no validation.
    """
    def __init__(self, **kwargs):
        pass

    def is_valid(self, bundle, request=None):
        """
        Performs a check on the data within the bundle (and optionally the
        request) to ensure it is valid.

        Should return a dictionary of error messages. If the dictionary has
        zero items, the data is considered valid. If there are errors, keys
        in the dictionary should be field names and the values should be a list
        of errors, even if there is only one.
        """
        return {}


class FormValidation(Validation):
    """
    A validation class that uses a Django ``Form`` to validate the data.

    This class **DOES NOT** alter the data sent, only verifies it. If you
    want to alter the data, please use the ``CleanedDataFormValidation`` class
    instead.

    This class requires a ``form_class`` argument, which should be a Django
    ``Form`` (or ``ModelForm``, though ``save`` will never be called) class.
    This form will be used to validate the data in ``bundle.data``.
    """
    def __init__(self, **kwargs):
        if 'form_class' not in kwargs:
            raise ImproperlyConfigured(
                "You must provide a 'form_class' to 'FormValidation' classes.")

        self.form_class = kwargs.pop('form_class')
        super(FormValidation, self).__init__(**kwargs)

    def form_args(self, bundle):
        '''
        Use the model data to generate the form arguments to be used for
        validation.  In the case of fields that had to be hydrated (such as
        FK relationships), be sure to use the hydrated value (comes from
        model_to_dict()) rather than the value in bundle.data, since the latter
        would likely not validate as the form won't expect a URI.
        '''
        data = bundle.data

        # Ensure we get a bound Form, regardless of the state of the bundle.
        if data is None:
            data = {}

        kwargs = {'data': {}}
        if hasattr(bundle.obj, 'pk'):
            if issubclass(self.form_class, ModelForm):
                kwargs['instance'] = bundle.obj

            kwargs['data'] = model_to_dict(bundle.obj)
            # iterate over the fields in the object and find those that are
            # related fields - FK, M2M, O2M, etc.  In those cases, we need
            # to *not* use the data in the bundle, since it is a URI to a
            # resource.  Instead, use the output of model_to_dict for
            # validation, since that is already properly hydrated.
            for field in bundle.obj._meta.fields:
                if field.name in bundle.data:
                    if not isinstance(field, RelatedField):
                        kwargs['data'][field.name] = bundle.data[field.name]
        else:
            kwargs['data'].update(data)
        return kwargs

    def is_valid(self, bundle, request=None):
        """
        Performs a check on ``bundle.data``to ensure it is valid.

        If the form is valid, an empty list (all valid) will be returned. If
        not, a list of errors will be returned.
        """

        form = self.form_class(**self.form_args(bundle))

        if form.is_valid():
            return {}

        # The data is invalid. Let's collect all the error messages & return
        # them.
        return form.errors


class CleanedDataFormValidation(FormValidation):
    """
    A validation class that uses a Django ``Form`` to validate the data.

    This class **ALTERS** data sent by the user!!!

    This class requires a ``form_class`` argument, which should be a Django
    ``Form`` (or ``ModelForm``, though ``save`` will never be called) class.
    This form will be used to validate the data in ``bundle.data``.
    """
    def is_valid(self, bundle, request=None):
        """
        Checks ``bundle.data``to ensure it is valid & replaces it with the
        cleaned results.

        If the form is valid, an empty list (all valid) will be returned. If
        not, a list of errors will be returned.
        """
        form = self.form_class(**self.form_args(bundle))

        if form.is_valid():
            # We're different here & relying on having a reference to the same
            # bundle the rest of the process is using.
            bundle.data = form.cleaned_data
            return {}

        # The data is invalid. Let's collect all the error messages & return
        # them.
        return form.errors
