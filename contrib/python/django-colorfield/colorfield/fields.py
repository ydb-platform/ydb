from django.core.exceptions import FieldDoesNotExist, ImproperlyConfigured
from django.db.models import CharField, signals
from django.db.models.fields.files import ImageField

from colorfield.utils import get_image_file_background_color
from colorfield.validators import (
    color_hex_validator,
    color_hexa_validator,
    color_rgb_validator,
    color_rgba_validator,
)
from colorfield.widgets import ColorWidget

VALIDATORS_PER_FORMAT = {
    "hex": color_hex_validator,
    "hexa": color_hexa_validator,
    "rgb": color_rgb_validator,
    "rgba": color_rgba_validator,
}

DEFAULT_PER_FORMAT = {
    "hex": "#FFFFFF",
    "hexa": "#FFFFFFFF",
    "rgb": "rgb(255, 255, 255)",
    "rgba": "rgba(255, 255, 255, 1)",
}


class ColorField(CharField):
    default_validators = []

    def __init__(self, *args, **kwargs):
        # works like Django choices, but does not restrict input to the given choices
        self.samples = kwargs.pop("samples", None)
        self.format = kwargs.pop("format", "hex").lower()
        if self.format not in ["hex", "hexa", "rgb", "rgba"]:
            raise ValueError(f"Unsupported color format: {self.format}")
        self.default_validators = [VALIDATORS_PER_FORMAT[self.format]]

        self.image_field = kwargs.pop("image_field", None)
        if self.image_field:
            kwargs.setdefault("blank", True)

        kwargs.setdefault("max_length", 25)
        if kwargs.get("null"):
            kwargs.setdefault("blank", True)
            kwargs.setdefault("default", None)
        elif kwargs.get("blank"):
            kwargs.setdefault("default", "")
        else:
            kwargs.setdefault("default", DEFAULT_PER_FORMAT[self.format])
        super().__init__(*args, **kwargs)

        if self.choices and self.samples:
            raise ImproperlyConfigured(
                "Invalid options: 'choices' and 'samples' are mutually exclusive, "
                "you can set only one of the two for a ColorField instance."
            )

        # change choices to lower case (case-insensitive workaround)
        if self.choices:
            self.choices = [(k.lower(), *v) for (k, *v) in self.choices]

    def formfield(self, **kwargs):
        palette = []
        if self.choices:
            choices = self.get_choices(include_blank=False)
            palette = [choice[0] for choice in choices]
        elif self.samples:
            palette = [choice[0] for choice in self.samples]
        kwargs["widget"] = ColorWidget(
            attrs={
                "default": self.get_default(),
                "format": self.format[0:3] if self.format else "hex",
                "alpha": self.format[-1] == "a" if self.format else False,
                "swatches": palette,
                "swatches_only": bool(self.choices),
            }
        )
        return super().formfield(**kwargs)

    def contribute_to_class(self, cls, name, **kwargs):
        super().contribute_to_class(cls, name, **kwargs)
        if cls._meta.abstract:
            return
        if self.image_field:
            signals.post_save.connect(self._update_from_image_field, sender=cls)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["samples"] = self.samples
        kwargs["image_field"] = self.image_field
        return name, path, args, kwargs

    def validate(self, value, *args, **kwargs):
        """
        Override validation logic to make it case-insensitive.
        """
        super().validate(value.lower() if value else value, *args, **kwargs)

    def _get_image_field_color(self, instance):
        color = ""
        image_file = getattr(instance, self.image_field)
        if image_file:
            with image_file.open() as _:
                color = get_image_file_background_color(image_file, self.format)
        return color

    def _update_from_image_field(self, instance, created, *args, **kwargs):
        if not instance or not instance.pk or not self.image_field:
            return
        # check if the field is a valid ImageField
        try:
            field_cls = instance._meta.get_field(self.image_field)
            if not isinstance(field_cls, ImageField):
                raise ImproperlyConfigured(
                    "Invalid 'image_field' field type, "
                    "expected an instance of 'models.ImageField'."
                )
        except FieldDoesNotExist as error:
            raise ImproperlyConfigured(
                "Invalid 'image_field' field name, "
                f"{self.image_field!r} field not found."
            ) from error
        # update value from picking color from image field
        color = self._get_image_field_color(instance)
        color_field_name = self.attname
        color_field_value = getattr(instance, color_field_name, None)
        if color_field_value != color and color:
            color_field_value = color or self.default
            # update in-memory value
            setattr(instance, color_field_name, color_field_value)
            # update stored value
            manager = instance.__class__.objects
            manager.filter(pk=instance.pk).update(
                **{color_field_name: color_field_value}
            )
