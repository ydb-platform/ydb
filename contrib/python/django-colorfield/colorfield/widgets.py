from django.conf import settings
from django.forms import TextInput
from django.template.loader import render_to_string

from colorfield.utils import get_random_string


class ColorWidget(TextInput):
    template_name = "colorfield/color.html"

    class Media:
        if settings.DEBUG:
            css = {"all": ["colorfield/coloris/coloris.css"]}
            js = [
                "colorfield/coloris/coloris.js",
                "colorfield/colorfield.js",
            ]
        else:
            css = {"all": ["colorfield/coloris/coloris.min.css"]}
            js = [
                "colorfield/coloris/coloris.min.js",
                "colorfield/colorfield.js",
            ]

    def get_context(self, name, value, attrs=None):
        context = {}
        context.update(self.attrs.copy() or {})
        context.update(attrs or {})
        context.update(
            {
                "widget": self,
                "name": name,
                "value": value,
                # ensure that there is an id
                "data_coloris_id": "coloris-" + context.get("id", get_random_string()),
                # data-coloris options
                "data_coloris_options": {
                    "format": context.get("format", "hex"),
                    "required": context.get("required", False),
                    "clearButton": not bool(context.get("required")),
                    "alpha": bool(context.get("alpha")),
                    "forceAlpha": bool(context.get("alpha")),
                    "swatches": context.get("swatches", []),
                    "swatchesOnly": bool(context.get("swatches", []))
                    and bool(context.get("swatches_only")),
                },
            }
        )
        return context

    def render(self, name, value, attrs=None, renderer=None):
        return render_to_string(
            self.template_name, self.get_context(name, value, attrs)
        )
