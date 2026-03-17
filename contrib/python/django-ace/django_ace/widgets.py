import warnings

from django import forms

try:
    from django.forms.utils import flatatt
except ImportError:
    from django.forms.util import flatatt

from django.utils.safestring import mark_safe

_NOT_PROVIDED_SENTINEL = object()


class AceWidget(forms.Textarea):
    def __init__(
        self,
        mode=None,
        theme=None,
        wordwrap=False,
        width=_NOT_PROVIDED_SENTINEL,
        height=_NOT_PROVIDED_SENTINEL,
        minlines=None,
        maxlines=None,
        showprintmargin=True,
        showinvisibles=False,
        usesofttabs=True,
        tabsize=None,
        fontsize=None,
        toolbar=True,
        readonly=False,
        showgutter=True,
        behaviours=True,
        useworker=True,
        extensions=None,
        basicautocompletion=False,
        liveautocompletion=False,
        useStrictCSP=False,
        vimKeyBinding=False,
        highlightActiveLine=True,
        *args,
        **kwargs,
    ):
        self.mode = mode
        self.theme = theme
        self.wordwrap = wordwrap
        if width is _NOT_PROVIDED_SENTINEL:
            width = "500px"
            warnings.warn(
                "Django Ace Widget: width and height argument have to be given "
                "explicitly as their default will change, see "
                "https://github.com/django-ace/django-ace#v1380",
                DeprecationWarning,
                stacklevel=2,
            )
        if height is _NOT_PROVIDED_SENTINEL:
            height = "300px"
            warnings.warn(
                "Django Ace Widget: width and height argument have to be given "
                "explicitly as their default will change, see "
                "https://github.com/django-ace/django-ace#v1380",
                DeprecationWarning,
                stacklevel=2,
            )
        self.width = width
        self.height = height
        self.minlines = minlines
        self.maxlines = maxlines
        self.showprintmargin = showprintmargin
        self.showinvisibles = showinvisibles
        self.tabsize = tabsize
        self.fontsize = fontsize
        self.toolbar = toolbar
        self.readonly = readonly
        self.behaviours = behaviours
        self.showgutter = showgutter
        self.usesofttabs = usesofttabs
        self.extensions = extensions
        self.useworker = useworker
        self.basicautocompletion = basicautocompletion
        self.liveautocompletion = liveautocompletion
        self.useStrictCSP = useStrictCSP
        self.vimKeyBinding = vimKeyBinding
        self.highlightActiveLine = highlightActiveLine

        super(AceWidget, self).__init__(*args, **kwargs)

    @property
    def media(self):
        js = ["django_ace/ace/ace.js", "django_ace/widget.js"]
        css = ["django_ace/widget.css"]

        if self.useStrictCSP:
            css.append(f"django_ace/ace/css/ace.css")
        if self.mode:
            js.append(f"django_ace/ace/mode-{self.mode}.js")
        if self.theme:
            js.append(f"django_ace/ace/theme-{self.theme}.js")
            if self.useStrictCSP:
                css.append(f"django_ace/ace/css/theme/{self.theme}.css")
        if self.extensions:
            for extension in self.extensions:
                js.append(f"django_ace/ace/ext-{extension}.js")
        if self.basicautocompletion or self.liveautocompletion:
            language_tools = "django_ace/ace/ext-language_tools.js"
            if language_tools not in js:
                js.append(language_tools)
        if self.vimKeyBinding:
            js.append("django_ace/ace/keybinding-vim.js")

        return forms.Media(js=js, css={"screen": css})

    def render(self, name, value, attrs=None, renderer=None):
        attrs = attrs or {}

        ace_attrs = {
            "class": "django-ace-widget loading",
        }

        if (self.width, self.height) != (None, None):
            style = []
            if self.width is not None:
                style.append(f"width:{self.width}")
            if self.height is not None:
                style.append(f"height:{self.height}")
            ace_attrs["style"] = "; ".join(style)

        if self.mode:
            ace_attrs["data-mode"] = self.mode
        if self.theme:
            ace_attrs["data-theme"] = self.theme
        if self.wordwrap:
            ace_attrs["data-wordwrap"] = "true"
        if self.minlines:
            ace_attrs["data-minlines"] = str(self.minlines)
        if self.maxlines:
            ace_attrs["data-maxlines"] = str(self.maxlines)
        if self.tabsize:
            ace_attrs["data-tabsize"] = str(self.tabsize)
        if self.fontsize:
            ace_attrs["data-fontsize"] = str(self.fontsize)

        ace_attrs["data-readonly"] = "true" if self.readonly else "false"
        ace_attrs["data-showgutter"] = "true" if self.showgutter else "false"
        ace_attrs["data-behaviours"] = "true" if self.behaviours else "false"
        ace_attrs["data-showprintmargin"] = "true" if self.showprintmargin else "false"
        ace_attrs["data-showinvisibles"] = "true" if self.showinvisibles else "false"
        ace_attrs["data-usesofttabs"] = "true" if self.usesofttabs else "false"
        ace_attrs["data-useworker"] = "true" if self.useworker else "false"
        ace_attrs["data-basicautocompletion"] = (
            "true" if self.basicautocompletion else "false"
        )
        ace_attrs["data-liveautocompletion"] = (
            "true" if self.liveautocompletion else "false"
        )
        ace_attrs["data-usestrictcsp"] = "true" if self.useStrictCSP else "false"
        ace_attrs["data-vimkeybinding"] = "true" if self.vimKeyBinding else "false"
        ace_attrs["data-highlightactiveline"] = (
            "true" if self.highlightActiveLine else "false"
        )

        textarea = super(AceWidget, self).render(name, value, attrs, renderer)

        html = "<div{}><div></div></div>{}".format(flatatt(ace_attrs), textarea)

        if self.toolbar:
            if self.width is not None:
                style_width = f'style="width: {self.width}"'
            else:
                style_width = ""
            toolbar = (
                f'<div {style_width} class="django-ace-toolbar">'
                '<a href="./" class="django-ace-max_min"></a>'
                "</div>"
            )
            html = toolbar + html

        html = '<div class="django-ace-editor">{}</div>'.format(html)
        return mark_safe(html)
