from ...base import get_match_extension


class Jinja2TemplateResponseMixin:
    jinja2_template_extension = None

    def get_template_names(self):
        """
        Return a list of template names to be used for the request.
        This calls the super class's get_template_names and appends
        the Jinja2 match extension is suffixed to the returned values.

        If you specify jinja2_template_extension then that value will
        be used. Otherwise it tries to detect the extension based on
        values in settings.

        If you would like to not have it append an extension, set
        jinja2_template_extension to '' (empty string).
        """
        vals = super().get_template_names()

        ext = self.jinja2_template_extension
        if ext is None:
            ext = get_match_extension(using=getattr(self, 'template_engine', None))

        # Exit early if the user has specified an empty match extension
        if not ext:
            return vals

        names = []
        for val in vals:
            if not val.endswith(ext):
                val += ext
            names.append(val)

        return names
