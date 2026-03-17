# coding: utf-8

from django.conf import settings
from django.core.files import File
from django.template import Library, Node, Variable, VariableDoesNotExist, TemplateSyntaxError

from filebrowser.settings import VERSIONS, PLACEHOLDER, SHOW_PLACEHOLDER, FORCE_PLACEHOLDER
from filebrowser.base import FileObject
from filebrowser.sites import get_default_site


register = Library()


class VersionNode(Node):
    def __init__(self, src, suffix, var_name):
        self.src = src
        self.suffix = suffix
        self.var_name = var_name

    def render(self, context):
        try:
            version_suffix = self.suffix.resolve(context)
            source = self.src.resolve(context)
        except VariableDoesNotExist:
            if self.var_name:
                return None
            return ""
        if version_suffix not in VERSIONS:
            return ""  # FIXME: should this throw an error?
        if isinstance(source, FileObject):
            source = source.path
        elif isinstance(source, File):
            source = source.name
        else:  # string
            source = source
        if 'filebrowser_site' in context:
            site = context['filebrowser_site']
        else:
            site = get_default_site()
        if FORCE_PLACEHOLDER or (SHOW_PLACEHOLDER and not site.storage.isfile(source)):
            source = PLACEHOLDER
        fileobject = FileObject(source, site=site)
        try:
            version = fileobject.version_generate(version_suffix)
            if self.var_name:
                context[self.var_name] = version
            else:
                return version.url
        except Exception:
            if context.template.engine.debug:
                raise
            if self.var_name:
                context[self.var_name] = ""
        return ""


def version(parser, token):
    """
    Displaying a version of an existing Image according to the predefined VERSIONS settings (see filebrowser settings).
    {% version fileobject version_suffix %}

    Use {% version fileobject 'medium' %} in order to
    display the medium-size version of an image.
    version_suffix can be a string or a variable. if version_suffix is a string, use quotes.

    Return a context variable 'var_name' with the FileObject
    {% version fileobject version_suffix as var_name %}

    Use {% version fileobject 'medium' as version_medium %} in order to
    retrieve the medium version of an image stored in a variable version_medium.
    version_suffix can be a string or a variable. If version_suffix is a string, use quotes.
    """

    bits = token.split_contents()
    if len(bits) != 3 and len(bits) != 5:
        raise TemplateSyntaxError("'version' tag takes 2 or 4 arguments")
    if len(bits) == 5 and bits[3] != 'as':
        raise TemplateSyntaxError("second argument to 'version' tag must be 'as'")
    if len(bits) == 3:
        return VersionNode(parser.compile_filter(bits[1]), parser.compile_filter(bits[2]), None)
    if len(bits) == 5:
        return VersionNode(parser.compile_filter(bits[1]), parser.compile_filter(bits[2]), bits[4])


class VersionSettingNode(Node):
    def __init__(self, version_suffix):
        if (version_suffix[0] == version_suffix[-1] and version_suffix[0] in ('"', "'")):
            self.version_suffix = version_suffix[1:-1]
        else:
            self.version_suffix = None
            self.version_suffix_var = Variable(version_suffix)

    def render(self, context):
        if self.version_suffix:
            version_suffix = self.version_suffix
        else:
            try:
                version_suffix = self.version_suffix_var.resolve(context)
            except VariableDoesNotExist:
                return None
        context['version_setting'] = VERSIONS[version_suffix]
        return ''


def version_setting(parser, token):
    """
    Get Information about a version setting.
    """

    try:
        tag, version_suffix = token.split_contents()
    except:
        raise TemplateSyntaxError("%s tag requires 1 argument" % token.contents.split()[0])
    if (version_suffix[0] == version_suffix[-1] and version_suffix[0] in ('"', "'")) and version_suffix.lower()[1:-1] not in VERSIONS:
        raise TemplateSyntaxError("%s tag received bad version_suffix %s" % (tag, version_suffix))
    return VersionSettingNode(version_suffix)

register.tag(version)
register.tag(version_setting)
