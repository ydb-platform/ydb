import logging

from django.template import Template
from django.utils.safestring import SafeString

from aws_xray_sdk.core import xray_recorder

log = logging.getLogger(__name__)


def patch_template():

    attr = '_xray_original_render'

    if getattr(Template, attr, None):
        log.debug("already patched")
        return

    setattr(Template, attr, Template.render)

    @xray_recorder.capture('template_render')
    def xray_render(self, context):
        template_name = self.name or getattr(context, 'template_name', None)
        if template_name:
            name = str(template_name)
            # SafeString are not properly serialized by jsonpickle,
            # turn them back to str by adding a non-safe str.
            if isinstance(name, SafeString):
                name += ''
            subsegment = xray_recorder.current_subsegment()
            if subsegment:
                subsegment.name = name

        return Template._xray_original_render(self, context)

    Template.render = xray_render
