"""Jinja2's i18n functionality is not exactly the same as Django's.
In particular, the tags names and their syntax are different:

  1. The Django ``trans`` tag is replaced by a _() global.
  2. The Django ``blocktrans`` tag is called ``trans``.

(1) isn't an issue, since the whole ``makemessages`` process is based on
converting the template tags to ``_()`` calls. However, (2) means that
those Jinja2 ``trans`` tags will not be picked up my Django's
``makemessage`` command.

There aren't any nice solutions here. While Jinja2's i18n extension does
come with extraction capabilities built in, the code behind ``makemessages``
unfortunately isn't extensible, so we can:

  * Duplicate the command + code behind it.
  * Offer a separate command for Jinja2 extraction.
  * Try to get Django to offer hooks into makemessages().
  * Monkey-patch.

We are currently doing that last thing. It turns out there we are lucky
for once: It's simply a matter of extending two regular expressions.
Credit for the approach goes to:
http://stackoverflow.com/questions/2090717/getting-translation-strings-for-jinja2-templates-integrated-with-django-1-x
"""

import re

from django.core.management.commands import makemessages
from django.template import engines
from django.template.base import BLOCK_TAG_START, BLOCK_TAG_END
from django.utils.translation import template as trans_real


strip_whitespace_right = re.compile(fr"({BLOCK_TAG_START}-?\s*(trans|pluralize).*?-{BLOCK_TAG_END})\s+", re.U)
strip_whitespace_left = re.compile(fr"\s+({BLOCK_TAG_START}-\s*(endtrans|pluralize).*?-?{BLOCK_TAG_END})", re.U)


def strip_whitespaces(src):
    src = strip_whitespace_left.sub(r'\1', src)
    src = strip_whitespace_right.sub(r'\1', src)
    return src


# this regex looks for {% trans %} blocks that don't have 'trimmed' or 'notrimmed' set.
# capturing {% endtrans %} ensures this doesn't affect DTL {% trans %} tags.
trans_block_re = re.compile(
    fr"({BLOCK_TAG_START}-?\s*trans)(?!\s+(?:no)?trimmed)"
    fr"(.*?{BLOCK_TAG_END}.*?{BLOCK_TAG_START}-?\s*?endtrans\s*?-?{BLOCK_TAG_END})",
    re.U | re.DOTALL
)


def apply_i18n_trimmed_policy(src, engine):
    # if env.policies["ext.i18n.trimmed"]: insert 'trimmed' flag on jinja {% trans %} blocks.
    i18n_trim_policy = engine.get("OPTIONS", {}).get("policies", {}).get("ext.i18n.trimmed")
    if not i18n_trim_policy:
        return src
    return trans_block_re.sub(r"\1 trimmed \2", src)


class Command(makemessages.Command):

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument('--jinja2-engine-name', default=None, dest='jinja_engine')

    def _get_default_jinja_template_engine(self):
        # dev's note: i would love to have this easy default: --jinja2-engine-name=jinja2
        # but due to historical reasons, django-jinja's engine's name can default to either `jinja2` or 'backend'.
        # see: https://docs.djangoproject.com/en/dev/ref/settings/#std:setting-TEMPLATES-NAME
        # the default engine will be the first one exactly matching either of the new or old import paths.
        supported_import_paths = ["django_jinja.backend.Jinja2", "django_jinja.jinja2.Jinja2"]
        return [e for e in engines.templates.values() if e["BACKEND"] in supported_import_paths][0]

    def handle(self, *args, **options):
        old_endblock_re = trans_real.endblock_re
        old_block_re = trans_real.block_re
        old_constant_re = trans_real.constant_re

        old_templatize = trans_real.templatize
        # Extend the regular expressions that are used to detect
        # translation blocks with an "OR jinja-syntax" clause.
        trans_real.endblock_re = re.compile(
            trans_real.endblock_re.pattern + '|' + r"""^-?\s*endtrans\s*-?$""")
        trans_real.block_re = re.compile(
            trans_real.block_re.pattern + '|' + r"""^-?\s*trans(?:\s+(?:no)?trimmed)?(?:\s+(?!'|")(?=.*?=.*?)|\s*-?$)""")
        trans_real.plural_re = re.compile(
            trans_real.plural_re.pattern + '|' + r"""^-?\s*pluralize(?:\s+.+|-?$)""")
        trans_real.constant_re = re.compile(r""".*?_\(((?:".*?(?<!\\)")|(?:'.*?(?<!\\)')).*?\)""")

        if options['jinja_engine']:
            jinja_engine = engines[options['jinja_engine']]
        else:
            jinja_engine = self._get_default_jinja_template_engine()

        def my_templatize(src, origin=None, **kwargs):
            new_src = strip_whitespaces(src)
            new_src = apply_i18n_trimmed_policy(new_src, jinja_engine)
            return old_templatize(new_src, origin, **kwargs)

        trans_real.templatize = my_templatize

        try:
            super().handle(*args, **options)
        finally:
            trans_real.endblock_re = old_endblock_re
            trans_real.block_re = old_block_re
            trans_real.templatize = old_templatize
            trans_real.constant_re = old_constant_re
