from diff_match_patch import diff_match_patch
from django import template

register = template.Library()


@register.simple_tag
def compare_values(value1, value2):
    dmp = diff_match_patch()
    diff = dmp.diff_main(value1, value2)
    dmp.diff_cleanupSemantic(diff)
    html = dmp.diff_prettyHtml(diff)
    return html
