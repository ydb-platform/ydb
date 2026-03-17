from typing import Union

from django.template import Library

register = Library()


@register.filter
def commafy(value: Union[int, float, str, None]) -> str:
    return f'{value:,}' if value is not None else '-'
