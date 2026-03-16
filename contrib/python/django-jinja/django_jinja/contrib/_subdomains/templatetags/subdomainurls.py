from django_jinja import library
from jinja2 import pass_context
from subdomains.templatetags.subdomainurls import url as subdomain_url


@library.global_function
@pass_context
def url(context, *args, **kwargs):
    return subdomain_url(context, *args, **kwargs)
