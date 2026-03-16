from django_hosts import patterns, host

host_patterns = patterns('',
    host(r'spam\.eggs', 'tests.urls.multiple', name='multiple'),
)
