from django_hosts import patterns, host

host_patterns = patterns('',
    host(r'', 'tests.urls.root', name='root'),
    host(r'(\w+)', 'tests.urls.simple', name='wildcard'),
)
