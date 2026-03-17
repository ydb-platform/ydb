from django_hosts import patterns, host
from tests.hosts.simple import host_patterns

host_patterns += patterns('',
    host(r'special', 'tests.urls.simple', name='special'),
)
