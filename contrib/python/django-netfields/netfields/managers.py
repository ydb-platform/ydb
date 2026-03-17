from django.db import models
from ipaddress import _BaseNetwork

try:
    str_type = unicode
except NameError:
    str_type = str


class NetManager(models.Manager):
    use_for_related_fields = True

    def filter(self, *args, **kwargs):
        for key, val in kwargs.items():
            if isinstance(val, _BaseNetwork):
                # Django will attempt to consume the _BaseNetwork iterator, which
                # will convert it to a list of every address in the network
                kwargs[key] = str_type(val)
        return super(NetManager, self).filter(*args, **kwargs)
