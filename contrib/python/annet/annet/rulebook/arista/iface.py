import re

from annet.rulebook import common


# Добавление возможности удаления агрегатов, лупбэков, SVI и сабинтерфейсов
def permanent(rule, key, diff, **kwargs):  # pylint: disable=redefined-outer-name
    ifname = key[0]
    # Match group examples:
    # Group 01: Port-Channel10, Loopback1, Vlan800
    # Group 02: Ethernet2/1.20, Port-Channel10.200
    if re.match(r"((?:Port-Channel|Loopback|Vlan)\d+$)|((?:Ethernet|Port-Channel)[\d/]+\.\d+$)", ifname):
        # Эти интерфейсы можно удалять
        yield from common.default(rule, key, diff, **kwargs)
    else:
        yield from common.permanent(rule, key, diff, **kwargs)
