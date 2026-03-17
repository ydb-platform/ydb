"""fast, simple packet creation and parsing."""
from __future__ import absolute_import
from __future__ import division
import sys

__author__ = 'Various'
__author_email__ = ''
__license__ = 'BSD-3-Clause'
__url__ = 'https://github.com/kbandla/dpkt'
__version__ = '1.9.8'

from .dpkt import *

from . import ah
from . import aoe
from . import aim
from . import arp
from . import asn1
from . import bgp
from . import cdp
from . import dhcp
from . import diameter
from . import dns
from . import dtp
from . import esp
from . import ethernet
from . import gre
from . import gzip
from . import h225
from . import hsrp
from . import http
from . import http2
from . import icmp
from . import icmp6
from . import ieee80211
from . import igmp
from . import ip
from . import ip6
from . import ipx
from . import llc
from . import loopback
from . import mrt
from . import netbios
from . import netflow
from . import ntp
from . import ospf
from . import pcap
from . import pcapng
from . import pim
from . import pmap
from . import ppp
from . import pppoe
from . import qq
from . import radiotap
from . import radius
from . import rfb
from . import rip
from . import rpc
from . import rtp
from . import rx
from . import sccp
from . import sctp
from . import sip
from . import sll
from . import sll2
from . import smb
from . import ssl
from . import stp
from . import stun
from . import tcp
from . import telnet
from . import tftp
from . import tns
from . import tpkt
from . import udp
from . import vrrp
from . import yahoo

# Note: list() is used to get a copy of the dict in order to avoid
# "RuntimeError: dictionary changed size during iteration"
# exception in Python 3 caused by _mod_init() funcs that load another modules
for name, mod in list(sys.modules.items()):
    if name.startswith('dpkt.') and hasattr(mod, '_mod_init'):
        mod._mod_init()
