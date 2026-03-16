# -*- coding: utf-8 -*-

"""
.. module:: iptc
   :synopsis: Python bindings for libiptc.

.. moduleauthor:: Vilmos Nebehaj
"""

from iptc.ip4tc import (is_table_available, Table, Chain, Rule, Match, Target, Policy, IPTCError)
from iptc.ip6tc import is_table6_available, Table6, Rule6
from iptc.errors import *
import iptc.easy


__all__ = []
