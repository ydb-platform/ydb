# -*- coding: utf-8 -*-
import os
import sys

import betamax

sys.path.insert(0, '.')

placeholders = {
    '<IPADDR>': os.environ.get('IPADDR', '127.0.0.1'),
}

with betamax.Betamax.configure() as config:
    for placeholder, value in placeholders.items():
        config.define_cassette_placeholder(placeholder, value)
