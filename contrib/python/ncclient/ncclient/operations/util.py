# Copyright 2009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'Boilerplate ugliness'

from ncclient.xml_ import *
from ncclient.operations.errors import OperationError, MissingCapabilityError
try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse

def one_of(*args):
    "Verifies that only one of the arguments is not None"
    for i, arg in enumerate(args):
        if arg is not None:
            for argh in args[i+1:]:
                if argh is not None:
                    raise OperationError("Too many parameters")
            else:
                return
    raise OperationError("Insufficient parameters")

def datastore_or_url(wha, loc, capcheck=None):
    node = new_ele(wha)
    if "://" in loc: # e.g. http://, file://, ftp://
        if capcheck is not None:
            capcheck(":url") # url schema check at some point!
            sub_ele(node, "url").text = loc
    else:
        #if loc == 'candidate':
        #    capcheck(':candidate')
        #elif loc == 'startup':
        #    capcheck(':startup')
        #elif loc == 'running' and wha == 'target':
        #    capcheck(':writable-running')
        sub_ele(node, loc)
    return node

def build_filter(spec, capcheck=None):
    type = None
    if isinstance(spec, tuple):
        type, criteria = spec
        if type == "xpath":
            if isinstance(criteria, tuple):
                ns, select = criteria
                rep = new_ele_nsmap("filter", ns, type=type)
                rep.attrib["select"] = select
            else:
                rep = new_ele("filter", type=type)
                rep.attrib["select"]=criteria
        elif type == "subtree":
            rep = new_ele("filter", type=type)
            rep.append(to_ele(criteria))
        else:
            raise OperationError("Invalid filter type")
    elif isinstance(spec, list):
        rep = new_ele("filter", type="subtree")
        for cri in spec:
            rep.append(to_ele(cri))
    else:

        rep = validated_element(spec, ("filter", qualify("filter"),
                                       qualify("filter", ns=NETCONF_NOTIFICATION_NS)))
        # results in XMLError: line 105 ncclient/xml_.py - commented by earies - 5/10/13
        #rep = validated_element(spec, ("filter", qualify("filter")),
        #                                attrs=("type",))
        # TODO set type var here, check if select attr present in case of xpath..
    if type == "xpath" and capcheck is not None:
        capcheck(":xpath")
    return rep

def validate_args(arg_name, value, args_list):
    # this is a common method, which used to check whether a value is in args_list
    if value not in args_list:
        raise OperationError('Invalid value "%s" in "%s" element' % (value, arg_name))
    return True

def url_validator(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False
