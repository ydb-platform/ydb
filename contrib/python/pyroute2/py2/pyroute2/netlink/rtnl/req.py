from socket import AF_INET
from socket import AF_INET6
from collections import OrderedDict
from pyroute2.common import AF_MPLS
from pyroute2.common import basestring
from pyroute2.netlink.rtnl import rt_type
from pyroute2.netlink.rtnl import rt_proto
from pyroute2.netlink.rtnl import rt_scope
from pyroute2.netlink.rtnl import encap_type
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg
from pyroute2.netlink.rtnl.ifinfmsg import protinfo_bridge
from pyroute2.netlink.rtnl.ifinfmsg.plugins.vlan import flags as vlan_flags
from pyroute2.netlink.rtnl.rtmsg import rtmsg
from pyroute2.netlink.rtnl.rtmsg import nh as nh_header
from pyroute2.netlink.rtnl.rtmsg import LWTUNNEL_ENCAP_MPLS
from pyroute2.netlink.rtnl.fibmsg import FR_ACT_NAMES


encap_types = {'mpls': 1,
               AF_MPLS: 1,
               'seg6': 5,
               'bpf': 6,
               'seg6local': 7}


class IPRequest(OrderedDict):

    def __init__(self, obj=None):
        super(IPRequest, self).__init__()
        if obj is not None:
            self.update(obj)

    def update(self, obj):
        if obj.get('family', None):
            self['family'] = obj['family']
        for key in obj:
            if key == 'family':
                continue
            v = obj[key]
            if isinstance(v, dict):
                self[key] = dict((x for x in v.items() if x[1] is not None))
            elif v is not None:
                self[key] = v


class IPRuleRequest(IPRequest):

    def update(self, obj):
        super(IPRuleRequest, self).update(obj)
        # now fix the rest
        if 'family' not in self:
            self['family'] = AF_INET
        if ('priority' not in self) and ('FRA_PRIORITY' not in self):
            self['priority'] = 32000
        if 'table' in self and 'action' not in self:
            self['action'] = 'to_tbl'
        for key in ('src_len', 'dst_len'):
            if self.get(key, None) is None and key[:3] in self:
                self[key] = {AF_INET6: 128, AF_INET: 32}[self['family']]

    def __setitem__(self, key, value):
        if key.startswith('ipdb_'):
            return

        if key in ('src', 'dst'):
            v = value.split('/')
            if len(v) == 2:
                value, self['%s_len' % key] = v[0], int(v[1])
        elif key == 'action' and isinstance(value, basestring):
            value = (FR_ACT_NAMES
                     .get(value, (FR_ACT_NAMES
                                  .get('FR_ACT_' + value.upper(), value))))

        super(IPRuleRequest, self).__setitem__(key, value)


class IPAddrRequest(IPRequest):

    def __setitem__(self, key, value):
        if key in ('preferred_lft', 'valid_lft'):
            key = key[:-4]
        if key in ('preferred', 'valid'):
            super(IPAddrRequest, self).__setitem__('IFA_CACHEINFO', {})
        super(IPAddrRequest, self).__setitem__(key, value)

    def sync_cacheinfo(self):
        cacheinfo = self.get('IFA_CACHEINFO', self.get('cacheinfo', None))
        if cacheinfo is not None:
            for i in ('preferred', 'valid'):
                cacheinfo['ifa_%s' % i] = self.get(i, pow(2, 32) - 1)


class IPRouteRequest(IPRequest):
    '''
    Utility class, that converts human-readable dictionary
    into RTNL route request.
    '''
    resolve = {'encap_type': encap_type,
               'type': rt_type,
               'proto': rt_proto,
               'scope': rt_scope}

    def __init__(self, obj=None):
        self._mask = []
        IPRequest.__init__(self, obj)

    def encap_header(self, header):
        '''
        Encap header transform. Format samples:

            {'type': 'mpls',
             'labels': '200/300'}

            {'type': AF_MPLS,
             'labels': (200, 300)}

            {'type': 'mpls',
             'labels': 200}

            {'type': AF_MPLS,
             'labels': [{'bos': 0, 'label': 200, 'ttl': 16},
                        {'bos': 1, 'label': 300, 'ttl': 16}]}
        '''
        if isinstance(header['type'], int) or \
                (header['type'] in ('mpls', AF_MPLS, LWTUNNEL_ENCAP_MPLS)):
            ret = []
            override_bos = True
            labels = header['labels']
            if isinstance(labels, basestring):
                labels = labels.split('/')
            if not isinstance(labels, (tuple, list, set)):
                labels = (labels, )
            for label in labels:
                if isinstance(label, dict):
                    # dicts append intact
                    override_bos = False
                    ret.append(label)
                else:
                    # otherwise construct label dict
                    if isinstance(label, basestring):
                        label = int(label)
                    ret.append({'bos': 0, 'label': label})
            # the last label becomes bottom-of-stack
            if override_bos:
                ret[-1]['bos'] = 1
            return {'attrs': [['MPLS_IPTUNNEL_DST', ret]]}
        '''
        Seg6 encap header transform. Format samples:

            {'type': 'seg6',
             'mode': 'encap',
             'segs': '2000::5,2000::6'}

            {'type': 'seg6',
             'mode': 'encap'
             'segs': '2000::5,2000::6',
             'hmac': 1}
        '''
        if header['type'] == 'seg6':
            # Init step
            ret = {}
            # Parse segs
            segs = header['segs']
            # If they are in the form in_addr6,in_addr6
            if isinstance(segs, basestring):
                # Create an array with the splitted values
                temp = segs.split(',')
                # Init segs
                segs = []
                # Iterate over the values
                for seg in temp:
                    # Discard empty string
                    if seg != '':
                        # Add seg to segs
                        segs.append(seg)
            # Retrieve mode
            mode = header['mode']
            # hmac is optional and contains the hmac key
            hmac = header.get('hmac', None)
            # Construct the new object
            ret = {'mode': mode, 'segs': segs}
            # If hmac is present convert to u32
            if hmac:
                # Add to ret the hmac key
                ret['hmac'] = hmac & 0xffffffff
            # Done return the object
            return {'attrs': [['SEG6_IPTUNNEL_SRH', ret]]}
        '''
        BPF encap header transform. Format samples:

            {'type': 'bpf',
             'in': {'fd':4, 'name':'firewall'}}

            {'type': 'bpf',
             'in'  : {'fd':4, 'name':'firewall'},
             'out' : {'fd':5, 'name':'stats'},
             'xmit': {'fd':6, 'name':'vlan_push', 'headroom':4}}
        '''
        if header['type'] == 'bpf':
            attrs = {}
            for key, value in header.items():
                if key not in ['in', 'out', 'xmit']:
                    continue

                obj = [['LWT_BPF_PROG_FD', value['fd']],
                       ['LWT_BPF_PROG_NAME', value['name']]]
                if key == 'in':
                    attrs['LWT_BPF_IN'] = {'attrs': obj}
                elif key == 'out':
                    attrs['LWT_BPF_OUT'] = {'attrs': obj}
                elif key == 'xmit':
                    attrs['LWT_BPF_XMIT'] = {'attrs': obj}
                    if 'headroom' in value:
                        attrs['LWT_BPF_XMIT_HEADROOM'] = value['headroom']

            return {'attrs': attrs.items()}
        '''
        Seg6 encap header transform. Format samples:

            {'type': 'seg6local',
             'action': 'End.DT6',
             'table': '10'}

            {'type': 'seg6local',
             'action': 'End.B6',
             'table': '10'
             'srh': {'segs': '2000::5,2000::6'}}
        '''
        if header['type'] == 'seg6local':
            # Init step
            ret = {}
            table = None
            nh4 = None
            nh6 = None
            iif = None  # Actually not used
            oif = None
            srh = {}
            segs = []
            hmac = None
            prog_fd = None
            prog_name = None
            # Parse segs
            if srh:
                segs = header['srh']['segs']
                # If they are in the form in_addr6,in_addr6
                if isinstance(segs, basestring):
                    # Create an array with the splitted values
                    temp = segs.split(',')
                    # Init segs
                    segs = []
                    # Iterate over the values
                    for seg in temp:
                        # Discard empty string
                        if seg != '':
                            # Add seg to segs
                            segs.append(seg)
                # hmac is optional and contains the hmac key
                hmac = header.get('hmac', None)
            # Retrieve action
            action = header['action']
            if action == 'End.X':
                # Retrieve nh6
                nh6 = header['nh6']
            elif action == 'End.T':
                # Retrieve table and convert to u32
                table = header['table'] & 0xffffffff
            elif action == 'End.DX2':
                # Retrieve oif and convert to u32
                oif = header['oif'] & 0xffffffff
            elif action == 'End.DX6':
                # Retrieve nh6
                nh6 = header['nh6']
            elif action == 'End.DX4':
                # Retrieve nh6
                nh4 = header['nh4']
            elif action == 'End.DT6':
                # Retrieve table
                table = header['table']
            elif action == 'End.DT4':
                # Retrieve table
                table = header['table']
            elif action == 'End.B6':
                # Parse segs
                segs = header['srh']['segs']
                # If they are in the form in_addr6,in_addr6
                if isinstance(segs, basestring):
                    # Create an array with the splitted values
                    temp = segs.split(',')
                    # Init segs
                    segs = []
                    # Iterate over the values
                    for seg in temp:
                        # Discard empty string
                        if seg != '':
                            # Add seg to segs
                            segs.append(seg)
                # hmac is optional and contains the hmac key
                hmac = header.get('hmac', None)
                srh['segs'] = segs
                # If hmac is present convert to u32
                if hmac:
                    # Add to ret the hmac key
                    srh['hmac'] = hmac & 0xffffffff
                srh['mode'] = 'inline'
            elif action == 'End.B6.Encaps':
                # Parse segs
                segs = header['srh']['segs']
                # If they are in the form in_addr6,in_addr6
                if isinstance(segs, basestring):
                    # Create an array with the splitted values
                    temp = segs.split(',')
                    # Init segs
                    segs = []
                    # Iterate over the values
                    for seg in temp:
                        # Discard empty string
                        if seg != '':
                            # Add seg to segs
                            segs.append(seg)
                # hmac is optional and contains the hmac key
                hmac = header.get('hmac', None)
                srh['segs'] = segs
                if hmac:
                    # Add to ret the hmac key
                    srh['hmac'] = hmac & 0xffffffff
                srh['mode'] = 'encap'
            elif action == 'End.BPF':
                prog_fd = header['bpf']['fd']
                prog_name = header['bpf']['name']

            # Construct the new object
            ret = []
            ret.append(['SEG6_LOCAL_ACTION', {'value': action}])
            if table:
                # Add the table to ret
                ret.append(['SEG6_LOCAL_TABLE', {'value': table}])
            if nh4:
                # Add the nh4 to ret
                ret.append(['SEG6_LOCAL_NH4', {'value': nh4}])
            if nh6:
                # Add the nh6 to ret
                ret.append(['SEG6_LOCAL_NH6', {'value': nh6}])
            if iif:
                # Add the iif to ret
                ret.append(['SEG6_LOCAL_IIF', {'value': iif}])
            if oif:
                # Add the oif to ret
                ret.append(['SEG6_LOCAL_OIF', {'value': oif}])
            if srh:
                # Add the srh to ret
                ret.append(['SEG6_LOCAL_SRH', srh])
            if prog_fd and prog_name:
                # Add the prog_fd and prog_name to ret
                ret.append(['SEG6_LOCAL_BPF', {'attrs': [
                    ['LWT_BPF_PROG_FD', prog_fd],
                    ['LWT_BPF_PROG_NAME', prog_name],
                ]}])
            # Done return the object
            return {'attrs': ret}

    def mpls_rta(self, value):
        ret = []
        if not isinstance(value, (list, tuple, set)):
            value = (value, )
        for label in value:
            if isinstance(label, int):
                label = {'label': label,
                         'bos': 0}
            elif isinstance(label, basestring):
                label = {'label': int(label),
                         'bos': 0}
            elif not isinstance(label, dict):
                raise ValueError('wrong MPLS label')
            ret.append(label)
        if ret:
            ret[-1]['bos'] = 1
        return ret

    def __setitem__(self, key, value):
        if key[:4] == 'RTA_':
            key = key[4:].lower()
        # skip virtual IPDB fields
        if key.startswith('ipdb_'):
            return
        # fix family
        if isinstance(value, basestring) and value.find(':') >= 0:
            self['family'] = AF_INET6
        # ignore empty strings as value
        if value == '':
            return
        # work on the rest
        if key == 'family' and value == AF_MPLS:
            super(IPRouteRequest, self).__setitem__('family', value)
            super(IPRouteRequest, self).__setitem__('dst_len', 20)
            super(IPRouteRequest, self).__setitem__('table', 254)
            super(IPRouteRequest, self).__setitem__('type', 1)
        elif key == 'flags' and self.get('family', None) == AF_MPLS:
            return
        elif key in ('dst', 'src'):
            if isinstance(value, (tuple, list, dict)):
                super(IPRouteRequest, self).__setitem__(key, value)
            elif isinstance(value, int):
                super(IPRouteRequest, self).__setitem__(key, {'label': value,
                                                              'bos': 1})
            elif value != 'default':
                value = value.split('/')
                mask = None
                if len(value) == 1:
                    dst = value[0]
                    if '%s_len' % key not in self:
                        if self.get('family', 0) == AF_INET:
                            mask = 32
                        elif self.get('family', 0) == AF_INET6:
                            mask = 128
                        else:
                            self._mask.append('%s_len' % key)
                elif len(value) == 2:
                    dst = value[0]
                    mask = int(value[1])
                else:
                    raise ValueError('wrong address spec')
                super(IPRouteRequest, self).__setitem__(key, dst)
                if mask is not None:
                    (super(IPRouteRequest, self)
                     .__setitem__('%s_len' % key, mask))
        elif key == 'newdst':
            (super(IPRouteRequest, self)
             .__setitem__('newdst', self.mpls_rta(value)))
        elif key in self.resolve.keys():
            if isinstance(value, basestring):
                value = self.resolve[key][value]
            super(IPRouteRequest, self).__setitem__(key, value)
        elif key == 'encap':
            if isinstance(value, dict):
                # human-friendly form:
                #
                # 'encap': {'type': 'mpls',
                #           'labels': '200/300'}
                #
                # 'type' is mandatory
                if 'type' in value and 'labels' in value:
                    (super(IPRouteRequest, self)
                     .__setitem__('encap_type',
                                  encap_types.get(value['type'],
                                                  value['type'])))
                    (super(IPRouteRequest, self)
                     .__setitem__('encap', self.encap_header(value)))
                # human-friendly form:
                #
                # 'encap': {'type': 'seg6',
                #           'mode': 'encap'
                #           'segs': '2000::5,2000::6'}
                #
                # 'encap': {'type': 'seg6',
                #           'mode': 'inline'
                #           'segs': '2000::5,2000::6'
                #           'hmac': 1}
                #
                # 'encap': {'type': 'seg6',
                #           'mode': 'encap'
                #           'segs': '2000::5,2000::6'
                #           'hmac': 0xf}
                #
                # 'encap': {'type': 'seg6',
                #           'mode': 'inline'
                #           'segs': ['2000::5', '2000::6']}
                #
                # 'type', 'mode' and 'segs' are mandatory
                if 'type' in value and 'mode' in value and 'segs' in value:
                    (super(IPRouteRequest, self)
                     .__setitem__('encap_type',
                                  encap_types.get(value['type'],
                                                  value['type'])))
                    (super(IPRouteRequest, self)
                     .__setitem__('encap', self.encap_header(value)))
                elif 'type' in value and ('in' in value or 'out' in value or
                                          'xmit' in value):
                    (super(IPRouteRequest, self)
                     .__setitem__('encap_type',
                                  encap_types.get(value['type'],
                                                  value['type'])))
                    (super(IPRouteRequest, self)
                     .__setitem__('encap', self.encap_header(value)))
                # human-friendly form:
                #
                # 'encap': {'type': 'seg6local',
                #           'action': 'End'}
                #
                # 'encap': {'type': 'seg6local',
                #           'action': 'End.DT6',
                #           'table': '10'}
                #
                # 'encap': {'type': 'seg6local',
                #           'action': 'End.DX6',
                #           'nh6': '2000::5'}
                #
                # 'encap': {'type': 'seg6local',
                #           'action': 'End.B6'
                #           'srh': {'segs': '2000::5,2000::6',
                #                   'hmac': 0xf}}
                #
                # 'type' and 'action' are mandatory
                elif 'type' in value and 'action' in value:
                    (super(IPRouteRequest, self)
                     .__setitem__('encap_type',
                                  encap_types.get(value['type'],
                                                  value['type'])))
                    (super(IPRouteRequest, self)
                     .__setitem__('encap', self.encap_header(value)))
                # assume it is a ready-to-use NLA
                elif 'attrs' in value:
                    super(IPRouteRequest, self).__setitem__('encap', value)
        elif key == 'via':
            # ignore empty RTA_VIA
            if isinstance(value, dict) and \
                    set(value.keys()) == set(('addr', 'family')) and \
                    value['family'] in (AF_INET, AF_INET6) and \
                    isinstance(value['addr'], basestring):
                super(IPRouteRequest, self).__setitem__('via', value)
        elif key == 'metrics':
            if 'attrs' in value:
                ret = value
            else:
                ret = {'attrs': []}
                for name in value:
                    rtax = rtmsg.metrics.name2nla(name)
                    ret['attrs'].append([rtax, value[name]])
            if ret['attrs']:
                super(IPRouteRequest, self).__setitem__('metrics', ret)
        elif key == 'multipath':
            ret = []
            for v in value:
                if 'attrs' in v:
                    ret.append(v)
                    continue
                nh = {'attrs': []}
                nh_fields = [x[0] for x in nh_header.fields]
                for name in nh_fields:
                    nh[name] = v.get(name, 0)
                for name in v:
                    if name in nh_fields or v[name] is None:
                        continue
                    if name == 'encap' and isinstance(v[name], dict):
                        if v[name].get('type', None) is None or \
                                v[name].get('labels', None) is None:
                            continue
                        nh['attrs'].append(['RTA_ENCAP_TYPE',
                                            encap_types.get(v[name]['type'],
                                                            v[name]['type'])])
                        nh['attrs'].append(['RTA_ENCAP',
                                            self.encap_header(v[name])])
                    elif name == 'newdst':
                        nh['attrs'].append(['RTA_NEWDST',
                                            self.mpls_rta(v[name])])
                    else:
                        rta = rtmsg.name2nla(name)
                        nh['attrs'].append([rta, v[name]])
                ret.append(nh)
            if ret:
                super(IPRouteRequest, self).__setitem__('multipath', ret)
        elif key == 'family':
            for d in self._mask:
                if d not in self:
                    if value == AF_INET:
                        super(IPRouteRequest, self).__setitem__(d, 32)
                    elif value == AF_INET6:
                        super(IPRouteRequest, self).__setitem__(d, 128)
            self._mask = []
            super(IPRouteRequest, self).__setitem__(key, value)
        else:
            super(IPRouteRequest, self).__setitem__(key, value)


class CBRequest(IPRequest):
    '''
    FIXME
    '''
    commands = None
    msg = None

    def __init__(self, *argv, **kwarg):
        self['commands'] = {'attrs': []}

    def __setitem__(self, key, value):
        if value is None:
            return
        if key in self.commands:
            self['commands']['attrs'].\
                append([self.msg.name2nla(key), value])
        else:
            super(CBRequest, self).__setitem__(key, value)


class IPBridgeRequest(IPRequest):

    def __setitem__(self, key, value):
        if key in ('vlan_info', 'mode', 'vlan_flags'):
            if 'IFLA_AF_SPEC' not in self:
                (super(IPBridgeRequest, self)
                 .__setitem__('IFLA_AF_SPEC', {'attrs': []}))
            nla = ifinfmsg.af_spec_bridge.name2nla(key)
            self['IFLA_AF_SPEC']['attrs'].append([nla, value])
        else:
            super(IPBridgeRequest, self).__setitem__(key, value)


class IPBrPortRequest(dict):

    def __init__(self, obj=None):
        dict.__init__(self)
        dict.__setitem__(self, 'attrs', [])
        self.allowed = [x[0] for x in protinfo_bridge.nla_map]
        if obj is not None:
            self.update(obj)

    def update(self, obj):
        for key in obj:
            self[key] = obj[key]

    def __setitem__(self, key, value):
        key = protinfo_bridge.name2nla(key)
        if key in self.allowed:
            self['attrs'].append((key, value))


class IPLinkRequest(IPRequest):
    '''
    Utility class, that converts human-readable dictionary
    into RTNL link request.
    '''
    blacklist = ['carrier',
                 'carrier_changes',
                 'info_slave_kind']

    # get common ifinfmsg NLAs
    common = []
    for (key, _) in ifinfmsg.nla_map:
        common.append(key)
        common.append(key[len(ifinfmsg.prefix):].lower())
    common.append('family')
    common.append('ifi_type')
    common.append('index')
    common.append('flags')
    common.append('change')

    def __init__(self, *argv, **kwarg):
        self.deferred = []
        self.kind = None
        self.specific = {}
        self.linkinfo = None
        self._info_data = None
        IPRequest.__init__(self, *argv, **kwarg)
        if 'index' not in self:
            self['index'] = 0

    @property
    def info_data(self):
        if self._info_data is None:
            info_data = ('IFLA_INFO_DATA', {'attrs': []})
            self._info_data = info_data[1]['attrs']
            self.linkinfo.append(info_data)
        return self._info_data

    def flush_deferred(self):
        # create IFLA_LINKINFO
        linkinfo = {'attrs': []}
        self.linkinfo = linkinfo['attrs']
        super(IPLinkRequest, self).__setitem__('IFLA_LINKINFO', linkinfo)
        self.linkinfo.append(['IFLA_INFO_KIND', self.kind])
        # load specific NLA names
        cls = ifinfmsg.ifinfo.data_map.get(self.kind, None)
        if cls is not None:
            prefix = cls.prefix or 'IFLA_'
            for nla, _ in cls.nla_map:
                self.specific[nla] = nla
                self.specific[nla[len(prefix):].lower()] = nla

        # flush deferred NLAs
        for (key, value) in self.deferred:
            if not self.set_specific(key, value):
                super(IPLinkRequest, self).__setitem__(key, value)

        self.deferred = []

    def set_vf(self, spec):
        vflist = []
        if not isinstance(spec, (list, tuple)):
            spec = (spec, )
        for vf in spec:
            vfcfg = []
            # pop VF index
            vfid = vf.pop('vf')          # mandatory
            # pop VLAN spec
            vlan = vf.pop('vlan', None)  # optional
            if isinstance(vlan, int):
                vfcfg.append(('IFLA_VF_VLAN', {'vf': vfid,
                                               'vlan': vlan}))
            elif isinstance(vlan, dict):
                vlan['vf'] = vfid
                vfcfg.append(('IFLA_VF_VLAN', vlan))
            elif isinstance(vlan, (list, tuple)):
                vlist = []
                for vspec in vlan:
                    vspec['vf'] = vfid
                    vlist.append(('IFLA_VF_VLAN_INFO', vspec))
                vfcfg.append(('IFLA_VF_VLAN_LIST', {'attrs': vlist}))
            # pop rate spec
            rate = vf.pop('rate', None)  # optional
            if rate is not None:
                rate['vf'] = vfid
                vfcfg.append(('IFLA_VF_RATE', rate))
            # create simple VF attrs
            for attr in vf:
                vfcfg.append((ifinfmsg.vflist.vfinfo.name2nla(attr),
                              {'vf': vfid,
                               attr: vf[attr]}))
            vflist.append(('IFLA_VF_INFO', {'attrs': vfcfg}))
        (super(IPLinkRequest, self)
         .__setitem__('IFLA_VFINFO_LIST', {'attrs': vflist}))

    def set_specific(self, key, value):
        # FIXME: vlan hack
        if self.kind == 'vlan' and key == 'vlan_flags':
            if isinstance(value, (list, tuple)):
                if len(value) == 2 and \
                        all((isinstance(x, int) for x in value)):
                    value = {'flags': value[0],
                             'mask': value[1]}
                else:
                    ret = 0
                    for x in value:
                        ret |= vlan_flags.get(x, 1)
                    value = {'flags': ret,
                             'mask': ret}
            elif isinstance(value, int):
                value = {'flags': value,
                         'mask': value}
            elif isinstance(value, basestring):
                value = vlan_flags.get(value, 1)
                value = {'flags': value,
                         'mask': value}
            elif not isinstance(value, dict):
                raise ValueError()
        # the kind is known: lookup the NLA
        if key in self.specific:
            self.info_data.append((self.specific[key], value))
            return True
        elif key == 'peer' and self.kind == 'veth':
            # FIXME: veth hack
            if isinstance(value, dict):
                attrs = []
                for k, v in value.items():
                    attrs.append([ifinfmsg.name2nla(k), v])
            else:
                attrs = [['IFLA_IFNAME', value], ]
            nla = ['VETH_INFO_PEER', {'attrs': attrs}]
            self.info_data.append(nla)
            return True
        elif key == 'mode':
            # FIXME: ipvlan / tuntap / bond hack
            if self.kind == 'tuntap':
                nla = ['IFTUN_MODE', value]
            else:
                nla = ['IFLA_%s_MODE' % self.kind.upper(), value]
            self.info_data.append(nla)
            return True

        return False

    def __setitem__(self, key, value):
        # ignore blacklisted attributes
        if key in self.blacklist:
            return

        # there must be no "None" values in the request
        if value is None:
            return

        # all the values must be in ascii
        try:
            if isinstance(value, unicode):
                value = value.encode('ascii')
        except NameError:
            pass

        if key in ('kind', 'info_kind') and not self.kind:
            self.kind = value
            self.flush_deferred()
        elif key == 'vf':  # SR-IOV virtual function setup
            self.set_vf(value)
        elif self.kind is None:
            if key in self.common:
                super(IPLinkRequest, self).__setitem__(key, value)
            else:
                self.deferred.append((key, value))
        else:
            if not self.set_specific(key, value):
                super(IPLinkRequest, self).__setitem__(key, value)
