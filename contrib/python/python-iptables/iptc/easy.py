# -*- coding: utf-8 -*-

# TODO:
# - Add documentation
# - Add HowToUse examples

from .ip4tc import Rule, Table, Chain, IPTCError
from .ip6tc import Rule6, Table6

_BATCH_MODE = False

def flush_all(ipv6=False):
    """ Flush all available tables """
    for table in get_tables(ipv6):
        flush_table(table, ipv6)

def flush_table(table, ipv6=False, raise_exc=True):
    """ Flush a table """
    try:
        iptc_table = _iptc_gettable(table, ipv6)
        iptc_table.flush()
    except Exception as e:
        if raise_exc: raise

def flush_chain(table, chain, ipv6=False, raise_exc=True):
    """ Flush a chain in table """
    try:
        iptc_chain = _iptc_getchain(table, chain, ipv6)
        iptc_chain.flush()
    except Exception as e:
        if raise_exc: raise

def zero_all(table, ipv6=False):
    """ Zero all tables """
    for table in get_tables(ipv6):
        zero_table(table, ipv6)

def zero_table(table, ipv6=False):
    """ Zero a table """
    iptc_table = _iptc_gettable(table, ipv6)
    iptc_table.zero_entries()

def zero_chain(table, chain, ipv6=False):
    """ Zero a chain in table """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_chain.zero_counters()

def has_chain(table, chain, ipv6=False):
    """ Return True if chain exists in table False otherwise """
    return _iptc_gettable(table, ipv6).is_chain(chain)

def has_rule(table, chain, rule_d, ipv6=False):
    """ Return True if rule exists in chain False otherwise """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_rule  = encode_iptc_rule(rule_d, ipv6)
    return iptc_rule in iptc_chain.rules

def add_chain(table, chain, ipv6=False, raise_exc=True):
    """ Return True if chain was added successfully to a table, raise Exception otherwise """
    try:
        iptc_table = _iptc_gettable(table, ipv6)
        iptc_table.create_chain(chain)
        return True
    except Exception as e:
        if raise_exc: raise
    return False

def add_rule(table, chain, rule_d, position=0, ipv6=False):
    """ Add a rule to a chain in a given position. 0=append, 1=first, n=nth position """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_rule  = encode_iptc_rule(rule_d, ipv6)
    if position == 0:
        # Insert rule in last position -> append
        iptc_chain.append_rule(iptc_rule)
    elif position > 0:
        # Insert rule in given position -> adjusted as iptables CLI
        iptc_chain.insert_rule(iptc_rule, position - 1)
    elif position < 0:
        # Insert rule in given position starting from bottom -> not available in iptables CLI
        nof_rules = len(iptc_chain.rules)
        _position = position + nof_rules
        # Insert at the top if the position has looped over
        if _position <= 0:
            _position = 0
        iptc_chain.insert_rule(iptc_rule, _position)

def insert_rule(table, chain, rule_d, ipv6=False):
    """ Add a rule to a chain in the 1st position """
    add_rule(table, chain, rule_d, position=1, ipv6=ipv6)

def delete_chain(table, chain, ipv6=False, flush=False, raise_exc=True):
    """ Delete a chain """
    try:
        if flush:
            flush_chain(table, chain, ipv6, raise_exc)
        iptc_table = _iptc_gettable(table, ipv6)
        iptc_table.delete_chain(chain)
    except Exception as e:
        if raise_exc: raise

def delete_rule(table, chain, rule_d, ipv6=False, raise_exc=True):
    """ Delete a rule from a chain """
    try:
        iptc_chain = _iptc_getchain(table, chain, ipv6)
        iptc_rule  = encode_iptc_rule(rule_d, ipv6)
        iptc_chain.delete_rule(iptc_rule)
    except Exception as e:
        if raise_exc: raise

def get_tables(ipv6=False):
    """ Get all tables """
    iptc_tables = _iptc_gettables(ipv6)
    return [t.name for t in iptc_tables]

def get_chains(table, ipv6=False):
    """ Return the existing chains of a table """
    iptc_table = _iptc_gettable(table, ipv6)
    return [iptc_chain.name for iptc_chain in iptc_table.chains]

def get_rule(table, chain, position=0, ipv6=False, raise_exc=True):
    """ Get a rule from a chain in a given position. 0=all rules, 1=first, n=nth position """
    try:
        if position == 0:
            # Return all rules
            return dump_chain(table, chain, ipv6)
        elif position > 0:
            # Return specific rule by position
            iptc_chain = _iptc_getchain(table, chain, ipv6)
            iptc_rule = iptc_chain.rules[position - 1]
            return decode_iptc_rule(iptc_rule, ipv6)
        elif position < 0:
            # Return last rule  -> not available in iptables CLI
            iptc_chain = _iptc_getchain(table, chain, ipv6)
            iptc_rule = iptc_chain.rules[position]
            return decode_iptc_rule(iptc_rule, ipv6)
    except Exception as e:
        if raise_exc: raise

def replace_rule(table, chain, old_rule_d, new_rule_d, ipv6=False):
    """ Replaces an existing rule of a chain """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_old_rule = encode_iptc_rule(old_rule_d, ipv6)
    iptc_new_rule = encode_iptc_rule(new_rule_d, ipv6)
    iptc_chain.replace_rule(iptc_new_rule, iptc_chain.rules.index(iptc_old_rule))

def get_rule_counters(table, chain, rule_d, ipv6=False):
    """ Return a tuple with the rule counters (numberOfBytes, numberOfPackets) """
    if not has_rule(table, chain, rule_d, ipv6):
        raise AttributeError('Chain <{}@{}> has no rule <{}>'.format(chain, table, rule_d))
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_rule  = encode_iptc_rule(rule_d, ipv6)
    iptc_rule_index = iptc_chain.rules.index(iptc_rule)
    return iptc_chain.rules[iptc_rule_index].get_counters()

def get_rule_position(table, chain, rule_d, ipv6=False):
    """ Return the position of a rule within a chain """
    if not has_rule(table, chain, rule_d):
        raise AttributeError('Chain <{}@{}> has no rule <{}>'.format(chain, table, rule_d))
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_rule  = encode_iptc_rule(rule_d, ipv6)
    return iptc_chain.rules.index(iptc_rule)


def test_rule(rule_d, ipv6=False):
    """ Return True if the rule is a well-formed dictionary, False otherwise """
    try:
        encode_iptc_rule(rule_d, ipv6)
        return True
    except:
        return False

def test_match(name, value, ipv6=False):
    """ Return True if the match is valid, False otherwise """
    try:
        iptc_rule = Rule6() if ipv6 else Rule()
        _iptc_setmatch(iptc_rule, name, value)
        return True
    except:
        return False

def test_target(name, value, ipv6=False):
    """ Return True if the target is valid, False otherwise """
    try:
        iptc_rule = Rule6() if ipv6 else Rule()
        _iptc_settarget(iptc_rule, {name:value})
        return True
    except:
        return False


def get_policy(table, chain, ipv6=False):
    """ Return the default policy of chain in a table """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    return iptc_chain.get_policy().name

def set_policy(table, chain, policy='ACCEPT', ipv6=False):
    """ Set the default policy of chain in a table """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    iptc_chain.set_policy(policy)


def dump_all(ipv6=False):
    """ Return a dictionary representation of all tables """
    return {table: dump_table(table, ipv6) for table in get_tables(ipv6)}

def dump_table(table, ipv6=False):
    """ Return a dictionary representation of a table """
    return {chain: dump_chain(table, chain, ipv6) for chain in get_chains(table, ipv6)}

def dump_chain(table, chain, ipv6=False):
    """ Return a list with the dictionary representation of the rules of a table """
    iptc_chain = _iptc_getchain(table, chain, ipv6)
    return [decode_iptc_rule(iptc_rule, ipv6) for iptc_rule in iptc_chain.rules]


def batch_begin(table = None, ipv6=False):
    """ Disable autocommit on a table """
    _BATCH_MODE = True
    if table:
        tables = (table, )
    else:
        tables = get_tables(ipv6)
    for table in tables:
        iptc_table = _iptc_gettable(table, ipv6)
        iptc_table.autocommit = False

def batch_end(table = None, ipv6=False):
    """ Enable autocommit on table and commit changes """
    _BATCH_MODE = False
    if table:
        tables = (table, )
    else:
        tables = get_tables(ipv6)
    for table in tables:
        iptc_table = _iptc_gettable(table, ipv6)
        iptc_table.autocommit = True

def batch_add_chains(table, chains, ipv6=False, flush=True):
    """ Add multiple chains to a table """
    iptc_table = _batch_begin_table(table, ipv6)
    for chain in chains:
        if iptc_table.is_chain(chain):
            iptc_chain = Chain(iptc_table, chain)
        else:
            iptc_chain = iptc_table.create_chain(chain)
        if flush:
            iptc_chain.flush()
    _batch_end_table(table, ipv6)

def batch_delete_chains(table, chains, ipv6=False):
    """ Delete multiple chains of a table """
    iptc_table = _batch_begin_table(table, ipv6)
    for chain in chains:
        if iptc_table.is_chain(chain):
            iptc_chain = Chain(iptc_table, chain)
            iptc_chain.flush()
            iptc_table.delete_chain(chain)
    _batch_end_table(table, ipv6)

def batch_add_rules(table, batch_rules, ipv6=False):
    """ Add multiple rules to a table with format (chain, rule_d, position) """
    iptc_table = _batch_begin_table(table, ipv6)
    for (chain, rule_d, position) in batch_rules:
        iptc_chain = Chain(iptc_table, chain)
        iptc_rule  = encode_iptc_rule(rule_d, ipv6)
        if position == 0:
            # Insert rule in last position -> append
            iptc_chain.append_rule(iptc_rule)
        elif position > 0:
            # Insert rule in given position -> adjusted as iptables CLI
            iptc_chain.insert_rule(iptc_rule, position-1)
        elif position < 0:
            # Insert rule in given position starting from bottom -> not available in iptables CLI
            nof_rules = len(iptc_chain.rules)
            iptc_chain.insert_rule(iptc_rule, position + nof_rules)
    _batch_end_table(table, ipv6)

def batch_delete_rules(table, batch_rules, ipv6=False, raise_exc=True):
    """ Delete  multiple rules from table with format (chain, rule_d) """
    try:
        iptc_table = _batch_begin_table(table, ipv6)
        for (chain, rule_d) in batch_rules:
            iptc_chain = Chain(iptc_table, chain)
            iptc_rule  = encode_iptc_rule(rule_d, ipv6)
            iptc_chain.delete_rule(iptc_rule)
        _batch_end_table(table, ipv6)
    except Exception as e:
        if raise_exc: raise


def encode_iptc_rule(rule_d, ipv6=False):
    """ Return a Rule(6) object from the input dictionary """
    # Sanity check
    assert(isinstance(rule_d, dict))
    # Basic rule attributes
    rule_attr = ('src', 'dst', 'protocol', 'in-interface', 'out-interface', 'fragment')
    iptc_rule = Rule6() if ipv6 else Rule()
    # Set default target
    rule_d.setdefault('target', '')
    # Avoid issues with matches that require basic parameters to be configured first
    for name in rule_attr:
        if name in rule_d:
            setattr(iptc_rule, name.replace('-', '_'), rule_d[name])
    for name, value in rule_d.items():
        try:
            if name in rule_attr:
                continue
            elif name == 'counters':
                _iptc_setcounters(iptc_rule, value)
            elif name == 'target':
                _iptc_settarget(iptc_rule, value)
            else:
                _iptc_setmatch(iptc_rule, name, value)
        except Exception as e:
            #print('Ignoring unsupported field <{}:{}>'.format(name, value))
            continue
    return iptc_rule

def decode_iptc_rule(iptc_rule, ipv6=False):
    """ Return a dictionary representation of the Rule(6) object
    Note: host IP addresses are appended their corresponding CIDR """
    d = {}
    if ipv6==False and iptc_rule.src != '0.0.0.0/0.0.0.0':
        _ip, _netmask = iptc_rule.src.split('/')
        _netmask = _netmask_v4_to_cidr(_netmask)
        d['src'] = '{}/{}'.format(_ip, _netmask)
    elif ipv6==True and iptc_rule.src != '::/0':
        d['src'] = iptc_rule.src
    if ipv6==False and iptc_rule.dst != '0.0.0.0/0.0.0.0':
        _ip, _netmask = iptc_rule.dst.split('/')
        _netmask = _netmask_v4_to_cidr(_netmask)
        d['dst'] = '{}/{}'.format(_ip, _netmask)
    elif ipv6==True and iptc_rule.dst != '::/0':
        d['dst'] = iptc_rule.dst
    if iptc_rule.protocol != 'ip':
        d['protocol'] = iptc_rule.protocol
    if iptc_rule.in_interface is not None:
        d['in-interface'] = iptc_rule.in_interface
    if iptc_rule.out_interface is not None:
        d['out-interface'] = iptc_rule.out_interface
    if ipv6 == False and iptc_rule.fragment:
        d['fragment'] = iptc_rule.fragment
    for m in iptc_rule.matches:
        if m.name not in d:
            d[m.name] = m.get_all_parameters()
        elif isinstance(d[m.name], list):
            d[m.name].append(m.get_all_parameters())
        else:
            d[m.name] = [d[m.name], m.get_all_parameters()]
    if iptc_rule.target and iptc_rule.target.name and len(iptc_rule.target.get_all_parameters()):
        name = iptc_rule.target.name.replace('-', '_')
        d['target'] = {name:iptc_rule.target.get_all_parameters()}
    elif iptc_rule.target and iptc_rule.target.name:
        if iptc_rule.target.goto:
            d['target'] = {'goto':iptc_rule.target.name}
        else:
            d['target'] = iptc_rule.target.name
    # Get counters
    d['counters'] = iptc_rule.counters
    # Return a filtered dictionary
    return _filter_empty_field(d)

### INTERNAL FUNCTIONS ###
def _iptc_table_available(table, ipv6=False):
    """ Return True if the table is available, False otherwise """
    try:
        iptc_table = Table6(table) if ipv6 else Table(table)
        return True
    except:
        return False

def _iptc_gettables(ipv6=False):
    """ Return an updated view of all available iptc_table """
    iptc_cls = Table6 if ipv6 else Table
    return [_iptc_gettable(t, ipv6) for t in iptc_cls.ALL if _iptc_table_available(t, ipv6)]

def _iptc_gettable(table, ipv6=False):
    """ Return an updated view of an iptc_table """
    iptc_table = Table6(table) if ipv6 else Table(table)
    if _BATCH_MODE is False:
        iptc_table.commit()
        iptc_table.refresh()
    return iptc_table

def _iptc_getchain(table, chain, ipv6=False, raise_exc=True):
    """ Return an iptc_chain of an updated table """
    try:
        iptc_table = _iptc_gettable(table, ipv6)
        if not iptc_table.is_chain(chain):
            raise AttributeError('Table <{}> has no chain <{}>'.format(table, chain))
        return Chain(iptc_table, chain)
    except Exception as e:
        if raise_exc: raise

def _iptc_setcounters(iptc_rule, value):
    # Value is a tuple (numberOfBytes, numberOfPackets)
    iptc_rule.counters = value

def _iptc_setmatch(iptc_rule, name, value):
    # Iterate list/tuple recursively
    if isinstance(value, list) or isinstance(value, tuple):
        for inner_value in value:
            _iptc_setmatch(iptc_rule, name, inner_value)
    # Assign dictionary value
    elif isinstance(value, dict):
        iptc_match = iptc_rule.create_match(name)
        [iptc_match.set_parameter(k, v) for k, v in value.items()]
    # Assign value directly
    else:
        iptc_match = iptc_rule.create_match(name)
        iptc_match.set_parameter(name, value)

def _iptc_settarget(iptc_rule, value):
    # Target is dictionary - Use only 1st pair key/value
    if isinstance(value, dict):
        t_name, t_value = next(iter(value.items()))
        if t_name == 'goto':
            iptc_target = iptc_rule.create_target(t_value, goto=True)
        else:
            iptc_target = iptc_rule.create_target(t_name)
            [iptc_target.set_parameter(k, v) for k, v in t_value.items()]
    # Simple target
    else:
        iptc_target = iptc_rule.create_target(value)

def _batch_begin_table(table, ipv6=False):
    """ Disable autocommit on a table """
    iptc_table = _iptc_gettable(table, ipv6)
    iptc_table.autocommit = False
    return iptc_table

def _batch_end_table(table, ipv6=False):
    """ Enable autocommit on table and commit changes """
    iptc_table = _iptc_gettable(table, ipv6)
    iptc_table.autocommit = True
    return iptc_table

def _filter_empty_field(data_d):
    """
    Remove empty lists from dictionary values
    Before: {'target': {'CHECKSUM': {'checksum-fill': []}}}
    After:  {'target': {'CHECKSUM': {'checksum-fill': ''}}}
    Before: {'tcp': {'dport': ['22']}}}
    After:  {'tcp': {'dport': '22'}}}
    """
    for k, v in data_d.items():
        if isinstance(v, dict):
            data_d[k] = _filter_empty_field(v)
        elif isinstance(v, list) and len(v) != 0:
            v = [_filter_empty_field(_v) if isinstance(_v, dict) else _v for _v in v ]
        if isinstance(v, list) and len(v) == 1:
            data_d[k] = v.pop()
        elif isinstance(v, list) and len(v) == 0:
            data_d[k] = ''
    return data_d

def _netmask_v4_to_cidr(netmask_addr):
    # Implement Subnet Mask conversion without dependencies
    return sum([bin(int(x)).count('1') for x in netmask_addr.split('.')])

### /INTERNAL FUNCTIONS ###
