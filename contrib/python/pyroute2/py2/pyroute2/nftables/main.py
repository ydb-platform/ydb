'''
'''
from pyroute2.netlink.nfnetlink import nfgen_msg
from pyroute2.netlink.nfnetlink.nftsocket import \
    (NFTSocket,
     nft_table_msg,
     nft_chain_msg,
     nft_rule_msg,
     NFT_MSG_NEWTABLE,
     NFT_MSG_GETTABLE,
     NFT_MSG_DELTABLE,
     NFT_MSG_NEWCHAIN,
     NFT_MSG_GETCHAIN,
     NFT_MSG_DELCHAIN,
     NFT_MSG_NEWRULE,
     NFT_MSG_GETRULE,
     NFT_MSG_DELRULE,
     NFT_MSG_GETSET)


class NFTables(NFTSocket):

    # TODO: documentation
    # TODO: tests
    # TODO: dump()/load() with support for json and xml

    def get_tables(self):
        return self.request_get(nfgen_msg(), NFT_MSG_GETTABLE)

    def get_chains(self):
        return self.request_get(nfgen_msg(), NFT_MSG_GETCHAIN)

    def get_rules(self):
        return self.request_get(nfgen_msg(), NFT_MSG_GETRULE)

    def get_sets(self):
        return self.request_get(nfgen_msg(), NFT_MSG_GETSET)

    #
    # The nft API is in the prototype stage and may be
    # changed until the release. The planned release for
    # the API is 0.5.2
    #

    def table(self, cmd, **kwarg):
        '''
        Example::

            nft.table('add', name='test0')
        '''
        commands = {'add': NFT_MSG_NEWTABLE,
                    'del': NFT_MSG_DELTABLE}
        # fix default kwargs
        if 'flags' not in kwarg:
            kwarg['flags'] = 0
        return self._command(nft_table_msg, commands, cmd, kwarg)

    def chain(self, cmd, **kwarg):
        '''
        Example::

            #
            # default policy 'drop' for input
            #
            nft.chain('add',
                      table='test0',
                      name='test_chain0',
                      hook='input',
                      type='filter',
                      policy=0)
        '''
        commands = {'add': NFT_MSG_NEWCHAIN,
                    'del': NFT_MSG_DELCHAIN}
        # TODO: What about 'ingress' (netdev family)?
        hooks = {'prerouting': 0,
                 'input': 1,
                 'forward': 2,
                 'output': 3,
                 'postrouting': 4}
        if 'hook' in kwarg:
            kwarg['hook'] = {'attrs':
                             [['NFTA_HOOK_HOOKNUM',
                               hooks[kwarg['hook']]],
                              ['NFTA_HOOK_PRIORITY',
                               kwarg.pop('priority', 0)]]}
        if 'type' not in kwarg:
            kwarg['type'] = 'filter'
        return self._command(nft_chain_msg, commands, cmd, kwarg)

    def rule(self, cmd, **kwarg):
        '''
        Example::

            from pyroute2.nftables.expressions import ipv4addr, verdict
            #
            # allow all traffic from 192.168.0.0/24
            #
            nft.rule('add',
                     table='test0',
                     chain='test_chain0',
                     expressions=(ipv4addr(src='192.168.0.0/24'),
                                  verdict(code=1)))
        '''
        # TODO: more operations
        commands = {'add': NFT_MSG_NEWRULE,
                    'del': NFT_MSG_DELRULE}

        if 'expressions' in kwarg:
            expressions = []
            for exp in kwarg['expressions']:
                expressions.extend(exp)
            kwarg['expressions'] = expressions
        # FIXME: flags!!!
        return self._command(nft_rule_msg, commands, cmd, kwarg, flags=3585)
