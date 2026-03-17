#!/usr/bin/env python
'''
A CLI interface to a remote salt-api instance
'''
from __future__ import print_function

import sys
import json
import logging

from pepper.cli import PepperCli
from pepper.retcode import PepperRetcode
from pepper.exceptions import (
    PepperException,
    PepperAuthException,
    PepperArgumentsException,
)

try:
    import salt.loader
    import salt.config
    import salt.output
    HAS_SALT = True
except ImportError:
    HAS_SALT = False

logger = logging.getLogger(__name__)


class Pepper(object):
    def __init__(self):
        self.cli = PepperCli()
        if HAS_SALT:
            self.opts = salt.config.client_config(self.cli.options.master)
        else:
            self.opts = {}
        if self.cli.options.output_file is not None:
            self.opts['output_file'] = self.cli.options.output_file

    @property
    def output(self):
        if not hasattr(self, 'modules'):
            self.modules = salt.loader.minion_mods(self.opts)
        try:
            oput = self.modules[self.cli.args[1]].__outputter__
        except (KeyError, AttributeError, TypeError):
            oput = 'nested'
        return oput

    def __call__(self):
        try:
            for exit_code, result in self.cli.run():
                if HAS_SALT and self.opts:
                    logger.debug('Use Salt outputters')
                    result = json.loads(result)

                    # unwrap ret in some cases
                    if 'return' in result:
                        result = result['return']

                    for ret in result:
                        if isinstance(ret, dict):
                            if self.cli.options.client.startswith('local'):
                                for minionid, minionret in ret.items():
                                    # rest_tornado doesnt return full_return directly
                                    # it will always be from get_event, so the output differs slightly
                                    if isinstance(minionret, dict) and 'return' in minionret:
                                        # version >= 2017.7
                                        salt.output.display_output(
                                            {minionid: minionret['return']},
                                            self.cli.options.output or minionret.get('out', None) or 'nested',
                                            opts=self.opts
                                        )
                                    # cherrypy returns with ret via full_return
                                    elif isinstance(minionret, dict) and 'ret' in minionret:
                                        # version >= 2017.7
                                        salt.output.display_output(
                                            {minionid: minionret['ret']},
                                            self.cli.options.output or minionret.get('out', None) or 'nested',
                                            opts=self.opts
                                        )
                                    else:
                                        salt.output.display_output(
                                            {minionid: minionret},
                                            self.cli.options.output or self.output,
                                            opts=self.opts
                                        )
                            elif 'data' in ret:
                                # unfold runners
                                outputter = ret.get('outputter', 'nested')
                                if isinstance(ret['data'], dict) and 'return' in ret['data']:
                                    ret = ret['data']['return']
                                salt.output.display_output(
                                    ret,
                                    self.cli.options.output or outputter,
                                    opts=self.opts
                                )
                            else:
                                salt.output.display_output(
                                    {self.cli.options.client: ret},
                                    self.cli.options.output or ret.get('outputter', 'nested'),
                                    opts=self.opts
                                )
                        else:
                            salt.output.display_output(
                                {self.cli.options.client: ret},
                                self.cli.options.output or 'nested',
                                opts=self.opts,
                            )
                else:
                    if self.cli.options.output_file is not None:
                        with open(self.cli.options.output_file, 'a') as ofile:
                            print(result, file=ofile)
                    else:
                        print(result)
                if exit_code is not None:
                    if exit_code == 0:
                        return PepperRetcode().validate(self.cli.options, result)
                    return exit_code
        except (PepperException, PepperAuthException, PepperArgumentsException) as exc:
            print('Pepper error: {0}'.format(exc), file=sys.stderr)
            return 1
        except KeyboardInterrupt:
            # TODO: mimic CLI and output JID on ctrl-c
            return 0
        except Exception as e:
            print(e)
            print('Uncaught Pepper error (increase verbosity for the full traceback).', file=sys.stderr)
            logger.debug('Uncaught traceback:', exc_info=True)
            return 1
