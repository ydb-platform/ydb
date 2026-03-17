# Copyright 2018 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Console output utilities for OpenHTF.

This module provides convenience methods to format output for the CLI, along
with the ability to fork output to both a logger (e.g. a test record logger)
and to the CLI directly (e.g. sys.stdout).

Under the default configuration, messages printed with these utilities will be
saved to the test record logs of all running tests. To change this behavior,
the `logger` parameter should be overridden, e.g. by passing in the test record
logger for the current test.
"""

import logging
import math
import os
import re
import string
import sys
import textwrap
import time

import colorama
import contextlib2 as contextlib

from openhtf.util import argv

# Colorama module has to be initialized before use.
colorama.init()

_LOG = logging.getLogger(__name__)

# If True, all CLI output through this module will be suppressed, as well as any
# logging that uses a CliQuietFilter.
CLI_QUIET = False

ARG_PARSER = argv.ModuleParser()
ARG_PARSER.add_argument(
    '--quiet', action=argv.StoreTrueInModule, target='%s.CLI_QUIET' % __name__,
    help=textwrap.dedent('''\
        Suppress all CLI output from OpenHTF's printing functions and logging.
        This flag will override any verbosity levels set with -v.'''))

ANSI_ESC_RE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


class ActionFailedError(Exception):
  """Indicates an action failed. Used internally by action_result_context()."""


def _printed_len(some_string):
  """Compute the visible length of the string when printed."""
  return len([x for  x in ANSI_ESC_RE.sub('', some_string)
              if x in string.printable])


def _linesep_for_file(file):
  """Determine which line separator to use based on the file's mode."""
  if 'b' in file.mode:
    return os.linesep
  return '\n'


def banner_print(msg, color='', width=60, file=sys.stdout, logger=_LOG):
  """Print the message as a banner with a fixed width.

  Also logs the message (un-bannered) to the given logger at the debug level.

  Args:
    msg: The message to print.
    color: Optional colorama color string to be applied to the message. You can
        concatenate colorama color strings together in order to get any set of
        effects you want.
    width: Total width for the resulting banner.
    file: A file object to which the banner text will be written. Intended for
        use with CLI output file objects like sys.stdout.
    logger: A logger to use, or None to disable logging.

  Example:

    >>> banner_print('Foo Bar Baz')

    ======================== Foo Bar Baz =======================

  """
  if logger:
    logger.debug(ANSI_ESC_RE.sub('', msg))
  if CLI_QUIET:
    return
  lpad = int(math.ceil((width - _printed_len(msg) - 2) / 2.0)) * '='
  rpad = int(math.floor((width - _printed_len(msg) - 2) / 2.0)) * '='
  file.write('{sep}{color}{lpad} {msg} {rpad}{reset}{sep}{sep}'.format(
      sep=_linesep_for_file(file), color=color, lpad=lpad, msg=msg, rpad=rpad,
      reset=colorama.Style.RESET_ALL))
  file.flush()


def bracket_print(msg, color='', width=8, file=sys.stdout):
  """Prints the message in brackets in the specified color and end the line.

  Args:
    msg: The message to put inside the brackets (a brief status message).
    color: Optional colorama color string to be applied to the message. You can
        concatenate colorama color strings together in order to get any set of
        effects you want.
    width: Total desired width of the bracketed message.
    file: A file object to which the bracketed text will be written. Intended
        for use with CLI output file objects like sys.stdout.
    """
  if CLI_QUIET:
    return
  lpad = int(math.ceil((width - 2 - _printed_len(msg)) / 2.0)) * ' '
  rpad = int(math.floor((width - 2 - _printed_len(msg)) / 2.0)) * ' '
  file.write('[{lpad}{bright}{color}{msg}{reset}{rpad}]'.format(
      lpad=lpad, bright=colorama.Style.BRIGHT, color=color, msg=msg,
      reset=colorama.Style.RESET_ALL, rpad=rpad))
  file.write(colorama.Style.RESET_ALL)
  file.write(_linesep_for_file(file))
  file.flush()


def cli_print(msg, color='', end=None, file=sys.stdout, logger=_LOG):
  """Print the message to file and also log it.

  This function is intended as a 'tee' mechanism to enable the CLI interface as
  a first-class citizen, while ensuring that everything the operator sees also
  has an analogous logging entry in the test record for later inspection.

  Args:
    msg: The message to print/log.
    color: Optional colorama color string to be applied to the message. You can
        concatenate colorama color strings together in order to get any set of
        effects you want.
    end: A custom line-ending string to print instead of newline.
    file: A file object to which the baracketed text will be written. Intended
        for use with CLI output file objects like sys.stdout.
    logger: A logger to use, or None to disable logging.
  """
  if logger:
    logger.debug('-> {}'.format(msg))
  if CLI_QUIET:
    return
  if end is None:
    end = _linesep_for_file(file)
  file.write('{color}{msg}{reset}{end}'.format(
      color=color, msg=msg, reset=colorama.Style.RESET_ALL, end=end))


def error_print(msg, color=colorama.Fore.RED, file=sys.stderr):
  """Print the error message to the file in the specified color.

  Args:
    msg: The error message to be printed.
    color: Optional colorama color string to be applied to the message. You can
        concatenate colorama color strings together here, but note that style
        strings will not be applied.
    file: A file object to which the baracketed text will be written. Intended
        for use with CLI output file objects, specifically sys.stderr.
  """
  if CLI_QUIET:
    return
  file.write('{sep}{bright}{color}Error: {normal}{msg}{sep}{reset}'.format(
      sep=_linesep_for_file(file), bright=colorama.Style.BRIGHT, color=color,
      normal=colorama.Style.NORMAL, msg=msg, reset=colorama.Style.RESET_ALL))
  file.flush()


class ActionResult(object):
  """Used with an action_result_context to signal the result of an action."""

  def __init__(self):
    self.success = None

  def succeed(self):
    """Mark the action as having succeeded."""
    self.success = True

  def fail(self):
    """Mark the action as having failed.

    Raises:
      ActionFailedError: This exception is always raised, by this function,
          but should be caught by the contextmanager.
    """
    self.success = False
    raise ActionFailedError()


@contextlib.contextmanager
def action_result_context(action_text,
                          width=60,
                          status_width=8,
                          succeed_text='OK',
                          fail_text='FAIL',
                          unknown_text='????',
                          file=sys.stdout,
                          logger=_LOG):
  """A contextmanager that prints actions and results to the CLI.

  When entering the context, the action will be printed, and when the context
  is exited, the result will be printed. The object yielded by the context is
  used to mark the action as a success or failure, and a raise from inside the
  context will also result in the action being marked fail. If the result is
  left unset, then indicative text ("????") will be printed as the result.

  Args:
    action_text: Text to be displayed that describes the action being taken.
    width: Total width for each line of output.
    status_width: Width of the just the status message portion of each line.
    succeed_text: Status message displayed when the action succeeds.
    fail_text: Status message displayed when the action fails.
    unknown_text: Status message displayed when the result is left unset.
    file: Specific file object to write to write CLI output to.
    logger: A logger to use, or None to disable logging.

  Example usage:
    with action_result_context('Doing an action that will succeed...') as act:
      time.sleep(2)
      act.succeed()

    with action_result_context('Doing an action with unset result...') as act:
      time.sleep(2)

    with action_result_context('Doing an action that will fail...') as act:
      time.sleep(2)
      act.fail()

    with action_result_context('Doing an action that will raise...') as act:
      time.sleep(2)
      import textwrap
      raise RuntimeError(textwrap.dedent('''\
          Uh oh, looks like there was a raise in the mix.

          If you see this message, it means you are running the console_output
          module directly rather than using it as a library. Things to try:

            * Not running it as a module.
            * Running it as a module and enjoying the preview text.
            * Getting another coffee.'''))

  Example output:
    Doing an action that will succeed...                [  OK  ]
    Doing an action with unset result...                [ ???? ]
    Doing an action that will fail...                   [ FAIL ]
    Doing an action that will raise...                  [ FAIL ]
    ...
  """
  if logger:
    logger.debug('Action - %s', action_text)
  if not CLI_QUIET:
    file.write(''.join((action_text, '\r')))
    file.flush()
    spacing = (width - status_width - _printed_len(action_text)) * ' '

  result = ActionResult()
  try:
    yield result
  except Exception as err:
    if logger:
      logger.debug('Result - %s [ %s ]', action_text, fail_text)
    if not CLI_QUIET:
      file.write(''.join((action_text, spacing)))
      bracket_print(fail_text, width=status_width, color=colorama.Fore.RED,
                    file=file)
    if not isinstance(err, ActionFailedError):
      raise
    return

  result_text = succeed_text if result.success else unknown_text
  result_color = colorama.Fore.GREEN if result.success else colorama.Fore.YELLOW
  if logger:
    logger.debug('Result - %s [ %s ]', action_text, result_text)
  if not CLI_QUIET:
    file.write(''.join((action_text, spacing)))
    bracket_print(result_text, width=status_width, color=result_color,
                  file=file)


# If invoked as a runnable module, this module will invoke its action result
# context in order to print colorized example output.
if __name__ == '__main__':
  banner_print('Running pre-flight checks.')

  with action_result_context('Doing an action that will succeed...') as act:
    time.sleep(2)
    act.succeed()

  with action_result_context('Doing an action with unset result...') as act:
    time.sleep(2)

  with action_result_context('Doing an action that will fail...') as act:
    time.sleep(2)
    act.fail()

  with action_result_context('Doing an action that will raise...') as act:
    time.sleep(2)
    raise RuntimeError(textwrap.dedent('''\
        Uh oh, looks like there was a raise in the mix.

        If you see this message, it means you are running the console_output
        module directly rather than using it as a library. Things to try:

          * Not running it as a module.
          * Running it as a module and enjoying the preview text.
          * Getting another coffee.'''))


class CliQuietFilter(logging.Filter):
  """Filter that suppresses logging output if the --quiet CLI option is set.

  Note that the --quiet CLI option is stored in the CLI_QUIET member of this
  module, and can thus be overridden in test scripts. This filter should only
  be used with loggers that print to the CLI.
  """
  def filter(self, record):
    return not CLI_QUIET
