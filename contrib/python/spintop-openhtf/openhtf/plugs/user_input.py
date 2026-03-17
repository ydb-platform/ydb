# Copyright 2015 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""User input module for OpenHTF.

Provides a plug which can be used to prompt the user for input. The prompt can
be displayed in the console, the OpenHTF web GUI, and custom frontends.
"""

from __future__ import print_function

import collections
import functools
import logging
import os
import platform
import select
import sys
import threading
import uuid
import time
import enum

from openhtf import PhaseOptions
from openhtf import plugs
from openhtf.util import console_output, conf
from six.moves import input

if platform.system() != 'Windows':
  import termios  # pylint: disable=g-import-not-at-top

_LOG = logging.getLogger(__name__)

PROMPT = '--> '

PromptType = enum.Enum('PromptType', [
  'OKAY',
  'FORM',
  'OKAY_CANCEL',
  'PASS_FAIL',
  'PASS_FAIL_RETRY'
])

conf.declare(
  'user_input_enable_console', 
  'If True, enables user input collection in console prompt.', 
  default_value=True
)

class InvalidOption(Exception):
  def __init__(self, option):
    self.option = option
    super(InvalidOption, self).__init__('Option %s is not valid' % self.option)

class SecondaryOptionOccured(Exception):
  pass

class CancelOption(SecondaryOptionOccured):
  pass

class FailOption(SecondaryOptionOccured):
  pass

class RetryOption(SecondaryOptionOccured):
  pass

class PromptOption(object):
  def __init__(self, key, error=None, name=None, error_level=False, warning_level=False, success_level=False):
    self.key = key
    self.error = error
    
    if (error and not warning_level) or error_level:
      self.level = 'error'
    elif warning_level:
      self.level = 'warning'
    elif success_level:
      self.level = 'success'
    else:
      self.level = 'info'
      
    if name is None:
      name = key
    self.name = name
    
  def as_dict(self):
    return {'key': self.key, 'name': self.name, 'level': self.level}
    
  def raise_if_error(self):
    if self.error:
      raise self.error
  
class PromptResponse(object):
  def __init__(self, response, prompt_form):
    self.prompt_form = prompt_form
    if isinstance(response, collections.abc.Mapping):
      self.content = response.get('content', {})
      self.option = response.get('option', None)
    # elif prompt_type != PromptType.FORM:
    #   self.option = response
    else:
      self.content = str(response)
      self.option = None

    
    if not self.option:
      self.option = None
    elif isinstance(self.option, PromptOption):
      self.option = self.option.key
    
    if self.option not in OPTIONS:
      raise InvalidOption(self.option)
    
    self.option = OPTIONS[self.option]
  
  @property
  def schema(self):
    message = getattr(self.prompt_form, 'message', None)
    try:
      return message.get('schema', {})
    except AttributeError:
      return {}
  
  @property
  def required_fields(self):
    try:
      return self.schema['required']
    except TypeError:
      # Not a dict probably
      return []
    except KeyError:
      # Not required fields possibly
      return []

  def check_required_fields(self):
    if isinstance(self.content, collections.abc.Mapping):
      return [f for f in self.required_fields if f not in self.content]
    else:
      return self.required_fields # Return True only if not required fields.

  def fill_in_defaults(self):
    # First copy the returned values
    try:
      content_with_defaults = {key: value for key, value in self.content.items()}
    except AttributeError:
      # self.content has not attribute 'items'
      # No need to fill in defaults.
      return self.content
    
    schema_properties = self.schema.get('properties', {})

    for key, value in schema_properties.items():
      # As defined in schema
      if key not in content_with_defaults:
        default_value = value.get('default', None)
        content_with_defaults[key] = default_value

    return content_with_defaults

  def return_value(self):
    self.option.raise_if_error()
    return self.fill_in_defaults()

OKAY = PromptOption('OKAY')

OPTIONS = {
  None: OKAY,
  'OKAY': OKAY,
  'SUBMIT': PromptOption('SUBMIT', name='Submit'),
  'CANCEL': PromptOption('CANCEL', CancelOption, warning_level=True),
  'PASS': PromptOption('PASS', success_level=True),
  'FAIL': PromptOption('FAIL', FailOption),
  'RETRY': PromptOption('RETRY', RetryOption, warning_level=True)
}

PROMPT_TO_OPTIONS = {
  PromptType.OKAY: [OPTIONS['OKAY']],
  PromptType.FORM: [OPTIONS['SUBMIT']],
  PromptType.OKAY_CANCEL: [OPTIONS['OKAY'], OPTIONS['CANCEL']],
  PromptType.PASS_FAIL: [OPTIONS['PASS'], OPTIONS['FAIL']]
}

def create_text_input_form(input_message='Input'):
  return {
      'schema': {
          'title': "Generic Input",
          'type': "object",
          'required': ["_input"],
          'properties': {
              '_input': {
                  'type': "string", 
                  'title': input_message
              }
          }
      }
  }

class PromptInputError(Exception):
  """Raised in the event that a prompt returns without setting the response."""


class MultiplePromptsError(Exception):
  """Raised if a prompt is invoked while there is an existing prompt."""


class PromptUnansweredError(Exception):
  """Raised when a prompt times out or otherwise comes back unanswered."""


Prompt = collections.namedtuple('Prompt', 'id message prompt_type')

CONSOLE_PROMPT = None

class ConsolePrompt(threading.Thread):
  """Thread that displays a prompt to the console and waits for a response.

  This should not be used for processes that run in the background.
  """

  def __init__(self, message, callback, color=''):
    """Initializes a ConsolePrompt.

    Args:
      message: A string to be presented to the user.
      callback: A function to be called with the response string.
      color: An ANSI color code, or the empty string.
    """
    super(ConsolePrompt, self).__init__()
    self.daemon = True
    self._message = str(message)
    self._callback = callback
    self._color = color
    self._stop_event = threading.Event()
    self._answered = False

  def Stop(self):
    """Mark this ConsolePrompt as stopped."""
    self._stop_event.set()
    if not self._answered:
      _LOG.debug('Stopping ConsolePrompt--prompt was answered from elsewhere.')

  def run(self):
    """Main logic for this thread to execute."""
    if platform.system() == 'Windows':
      # Windows doesn't support file-like objects for select(), so fall back
      # to raw_input().
      response = input(''.join((self._message,
                                os.linesep,
                                PROMPT)))
      self._answered = True
      self._callback(response)
      return

    # First, display the prompt to the console.
    console_output.cli_print(self._message, color=self._color,
                             end=os.linesep, logger=None)
    console_output.cli_print(PROMPT, color=self._color, end='', logger=None)
    sys.stdout.flush()

    # Before reading, clear any lingering buffered terminal input.
    termios.tcflush(sys.stdin, termios.TCIFLUSH)

    line = ''
    while not self._stop_event.is_set():
      inputs, _, _ = select.select([sys.stdin], [], [], 0.001)
      if sys.stdin in inputs:
        new = os.read(sys.stdin.fileno(), 1024)
        if not new:
          # Hit EOF!
          # They hit ^D (to insert EOF). Tell them to hit ^C if they
          # want to actually quit.
          print('Hit ^C (Ctrl+c) to exit.')
          break
        line += new.decode('utf-8')
        if '\n' in line:
          response = line[:line.find('\n')]
          self._answered = True
          self._callback(response)
          return


def prompt_type_console_message(options=[]):
  if len(options) <= 1:
    return "Press enter to continue."
  else:
    return "Select one of the following option: " + " or ".join(opt.key for opt in options)
  
def prompt_options(prompt_type):
  if prompt_type in PROMPT_TO_OPTIONS:
    return PROMPT_TO_OPTIONS[prompt_type]
  else:
    raise RuntimeError('PromptType %s does not exists.' % prompt_type)
 
class UserInput(plugs.FrontendAwareBasePlug):
  """Get user input from inside test phases.

  Attributes:
    last_response: None, or a pair of (prompt_id, response) indicating the last
        user response that was received by the plug.
  """

  def __init__(self):
    super(UserInput, self).__init__()
    self.last_response = None
    self._prompt = None
    self._response = None
    self._cond = threading.Condition(threading.RLock())

  @property
  def _console_prompt(self):
    """ Console prompt is global because it uses the only available once stdin to capture input.
    If not global, a new test with create a new plug and any old console prompt will be left hanging.""" 
    return CONSOLE_PROMPT

  @_console_prompt.setter
  def _console_prompt(self, value):
    global CONSOLE_PROMPT
    CONSOLE_PROMPT = value

  def _asdict(self):
    """Return a dictionary representation of the current prompt."""
    with self._cond:
      if self._prompt is None:
        return
      return {'id': self._prompt.id,
              'message': self._prompt.message,
              'prompt_type': self._prompt.prompt_type,
              'options': [opt.as_dict() for opt in self._options]}

  def tearDown(self):
    self.remove_prompt()

  def remove_prompt(self):
    """Remove the prompt."""
    with self._cond:
      self._prompt = None
      if self._console_prompt:
        self._console_prompt.Stop()
      self.notify_update()

  def prompt_form(self, json_schema_form, prompt_type=PromptType.OKAY, timeout_s=None):
    message = json_schema_form
    return self.prompt(message, prompt_type=prompt_type, timeout_s=timeout_s)
    
  def prompt(self, message, prompt_type=PromptType.OKAY, text_input=False, cli_color='', timeout_s=None):
    """Display a prompt and wait for a response.

    Args:
      message: A string to be presented to the user.
      prompt_type: The type of prompt:
        OKAY: Simple info prompt with an okay button.
        OKAY_CANCEL: Okay or Cancel buttons.
        PASS_FAIL: PASS or FAIL buttons.
        PASS_FAIL_RETRY: PASS, FAIL, RETRY buttons. (not working)
        
      timeout_s: Seconds to wait before raising a PromptUnansweredError.
      cli_color: An ANSI color code, or the empty string.

    Returns:
      A string response, or the empty string if text_input was False.

    Raises:
      MultiplePromptsError: There was already an existing prompt.
      PromptUnansweredError: Timed out waiting for the user to respond.
    """
    if text_input:
      ret_val = self.prompt_form(create_text_input_form(message))
      try:
        return ret_val['_input']
      except (TypeError, KeyError):
        return ret_val
    
    self.start_prompt(message, prompt_type, cli_color)
    try:
      return self.wait_for_prompt(timeout_s)
    except InvalidOption as e:
      _LOG.warning('Option %s is invalid. Please retry.' % e.option)
      return self.prompt(message, prompt_type, cli_color, timeout_s)

  def start_prompt(self, message, prompt_type=PromptType.OKAY, cli_color=''):
    """Display a prompt.

    Args:
      message: A string to be presented to the user.
      prompt_type: The type of prompt:
        OKAY: Simple info prompt with an okay button.
        OKAY_CANCEL: Okay or Cancel buttons.
        PASS_FAIL: PASS or FAIL buttons.
        PASS_FAIL_RETRY: PASS, FAIL, RETRY buttons. (not working)
      cli_color: An ANSI color code, or the empty string.

    Raises:
      MultiplePromptsError: There was already an existing prompt.

    Returns:
      A string uniquely identifying the prompt.
    """
    with self._cond:
      if self._prompt:
        raise MultiplePromptsError
      
      prompt_id = uuid.uuid4().hex
      _LOG.debug('Displaying prompt (%s): "%s"', prompt_id, message)

      self._response = None
      self._prompt = Prompt(
          id=prompt_id, message=message, prompt_type=prompt_type)
      self._options = prompt_options(prompt_type)
      self._prompt_type = prompt_type
      if conf['user_input_enable_console'] and sys.stdin.isatty():
        console_message = str(message) + ', ' + prompt_type_console_message(options=self._options)
        
        if self._console_prompt and self._console_prompt.is_alive():
          # Console prompt still active; replacing callback with new prompt.
          # Prolly on Windows since the prompt stop function does not interrupt input()
          self._console_prompt._callback = functools.partial(self.respond, prompt_id)
        else:
          self._console_prompt = ConsolePrompt(
              console_message, functools.partial(self.respond, prompt_id), cli_color)
          self._console_prompt.start()

      self.notify_update()
      return prompt_id

  def wait_for_prompt(self, timeout_s=None):
    """Wait for the user to respond to the current prompt.

    Args:
      timeout_s: Seconds to wait before raising a PromptUnansweredError.

    Returns:
      A string response, or the empty string if text_input was False.

    Raises:
      PromptUnansweredError: Timed out waiting for the user to respond.
    """
    start_time = time.time()

    with self._cond:
      # Waiting in short increments allows sys interrupts to be captured.
      # This allows Ctrl-C to be used while waiting for a prompt.
      while self._prompt and not self._cond.wait(0.5):
        # If no timeout, we just wait continuously
        if timeout_s is not None and time.time() - start_time > timeout_s:
          break

    if self._response is None:
      raise PromptUnansweredError
    
    response = self._response
    return response.return_value()

  def respond(self, prompt_id, response):
    """Respond to the prompt with the given ID.

    If there is no active prompt or the given ID doesn't match the active
    prompt, do nothing.

    Args:
      prompt_id: A string uniquely identifying the prompt.
      response: A string response to the given prompt.

    Returns:
      True if the prompt with the given ID was active, otherwise False.
    """
    _LOG.debug('Responding to prompt (%s): "%s"', prompt_id, response)
    with self._cond:
      if not (self._prompt and self._prompt.id == prompt_id):
        return False

      raw_response = response
      response = PromptResponse(raw_response, self._prompt)

      missing_required_fields = response.check_required_fields()
      if missing_required_fields:
        _LOG.warning(f'Prompt response missing required fields: {missing_required_fields!r}')
        return False
      
      self._response = response
      self.last_response = (prompt_id, raw_response)
      self.remove_prompt()
      self._cond.notifyAll()
    return True

def prompt_for_test_start(
    message='Enter a DUT ID in order to start the test.', timeout_s=60*60*24,
    validator=lambda sn: sn, cli_color=''):
  """Returns an OpenHTF phase for use as a prompt-based start trigger.

  Args:
    message: The message to display to the user.
    timeout_s: Seconds to wait before raising a PromptUnansweredError.
    validator: Function used to validate or modify the serial number.
    cli_color: An ANSI color code, or the empty string.
  """

  @PhaseOptions(timeout_s=timeout_s)
  @plugs.plug(prompts=UserInput)
  def trigger_phase(test, prompts):
    """Test start trigger that prompts the user for a DUT ID."""
    dut_id = prompts.prompt(
        message, text_input=True, timeout_s=timeout_s, cli_color=cli_color)
    test.test_record.dut_id = validator(dut_id)

  return trigger_phase
