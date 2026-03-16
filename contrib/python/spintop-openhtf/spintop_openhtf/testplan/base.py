import os
import sys
import inspect
import datetime
import warnings

from typing import Callable, Union

from copy import copy
from contextlib import contextmanager

import pkg_resources

import openhtf as htf

from openhtf.util import conf
from openhtf.plugs import user_input, BasePlug

import webbrowser

from ..storage import SITE_DATA_DIR
from ..callbacks.file_provider import TemporaryFileProvider
from ..callbacks.local_storage import LocalStorageOutput

try:
    import tornado, sockjs
    tornado_version = pkg_resources.get_distribution("tornado").version
    major, *rest = tornado_version.split('.')
    if int(major) > 4:
        raise ValueError('Tornado version must be <= 4.x.x')
except (ImportError, ValueError):
    warnings.warn(
        'Tornado 4.x.x not available. GUI server will not work. '
        'If you wish to install it, you may install spintop-openhtf '
        'with the [server] dependency, such as '
        'pip install spintop-openhtf[server]. Do note that this requires '
        'tornado<5.0.'
    )
else:
    from ..callbacks import station_server

from .. import (
    Test,
    # load_component_file,
    # CoverageAnalysis
)

HISTORY_BASE_PATH = os.path.join(SITE_DATA_DIR, 'openhtf-history')

DEFAULT = object()

class TestPlanError(Exception): pass

class TestSequence(object):
    """The base sequence object: defines a sequence of setup, test and teardown openhtf phases.
    
    TestSequences can be nested together in two different ways:
    
    Option 1. 
        If the sequence should be re-usable, create an explicit :class:`TestSequence` object 
        and add :meth:`~TestSequence.setup`, :meth:`~TestSequence.testcase` and :meth:`~TestSequence.teardown`
        phases using the decorator methods. Afterwards, add it to a specific TestPlan (or sub TestSequence)
        using :meth:`~TestSequence.append`:

        .. highlight:: python
        .. code-block:: python

            from spintop_openhtf import TestSequence, TestPlan

            my_sequence = TestSequence('my-sequence')

            @my_sequence.testcase('Test1')
            def do_something(test):
                pass

            # Elsewhere probably

            plan = TestPlan('my-plan')
            plan.append(my_sequence)

    Option 2.
        If the sequence is defined simply to nest phases inside the same test plan, the
        :meth:`~TestSequence.sub_sequence` simplifies the declaration and usage of a sub
        sequence. This code block is equivalent to the previous one, but does not allow
        re-usage of the sub_sequence object:
        
        .. highlight:: python
        .. code-block:: python

            from spintop_openhtf import TestPlan

            plan = TestPlan('my-plan')

            my_sequence = plan.sub_sequence('my-sequence')

            @my_sequence.testcase('Test1')
            def do_something(test):
                pass

    Since TestPlans are TestSequences, and sub sequences are also TestSequences,
    they can be infinitely nested using :meth:`~TestSequence.append` and :meth:`~TestSequence.sub_sequence`.

    Args:
        name: The name of this test node.
    """
    def __init__(self, name):
        self._setup_phases = []
        self._test_phases = []
        self._teardown_phases = []
        self.name = name
    
    def setup(self, name, **options):
        """Decorator factory for a setup function.
        
        A setup function will executed if the sequence is entered. 
        All setup functions are executed before the testcases, regardless of the order
        of declaration.

        See :meth:`~TestSequence.testcase` for usage.
        """
        return self._decorate_phase(name, self._setup_phases, options)
    
    def testcase(self, name, **options):
        """Decorator factory for a normal phase. 
        
        A testcase function is a normal openhtf phase.
        
        The options parameter is a proxy for the PhaseOptions arguments.
                
        .. highlight:: python
        .. code-block:: python
        
            my_sequence = TestSequence('Parent')
        
            @my_sequence.testcase('my-testcase-name')
            def setup_fn(test):
                (...)
        
        {phase_options_doc}
        """
        return self._decorate_phase(name, self._test_phases, options)
    
    # Add PhaseOptions documentation
    testcase.__doc__ = testcase.__doc__.format(phase_options_doc=htf.PhaseOptions.__doc__)
    
    def teardown(self, name, **options):
        """Decorator factory for a teardown phase. 

        A teardown function will always be executed if the sequence is entered, regardless
        of the outcome of normal or setup phases.

        See :meth:`~TestSequence.testcase` for usage.
        """
        return self._decorate_phase(name, self._teardown_phases, options)
    
    def plug(self, *args, **kwargs):
        """Helper method: shortcut to :func:`openhtf.plug`"""
        return htf.plugs.plug(*args, **kwargs)
    
    def measures(self, *args, **kwargs):
        """Helper method: shortcut to :func:`openhtf.measures`"""
        return htf.measures(*args, **kwargs)
    
    def sub_sequence(self, name):
        """Create new empty TestSequence and append it to this sequence.
        
        The following two snippets are equivalent:
        
        .. highlight:: python
        .. code-block:: python
        
            my_sequence = TestSequence('Parent')
            sub_sequence = my_sequence.sub_sequence('Child')
        
        .. highlight:: python
        .. code-block:: python
        
            my_sequence = TestSequence('Parent')
            sub_sequence = TestSequence('Child')
            my_sequence.append(sub_sequence)
            
        """
        group = TestSequence(name)
        self.append(group)
        return group
    
    def append(self, *phases):
        """ Append normal phases (or sequences) to this test plan.

        Args:
            phases: The phases to append.
        """
        for phase in phases:
            self._test_phases.append(phase)
        
    def _decorate_phase(self, name, array, options=None):
        if not options:
            options = {}
        options['name'] = name
        def _note_fn(fn):
            phase = self._add_phase(fn, array, options)
            return phase
        return _note_fn
    
    def _add_phase(self, fn, array, options):
        phase = ensure_htf_phase(fn)
        phase.options.update(**options)
        array.append(phase)
        return phase
        
    @property
    def phase_group(self):
        # Recursively get phase groups of sub phases if available, else the phase itself.
        _test_phases = [getattr(phase, 'phase_group', phase) for phase in self._test_phases]
        
        return htf.PhaseGroup(
            setup=self._setup_phases,
            main=_test_phases,
            teardown=self._teardown_phases,
            name=self.name
        )
    
class TestPlan(TestSequence):
    """ The core spintop-openhtf interface to create an openhtf sequence.

    :class:`TestPlan` simplifies the creating of openhtf sequences. Instead of declaring
    functions and adding them as an array to openhtf, the test plan allows 
    declarative addition of function using decorators.

    The TestPlan is itself a :class:`TestSequence` and therefore implements all methods
    defined there.

    In OpenHTF, you would do the following:

    .. highlight:: python
    .. code-block:: python

        import openhtf as htf

        @htf.plug(my_plug=...)
        def example_test(test, my_plug):
            time.sleep(.2)

        @htf.plug(my_plug=...)
        def example_test2(test, my_plug):
            time.sleep(.2)

        test = htf.Test(example_test, example_test2)
        test.run() # Run once only

    With the TestPlan, you can do the following (equivalent):
    
    .. highlight:: python
    .. code-block:: python

        from spintop_openhtf import TestPlan

        plan = TestPlan('my-test-name')

        @plan.testcase('Test1')
        @plan.plug(my_plug=...)
        def example_test(test, my_plug):
            time.sleep(.2)

        @plan.testcase('Test2')
        @plan.plug(my_plug=...)
        def example_test2(test, my_plug):
            time.sleep(.2)

        plan.run_once()

    Args:
        name: The name of the test plan. Used to identify the 'type' of the test.
        store_result: Whether results should automatically be stored as JSON in the
            spintop_openhtf site directory.
        store_location: When store_result is True, where test results will be stored.
            This can either be a callable function that receives the test record attributes
            as kwargs arguments (e.g. fn(**test_record)) or a string that will be formatted with the
            test record attributes (e.g. '{metadata[test_name]}/{outcome}').
            This uses LocalStorageOutput, which instead of only writing the result as a JSON file
            as OpenHTF does, writes that json file as result.json in the folder and writes all
            test-attached files next to it.
    """
    DEFAULT_CONF = dict(
        station_server_port='4444', 
        capture_docstring=True
    )

    def __init__(self, name:str='testplan', store_result:bool=True, store_location:Union[str,Callable]=None):
        super(TestPlan, self).__init__(name=name)
        
        self._execute_test = None
        self._top_level_component = None
        self.coverage = None
        self.file_provider = TemporaryFileProvider()
        self.callbacks = []
        
        # Array but must contain only one phase.
        # Array is for compatibility with self._decorate_phase function of parent class.
        self._trigger_phases = []
        self._no_trigger = False
        
        if store_result:
            if store_location is None:
                store_location = _local_storage_filename_pattern
            self.add_callbacks(LocalStorageOutput(store_location, indent=4))
            
        self.failure_exceptions = (user_input.SecondaryOptionOccured,)

    @property
    def execute_test(self) -> Callable:
        """Returns a function that takes no arguments and that executes the test described by this test plan."""
        if not self._execute_test:
            self.freeze_test()
        return self._execute_test
    
    @property
    def history_path(self) -> str:
        """The base path where results of the tests defined by this object are stored."""
        path = os.path.join(HISTORY_BASE_PATH, self.name)
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    
    @property
    def is_runnable(self) -> bool:
        """Whether this test plan contains any runnable phases."""
        phases = self._test_phases + self._trigger_phases
        return bool(phases)
    
    @property
    def trigger_phase(self) -> Union[Callable, None]:
        """Returns the defined """
        return self._trigger_phases[0] if self._trigger_phases else None
        
    @property
    def is_test_frozen(self):
        return bool(self._execute_test)
        
    def image_url(self, url:str) -> str:
        """Creates a temporary hosted image based on the specified file path.
        
        Args:
            url: The image path.
        
        Returns:
            The temporary url associated with this image. Can be used in custom forms to show images.
        """
        return self.file_provider.create_url(url)
    
    def trigger(self, name:str, **options):
        """Decorator factory for the trigger phase. 
        
        Similar to :meth:`~TestPlan.testcase`, except that this function will be used as the test trigger.
        
        The test trigger is a special test phase that is executed before test officialy start.
        Once this phase is complete, the test will start. Usually used to configure the test with
        the DUT id for example."""
        if self.trigger_phase:
            raise TestPlanError('There can only be one @trigger function.')
        
        return self._decorate_phase(name, self._trigger_phases, options)

    def no_trigger(self):
        """Removes the need for a trigger and removes the default trigger.
        
        .. highlight:: python
        .. code-block:: python

            plan = TestPlan('my-plan')
            plan.no_trigger()

            # ... 
            
            plan.run() # Will start immediately.

        """
        self._no_trigger = True
    
    def add_callbacks(self, *callbacks):
        """Add custom callbacks to the underlying openhtf test.
        
        Args:
            callbacks:
                The callbacks to add.
        """
        if self.is_test_frozen:
            raise RuntimeError('Cannot add callbacks to the test plan after the test was frozen.')
        self.callbacks += callbacks
    
    def assert_runnable(self):
        if not self.is_runnable:
            # No phases ! Abort now.
            raise RuntimeError('Test is empty, aborting.')
    
    def execute(self):
        """ Execute the configured test using the test_start function as a trigger.
        """
        return self.execute_test()
    
    def run_once(self, launch_browser=True):
        """Shortcut for :meth:`~TestPlan.run` with once=True."""
        return self.run(launch_browser=launch_browser, once=True)
    
    def run(self, launch_browser=True, once=False):
        """Run this test with the OpenHTF frontend. 

        Requires the [server] extra to work. If you do not need the server, you should use
        :meth:`~TestPlan.run_console` which disables completely the GUI server and increases
        execution speed therefore.
        
        Args:
            launch_browser: When True, a browser page will open automatically when this is called.
            once: When False, the test will run in a loop; i.e. when a test ends a new one will start immediately.
        
        """
        conf.load(user_input_enable_console=False, _override=False)
        with self._station_server_context(launch_browser):
            return self._run(once)
    
    def run_console(self, once=False):
        """Run this test without a frontend server and enables console input. 

        To enable the server, use :meth:`~TestPlan.run` instead. 

        Args:
            once: When False, the test will run in a loop; i.e. when a test ends a new one will start immediately.
        """
        return self._run(once)

    def _run(self, once):
        result = None
        while True:
            try:
                result = self.execute()
            except KeyboardInterrupt:
                break
            finally:
                if once:
                    break
        
        return result
    
    def freeze_test(self):
        self._load_default_conf()
        self._execute_test = self._create_execute_test()
        
    @contextmanager
    def _station_server_context(self, launch_browser=True):
        self._load_default_conf() # preload conf so that the server port conf values is populated.
        with station_server.StationServer(self.file_provider) as server:
            self.add_callbacks(server.publish_final_state)
            self.assert_runnable() # Check before launching browser. self.execute also checks when freezing the test.
            
            if launch_browser and conf['station_server_port']:
                webbrowser.open('http://localhost:%s' % conf['station_server_port'])
            
            yield
    
    def _load_default_conf(self):
        conf.load_from_dict(self.DEFAULT_CONF, _override=False)

    def _create_execute_test(self):
        self.assert_runnable()
        
        test = Test(self.phase_group, test_name=self.name, _code_info_after_file=__file__)
        test.configure(failure_exceptions=self.failure_exceptions)
        test.add_output_callbacks(*self.callbacks)
        
        
        trigger_phase = self._build_trigger_phase()
        
        def execute_test():
            return test.execute(test_start=trigger_phase)

        return execute_test
        
    def _build_trigger_phase(self):
        if self.trigger_phase:
            return self.trigger_phase
        elif self._no_trigger:
            return None
        else:
            return create_default_trigger()
    
    def enable_spintop(self, spintop=None):
        from ..callbacks.spintop_cb import spintop_callback_factory
        callback = spintop_callback_factory(spintop)
        self.add_callbacks(callback)

def _local_storage_filename_pattern(**test_record):
    folder = '{metadata[test_name]}'.format(**test_record)
    start_time_datetime = datetime.datetime.utcfromtimestamp(test_record['start_time_millis']/1000.0)
    start_time = start_time_datetime.strftime(r"%Y_%m_%d_%H%M%S_%f")
    subfolder = '{dut_id}_{start_time}_{outcome}'.format(start_time=start_time, **test_record)
    return os.path.join(HISTORY_BASE_PATH, folder, subfolder)
    
def create_default_trigger(message='Enter a DUT ID in order to start the test.',
        validator=lambda sn: sn, **state):
    
    @htf.PhaseOptions(timeout_s=None, requires_state=True)
    @htf.plugs.plug(prompts=user_input.UserInput)
    def trigger_phase(state, prompts):
        """Test start trigger that prompts the user for a DUT ID."""
        dut_id = prompts.prompt(message, text_input=True)
        state.test_record.dut_id = validator(dut_id)
        
    return trigger_phase
    
def ensure_htf_phase(fn):
    if not hasattr(fn, 'options'):
        # Not a htf phase, decorate it so it becomes one.
        fn = htf.PhaseOptions()(fn) 
    return fn
