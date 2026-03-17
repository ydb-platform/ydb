import math
import sys
import time

import param
from IPython.display import clear_output

from ..core.util import ProgressIndicator


class ProgressBar(ProgressIndicator):
    """A simple text progress bar suitable for both the IPython notebook
    and the IPython interactive prompt.

    ProgressBars are automatically nested if a previous instantiated
    progress bars has not achieved 100% completion.

    """

    display = param.Selector(default='stdout',
                  objects=['stdout', 'disabled', 'broadcast'], doc="""
        Parameter to control display of the progress bar. By default,
        progress is shown on stdout but this may be disabled e.g. for
        jobs that log standard output to file.

        If the output mode is set to 'broadcast', a socket is opened on
        a stated port to broadcast the completion percentage. The
        RemoteProgress class may then be used to view the progress from
        a different process.""")

    width = param.Integer(default=70, doc="""
        The width of the progress bar as the number of characters""")

    fill_char = param.String(default='#', doc="""
        The character used to fill the progress bar.""")

    blank_char = param.String(default=' ', doc="""
        The character for the blank portion of the progress bar.""")

    elapsed_time = param.Boolean(default=True, doc="""
        If enabled, the progress bar will disappear and display the
        total elapsed time once 100% completion is reached.""")

    cache = {}

    current_progress = []

    def __init__(self, **params):
        self.start_time = None
        self._stdout_display(0, False)
        ProgressBar.current_progress.append(self)
        super().__init__(**params)

    def __call__(self, percentage):
        """Update the progress bar within the specified percent_range

        """
        if self.start_time is None: self.start_time = time.time()
        span = (self.percent_range[1]-self.percent_range[0])
        percentage = self.percent_range[0] + ((percentage/100.0) * span)

        if self.display == 'disabled': return
        elif self.display == 'stdout':
            if percentage==100 and self.elapsed_time:
                elapsed = time.time() -  self.start_time
                if clear_output:
                    clear_output()
                self.out = f'\r100% {self.label.lower()} {elapsed//3600:02d}:{elapsed//60:02d}:{elapsed%60:02d}'
                output = ''.join([pg.out for pg in self.current_progress])
                sys.stdout.write(output)
            else:
                self._stdout_display(percentage)
            if percentage == 100 and ProgressBar.current_progress:
                ProgressBar.current_progress.pop()
            return

        if 'socket' not in self.cache:
            self.cache['socket'] = self._get_socket()

        if self.cache['socket'] is not None:
            self.cache['socket'].send(f'{percentage}|{self.label}')


    def _stdout_display(self, percentage, display=True):
        if clear_output:
            clear_output()
        percent_per_char = 100.0 / self.width
        char_count = int(math.floor(percentage/percent_per_char)
                         if percentage<100.0 else self.width)
        blank_count = self.width - char_count
        prefix = '\n' if len(self.current_progress) > 1 else ''
        self.out =  prefix + ("{}[{}{}] {:0.1f}%".format(self.label+':\n' if self.label else '',
                               self.fill_char * char_count,
                               ' '*len(self.fill_char) * blank_count,
                               percentage))
        if display:
            sys.stdout.write(''.join([pg.out for pg in self.current_progress]))
            sys.stdout.flush()
            time.sleep(0.0001)


    def _get_socket(self, min_port=8080, max_port=8100, max_tries=20):
        import zmq
        context = zmq.Context()
        sock = context.socket(zmq.PUB)
        try:
            port = sock.bind_to_random_port('tcp://*',
                                            min_port=min_port,
                                            max_port=max_port,
                                            max_tries=max_tries)
            self.param.message(f"Progress broadcast bound to port {port}")
            return sock
        except Exception:
            self.param.message("No suitable port found for progress broadcast.")
            return None


class RemoteProgress(ProgressBar):
    """Connect to a progress bar in a separate process with output_mode
    set to 'broadcast' in order to display the results (to stdout).

    """

    hostname=param.String(default='localhost', doc="""
        Hostname where progress is being broadcast.""")

    port = param.Integer(default=8080, doc="Target port on hostname.")

    def __init__(self, port, **params):
        super().__init__(port=port, **params)

    def __call__(self):
        import zmq
        context = zmq.Context()
        sock = context.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, '')
        sock.connect('tcp://' + self.hostname +':'+str(self.port))
        # Get progress via socket
        percent = None
        while True:
            try:
                message= sock.recv()
                [percent_str, label] = message.split('|')
                percent = float(percent_str)
                self.label = label
                super().__call__(percent)
            except KeyboardInterrupt:
                if percent is not None:
                    self.param.message(f"Exited at {percent:.3f}% completion")
                break
            except Exception:
                self.param.message(f"Could not process socket message: {message!r}")


class RunProgress(ProgressBar):
    """RunProgress breaks up the execution of a slow running command so
    that the level of completion can be displayed during execution.

    This class is designed to run commands that take a single numeric
    argument that acts additively. Namely, it is expected that a slow
    running command 'run_hook(X+Y)' can be arbitrarily broken up into
    multiple, faster executing calls 'run_hook(X)' and 'run_hook(Y)'
    without affecting the overall result.

    For instance, this is suitable for simulations where the numeric
    argument is the simulated time - typically, advancing 10 simulated
    seconds takes about twice as long as advancing by 5 seconds.

    """

    interval = param.Number(default=100, doc="""
        The run interval used to break up updates to the progress bar.""")

    run_hook = param.Callable(default=param.Dynamic.time_fn.advance, doc="""
        By default updates time in param which is very fast and does
        not need a progress bar. Should be set to some slower running
        callable where display of progress level is desired.""")


    def __init__(self, **params):
        super().__init__(**params)

    def __call__(self, value):
        """Execute the run_hook to a total of value, breaking up progress
        updates by the value specified by interval.

        """
        completed = 0
        while (value - completed) >= self.interval:
            self.run_hook(self.interval)
            completed += self.interval
            super().__call__(100 * (completed / float(value)))
        remaining = value - completed
        if remaining != 0:
            self.run_hook(remaining)
            super().__call__(100)


def progress(iterator, enum=False, length=None):
    """A helper utility to display a progress bar when iterating over a
    collection of a fixed length or a generator (with a declared
    length).

    If enum=True, then equivalent to enumerate with a progress bar.

    """
    progress = ProgressBar()
    length = len(iterator) if length is None else length
    gen = enumerate(iterator)
    while True:
        i, val = next(gen)
        progress((i+1.0)/length * 100)
        if enum:
            yield i, val
        else:
            yield val
