from ..base import OptionsGroup


class Algo(OptionsGroup):

    def __init__(self, *args, **kwargs):
        self._make_section_like()
        super().__init__(*args, **kwargs)


class AlgoSpare(Algo):
    """The **default** algorithm.

    If all workers are busy for a certain amount of time seconds then uWSGI will spawn new workers.
    When the load is gone it will begin stopping processes one at a time.

    * http://uwsgi.readthedocs.io/en/latest/Cheaper.html#spare-cheaper-algorithm

    """
    name = 'spare'

    def set_basic_params(self, *, check_interval_overload=None):
        """
        :param int check_interval_overload: Interval (sec) to wait after all workers are busy
            before new worker spawn.

        """
        self._set('cheaper-overload', check_interval_overload)

        return self


class AlgoSpare2(Algo):
    """This algorithm is similar to ``spare``, but suitable for large scale
    by increase workers faster (before overload) and decrease them slower.

    * http://uwsgi.readthedocs.io/en/latest/Cheaper.html#spare2-cheaper-algorithm

    """

    name = 'spare2'

    def set_basic_params(self, *, check_interval_idle=None):
        """
        :param int check_interval_idle: Decrease workers after specified idle. Default: 10.

        """
        self._set('cheaper-idle', check_interval_idle)

        return self


class AlgoQueue(Algo):
    """If the socket's listen queue has more than ``cheaper_overload`` requests
    waiting to be processed, uWSGI will spawn new workers.

    If the backlog is lower it will begin killing processes one at a time.

    * http://uwsgi.readthedocs.io/en/latest/Cheaper.html#backlog-cheaper-algorithm

    .. note: Only available on Linux and only on TCP sockets (not UNIX domain sockets).

    """
    name = 'backlog'

    def set_basic_params(self, *, check_num_overload=None):
        """
        :param int check_num_overload: Number of backlog items in queue.

        """
        self._set('cheaper-overload', check_num_overload)

        return self._section


class AlgoBusyness(Algo):
    """Algorithm adds or removes workers based on average utilization
    for a given time period. It's goal is to keep more workers than
    the minimum needed available at any given time, so the app will always
    have capacity for new requests.

    * http://uwsgi.readthedocs.io/en/latest/Cheaper.html#busyness-cheaper-algorithm

    .. note:: Requires ``cheaper_busyness`` plugin.

    """
    name = 'busyness'
    plugin = 'cheaper_busyness'

    def set_basic_params(
            self, *, check_interval_busy=None,
            busy_max=None, busy_min=None,
            idle_cycles_max=None, idle_cycles_penalty=None,
            verbose=None):
        """
        :param int check_interval_busy: Interval (sec) to check worker busyness.

        :param int busy_max: Maximum busyness (percents). Every time the calculated busyness
            is higher than this value, uWSGI will spawn new workers. Default: 50.

        :param int busy_min: Minimum busyness (percents). If busyness is below this value,
            the app is considered in an "idle cycle" and uWSGI will start counting them.
            Once we reach needed number of idle cycles uWSGI will kill one worker. Default: 25.

        :param int idle_cycles_max: This option tells uWSGI how many idle cycles are allowed
            before stopping a worker.

        :param int idle_cycles_penalty: Number of idle cycles to add to ``idle_cycles_max``
            in case worker spawned too early. Default is 1.

        :param bool verbose: Enables debug logs for this algo.

        """
        self._set('cheaper-overload', check_interval_busy)
        self._set('cheaper-busyness-max', busy_max)
        self._set('cheaper-busyness-min', busy_min)
        self._set('cheaper-busyness-multiplier', idle_cycles_max)
        self._set('cheaper-busyness-penalty', idle_cycles_penalty)
        self._set('cheaper-busyness-verbose', verbose, cast=bool)

        return self._section

    def set_emergency_params(
            self, *, workers_step=None, idle_cycles_max=None, queue_size=None, queue_nonzero_delay=None):
        """Sets busyness algorithm emergency workers related params.

        Emergency workers could be spawned depending upon uWSGI backlog state.

        .. note:: These options are Linux only.

        :param int workers_step: Number of emergency workers to spawn. Default: 1.

        :param int idle_cycles_max: Idle cycles to reach before stopping an emergency worker. Default: 3.

        :param int queue_size: Listen queue (backlog) max size to spawn an emergency worker. Default: 33.

        :param int queue_nonzero_delay: If the request listen queue is > 0 for more than given amount of seconds
            new emergency workers will be spawned. Default: 60.

        """
        self._set('cheaper-busyness-backlog-step', workers_step)
        self._set('cheaper-busyness-backlog-multiplier', idle_cycles_max)
        self._set('cheaper-busyness-backlog-alert', queue_size)
        self._set('cheaper-busyness-backlog-nonzero', queue_nonzero_delay)

        return self


class AlgoManual(Algo):
    """Algorithm allows to adjust number of workers using Master FIFO commands.

    * http://uwsgi.readthedocs.io/en/latest/MasterFIFO.html#available-commands

    """
    name = 'manual'


class Cheapening(OptionsGroup):
    """uWSGI provides the ability to dynamically scale
    the number of running workers (adaptive process spawning) via pluggable algorithms.

    .. note:: This uses master process.

    """

    class algorithms:
        """Algorithms available to use with ``cheaper_algorithm``."""

        busyness = AlgoBusyness
        manual = AlgoManual
        queue = AlgoQueue
        spare = AlgoSpare
        spare2 = AlgoSpare2

    def set_basic_params(
            self, *, spawn_on_request=None,
            cheaper_algo=None, workers_min=None, workers_startup=None, workers_step=None):
        """
        :param bool spawn_on_request: Spawn workers only after the first request.

        :param Algo cheaper_algo: The algorithm object to be used used for adaptive process spawning.
            Default: ``spare``. See ``.algorithms``.

        :param int workers_min: Minimal workers count. Enables cheaper mode (adaptive process spawning).

            .. note:: Must be lower than max workers count.

        :param int workers_startup: The number of workers to be started when starting the application.
            After the app is started the algorithm can stop or start workers if needed.

        :param int workers_step: Number of additional processes to spawn at a time if they are needed,

        """
        self._set('cheap', spawn_on_request, cast=bool)

        if cheaper_algo:
            self._set('cheaper-algo', cheaper_algo.name)

            if cheaper_algo.plugin:
                self._section.set_plugins_params(plugins=cheaper_algo.plugin)

            cheaper_algo._contribute_to_opts(self)

        self._set('cheaper', workers_min)
        self._set('cheaper-initial', workers_startup)
        self._set('cheaper-step', workers_step)

        return self._section

    def set_memory_limits(self, *, rss_soft=None, rss_hard=None):
        """Sets worker memory limits for cheapening.

        :param int rss_soft: Don't spawn new workers if total resident memory usage
            of all workers is higher than this limit in bytes.
            
            .. warning:: This option expects memory reporting enabled:
                ``.logging.set_basic_params(memory_report=1)``

        :param int rss_hard: Try to stop workers if total workers resident memory usage
            is higher that thi limit in bytes.

        """
        self._set('cheaper-rss-limit-soft', rss_soft)           
        self._set('cheaper-rss-limit-hard', rss_hard)

        return self._section

    def print_alorithms(self):
        """Print out enabled cheaper algorithms."""

        self._set('cheaper-algo-list', True, cast=bool)

        return self._section
