import asyncio
import logging.config
import os
import sys
from signal import Signals
from typing import TYPE_CHECKING, cast

import click

from .logs import default_log_config
from .utils import import_string
from .version import VERSION
from .worker import check_health, create_worker, run_worker

if TYPE_CHECKING:
    from .typing import WorkerSettingsType

burst_help = 'Batch mode: exit once no jobs are found in any queue.'
health_check_help = 'Health Check: run a health check and exit.'
watch_help = 'Watch a directory and reload the worker upon changes.'
verbose_help = 'Enable verbose output.'
logdict_help = "Import path for a dictionary in logdict form, to configure Arq's own logging."


@click.command('arq')
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-settings', type=str, required=True)
@click.option('--burst/--no-burst', default=None, help=burst_help)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option('--watch', type=click.Path(exists=True, dir_okay=True, file_okay=False), help=watch_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
@click.option('--custom-log-dict', type=str, help=logdict_help)
def cli(*, worker_settings: str, burst: bool, check: bool, watch: str, verbose: bool, custom_log_dict: str) -> None:
    """
    Job queues in python with asyncio and redis.

    CLI to run the arq worker.
    """
    sys.path.append(os.getcwd())
    worker_settings_ = cast('WorkerSettingsType', import_string(worker_settings))
    if custom_log_dict:
        log_config = import_string(custom_log_dict)
    else:
        log_config = default_log_config(verbose)
    logging.config.dictConfig(log_config)

    if check:
        exit(check_health(worker_settings_))
    else:
        kwargs = {} if burst is None else {'burst': burst}
        if watch:
            asyncio.run(watch_reload(watch, worker_settings_))
        else:
            run_worker(worker_settings_, **kwargs)


async def watch_reload(path: str, worker_settings: 'WorkerSettingsType') -> None:
    try:
        from watchfiles import awatch
    except ImportError as e:  # pragma: no cover
        raise ImportError('watchfiles not installed, use `pip install watchfiles`') from e

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def worker_on_stop(s: Signals) -> None:
        if s != Signals.SIGUSR1:  # pragma: no cover
            stop_event.set()

    worker = create_worker(worker_settings)
    try:
        worker.on_stop = worker_on_stop
        loop.create_task(worker.async_run())
        async for _ in awatch(path, stop_event=stop_event):
            print('\nfiles changed, reloading arq worker...')
            worker.handle_sig(Signals.SIGUSR1)
            await worker.close()
            loop.create_task(worker.async_run())
    finally:
        await worker.close()
