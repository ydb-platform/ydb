import asyncio
import os
import sys

from os.path import realpath

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler as EventHandler
from watchdog.events import FileSystemEvent as Event


# Event handler class for watchdog
class Handler(EventHandler):
    # Private
    _future_resolved = False

    # Common filetypes to watch
    patterns = ["*.py", "*.txt", "*.aiml", "*.json", "*.cfg", "*.xml", "*.html"]

    def __init__(self, loop, *args, **kwargs):
        self.loop = loop

        # awaitable future to race on
        self.changed = asyncio.Future(loop=loop)

        # Continue init for EventHandler
        return super(Handler, self).__init__(*args, **kwargs)

    def on_any_event(self, event):
        # Resolve future
        if isinstance(event, Event) and not self._future_resolved:
            self.loop.call_soon_threadsafe(self.changed.set_result, event)
            self._future_resolved = True


def clear_screen():
    if os.name == "nt":
        seq = "\x1Bc"
    else:
        seq = "\x1B[2J\x1B[H"

    sys.stdout.write(seq)


def reload():
    """Reload process"""
    try:
        # Reload and replace current process
        os.execv(sys.executable, [sys.executable] + sys.argv)

    except OSError:
        # Ugh, that failed
        # Try spawning a new process and exitj
        os.spawnv(os.P_NOWAIT, sys.executable, [sys.executable] + sys.argv)
        os._exit(os.EX_OK)


async def run_with_reloader(loop, coroutine, cleanup=None, *args, **kwargs):
    """Run coroutine with reloader"""

    clear_screen()
    print("🤖  Running in debug mode with live reloading")
    print("    (don't forget to disable it for production)")

    # Create watcher
    handler = Handler(loop)
    watcher = Observer()

    # Setup
    path = realpath(os.getcwd())
    watcher.schedule(handler, path=path, recursive=True)
    watcher.start()

    print("    (watching {})".format(path))

    # Run watcher and coroutine together
    done, pending = await asyncio.wait(
        [coroutine, handler.changed], return_when=asyncio.FIRST_COMPLETED
    )

    # Cleanup
    cleanup and cleanup()
    watcher.stop()

    for fut in done:
        # If change event, then reload
        if isinstance(fut.result(), Event):
            print("Reloading...")
            reload()
