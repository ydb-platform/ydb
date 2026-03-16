# SYSV IPC

`sysv_ipc` is a Python module (written in C) that permits creation and manipulation of System V semaphores, shared memory and message queues. Most (all?) Unixes (including OS X) support System V IPC well. Windows plus WSL or Cygwin might work.

**For complete documentation, see [the usage notes](USAGE.md).**

`sysv_ipc` is compatible with all supported versions of Python 3. Older versions of `sysv_ipc` may [still work under Python 2.x](USAGE.md#support-for-older-pythons).

If you want to build your own copy of `sysv_ipc`, see [the build notes](building.md).

## Installation

`sysv_ipc` is available from PyPI:

	pip install sysv-ipc

If you have the source code, you can install `sysv_ipc` with this command:

	python -m pip install .

## Tests

`sysv_ipc` has a robust test suite. To run tests --

	python -m unittest discover --verbose

## License

`sysv_ipc` is free software (free as in speech and free as in beer) released under a 3-clause BSD license. Complete licensing information is available in [the LICENSE file](LICENSE).

## Support

If you have comments, questions, or ideas to share, please use the mailing list:
https://groups.io/g/python-sysv-ipc/

If you think you've found a bug, you can file an issue on GitHub:
https://github.com/osvenskan/sysv_ipc/issues

Please note that as of this writing (2025), it's been six years since anyone found a bug in the core code, so maybe ask on the mailing list first. 🙂

## Related

You might also be interested in the similar POSIX IPC module: https://github.com/osvenskan/posix_ipc
