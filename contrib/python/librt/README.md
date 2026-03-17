# librt: mypyc runtime library

This library contains basic functionality that is useful in code compiled
using mypyc, and efficient C implementations of various Python standard library
classes and functions. Mypyc can produce faster extensions when you use `librt` in
the code you compile. `librt` also contains some internal library features used by mypy.

This repository is only used to build and publish the mypyc runtime library. Development
happens in the [mypy repository](https://github.com/python/mypy). Code is then perodically
synced from the `mypyc/lib-rt`
[subdirectory in the mypy repository](https://github.com/python/mypy/tree/master/mypyc/lib-rt).

Report any issues in the [mypyc issue tracker](https://github.com/mypyc/mypyc/issues).

## Making a release

1. As a prerequisite, there generally will be some changes in the mypy repository under `mypyc/lib-rt`
  that you want to release.
2. Run the `sync-mypy.py` script in this repository to sync changes from the mypy repository.
3. Bump the version number in `pyproject.toml` in this repository.
4. Update `smoke_tests.py` (optional but recommended for new features). Here's how to run tests:
    * Activate a dedicated virtualenv (don't reuse your mypy virtualenv).
    * `pip install -U ./lib-rt`
    * `pip install pytest mypy-extensions`
    * `pytest smoke_tests.py`
5. Commit and push (pushing directly to master is fine).
6. Wait until all [builds](https://github.com/mypyc/librt/actions) complete successfully
   (no release is triggered yet).
7. Once builds are complete, tag the release (`git tag vX.Y.Z`; `git push origin vX.Y.Z`).
8. Go to the ["Actions" tab](https://github.com/mypyc/librt/actions) and click "Build wheels"
   on the left.
9. Click "Run workflow" and pick the newly created tag from the drop-down list. This will build
   *and upload* the wheels.
10. After the workflow completes, verify that `pip install -U librt` installs the new version from PyPI.
11. Create a PR to update the `librt` version in `mypy-requirements.txt`, `test-requirements.txt` and
  `pyproject.toml` (`dependencies`, and `requires` under `build-system`) in the mypy repository.

The process should take about 20 minutes.
