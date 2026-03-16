# Copyright (c) 2015-2019, Activision Publishing, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import sys
import datetime
import inspect
import json

__tracebackhide__ = True


class SnapshotMixin(object):
    """Snapshot mixin.

    Take a snapshot of a python data structure, store it on disk in JSON format, and automatically
    compare the latest data to the stored data on every test run.

    Functional testing (which snapshot testing falls under) is very much blackbox testing.  When
    something goes wrong, it's hard to pinpoint the issue, because functional tests typically
    provide minimal *isolation* as compared to unit tests.  On the plus side, snapshots typically
    do provide enormous *leverage* as a few well-placed snapshot tests can strongly verify that an
    application is working.  Similar coverage would otherwise require dozens if not hundreds of
    unit tests.

    **On-disk Format**

    Snapshots are stored in a readable JSON format.  For example::

        assert_that({'a': 1, 'b': 2, 'c': 3}).snapshot()

    Would be stored as::

        {
            "a": 1,
            "b": 2,
            "c": 3
        }

    The JSON formatting support most python data structures (dict, list, object, etc), but not custom
    binary data.

    **Updating**

    It's easy to update your snapshots...just delete them all and re-run the test suite to regenerate all snapshots.

    Note:
        Snapshots require Python 3.x
    """

    def snapshot(self, id=None, path='__snapshots'):
        """Asserts that val is identical to the on-disk snapshot stored previously.

        On the first run of a test before the snapshot file has been saved, a snapshot is created,
        stored to disk, and the test *always* passes.  But on all subsequent runs, val is compared
        to the on-disk snapshot, and the test fails if they don't match.

        Snapshot artifacts are stored in the ``__snapshots`` directory by default, and should be
        committed to source control alongside any code changes.

        Snapshots are identified by test filename plus line number by default.

        Args:
            id: the item or items expected to be contained
            path: the item or items expected to be contained

        Examples:
            Usage::

                assert_that(None).snapshot()
                assert_that(True).snapshot()
                assert_that(1).snapshot()
                assert_that(123.4).snapshot()
                assert_that('foo').snapshot()
                assert_that([1, 2, 3]).snapshot()
                assert_that({'a': 1, 'b': 2, 'c': 3}).snapshot()
                assert_that({'a', 'b', 'c'}).snapshot()
                assert_that(1 + 2j).snapshot()
                assert_that(someobj).snapshot()

            By default, snapshots are identified by test filename plus line number.  Alternately, you can specify a custom identifier using the ``id`` arg::

                assert_that({'a': 1, 'b': 2, 'c': 3}).snapshot(id='foo-id')


            By default, snapshots are stored in the ``__snapshots`` directory.  Alternately, you can specify a custom path using the ``path`` arg::

                assert_that({'a': 1, 'b': 2, 'c': 3}).snapshot(path='my-custom-folder')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** equal to on-disk snapshot
        """
        if sys.version_info[0] < 3:
            raise NotImplementedError('snapshot testing requires Python 3')

        class _Encoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, set):
                    return {'__type__': 'set', '__data__': list(o)}
                elif isinstance(o, complex):
                    return {'__type__': 'complex', '__data__': [o.real, o.imag]}
                elif isinstance(o, datetime.datetime):
                    return {'__type__': 'datetime', '__data__': o.strftime('%Y-%m-%d %H:%M:%S')}
                elif '__dict__' in dir(o) and type(o) is not type:
                    return {
                        '__type__': 'instance',
                        '__class__': o.__class__.__name__,
                        '__module__': o.__class__.__module__,
                        '__data__': o.__dict__
                    }
                return json.JSONEncoder.default(self, o)

        class _Decoder(json.JSONDecoder):
            def __init__(self):
                json.JSONDecoder.__init__(self, object_hook=self.object_hook)

            def object_hook(self, d):
                if '__type__' in d and '__data__' in d:
                    if d['__type__'] == 'set':
                        return set(d['__data__'])
                    elif d['__type__'] == 'complex':
                        return complex(d['__data__'][0], d['__data__'][1])
                    elif d['__type__'] == 'datetime':
                        return datetime.datetime.strptime(d['__data__'], '%Y-%m-%d %H:%M:%S')
                    elif d['__type__'] == 'instance':
                        mod = __import__(d['__module__'], fromlist=[d['__class__']])
                        klass = getattr(mod, d['__class__'])
                        inst = klass.__new__(klass)
                        inst.__dict__ = d['__data__']
                        return inst
                return d

        def _save(name, val):
            with open(name, 'w') as fp:
                json.dump(val, fp, indent=2, separators=(',', ': '), sort_keys=True, cls=_Encoder)

        def _load(name):
            with open(name, 'r') as fp:
                return json.load(fp, cls=_Decoder)

        def _name(path, name):
            try:
                return os.path.join(path, 'snap-%s.json' % name.replace(' ', '_').lower())
            except Exception:
                raise ValueError('failed to create snapshot filename, either bad path or bad name')

        if id:
            # custom id
            snapname = _name(path, id)
        else:
            # make id from filename and line number
            f = inspect.currentframe()
            fpath = os.path.basename(f.f_back.f_code.co_filename)
            fname = os.path.splitext(fpath)[0]
            lineno = str(f.f_back.f_lineno)
            snapname = _name(path, fname)

        if not os.path.exists(path):
            os.makedirs(path)

        if os.path.isfile(snapname):
            # snap exists, so load
            snap = _load(snapname)

            if id:
                # custom id, so test
                return self.is_equal_to(snap)
            else:
                if lineno in snap:
                    # found sub-snap, so test
                    return self.is_equal_to(snap[lineno])
                else:
                    # lineno not in snap, so create sub-snap and pass
                    snap[lineno] = self.val
                    _save(snapname, snap)
        else:
            # no snap, so create and pass
            _save(snapname, self.val if id else {lineno: self.val})

        return self
