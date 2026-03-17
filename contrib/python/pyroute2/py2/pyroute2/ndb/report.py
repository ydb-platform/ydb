'''
.. note:: New in verision 0.5.11

Filtering examples::

    # 1. get all the routes
    # 2. join with interfaces on route.oif == interface.index
    # 3. select only fields dst, gateway, ifname and mac address
    # 4. transform the mac address into xxxx.xxxx.xxxx notation
    # 5. dump the info in the CSV format

    (ndb
     .routes
     .dump()
     .join(ndb.interfaces.dump(),
           condition=lambda l, r: l.oif == r.index)
     .select('dst', 'gateway', 'oif', 'ifname', 'address')
     .transform(address=lambda x: '%s%s.%s%s.%s%s' % tuple(x.split(':')))
     .format('csv'))

    'dst','gateway','oif','ifname','address'
    '172.16.20.0','127.0.0.2',1,'lo','0000.0000.0000'
    '172.16.22.0','127.0.0.4',1,'lo','0000.0000.0000'
    '','172.16.254.3',3,'wlp58s0','60f2.6289.400e'
    '10.250.3.0',,39,'lxcbr0','0016.3e00.0000'
    '10.255.145.0','10.255.152.254',42881,'prdc51e6d5','4a6a.60b1.8448'
    ...


'''
import json
from itertools import chain
from pyroute2 import cli
from pyroute2.common import basestring

MAX_REPORT_LINES = 10000


def format_json(dump, headless=False):

    buf = []
    fnames = None
    yield '['
    for record in dump:
        if fnames is None:
            if headless:
                fnames = record._names
            else:
                fnames = record
                continue
        if buf:
            buf[-1] += ','
            for line in buf:
                yield line
            buf = []
        lines = json.dumps(dict(zip(fnames, record)), indent=4).split('\n')
        buf.append('    {')
        for line in sorted(lines[1:-1]):
            if line[-1] == ',':
                line = line[:-1]
            buf.append('    %s,' % line)
        buf[-1] = buf[-1][:-1]
        buf.append('    }')
    for line in buf:
        yield line
    yield ']'


def format_csv(dump, headless=False):

    def dump_record(rec):
        row = []
        for field in rec:
            if isinstance(field, int):
                row.append('%i' % field)
            elif field is None:
                row.append('')
            else:
                row.append("'%s'" % field)
        return row

    fnames = None
    for record in dump:
        if fnames is None and headless:
            fnames = True
            yield ','.join(dump_record(record._names))
        yield ','.join(dump_record(record))


class Record(object):

    def __init__(self, names, values, ref_class=None):
        if len(names) != len(values):
            raise ValueError('names and values must have the same length')
        self._names = tuple(names)
        self._values = tuple(values)
        self._ref_class = ref_class

    def __getitem__(self, key):
        idx = len(self._names)
        for i in reversed(self._names):
            idx -= 1
            if i == key:
                return self._values[idx]

    def __setitem__(self, *argv, **kwarg):
        raise TypeError('immutable object')

    def __getattribute__(self, key):
        if key.startswith('_'):
            return object.__getattribute__(self, key)
        else:
            return self[key]

    def __setattr__(self, key, value):
        if not key.startswith('_'):
            raise TypeError('immutable object')
        return object.__setattr__(self, key, value)

    def __iter__(self):
        return iter(self._values)

    def __repr__(self):
        return repr(self._values)

    def __len__(self):
        return len(self._values)

    def _as_dict(self):
        ret = {}
        for key, value in zip(self._names, self._values):
            ret[key] = value
        return ret

    def __eq__(self, right):
        if hasattr(right, '_names'):
            n = all(x[0] == x[1] for x in zip(self._names, right._names))
            v = all(x[0] == x[1] for x in zip(self._values, right._values))
            return n and v
        elif self._ref_class is not None and isinstance(right, basestring):
            return self._ref_class.compare_record(self, right)
        else:
            return all(x[0] == x[1] for x in zip(self._values, right))


class BaseRecordSet(object):

    def __init__(self, generator, ellipsis=True):
        self.generator = generator
        self.ellipsis = ellipsis
        self.cached = []

    def __iter__(self):
        return self.generator

    def __repr__(self):
        counter = 0
        ret = []
        for record in self.generator:
            if isinstance(record, basestring):
                ret.append(record)
            else:
                ret.append(repr(record))
            ret.append('\n')
            counter += 1
            if self.ellipsis and counter > MAX_REPORT_LINES:
                ret.append('(...)')
                break
        if ret:
            ret.pop()
        return ''.join(ret)


class RecordSet(BaseRecordSet):
    '''
    NDB views return objects of this class with `summary()` and `dump()`
    methods. RecordSet objects are generator-based, they do not store the
    data in the memory, but transform them on the fly.

    RecordSet filters also return objects of this class, thus making possible
    to make chains of filters.
    '''

    @cli.show_result
    def transform(self, **kwarg):
        '''
        Transform record fields with a provided functions::

            view.transform(field_name_1=func1,
                           field_name_2=func2)

        Examples, transform MAC addresses into dots-format and IEEE 802::

            fmt = '%s%s.%s%s.%s%s'
            (ndb
             .interfaces
             .summary()
             .transform(address=lambda x: fmt % tuple(x.split(':')))

            (ndb
             .interfaces
             .summary()
             .transform(address=lambda x: x.replace(':', '-').upper()))
        '''
        def g():
            for record in self.generator:
                if isinstance(record, Record):
                    values = []
                    names = record._names
                    for name, value in zip(names, record._values):
                        if name in kwarg:
                            value = kwarg[name](value)
                        values.append(value)
                    record = Record(names, values, record._ref_class)
                yield record

        return RecordSet(g())

    @cli.show_result
    def filter(self, f=None, **kwarg):
        '''
        Filter records. This function may be called in two ways. One way
        is a simple match. Select ports of `br0` only in the `up` state::

            (ndb
             .interfaces
             .dump()
             .filter(master=ndb.interfaces['br0']['index'],
                     state='up'))

        When a simple match is not a solution, one can provide a matching
        function. Select only MPLS lwtunnel routes::

            (ndb
             .routes
             .dump()
             .filter(lambda x: x.encap_type == 1 and x.encap is not None))
        '''
        def g():
            for record in self.generator:
                m = True
                for key in kwarg:
                    if kwarg[key] != getattr(record, key):
                        m = False
                if m:
                    if f is None:
                        yield record
                    elif f(record):
                        yield record

        return RecordSet(g())

    @cli.show_result
    def select(self, *argv):
        '''
        Select fields from records::

            ndb.interfaces.dump().select('index', 'ifname', 'state')
        '''
        def g():
            for record in self.generator:
                ret = []
                for field in argv:
                    ret.append(getattr(record, field, None))
                yield Record(argv, ret, record._ref_class)

        return RecordSet(g())

    @cli.show_result
    def join(self, right, condition=lambda r1, r2: True, prefix=''):
        '''
        Join two reports.

            * right -- a report to join with
            * condition -- filter records with a function
            * prefix -- rename the "right" fields using the prefix

        The condition function must have two arguments, left record and
        rigth record, and must return True or False. The routine discards
        joined records when the condition is False.

        Example, provide interface names for routes, don't change field
        names::

            (ndb
             .routes
             .dump()
             .join(ndb.interfaces.dump(),
                   condition=lambda l, r: l.oif == r.index)
             .select('dst', 'gateway', 'ifname'))

        **Warning**: this method loads the whole data of the `right` report
        into the memory.

        '''
        # fetch all the records from the right
        # ACHTUNG it may consume a lot of memory
        right = tuple(right)

        def g():

            for r1 in self.generator:
                for r2 in right:
                    if condition(r1, r2):
                        n = tuple(chain(r1._names, ['%s%s' % (prefix, x)
                                                    for x in r2._names]))
                        v = tuple(chain(r1._values, r2._values))
                        yield Record(n, v, r1._ref_class)

        return RecordSet(g())

    @cli.show_result
    def format(self, kind):
        '''
        Convert report records into other formats. Supported formats are
        'json' and 'csv'.

        The resulting report can not use filters, transformations etc.
        Thus, the `format()` call should be the last in the chain::

            (ndb
             .addresses
             .summary()
             .format('csv'))

        '''
        if kind == 'json':
            return BaseRecordSet(format_json(self.generator, headless=True))
        elif kind == 'csv':
            return BaseRecordSet(format_csv(self.generator, headless=True))
        else:
            raise ValueError()

    def count(self):
        '''
        Return number of records.

        This method is destructive, as it exhausts the generator.
        '''
        counter = 0
        for record in self.generator:
            counter += 1
        return counter

    def __getitem__(self, key):
        if isinstance(key, int):
            if key >= 0:
                # positive indices
                for x in range(key):
                    try:
                        next(self.generator)
                    except StopIteration:
                        raise IndexError('index out of range')
                try:
                    return next(self.generator)
                except StopIteration:
                    raise IndexError('index out of range')
            else:
                # negative indices
                buf = []
                for i in self.generator:
                    buf.append(i)
                    if len(buf) > abs(key):
                        buf.pop(0)
                if len(buf) < abs(key):
                    raise IndexError('index out of range')
                return buf[0]
        elif isinstance(key, slice):
            count = 0
            buf = []
            start = key.start or 0
            stop = key.stop

            for i in self.generator:
                buf.append(i)
                if (start >= 0 and count < start) or \
                        (start < 0 and len(buf) > abs(start)):
                    buf.pop(0)
                count += 1
                if stop is not None and stop > 0 and count == stop:
                    if start < 0:
                        buf.pop(0)
                    break

            if stop is not None and stop < 0:
                buf = buf[:stop]

            return buf[::key.step]
        else:
            raise TypeError('illegal key format')
