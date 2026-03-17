from functools import partial
import sys
from threading import Thread

try:
    import cPickle as pickle
except:
    import pickle

import uwsgi

if uwsgi.masterpid() == 0:
    raise Exception(
        "you have to enable the uWSGI master process to use this module")

spooler_functions = {}
mule_functions = {}
postfork_chain = []


# Python3 compatibility
def _encode1(val):
    if sys.version_info >= (3, 0) and isinstance(val, str):
        return val.encode('utf-8')
    else:
        return val


def _decode1(val):
    if sys.version_info >= (3, 0) and isinstance(val, bytes):
        return val.decode('utf-8')
    else:
        return val


def _encode_to_spooler(vars):
    return dict((_encode1(K), _encode1(V)) for (K, V) in vars.items())


def _decode_from_spooler(vars):
    return dict((_decode1(K), _decode1(V)) for (K, V) in vars.items())


def get_free_signal():
    for signum in range(0, 256):
        if not uwsgi.signal_registered(signum):
            return signum

    raise Exception("No free uwsgi signal available")


def manage_spool_request(vars):
    # To check whether 'args' is in vals or not - decode the keys first,
    # because in python3 all keys in 'vals' are have 'byte' types
    vars = dict((_decode1(K), V) for (K, V) in vars.items())
    if 'args' in vars:
        for k in ('args', 'kwargs'):
            vars[k] = pickle.loads(vars.pop(k))

    vars = _decode_from_spooler(vars)
    f = spooler_functions[vars['ud_spool_func']]

    if 'args' in vars:
        ret = f(*vars['args'], **vars['kwargs'])
    else:
        ret = f(vars)

    return int(vars.get('ud_spool_ret', ret))


def postfork_chain_hook():
    for f in postfork_chain:
        f()

uwsgi.spooler = manage_spool_request
uwsgi.post_fork_hook = postfork_chain_hook


class postfork(object):
    def __init__(self, f):
        if callable(f):
            self.wid = 0
            self.f = f
        else:
            self.f = None
            self.wid = f
        postfork_chain.append(self)
    def __call__(self, *args, **kwargs):
        if self.f:
            if self.wid > 0 and self.wid != uwsgi.worker_id():
                return
            return self.f()
        self.f = args[0]


class _spoolraw(object):

    def __call__(self, *args, **kwargs):
        arguments = self.base_dict.copy()
        if not self.pass_arguments:
            if len(args) > 0:
                arguments.update(args[0])
            if kwargs:
                arguments.update(kwargs)
        else:
            spooler_args = {}
            for key in ('message_dict', 'spooler', 'priority', 'at', 'body'):
                if key in kwargs:
                    spooler_args.update({key: kwargs.pop(key)})
            arguments.update(spooler_args)
            arguments.update(
                {'args': pickle.dumps(args), 'kwargs': pickle.dumps(kwargs)})
        return uwsgi.spool(_encode_to_spooler(arguments))

    # For backward compatibility (uWSGI < 1.9.13)
    def spool(self, *args, **kwargs):
        return self.__class__.__call__(self, *args, **kwargs)

    def __init__(self, f, pass_arguments):
        if not 'spooler' in uwsgi.opt:
            raise Exception(
                "you have to enable the uWSGI spooler to use @%s decorator" % self.__class__.__name__)
        self.f = f
        spooler_functions[self.f.__name__] = self.f
        # For backward compatibility (uWSGI < 1.9.13)
        self.f.spool = self.__call__
        self.pass_arguments = pass_arguments
        self.base_dict = {'ud_spool_func': self.f.__name__}


class _spool(_spoolraw):

    def __call__(self, *args, **kwargs):
        self.base_dict['ud_spool_ret'] = str(uwsgi.SPOOL_OK)
        return _spoolraw.__call__(self, *args, **kwargs)


class _spoolforever(_spoolraw):

    def __call__(self, *args, **kwargs):
        self.base_dict['ud_spool_ret'] = str(uwsgi.SPOOL_RETRY)
        return _spoolraw.__call__(self, *args, **kwargs)


def spool_decorate(f=None, pass_arguments=False, _class=_spoolraw):
    if not f:
        return partial(_class, pass_arguments=pass_arguments)
    return _class(f, pass_arguments)


def spoolraw(f=None, pass_arguments=False):
    return spool_decorate(f, pass_arguments)


def spool(f=None, pass_arguments=False):
    return spool_decorate(f, pass_arguments, _spool)


def spoolforever(f=None, pass_arguments=False):
    return spool_decorate(f, pass_arguments, _spoolforever)


class mulefunc(object):

    def __init__(self, f):
        if callable(f):
            self.fname = f.__name__
            self.mule = 0
            mule_functions[f.__name__] = f
        else:
            self.mule = f
            self.fname = None

    def real_call(self, *args, **kwargs):
        uwsgi.mule_msg(pickle.dumps(
            {
                'service': 'uwsgi_mulefunc',
                'func': self.fname,
                'args': args,
                'kwargs': kwargs
            }
        ), self.mule)

    def __call__(self, *args, **kwargs):
        if not self.fname:
            self.fname = args[0].__name__
            mule_functions[self.fname] = args[0]
            return self.real_call

        return self.real_call(*args, **kwargs)


def mule_msg_dispatcher(message):
    msg = pickle.loads(message)
    if msg['service'] == 'uwsgi_mulefunc':
        return mule_functions[msg['func']](*msg['args'], **msg['kwargs'])

uwsgi.mule_msg_hook = mule_msg_dispatcher


class rpc(object):

    def __init__(self, name):
        self.name = name

    def __call__(self, f):
        uwsgi.register_rpc(self.name, f)
        return f


class farm_loop(object):

    def __init__(self, f, farm):
        self.f = f
        self.farm = farm

    def __call__(self):
        if uwsgi.mule_id() == 0:
            return
        if not uwsgi.in_farm(self.farm):
            return
        while True:
            message = uwsgi.farm_get_msg()
            if message:
                self.f(message)


class farm(object):

    def __init__(self, name=None, **kwargs):
        self.name = name

    def __call__(self, f):
        postfork_chain.append(farm_loop(f, self.name))


class mule_brain(object):

    def __init__(self, f, num):
        self.f = f
        self.num = num

    def __call__(self):
        if uwsgi.mule_id() == self.num:
            try:
                self.f()
            except:
                exc = sys.exc_info()
                sys.excepthook(exc[0], exc[1], exc[2])
                sys.exit(1)


class mule_brainloop(mule_brain):

    def __call__(self):
        if uwsgi.mule_id() == self.num:
            while True:
                try:
                    self.f()
                except:
                    exc = sys.exc_info()
                    sys.excepthook(exc[0], exc[1], exc[2])
                    sys.exit(1)


class mule(object):
    def __init__(self, num):
        self.num = num

    def __call__(self, f):
        postfork_chain.append(mule_brain(f, self.num))


class muleloop(mule):
    def __call__(self, f):
        postfork_chain.append(mule_brainloop(f, self.num))


class mulemsg_loop(object):

    def __init__(self, f, num):
        self.f = f
        self.num = num

    def __call__(self):
        if uwsgi.mule_id() == self.num:
            while True:
                message = uwsgi.mule_get_msg()
                if message:
                    self.f(message)


class mulemsg(object):
    def __init__(self, num):
        self.num = num

    def __call__(self, f):
        postfork_chain.append(mulemsg_loop(f, self.num))


class signal(object):

    def __init__(self, num, **kwargs):
        self.num = num
        self.target = kwargs.get('target', '')

    def __call__(self, f):
        uwsgi.register_signal(self.num, self.target, f)
        return f


class timer(object):

    def __init__(self, secs, **kwargs):
        self.num = kwargs.get('signum', get_free_signal())
        self.secs = secs
        self.target = kwargs.get('target', '')

    def __call__(self, f):
        uwsgi.register_signal(self.num, self.target, f)
        uwsgi.add_timer(self.num, self.secs)
        return f


class cron(object):

    def __init__(self, minute, hour, day, month, dayweek, **kwargs):
        self.num = kwargs.get('signum', get_free_signal())
        self.minute = minute
        self.hour = hour
        self.day = day
        self.month = month
        self.dayweek = dayweek
        self.target = kwargs.get('target', '')

    def __call__(self, f):
        uwsgi.register_signal(self.num, self.target, f)
        uwsgi.add_cron(self.num, self.minute, self.hour,
            self.day, self.month, self.dayweek)
        return f


class rbtimer(object):

    def __init__(self, secs, **kwargs):
        self.num = kwargs.get('signum', get_free_signal())
        self.secs = secs
        self.target = kwargs.get('target', '')

    def __call__(self, f):
        uwsgi.register_signal(self.num, self.target, f)
        uwsgi.add_rb_timer(self.num, self.secs)
        return f


class filemon(object):

    def __init__(self, fsobj, **kwargs):
        self.num = kwargs.get('signum', get_free_signal())
        self.fsobj = fsobj
        self.target = kwargs.get('target', '')

    def __call__(self, f):
        uwsgi.register_signal(self.num, self.target, f)
        uwsgi.add_file_monitor(self.num, self.fsobj)
        return f


class erlang(object):

    def __init__(self, name):
        self.name = name

    def __call__(self, f):
        uwsgi.erlang_register_process(self.name, f)
        return f


class lock(object):
    def __init__(self, f):
        self.f = f

    def __call__(self, *args, **kwargs):
        # ensure the spooler will not call it
        if uwsgi.i_am_the_spooler():
            return
        uwsgi.lock()
        try:
            return self.f(*args, **kwargs)
        finally:
            uwsgi.unlock()


class thread(object):

    def __init__(self, f):
        self.f = f

    def __call__(self, *args):
        t = Thread(target=self.f, args=args)
        t.daemon = True
        t.start()
        return self.f


class harakiri(object):

    def __init__(self, seconds):
        self.s = seconds

    def real_call(self, *args, **kwargs):
        uwsgi.set_user_harakiri(self.s)
        r = self.f(*args, **kwargs)
        uwsgi.set_user_harakiri(0)
        return r

    def __call__(self, f):
        self.f = f
        return self.real_call
