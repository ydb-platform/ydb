import gc
import sys
import time
import threading
import unittest
from abc import ABCMeta, abstractmethod

from greenlet import greenlet

# We manually manage locks in many tests
# pylint:disable=consider-using-with

class SomeError(Exception):
    pass


def fmain(seen):
    try:
        greenlet.getcurrent().parent.switch()
    except:
        seen.append(sys.exc_info()[0])
        raise
    raise SomeError


def send_exception(g, exc):
    # note: send_exception(g, exc)  can be now done with  g.throw(exc).
    # the purpose of this test is to explicitely check the propagation rules.
    def crasher(exc):
        raise exc
    g1 = greenlet(crasher, parent=g)
    g1.switch(exc)


class TestGreenlet(unittest.TestCase):
    def test_simple(self):
        lst = []

        def f():
            lst.append(1)
            greenlet.getcurrent().parent.switch()
            lst.append(3)
        g = greenlet(f)
        lst.append(0)
        g.switch()
        lst.append(2)
        g.switch()
        lst.append(4)
        self.assertEqual(lst, list(range(5)))

    def test_parent_equals_None(self):
        g = greenlet(parent=None)
        self.assertIsNotNone(g)
        self.assertIs(g.parent, greenlet.getcurrent())

    def test_run_equals_None(self):
        g = greenlet(run=None)
        self.assertIsNotNone(g)
        self.assertIsNone(g.run)

    def test_two_children(self):
        lst = []

        def f():
            lst.append(1)
            greenlet.getcurrent().parent.switch()
            lst.extend([1, 1])
        g = greenlet(f)
        h = greenlet(f)
        g.switch()
        self.assertEqual(len(lst), 1)
        h.switch()
        self.assertEqual(len(lst), 2)
        h.switch()
        self.assertEqual(len(lst), 4)
        self.assertEqual(h.dead, True)
        g.switch()
        self.assertEqual(len(lst), 6)
        self.assertEqual(g.dead, True)

    def test_two_recursive_children(self):
        lst = []

        def f():
            lst.append(1)
            greenlet.getcurrent().parent.switch()

        def g():
            lst.append(1)
            g = greenlet(f)
            g.switch()
            lst.append(1)
        g = greenlet(g)
        g.switch()
        self.assertEqual(len(lst), 3)
        self.assertEqual(sys.getrefcount(g), 2)

    def test_threads(self):
        success = []

        def f():
            self.test_simple()
            success.append(True)
        ths = [threading.Thread(target=f) for i in range(10)]
        for th in ths:
            th.start()
        for th in ths:
            th.join()
        self.assertEqual(len(success), len(ths))

    def test_exception(self):
        seen = []
        g1 = greenlet(fmain)
        g2 = greenlet(fmain)
        g1.switch(seen)
        g2.switch(seen)
        g2.parent = g1
        self.assertEqual(seen, [])
        self.assertRaises(SomeError, g2.switch)
        self.assertEqual(seen, [SomeError])
        g2.switch()
        self.assertEqual(seen, [SomeError])

    def test_send_exception(self):
        seen = []
        g1 = greenlet(fmain)
        g1.switch(seen)
        self.assertRaises(KeyError, send_exception, g1, KeyError)
        self.assertEqual(seen, [KeyError])

    def test_dealloc(self):
        seen = []
        g1 = greenlet(fmain)
        g2 = greenlet(fmain)
        g1.switch(seen)
        g2.switch(seen)
        self.assertEqual(seen, [])
        del g1
        gc.collect()
        self.assertEqual(seen, [greenlet.GreenletExit])
        del g2
        gc.collect()
        self.assertEqual(seen, [greenlet.GreenletExit, greenlet.GreenletExit])

    def test_dealloc_other_thread(self):
        seen = []
        someref = []
        lock = threading.Lock()
        lock.acquire()
        lock2 = threading.Lock()
        lock2.acquire()

        def f():
            g1 = greenlet(fmain)
            g1.switch(seen)
            someref.append(g1)
            del g1
            gc.collect()
            lock.release()
            lock2.acquire()
            greenlet()   # trigger release
            lock.release()
            lock2.acquire()
        t = threading.Thread(target=f)
        t.start()
        lock.acquire()
        self.assertEqual(seen, [])
        self.assertEqual(len(someref), 1)
        del someref[:]
        gc.collect()
        # g1 is not released immediately because it's from another thread
        self.assertEqual(seen, [])
        lock2.release()
        lock.acquire()
        self.assertEqual(seen, [greenlet.GreenletExit])
        lock2.release()
        t.join()

    def test_frame(self):
        def f1():
            f = sys._getframe(0) # pylint:disable=protected-access
            self.assertEqual(f.f_back, None)
            greenlet.getcurrent().parent.switch(f)
            return "meaning of life"
        g = greenlet(f1)
        frame = g.switch()
        self.assertTrue(frame is g.gr_frame)
        self.assertTrue(g)

        from_g = g.switch()
        self.assertFalse(g)
        self.assertEqual(from_g, 'meaning of life')
        self.assertEqual(g.gr_frame, None)

    def test_thread_bug(self):
        def runner(x):
            g = greenlet(lambda: time.sleep(x))
            g.switch()
        t1 = threading.Thread(target=runner, args=(0.2,))
        t2 = threading.Thread(target=runner, args=(0.3,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    def test_switch_kwargs(self):
        def run(a, b):
            self.assertEqual(a, 4)
            self.assertEqual(b, 2)
            return 42
        x = greenlet(run).switch(a=4, b=2)
        self.assertEqual(x, 42)

    def test_switch_kwargs_to_parent(self):
        def run(x):
            greenlet.getcurrent().parent.switch(x=x)
            greenlet.getcurrent().parent.switch(2, x=3)
            return x, x ** 2
        g = greenlet(run)
        self.assertEqual({'x': 3}, g.switch(3))
        self.assertEqual(((2,), {'x': 3}), g.switch())
        self.assertEqual((3, 9), g.switch())

    def test_switch_to_another_thread(self):
        data = {}
        error = None
        created_event = threading.Event()
        done_event = threading.Event()

        def run():
            data['g'] = greenlet(lambda: None)
            created_event.set()
            done_event.wait()
        thread = threading.Thread(target=run)
        thread.start()
        created_event.wait()
        try:
            data['g'].switch()
        except greenlet.error:
            error = sys.exc_info()[1]
        self.assertIsNotNone(error, "greenlet.error was not raised!")
        done_event.set()
        thread.join()

    def test_exc_state(self):
        def f():
            try:
                raise ValueError('fun')
            except: # pylint:disable=bare-except
                exc_info = sys.exc_info()
                greenlet(h).switch()
                self.assertEqual(exc_info, sys.exc_info())

        def h():
            self.assertEqual(sys.exc_info(), (None, None, None))

        greenlet(f).switch()

    def test_instance_dict(self):
        def f():
            greenlet.getcurrent().test = 42
        def deldict(g):
            del g.__dict__
        def setdict(g, value):
            g.__dict__ = value
        g = greenlet(f)
        self.assertEqual(g.__dict__, {})
        g.switch()
        self.assertEqual(g.test, 42)
        self.assertEqual(g.__dict__, {'test': 42})
        g.__dict__ = g.__dict__
        self.assertEqual(g.__dict__, {'test': 42})
        self.assertRaises(TypeError, deldict, g)
        self.assertRaises(TypeError, setdict, g, 42)

    def test_threaded_reparent(self):
        data = {}
        created_event = threading.Event()
        done_event = threading.Event()

        def run():
            data['g'] = greenlet(lambda: None)
            created_event.set()
            done_event.wait()

        def blank():
            greenlet.getcurrent().parent.switch()

        def setparent(g, value):
            g.parent = value

        thread = threading.Thread(target=run)
        thread.start()
        created_event.wait()
        g = greenlet(blank)
        g.switch()
        self.assertRaises(ValueError, setparent, g, data['g'])
        done_event.set()
        thread.join()

    def test_deepcopy(self):
        import copy
        self.assertRaises(TypeError, copy.copy, greenlet())
        self.assertRaises(TypeError, copy.deepcopy, greenlet())

    def test_parent_restored_on_kill(self):
        hub = greenlet(lambda: None)
        main = greenlet.getcurrent()
        result = []
        def worker():
            try:
                # Wait to be killed
                main.switch()
            except greenlet.GreenletExit:
                # Resurrect and switch to parent
                result.append(greenlet.getcurrent().parent)
                result.append(greenlet.getcurrent())
                hub.switch()
        g = greenlet(worker, parent=hub)
        g.switch()
        del g
        self.assertTrue(result)
        self.assertEqual(result[0], main)
        self.assertEqual(result[1].parent, hub)

    def test_parent_return_failure(self):
        # No run causes AttributeError on switch
        g1 = greenlet()
        # Greenlet that implicitly switches to parent
        g2 = greenlet(lambda: None, parent=g1)
        # AttributeError should propagate to us, no fatal errors
        self.assertRaises(AttributeError, g2.switch)

    def test_throw_exception_not_lost(self):
        class mygreenlet(greenlet):
            def __getattribute__(self, name):
                try:
                    raise Exception()
                except: # pylint:disable=bare-except
                    pass
                return greenlet.__getattribute__(self, name)
        g = mygreenlet(lambda: None)
        self.assertRaises(SomeError, g.throw, SomeError())

    def test_throw_doesnt_crash(self):
        result = []
        def worker():
            greenlet.getcurrent().parent.switch()
        def creator():
            g = greenlet(worker)
            g.switch()
            result.append(g)
        t = threading.Thread(target=creator)
        t.start()
        t.join()
        self.assertRaises(greenlet.error, result[0].throw, SomeError())

    def test_recursive_startup(self):
        class convoluted(greenlet):
            def __init__(self):
                greenlet.__init__(self)
                self.count = 0
            def __getattribute__(self, name):
                if name == 'run' and self.count == 0:
                    self.count = 1
                    self.switch(43)
                return greenlet.__getattribute__(self, name)
            def run(self, value):
                while True:
                    self.parent.switch(value)
        g = convoluted()
        self.assertEqual(g.switch(42), 43)

    def test_unexpected_reparenting(self):
        another = []
        def worker():
            g = greenlet(lambda: None)
            another.append(g)
            g.switch()
        t = threading.Thread(target=worker)
        t.start()
        t.join()
        class convoluted(greenlet):
            def __getattribute__(self, name):
                if name == 'run':
                    self.parent = another[0] # pylint:disable=attribute-defined-outside-init
                return greenlet.__getattribute__(self, name)
        g = convoluted(lambda: None)
        self.assertRaises(greenlet.error, g.switch)

    def test_threaded_updatecurrent(self):
        # released when main thread should execute
        lock1 = threading.Lock()
        lock1.acquire()
        # released when another thread should execute
        lock2 = threading.Lock()
        lock2.acquire()
        class finalized(object):
            def __del__(self):
                # happens while in green_updatecurrent() in main greenlet
                # should be very careful not to accidentally call it again
                # at the same time we must make sure another thread executes
                lock2.release()
                lock1.acquire()
                # now ts_current belongs to another thread
        def deallocator():
            greenlet.getcurrent().parent.switch()
        def fthread():
            lock2.acquire()
            greenlet.getcurrent()
            del g[0]
            lock1.release()
            lock2.acquire()
            greenlet.getcurrent()
            lock1.release()
        main = greenlet.getcurrent()
        g = [greenlet(deallocator)]
        g[0].bomb = finalized()
        g[0].switch()
        t = threading.Thread(target=fthread)
        t.start()
        # let another thread grab ts_current and deallocate g[0]
        lock2.release()
        lock1.acquire()
        # this is the corner stone
        # getcurrent() will notice that ts_current belongs to another thread
        # and start the update process, which would notice that g[0] should
        # be deallocated, and that will execute an object's finalizer. Now,
        # that object will let another thread run so it can grab ts_current
        # again, which would likely crash the interpreter if there's no
        # check for this case at the end of green_updatecurrent(). This test
        # passes if getcurrent() returns correct result, but it's likely
        # to randomly crash if it's not anyway.
        self.assertEqual(greenlet.getcurrent(), main)
        # wait for another thread to complete, just in case
        t.join()

    def test_dealloc_switch_args_not_lost(self):
        seen = []
        def worker():
            # wait for the value
            value = greenlet.getcurrent().parent.switch()
            # delete all references to ourself
            del worker[0]
            initiator.parent = greenlet.getcurrent().parent
            # switch to main with the value, but because
            # ts_current is the last reference to us we
            # return immediately
            try:
                greenlet.getcurrent().parent.switch(value)
            finally:
                seen.append(greenlet.getcurrent())
        def initiator():
            return 42 # implicitly falls thru to parent
        worker = [greenlet(worker)]
        worker[0].switch() # prime worker
        initiator = greenlet(initiator, worker[0])
        value = initiator.switch()
        self.assertTrue(seen)
        self.assertEqual(value, 42)



    def test_tuple_subclass(self):
        if sys.version_info[0] > 2:
            # There's no apply in Python 3.x
            def _apply(func, a, k):
                func(*a, **k)
        else:
            _apply = apply # pylint:disable=undefined-variable

        class mytuple(tuple):
            def __len__(self):
                greenlet.getcurrent().switch()
                return tuple.__len__(self)
        args = mytuple()
        kwargs = dict(a=42)
        def switchapply():
            _apply(greenlet.getcurrent().parent.switch, args, kwargs)
        g = greenlet(switchapply)
        self.assertEqual(g.switch(), kwargs)

    def test_abstract_subclasses(self):
        AbstractSubclass = ABCMeta(
            'AbstractSubclass',
            (greenlet,),
            {'run': abstractmethod(lambda self: None)})

        class BadSubclass(AbstractSubclass):
            pass

        class GoodSubclass(AbstractSubclass):
            def run(self):
                pass

        GoodSubclass() # should not raise
        self.assertRaises(TypeError, BadSubclass)

    def test_implicit_parent_with_threads(self):
        if not gc.isenabled():
            return # cannot test with disabled gc
        N = gc.get_threshold()[0]
        if N < 50:
            return # cannot test with such a small N
        def attempt():
            lock1 = threading.Lock()
            lock1.acquire()
            lock2 = threading.Lock()
            lock2.acquire()
            recycled = [False]
            def another_thread():
                lock1.acquire() # wait for gc
                greenlet.getcurrent() # update ts_current
                lock2.release() # release gc
            t = threading.Thread(target=another_thread)
            t.start()
            class gc_callback(object):
                def __del__(self):
                    lock1.release()
                    lock2.acquire()
                    recycled[0] = True
            class garbage(object):
                def __init__(self):
                    self.cycle = self
                    self.callback = gc_callback()
            l = []
            x = range(N*2)
            current = greenlet.getcurrent()
            g = garbage()
            for _ in x:
                g = None # lose reference to garbage
                if recycled[0]:
                    # gc callback called prematurely
                    t.join()
                    return False
                last = greenlet()
                if recycled[0]:
                    break # yes! gc called in green_new
                l.append(last) # increase allocation counter
            else:
                # gc callback not called when expected
                gc.collect()
                if recycled[0]:
                    t.join()
                return False
            self.assertEqual(last.parent, current)
            for g in l:
                self.assertEqual(g.parent, current)
            return True
        for _ in range(5):
            if attempt():
                break

    def test_issue_245_reference_counting_subclass_no_threads(self):
        # https://github.com/python-greenlet/greenlet/issues/245
        # Before the fix, this crashed pretty reliably on
        # Python 3.10, at least on macOS; but much less reliably on other
        # interpreters (memory layout must have changed).
        # The threaded test crashed more reliably on more interpreters.
        from greenlet import getcurrent
        from greenlet import GreenletExit

        class Greenlet(greenlet):
            pass

        initial_refs = sys.getrefcount(Greenlet)
        # This has to be an instance variable because
        # Python 2 raises a SyntaxError if we delete a local
        # variable referenced in an inner scope.
        self.glets = [] # pylint:disable=attribute-defined-outside-init

        def greenlet_main():
            try:
                getcurrent().parent.switch()
            except GreenletExit:
                self.glets.append(getcurrent())

        # Before the
        for _ in range(10):
            Greenlet(greenlet_main).switch()

        del self.glets
        self.assertEqual(sys.getrefcount(Greenlet), initial_refs)

    def test_issue_245_reference_counting_subclass_threads(self):
        # https://github.com/python-greenlet/greenlet/issues/245
        from threading import Thread
        from threading import Event

        from greenlet import getcurrent

        class MyGreenlet(greenlet):
            pass

        glets = []
        ref_cleared = Event()

        def greenlet_main():
            getcurrent().parent.switch()

        def thread_main(greenlet_running_event):
            mine = MyGreenlet(greenlet_main)
            glets.append(mine)
            # The greenlets being deleted must be active
            mine.switch()
            # Don't keep any reference to it in this thread
            del mine
            # Let main know we published our greenlet.
            greenlet_running_event.set()
            # Wait for main to let us know the references are
            # gone and the greenlet objects no longer reachable
            ref_cleared.wait()
            # The creating thread must call getcurrent() (or a few other
            # greenlet APIs) because that's when the thread-local list of dead
            # greenlets gets cleared.
            getcurrent()

        # We start with 3 references to the subclass:
        # - This module
        # - Its __mro__
        # - The __subclassess__ attribute of greenlet
        # - (If we call gc.get_referents(), we find four entries, including
        #   some other tuple ``(greenlet)`` that I'm not sure about but must be part
        #   of the machinery.)
        #
        # On Python 3.10 it's often enough to just run 3 threads; on Python 2.7,
        # more threads are needed, and the results are still
        # non-deterministic. Presumably the memory layouts are different
        initial_refs = sys.getrefcount(MyGreenlet)
        thread_ready_events = []
        for _ in range(
                initial_refs + 45
        ):
            event = Event()
            thread = Thread(target=thread_main, args=(event,))
            thread_ready_events.append(event)
            thread.start()


        for done_event in thread_ready_events:
            done_event.wait()


        del glets[:]
        ref_cleared.set()
        # Let any other thread run; it will crash the interpreter
        # if not fixed (or silently corrupt memory and we possibly crash
        # later).
        time.sleep(1)
        self.assertEqual(sys.getrefcount(MyGreenlet), initial_refs)


class TestRepr(unittest.TestCase):

    def assertEndsWith(self, got, suffix):
        self.assertTrue(got.endswith(suffix), (got, suffix))

    def test_main_while_running(self):
        r = repr(greenlet.getcurrent())
        self.assertEndsWith(r, " current active started main>")

    def test_main_in_background(self):
        main = greenlet.getcurrent()
        def run():
            return repr(main)

        g = greenlet(run)
        r = g.switch()
        self.assertEndsWith(r, ' suspended active started main>')

    def test_initial(self):
        r = repr(greenlet())
        self.assertEndsWith(r, ' pending>')

    def test_main_from_other_thread(self):
        main = greenlet.getcurrent()

        class T(threading.Thread):
            original_main = thread_main = None
            main_glet = None
            def run(self):
                self.original_main = repr(main)
                self.main_glet = greenlet.getcurrent()
                self.thread_main = repr(self.main_glet)

        t = T()
        t.start()
        t.join(10)

        self.assertEndsWith(t.original_main, ' suspended active started main>')
        self.assertEndsWith(t.thread_main, ' current active started main>')

        r = repr(t.main_glet)
        # main greenlets, even from dead threads, never really appear dead
        # TODO: Can we find a better way to differentiate that?
        assert not t.main_glet.dead
        self.assertEndsWith(r, ' suspended active started main>')

    def test_dead(self):
        g = greenlet(lambda: None)
        g.switch()
        self.assertEndsWith(repr(g), ' dead>')
        self.assertNotIn('suspended', repr(g))
        self.assertNotIn('started', repr(g))
        self.assertNotIn('active', repr(g))

    def test_formatting_produces_native_str(self):
        # https://github.com/python-greenlet/greenlet/issues/218
        # %s formatting on Python 2 was producing unicode, not str.

        g_dead = greenlet(lambda: None)
        g_not_started = greenlet(lambda: None)
        g_cur = greenlet.getcurrent()

        for g in g_dead, g_not_started, g_cur:

            self.assertIsInstance(
                '%s' % (g,),
                str
            )
            self.assertIsInstance(
                '%r' % (g,),
                str,
            )


if __name__ == '__main__':
    unittest.main()
