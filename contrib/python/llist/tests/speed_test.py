#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import deque
from llist import sllist, dllist
import time
# import gc
# gc.set_debug(gc.DEBUG_UNCOLLECTABLE | gc.DEBUG_STATS)

num = 10000


def append(c):
    for i in range(num):
        c.append(i)


def appendleft(c):
    for i in range(num):
        c.appendleft(i)


def pop(c):
    for i in range(num):
        c.pop()


def popleft(c):
    if isinstance(c, deque):
        for i in range(num):
            c.popleft()
    else:
        for i in range(num):
            c.pop()


def remove(c):
    for i in range(0, num, 2):
        try:
            c.remove(num)
        except:
            pass


def index_iter(c):
    for i in range(len(c)):
        c[i]


for container in [deque, dllist, sllist]:
    for operation in [append, appendleft, pop, popleft, remove, index_iter]:
        c = container(range(num))
        start = time.time()
        operation(c)
        elapsed = time.time() - start
        print("Completed %s/%s in \t\t%.8f seconds:\t %.1f ops/sec" % (
            container.__name__,
            operation.__name__,
            elapsed,
            num / elapsed))
