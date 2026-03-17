heapdict: a heap with decreased-key and increase-key operations
===============================================================

heapdict implements the MutableMapping ABC, meaning it works pretty
much like a regular Python dict.  It's designed to be used as a
priority queue, where items are added and consumed as follows:

::

    hd = heapdict()
    hd[obj1] = priority1
    hd[obj2] = priority2
    # ...
    (obj, priority) = hd.popitem()

Compared to an ordinary dict, a heapdict has the following differences:

popitem():
    Remove and return the (key, priority) pair with the lowest
    priority, instead of a random object.

peekitem():
    Return the (key, priority) pair with the lowest priority, without
    removing it.

Unlike the Python standard library's heapq module, the heapdict
supports efficiently changing the priority of an existing object
(often called "decrease-key" in textbooks).  Altering the priority is
important for many algorithms such as Dijkstra's Algorithm and A*.

