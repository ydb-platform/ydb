## GEOS is a port of JTS

* The algorithms that form the core value of GEOS are developed in Java in the [JTS library](https://github.com/locationtech/jts/). C++ developers will find this annoying, but:

  * This is just history, JTS was written first and GEOS was a slavish port.
  * Being memory managed, JTS is an easier language to prototype in.
  * Having various visual tooling, JTS is an easier platform to debug spatial algorithms in.
  * Being Java, JTS has less language legacy than GEOS, which was originally ported when STL was still not part of the standard, and therefor reflects a mix of styles and eras.

* Ideally, new algorithms will be implemented in JTS and then ported to GEOS.
* Smaller performance optimizations in GEOS can travel back to JTS.

  * Short circuits, indexes, other non-language optimizations, should be ticketed in JTS when they are added to GEOS.

### Follow JTS as Much as Possible

* Don't rename things! It makes it harder to port updates and fixes.

  * Class names
  * Method names
  * Variable names
  * Class members

    * Yes, we know in your last job you were taught all member variables are prefixed with `m_`, but please don't.



### Manage Lifecycles

* Frequently objects are only used local to a method and not returned to the caller.
* In such cases, avoid lifecycle issues entirely by **instantiating on the local stack**.

```java
MyObj foo = new MyObj("bar");
```

```c++
MyObj foo("bar");
```

* Long-lived members of objects that are passed around should be held using [std::unique_ptr<>](https://en.cppreference.com/w/cpp/memory/unique_ptr).

```java
private MyMember foo = new MyMember();
```

```c++
private:

   std::unique_ptr<MyMember> foo;

public:

    MyMember()
        : foo(new MyMember())
        {}
```

* You can pass pointers to the object to other methods using `std::unique_ptr<>.get()`.

### Avoid Many Small Heap Allocations

* Heap allocations (objects created using `new`) are more expensive than stack allocations, but they can show up in batchs in JTS in places where structures are built, like index trees, or graphs.
* To both lower the overhead of heap allocations, and to manage the life-cycle of the objects, we recommend storing small objects in an appropriate "double-ended queue", like [std::deque<>](https://en.cppreference.com/w/cpp/container/deque).
* The implementation of `edgegraph` is an example.

  * The `edgegraph` consists of a structure of many `HalfEdge` objects (two for each edge!), created in the `EdgeGraph::createEdge()` method and stored in a `std::deque<>`.
  * The `std::deque<>` provides two benefits:

    * It lowers the number of heap allocations, because it allocates larger blocks of space to store multiple `HalfEdge` objects.
    * It handles the lifecycle of the `HalfEdge` objects that make up the `EdgeGraph`, because when the `EdgeGraph` is deallocated, the `std::deque<>` and all its contents are also automatically deallocated.


