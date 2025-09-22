# Sensors owner

This is a persistent layer for NYT::NProfiling. `TSensorsOwner` contains a `TProfiler` object and additionally owns metric objects and managed their lifetimes.

`TSensorsOwner` can own other `TSensorsOwner` instances. It has an API for obtaining "child" objects with metrics.

## Usage examples

* Simplest usage example:

```cpp
sensorsOwner.Increment("/my_simple_counter", 1);
```

In this example just one counter is incremented. Thid counter object will be created once and stored inside sensorsOwner.  

This approach is not recommended for more complex cases.

* Incrementing metrics inside a function:

```cpp
void DoSmth(/*... , */ const TSensorsOwner& sensorsOwner)
{
    // You can declare a structure with metrics inline in the function and use it.
    struct TSensors
    {
        NYT::NProfiling::TProfiler Profiler;
        NYT::NProfiling::TCounter TotalCount = Profiler.Counter("/total_count");
        NYT::NProfiling::TCounter FailedCount = Profiler.Counter("/failed_count");
    };
    // Here is the same reference to the metric object assuming the same sensorsOwner is passed to the function.
    // The `.Get` method is quite efficient but it's better not to call it unnecessarily.
    const auto& sensors = sensorsOwner.Get<TSensors>();

    // ...
    bool failed = false;
    // ...

    sensors.TotalCount.Increment(1);
    if (failed) {
        sensors.FailedCount.Increment(1);
    }
}
```

* When you need to construct child metrics not by specifying not only the profiler and key:
```cpp
struct THistogramSensors
{
    NYT::NProfiling::TProfiler Profiler;
    int Key;
    std::vector<TDuration> Buckets;
    NYT::NProfiling::TEventTimer Histogram = Profiler.WithTag("tag", ToString(Key)).TimeHistogram("/another_counter", Buckets);
};

owner.Get<THistogramSensors>(/*Key*/ 132, /*Buckets*/ std::vector<TDuration>{5s, 10min}).Histogram.Record(6s);
```

* It is allowed to explicitly imlement a constructor for the structure with metrics:
```cpp
struct TChildSensors
{
    NYT::NProfiling::TCounter Counter;

    TChildSensors(const NYT::NProfiling::TProfiler& p)
        : Counter(p.Counter("/my_counter_2"))
    { }
};
```

* If you want to pass a metrics structure somewhere else and not worry about its lifetime:
```cpp
struct TSharedSensors final
{
    TProfiler Profiler;
    TCounter Counter = Profiler.Counter("/under_ptr_counter");
};
using TSharedSensorsPtr = NYT::TIntrusivePtr<TSharedSensors>;

owner.Get<TSharedSensorsPtr>()->Counter.Increment(1);
```

* `TSensorsOwner` mimics `TProfiler` in a number of ways:
```cpp
auto subOwner = owner.WithPrefix("/prefix").WithTags(NYT::NProfiling::TTagSet().WithTag({"key", "value2"}));
```

## When to use?

* When implementing logic inside functions and there is no need to have metric objects (counters and histograms usually) outside the function.

* In cases where the lifetime of the metrics must exceed the lifetime of the main class using these metrics.  

For example, if you want to report a metric upon an error and destroy the class,  
it is important that the error metric object does not die immediately. Otherwies the metric update will likely not have time to be reported to monitoring.

* When you simply do not want metric objects to be destroyed.  

In this case, you can attach everything to `GetRootSensorsOwner()`.

