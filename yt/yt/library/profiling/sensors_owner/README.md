# Sensors owner

Это персистентный слой для NYT::NProfiling. TSensorsOwner содержит внутри себя TProfiler и в дополнение
владеет множеством объектов метрик с точки зрения времени их жизни.
TSensorsOwner может владеть другими TSensorsOwner. Имеет апи для получения "дочерних" объектов с метриками.

## Примеры использования

* Простейший пример использования:
```cpp
sensorsOwner.Inc(".my_simple_counter", 1);
```
Когда в конкретном месте нужно проинкрементить всего один счетчик.
Объект счетчика в этом случае создатся один раз и будет храниться внутри sensorsOwner.
Не рекомендуется для более сложных случаев.

* Инкремент метрик в функции:
```cpp
void DoSmth(/*... , */ const TSensorsOwner& sensorsOwner)
{
    // В функции можно прям по месту объявлять структуру с метриками и пользоваться.
    struct TSensors
    {
        NYT::NProfiling::TProfiler Profiler;
        NYT::NProfiling::TCounter TotalCount = Profiler.Counter(".count");
        NYT::NProfiling::TCounter FailedCount = Profiler.Counter(".failed_count");
    };
    // Тут одна и та же ссылка на объект метрик при условии, что в функцию передается один и тот же sensorsOwner.
    // Метод `.Get` достаточно эффективен, но всё же лучше не вызывать лишний раз.
    const auto& sensors = sensorsOwner.Get<TSensors>();

    //...
    bool failed = false;
    //...

    sensors.TotalCount.Increment(1);
    if (failed) {
        sensors.FailedCount.Increment(1);
    }
}
```

* Когда очень хочется конструировать дочерние метрики не только от профайлера и ключа:
```cpp
struct THistogramSensors
{
    NYT::NProfiling::TProfiler Profiler;
    int Key;
    std::vector<TDuration> Buckets;
    NYT::NProfiling::TEventTimer Histogram = Profiler.WithTag("tag", ToString(Key)).TimeHistogram(".another_counter", Buckets);
};

owner.Get<THistogramSensors>(/*Key*/ 132, /*Buckets*/ std::vector<TDuration>{5s, 10min}).Histogram.Record(6s);
```

* Можно и явно написать конструктор для структурки с метриками:
```cpp
struct TChildSensors
{
    NYT::NProfiling::TCounter Counter;

    TChildSensors(const NYT::NProfiling::TProfiler& p)
        : Counter(p.Counter(".my_counter_2"))
    { }
};
```

* Если структурку с метриками хочется куда-то дальше передавать и не беспокоиться о времени жизни:
```cpp
struct TSharedSensors final
{
    TProfiler Profiler;
    TCounter Counter = Profiler.Counter(".under_ptr_counter");
};
using TSharedSensorsPtr = NYT::TIntrusivePtr<TSharedSensors>;

owner.Get<TSharedSensorsPtr>()->Counter.Increment(1);
```

* TSensorsOwner мимикрирует под TProfiler в ряде моментов:
```cpp
auto subOwner = owner.WithPrefix("prefix.").WithTags(NYT::NProfiling::TTagSet().WithTag({"key", "value2"}));
```

## Когда использовать?

* При реализации логики на функциях и отсутствии необходимости иметь объекты метрик
(счетчиков и гистограмм как правило) вне функции.

* В случаях, когда время жизни метрик должно превышать время жизни основного использующего эти метрики класса.
Например, если при возникновении ошибки, вы хотите репортить метрику и разрушать класс,
то вам важно, чтобы объект метрики ошибки не умер сразу - иначе апдейт метрики скорее всего не успеет отрепортиться мониторингу.

* Когда вы просто не хотите, чтобы объекты метрик когда-либо разрушались.
В этом случае можно подвешивать все к GetRootSensorsOwner().
