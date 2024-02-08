## Memory tracker

https://a.yandex-team.ru/arc/trunk/arcadia/ydb/library/actors/util/memory_track.h

Использование:

* отслеживание аллокаций экземпляров конкретного класса через new/delete и new[]/delete[]
* отслеживание аллокаций в контейнерах
* ручное отслеживание моментов аллокации/деаллокации

----

### Отслеживание аллокаций класса через new/delete

Использование с автоматически генерируемой меткой:

```cpp
#include <ydb/library/actors/util/memory_track.h>

struct TTypeLabeled
    : public NActors::NMemory::TTrack<TTypeLabeled>
{
    char payload[16];
};
```

Использование с пользовательским именем метки:

```cpp
#include <ydb/library/actors/util/memory_track.h>

static const char NamedLabel[] = "NamedLabel";

struct TNameLabeled
    : public NActors::NMemory::TTrack<TNameLabeled, NamedLabel>
{
    char payload[32];
};
```

----

### Отслеживание аллокаций в контейнерах

```cpp
#include <ydb/library/actors/util/memory_track.h>

static const char InContainerLabel[] = "InContainerLabel";

struct TInContainer {
    char payload[16];
};

std::vector<TInContainer, NActors::NMemory::TAlloc<TInContainer>> vecT;

std::vector<TInContainer, NActors::NMemory::TAlloc<TInContainer, InContainerLabel>> vecN;

using TKey = int;

std::map<TKey, TInContainer, std::less<TKey>,
    NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>>> mapT;

std::map<TKey, TInContainer, std::less<TKey>,
    NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>, InContainerLabel>> mapN;

std::unordered_map<TKey, TInContainer, std::hash<TKey>, std::equal_to<TKey>,
    NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>>> umapT;

std::unordered_map<TKey, TInContainer, std::hash<TKey>, std::equal_to<TKey>,
    NActors::NMemory::TAlloc<std::pair<const TKey, TInContainer>, InContainerLabel>> umapN;
```

----

### Ручное отслеживание аллокаций/деаллокаций

```cpp
#include <ydb/library/actors/util/memory_track.h>

static const char ManualLabel[] = "ManualLabel";

...
NActors::NMemory::TLabel<ManualLabel>::Add(size);

...
NActors::NMemory::TLabel<ManualLabel>::Sub(size);
```

----

### Собираемые метрики

Сервис **utils**, пользовательская метка **label**, сенсоры:

- MT/Count: количество аллокаций в моменте
- MT/Memory: аллоцированная память в моменте
- MT/PeakCount: пиковое значение количества аллокаций (сэмплится с фиксированной частотой)
- MT/PeakMemory: пиковое значение аллоцированной памяти

