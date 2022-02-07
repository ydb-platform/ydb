# TSlidingWindow - скользящее окно

[TSlidingWindow](/arc/trunk/arcadia/library/cpp/sliding_window/sliding_window.h) - класс скользящего окна, позволяющий поддерживать и обновлять определённое значение (максимум, сумму) в промежутке времени определённой длины. Разбивает общий временной промежуток на маленькие бакеты (число задаётся в конструкторе) и ротирует их, поддерживая значение за окно. Есть возможность также указать мьютекс или спинлок для синхронизации (по умолчанию TFakeMutex). Использование:

```
// Создаём окно, вычисляющее максимум за последние пять минут, поддерживая 50 бакетов со значениями.
TSlidingWindow<TMaxOperation<int>> window(TDuration::Minutes(5), 50);

// Загружаем значения в различные моменты времени
window.Update(42, TInstant::Now());

... // делаем какую-то работу
int currentMaximum = window.Update(50, TInstant::Now());

... // делаем ещё что-то
int currentMaximum = window.Update(25, TInstant::Now());

...
// Просто получаем значение максимума за последние 5 минут
int currentMaximum = window.Update(TInstant::Now());

...
int currentMaximum = window.GetValue(); // получение значения без обновления времени
```

# Поддерживаемые функции

* `TMaxOperation` - максимум
* `TMinOperation` - минимум
* `TSumOperation` - сумма
