# Восстановление TEvSlay после рестарта PDisk

## Состав каталога

Каждая версия хранится как самостоятельная Promela-модель без условной
компиляции:

| Версия | Модель | Результат |
| --- | --- | --- |
| `v0` — старый NodeWarden | `slay_recovery_v0.pml` | `slay_recovery_v0.trace` — counterexample упавшего safety-assert |
| `v1` — исправленный NodeWarden | `slay_recovery_v1.pml` | `slay_recovery_v1.safety.out` и `slay_recovery_v1.liveness.out` — успешные прогоны |

Правило для следующих версий такое же: если проверка прошла, рядом сохраняется
полный output verifier; если обнаружена ошибка, сохраняется replay trail с
последовательностью действий и служебным хвостом Spin.

## Установка

Ubuntu:

```bash
sudo apt install spin gcc
```

macOS:

```bash
brew install spin gcc
```

## Что моделируется

Обе модели описывают один запрос BSC на удаление VSlot и его прохождение через
BSC, NodeWarden, VDisk и PDisk. Первый `TEvSlay` теряется при рестарте PDisk.
После этого новых потерь нет и все повторно отправленные сообщения
доставляются.

Spin недетерминированно перебирает оба источника рестарта из основного кода:

1. BSC сам отправляет `ServiceSet(RESTART)` в NodeWarden.
2. PDisk отправляет `TEvAskWardenRestartPDisk`; NodeWarden запрашивает
   `TRestartPDisk` у BSC, а BSC возвращает `ServiceSet(RESTART)`.

После потери первого `TEvSlay` Spin также перебирает отсутствие рестарта
control plane, рестарт BSC с resync, полный рестарт NodeWarden с `RegisterNode`
и оба рестарта в обоих порядках.

Различается только recovery-логика:

- `v0` ищет операции для replay через `LocalVDisks`; удалённый VDisk там уже
  отсутствует;
- `v1` хранит identity и действие в `SlayInFlight` и повторяет `TEvSlay`
  независимо от `LocalVDisks`.

## Проверяемые свойства

Safety-инвариант после успешного callback рестарта:

```promela
assert(!nw_slay_inflight || len(nw_to_pdisk) > 0)
```

Если slay всё ещё считается in-flight, новый запрос уже должен находиться в
очереди PDisk. Второй assert запрещает BSC принять `DESTROYED`, пока owner ещё
физически присутствует:

```promela
assert(!pdisk_owner_present)
```

Eventual completion нельзя проверить point-in-time assert, поэтому оно
остаётся LTL-свойством:

```promela
ltl live_destroy_eventually_reported {
    [] (scenario_finished && bsc_pending -> <> bsc_reported_destroyed)
}
```

## v0: воспроизведение ошибки

```bash
spin -a slay_recovery_v0.pml
cc -O2 -w -DSAFETY -DNOCLAIM -o pan pan.c
./pan -E -m100000
./pan -r -S > slay_recovery_v0.trace
```

Ожидаемый результат — `errors: 1`. В `slay_recovery_v0.trace` видно, что
рестарт удаляет первый `TEvSlay`, старый callback не отправляет новый запрос и
assert падает на 39-м шаге.

## v1: успешные прогоны

Safety:

```bash
spin -a slay_recovery_v1.pml
cc -O2 -w -DSAFETY -DNOCLAIM -o pan pan.c
./pan -E -m100000 > slay_recovery_v1.safety.out
```

Liveness с weak process fairness:

```bash
spin -a slay_recovery_v1.pml
cc -O2 -w -DNFAIR=8 -o pan pan.c
./pan -a -f -E -m100000 > slay_recovery_v1.liveness.out
```

Оба сохранённых прогона завершились с `errors: 0`. В них
`unreached in proctype ScenarioDriver (0 of 30 states)`, то есть Spin посетил
все варианты источника рестарта PDisk и рестартов BSC/NodeWarden.

Флаг `-E` отключает сообщения про invalid end states: акторы модели намеренно
остаются в receive-loop после завершения единственной операции. Оставшиеся
actor `-end-` в разделе `unreached` поэтому нормальны.

## Ограничения

Модели ограничены одним VSlot, одним теряемым `TEvSlay` и одним рестартом
PDisk. Они не моделируют protobuf, логирование YDB, метрики и физический I/O.
BSC restart сохраняет desired state, а NodeWarden restart теряет volatile
state и выполняет `RegisterNode` — именно эти свойства важны для recovery.
