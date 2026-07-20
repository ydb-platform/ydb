# Восстановление TEvSlay после рестарта PDisk

## Состав каталога

Каждая версия хранится как самостоятельная Promela-модель без условной
компиляции:

| Версия | Модель | Результат |
| --- | --- | --- |
| `v0` — старый NodeWarden | `slay_recovery_v0.pml` | `slay_recovery_v0.trace` — counterexample упавшего safety-assert |
| `v1` — исправленный NodeWarden | `slay_recovery_v1.pml` | `slay_recovery_v1.safety.out` и `slay_recovery_v1.liveness.out` — успешные прогоны |

Если проверка версии прошла, рядом сохраняется полный output verifier. Если
Spin нашёл ошибку, сохраняется replay trail с последовательностью действий и
служебным хвостом.

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

Модель содержит BSC, NodeWarden, VDisk и PDisk для одного VSlot. BSC хранит
персистентные `Mood`, актуальный VDisk identity и признак того, что VSlotId
выделен. PDisk хранит физического owner. NodeWarden применяет ServiceSet и
держит одну операцию `TEvSlay`.

Spin недетерминированно перебирает:

1. Прямой `DESTROY`.
2. `WIPE` для одного VDisk identity, за которым приходит `DESTROY` для более
   нового identity, пока WIPE может оставаться in-flight.
3. Рестарт PDisk, инициированный BSC через `ServiceSet(RESTART)`.
4. Рестарт по цепочке `TEvAskWardenRestartPDisk` от PDisk через NodeWarden и
   BSC, после чего BSC присылает `ServiceSet(RESTART)`.
5. Отсутствие control-plane рестарта, отдельные рестарты BSC или NodeWarden и
   оба порядка этих рестартов.

`WIPED` снимает `Mood::Wipe`, но не освобождает VSlotId. Только `DESTROYED` с
актуальным VDisk identity освобождает VSlotId. Устаревший report игнорируется,
как в `TTxNodeReport::Execute`.

## Как моделируется потеря сообщения

PDisk описан состоянием, а не заранее заданным сценарием:

- `pdisk_state` принимает `PDISK_RUNNING` или `PDISK_RESTARTING`;
- `pdisk_epoch` обозначает incarnation PDisk-актора;
- в `PDISK_RUNNING` обычная обработка `TEvSlay` всегда разрешена;
- рестарт уничтожает mailbox старой incarnation, увеличивает `pdisk_epoch` и
  запускает новый актор.

ScenarioDriver лишь отправляет запрос рестарта. Дальше Spin сам выбирает гонку:
успеет старый актор обработать `TEvSlay` или рестарт уничтожит сообщение в его
mailbox. Сообщение не теряется произвольным nondeterministic drop: потеря
происходит только на границе жизни акторов.

После единственного инжектированного рестарта среда считается надёжной. Это
позволяет отличить ошибку recovery от бесконечной последовательности внешних
сбоев.

## Различие v0 и v1

- `v0` хранит для slay только round. После `DESTROY` VDisk уже отсутствует в
  `LocalVDisks`, поэтому старый callback рестарта не знает, что нужно повторить.
  При `WIPE -> DESTROY` старая операция также не сохраняет новое действие и
  identity.
- `v1` хранит `{VDiskId, Action, Round}`. `DESTROY` повышает in-flight WIPE до
  DESTROY и выпускает новый round, а callback рестарта повторяет сохранённую
  операцию независимо от `LocalVDisks`.

## Проверяемые свойства

После callback рестарта каждая операция NodeWarden должна быть либо уже
обработана своей incarnation PDisk, либо переотправлена текущей incarnation:

```promela
assert(!nw_slay_inflight ||
    (pdisk_last_processed_epoch == nw_slay_target_epoch &&
        pdisk_last_processed_round == nw_round) ||
    nw_slay_target_epoch == pdisk_epoch)
```

Проверка по длине очереди была бы неверна: PDisk мог уже обработать запрос, а
result мог ожидать NodeWarden в другой очереди.

BSC не может освободить VSlot, пока физический owner ещё существует:

```promela
assert(!pdisk_owner_present)
```

И главное liveness-свойство для утечки VSlotId:

```promela
ltl live_delete_eventually_frees_vslot {
    [] (bsc_delete_pending -> <> !bsc_vslot_allocated)
}
```

Оно проверяет и прямой DESTROY, и `WIPE -> DESTROY`: pending DELETE обязан
когда-нибудь закончиться освобождением BSC-side VSlotId.

## v0: воспроизведение ошибки

```bash
spin -a slay_recovery_v0.pml
cc -O2 -w -DSAFETY -DNOCLAIM -o pan pan.c
./pan -E -m100000
./pan -r -S > slay_recovery_v0.trace
```

Ожидаемый результат — `errors: 1`. В сохранённом trace старый PDisk-актор
теряет `TEvSlay` epoch 1, новый запускается с epoch 2, но v0 не делает replay,
поскольку удалённого VDisk уже нет в `LocalVDisks`. Assert падает на 46-м
шаге.

## v1: успешные прогоны

Safety:

```bash
spin -a slay_recovery_v1.pml
cc -O2 -w -DSAFETY -DNOCLAIM -o pan pan.c
./pan -E -m100000 > slay_recovery_v1.safety.out
```

Сохранённый прогон: 384577 states stored, 906437 transitions, `errors: 0`.

Liveness с weak process fairness:

```bash
spin -a slay_recovery_v1.pml
cc -O2 -w -DNFAIR=8 -o pan pan.c
./pan -a -f -E -m100000 > slay_recovery_v1.liveness.out
```

Сохранённый прогон: 3656440 states visited, 11179975 transitions, `errors: 0`.
`unreached in proctype ScenarioDriver (0 of 45 states)` подтверждает, что Spin
посетил обе операции, оба источника рестарта PDisk и все варианты рестартов
BSC/NodeWarden. Недостижимый resync WIPE ожидаем: control-plane рестарты
инжектируются только после финального DESTROY. Actor `-end-` также недостижим,
поскольку акторы намеренно остаются в receive-loop; `-E` отключает invalid end
states.

## Ограничения

Модели ограничены одним VSlot и одним инжектированным рестартом PDisk. Они не
моделируют protobuf, журналирование YDB, метрики, retry timer и физический I/O.
BSC restart сохраняет desired state, а полный NodeWarden restart теряет
volatile state и выполняет `RegisterNode` — именно эти свойства существенны
для рассматриваемого recovery.
