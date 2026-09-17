# CutHistory: один проход после Boot

## Задача

Заменить сложный CutHistory простым доказательством пустоты истории на каждом Boot. Пользователь требует реализацию с нуля поверх текущего main, только CutHistory, через subagent с reasoning effort medium в отдельном worktree. База: `f6542ac7f6773a9bfc3b3a82aea71dc904ca1041`, получена непосредственно из upstream main. Не переносить незамерженные изменения MoveData или зависеть от их API, flags, counters, tests. Предыдущий MoveData-based worktree не является deliverable.

## Алгоритм

1. После загрузки tablet и нормализаторов собрать закрытые интервалы истории data channels >= 2. Если их нет, ничего не читать. Активную последнюю запись не трогать.
2. Зафиксировать конечный список существующих порций: committed, uncommitted и removed до физического cleanup. Обходить небольшими пачками по `(PathId, PortionId)` — это физический ключ таблиц. Внутри каждого PathId порядок PortionId возрастает.
3. Читать metadata через существующий DataAccessorsManager/cache: кешированные accessors переиспользуются, промахи загружают IndexColumnsV2 и IndexIndexes. Не читать payload data blobs. Проверять DEFAULT storage на уровне column/index, поскольку индекс tiered-порции может оставаться в DEFAULT. Чужой TabletId не отмечает нашу историю.
4. На закрытый интервал хранить один бит NonEmpty. Найдена хотя бы одна наша DEFAULT-ссылка — установить бит. Точное число, дедупликация между порциями и обратные обновления при удалении не нужны.
5. Только после полного успешного прохода нулевые интервалы становятся кандидатами на cut. Перед отправкой повторно проверить точный интервал и существующие GC/sharing prerequisites: keep/delete/delayed/shared references, любой GC task in flight, persisted LastCollected barrier, покрывающий закрытый интервал. Новый GC текущей incarnation не обязателен, если прежний barrier уже покрывает интервал. GC очереди не включать в постоянные биты прохода.
6. Ожидание GC повторяет только проверку live GC/sharing state, не metadata scan. После crash следующий Boot начинает проход заново. Интервал, отмеченный NonEmpty, откладывается до следующего Boot. CutHistory не переписывает данные и не реализует эвакуацию групп.

## Минимальные требования корректности

- Неполный ответ, ошибка чтения или неоднозначно исчезнувшая порция не доказывают пустоту. Пропуск физически удалённой порции допустим только после публикации cleanup в Complete: её старые blob IDs уже защищены GC/reader bookkeeping.
- В существующем TTxAskPortionChunks результат парсинга передаётся из Execute. Передавать его после Complete, чтобы результат не обгонял cleanup publication.
- При page fault в IndexIndexes не устанавливать SetIndexes для неполного prefix: retry обязан дочитать индексы.
- Ordinary writes создают blob IDs текущего поколения; они не пополняют закрытые интервалы. При существующих resumable sharing sessions не авторизовать boot proof; новое sharing во время прохода отменяет proof. Не строить новый протокол версий/сертификатов и не запрещать все новые sharing sessions после обычного cut.
- Историю сопоставлять по channel/from/to/group, не по изменяемому номеру j. Повторное использование группы не запрещает удалять пустой интервал: cutter не посылает hard barrier, ради которого executor проверяет более ранние записи той же группы. Отправка cut не равна подтверждению Hive; допустим следующий Boot для повтора.
- Channels 0/1 остаются у executor. MoveData и executor vacuum не изменять. Использовать существующий main flag EnableCutHistory; не переносить EnableColumnshardGroupDecommission.
- Admission sharing фиксировать до постановки транзакции в очередь, включая отдельный TEvApplyLinksModification. При старте пропустить proof, если уже были admissions или persisted sessions. Во время scan/ожидания GC разрешать sharing; перед каждой отправкой проверять, что admissions не было. Если sharing успел начаться, отказаться от оставшегося proof. Не вводить post-cut latch, отказ фабрик сессий или новый протокол. Проверять live shared/borrowed ссылки собственных DEFAULT blobs; отдельный GC-путь TEvDeleteSharedBlobs продолжает работать.
- Ограничение совместимости: ownership-moving sharing не входит в CutHistory-only patch. Компонентный аудит MOVE обнаружил, что BorrowedBlobIds может исчезнуть на physical-origin tablet после удаления ссылки у нового owner, хотя сам blob остаётся нужен remote owner и защищён Keep. Локальный soft GC barrier этого не исключает. На текущем main заполнение source portions для sharing закомментировано; это не доказательство долговременной безопасности. Не заявлять совместимость с завершённым ownership transfer, пока physical-origin retention invariant не установлен. Отмена running proof по admissions решает только конкурентность, не этот исторический случай. Такое ограничение объявлено пользователю; расширение ownership bookkeeping требует отдельного изменения scope.

## Наблюдаемость

- Использовать штатные sensors: число отправок CutHistory и число записей, запрошенных к удалению (один sensor достаточен, если одно событие всегда удаляет одну запись).
- Измерять длительность завершённого metadata scan и ожидания готовности GC после scan. Явно определить начало и конец измерений; отправка не означает подтверждённый cut от Hive.
- Хранить ограниченный журнал отправленных событий в памяти и показывать через EvHttpInfo: время, канал, границы поколения, группа и фактический адресат. После Boot журнал начинается заново; новая LocalDB-таблица не нужна.
- Проверить sensors/monitoring в существующем реальном cut-сценарии, если fixture позволяет; не добавлять отдельную дублирующую матрицу.

## Ресурсы и простота

- Один metadata batch in flight; маленький фиксированный предел количества/оценки metadata и уступка между пачками через штатный scheduler.
- Переиспользовать существующие cache и memory controls. Не вводить новый кеш, адаптивный контроллер или бесконечный precharge.
- Не удерживать все прочитанные accessors: освобождать ссылки после batch; кеш управляет памятью сам.
- Порядок должен сохраняться в пути cache-miss loader, а не только при сортировке входного вектора перед помещением в hash container.
- Snapshot требует O(P) памяти и O(P log P) сортировки до SignalTabletActive. Предел cache batch не ограничивает эту boot-работу. После быстрых отказов по stopped/in-flight/barrier итоговая проверка может стоить O(I × Q), где I — число кандидатов, Q — размер pending/shared queues; жёсткого time budget для неё нет.
- Нулевую стоимость сканирования не обещать. Проверить ограничение пачек, отзывчивость чтения/записи и сообщить, если production p99/throughput ещё не измерены.
- Нет persisted MoveData rows, exact reference counters, seed transaction, audit, per-portion observers или дублирующего proof path.
- Комментарии в коде — только для неочевидных деталей, не длиннее одной строки; проверить это при self-review и независимом review.

## Проверка

Небольшой набор тестов реального поведения: пустой/живой интервал и активная запись; GC задерживает cut после завершения scan; DEFAULT index tiered-порции и uncommitted data сохраняют историю; несколько пачек используют cache и дают foreground работе выполняться. Проверку отсутствия истории включить в существующий fixture. Не создавать параллельную большую матрицу тестов той же логики.

Сначала минимальный failing integration regression на самостоятельном CutHistory fixture из доступных в main test helpers, затем реализация и focused tests. Проверить, что ранее завершённого barrier достаточно после reboot; задержку проверять только при действительно непокрытом интервале/незавершённом GC. Команды проекта: `./ya make --build relwithdebinfo -tA <target> -F '<filter>'`; без `-j`, без force rebuild, без изменения исходников во время build.

## База и границы патча

- Branch codex/cuthistory-main; worktree /home/kkhamitov/git/ydb/.claude/worktrees/codex-cuthistory-main. Никаких cherry-pick цепочек MoveData.
- Минимально выделить существующую отправку metadata requests из SetupMetadata, чтобы scan использовал тот же broker/cache. Не переносить MoveDataTaskSubscription и изменения actualizer/TTL/cleanup, не требуемые CutHistory.
- Добавить только отсутствующие в main range/barrier/GC-in-flight predicates, необходимые cutter. Не добавлять group-drain API, first-GC policy MoveData или executor changes.
- Тесты не вызывают TEvMoveData и не используют ut_movedata. Пустой/очищенный интервал создаётся обычным boot, записью, cleanup/GC и reassignment средствами fixture. Живые portion blobs обязаны сохраняться без переписывания cutter.
- Проверить diff и ancestry от указанного main: только CutHistory, необходимая metadata/GC/sharing integration, sensors, EvHttpInfo, docs и focused tests.
