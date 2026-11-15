# YDB: история развития и актуальная дорожная карта

**Срез: 15 ноября 2026 года.** Внутренний рабочий документ, восстановленный по Wiki, Tracker и документации в этом checkout. Это сводка существующих планов и результатов, а не новое обязательство команд по срокам.

Исходная точка — [ROADMAP.md](ROADMAP.md). Метки ㉓ и ㉔ в нём означают год **появления в плане**, а не год реализации. Например, версия **24.4.4.2 вышла 15 апреля 2025 года**, а 25.2.1.26 — 12 мая 2026 года. Ни номер ветки, ни дата закрытия эпика сами по себе не являются датой публичного выпуска. Даты релизов ниже взяты из [списка изменений сервера][releases].

## Как читать статусы

| Метка | Значение |
|---|---|
| ✅ | Подтверждён выпуск описанного объёма функциональности. Ограничения указаны рядом. |
| 🧪 | Реализовано в main, доступно под флагом, в пилоте или в релизе-кандидате. Общая промышленная доступность не предполагается. |
| 🚧 | Направление реализовано частично; остаётся существенная работа. |
| 📌 | Есть план, запрос или backlog. Сам по себе этот статус не обещает срок. |
| ❓ | Найденных доказательств недостаточно для определения полного статуса исходной формулировки. |
| ❌ | Отказ в исходном roadmap; нового решения о возобновлении не установлено. |

В Tracker отдельно приведён фактический статус карточки. `includedAsCodeReady`, `enabledByDefault`, `commited`, `resolved` и `closed` не сведены в одно «сделано». Закрытие с `duplicate` или `won'tFix` не считается реализацией. Для доступности продукта release notes и описание ограничений важнее состояния старого зонтичного тикета; для текущей незавершённой работы важнее свежая карточка, чем старый флажок в Wiki.

## Что существенно изменилось относительно исходного файла

- Выпущены базовые уникальные индексы, автоматический выбор индексов, константные значения DEFAULT, запросы без обязательного DECLARE, VIEW, ANN-индекс, волатильные транзакции, дисковый spilling, трансферы из топиков в таблицы и транзакции топик–таблица. Колоночные таблицы прошли путь от preview до доступной по умолчанию аналитики. [Release notes][releases]; [KIKIMR-8192](https://st.yandex-team.ru/KIKIMR-8192).
- CBO уже работает по умолчанию для запросов с колоночными таблицами. Новый оптимизатор, полная декорреляция, OLTP cost model и динамические фильтры — следующий этап, а не отсутствие CBO как такового. [Release notes, 25.2][releases]; [план оптимизатора][optimizer]; [QP Monthly 21.09.2026][qp-monthly].
- PostgreSQL wire protocol и часть SQL-совместимости существуют давно. Совместимость с прикладным ПО, прежде всего 1С, остаётся отдельным действующим проектом: [YDB-2975](https://st.yandex-team.ru/YDB-2975) — `inProgress`. Read Committed и новый RBO описаны как работающие в main/PG Proxy; это не равнозначно полной совместимости с PostgreSQL. [QP Monthly, с. 3–8][qp-monthly].
- Первая версия асинхронной репликации выпущена. Старый [KIKIMR-12336](https://st.yandex-team.ru/KIKIMR-12336) закрыт с `fixed`, но новый [мастер-план KIKIMR-27574](https://st.yandex-team.ru/KIKIMR-27574) — `inProgress`: стабильность, матрица объектов, GLOBAL-режим, изменения схемы и удобство миграций ещё требуют работы.
- **Incremental backup и PITR необходимо разделять.** В 26.2 инкрементальные копии присутствуют, но выключены по умолчанию. В актуальном [KIKIMR-27150](https://st.yandex-team.ru/KIKIMR-27150) первая версия прямо ограничена восстановлением на последнюю точку цепочки, строковыми таблицами и копированием внутри кластера; PITR и поддержка изменений схемы в v0 отсутствуют.
- Автосплит топиков выпущен; автоматическое уменьшение числа партиций ещё кандидат в план. «Topics autoscaling: increase/decrease» поэтому выполнено частично. [Release notes, 24.3/24.4][releases]; [LOGBROKER-10478](https://st.yandex-team.ru/LOGBROKER-10478).
- Заметки `Conf V2` и `Correlated Subqueries` из конца исходного файла включены в актуальные направления ниже. Конфигурация V2 уже существует, но миграция флота и эксплуатационная готовность продолжаются; коррелированные подзапросы поддержаны новым оптимизатором, но его полный rollout не завершён. [План эксплуатации H2 2026][ops-h2]; [QP Monthly][qp-monthly].

## История по календарным годам

### 2020 — базовые механизмы масштабируемой транзакционной БД

**Подтверждённые результаты:** персистентные мультиверсионные snapshot — [KIKIMR-7006](https://st.yandex-team.ru/KIKIMR-7006), закрыт 15.05.2020; построение вторичного индекса по существующей таблице — [KIKIMR-6940](https://st.yandex-team.ru/KIKIMR-6940), 28.09.2020; graceful restart процессов/таблеток — [KIKIMR-10027](https://st.yandex-team.ru/KIKIMR-10027), 29.09.2020; TTL — [KIKIMR-4683](https://st.yandex-team.ru/KIKIMR-4683), 26.10.2020. Это даты закрытия задач с `fixed`, не восстановленные даты релизов.

**Дополнительные направления того периода:** системные представления, export/restore S3, выделение ресурсов и Hive, индексы, Serverless, обработка нехватки места. [Архив июля][archive-2020a] и [архив октября][archive-2020b] помогают восстановить набор работ, но не доказывают дату их выпуска. Например, TTL уже находится в архиве от 20 октября, хотя карточка закрыта 26 октября; часть архивных зонтиков остаётся открытой и сейчас.

### 2021 — расширение экосистемы и сценариев использования

[Отчёт сезона Q2–Q3][report-2021] перечисляет Snapshot Isolation, JDBC, SQLAlchemy, PHP SDK, расширение C#/async Python, интеграцию с DataLens, аналитические таблицы и облачное хранение логов, provisioned pricing, sequences, move/rename, асинхронные и покрывающие индексы, QueryReplay, работу над Kubernetes/AWS/on-prem.

Отчёт содержит одновременно результаты и анонсы: C#/async Python и развёртывание в AWS/on-prem там описаны как предстоящие анонсы к Scale. Их нельзя механически объявлять уже выпущенными на дату отчёта. Асинхронные индексы отдельно подтверждены закрытием [KIKIMR-11166](https://st.yandex-team.ru/KIKIMR-11166) 30.11.2021. [Архив Query Processing][archive-2021] также фиксирует ScanQuery preview, планы запросов, JSON/JSONB, DDL/DCL и развитие Streaming Engine.

### 2022 — MVCC, эксплуатационная зрелость и enterprise-запросы

**Подтверждённые даты закрытия:** [Self-Heal в production, KIKIMR-9772](https://st.yandex-team.ru/KIKIMR-9772) — 15.02.2022; [MVCC, KIKIMR-10217](https://st.yandex-team.ru/KIKIMR-10217) — 26.04.2022; [перевод существующих БД на выделенные Hive, KIKIMR-8240](https://st.yandex-team.ru/KIKIMR-8240) — 03.10.2022. Отчёт 2021 года уже называл Self-Heal запущенным: это пример несовпадения продуктового результата и административного закрытия зонтика.

[Архивы 2022][archive-2022] содержат фоновые компакции, graceful restart, балансировку, аналитику, мониторинг и нагрузочные тесты MVCC. [Приоритетные улучшения 2022][enhancements-2022] задают следующий слой: видимость собственных изменений, снятие ограничения 1000 строк, SQL/PG-совместимость, выбор индексов, DEFAULT/NOT NULL, LDAP/аудит, репликация и ORM. Цвет «делаем» на этой странице — план, не подтверждение выпуска. В release notes ветки 22.4 описаны новый Topic API и Query Processor 2.0; точная календарная дата их выпуска в проверенном фрагменте не указана. [Release notes][releases].

### 2023 — Topics, CDC, аналитическое preview и PG-совместимость

**Результаты:** [CDC, KIKIMR-11675](https://st.yandex-team.ru/KIKIMR-11675) закрыт 18.01.2023; [`ydb workload topic`, KIKIMR-16325](https://st.yandex-team.ru/KIKIMR-16325) — 10.04.2023. [Таблеточный архив с заголовком 18.04.2023][archive-2023] отмечает итераторные MVCC-чтения, фоновые компакции, CDC и видимость собственных изменений. У страницы ошибочно выглядит год в slug (`2022-04-18-tablets`), поэтому использован её заголовок, а не имя URL.

Релиз 23.3 от **12.10.2023** добавил Kafka API для топиков, preview колоночных таблиц и экспериментальную PostgreSQL-совместимость. Это не промышленный выпуск всей аналитики и не полное PG feature parity. [Release notes][releases].

[Enterprise Features 2023][enterprise-2023] развивают защиту данных: первая LDAP-версия, гранулярные полномочия, аудит, роли; запросы на PITR, incremental backup, Kerberos/PAM, row/column permissions и маскирование. Страница актуализировалась: сегодняшние галочки не доказывают, что всё было выпущено именно в 2023 году.

### 2024 — расширение SQL и первая связная платформа репликации/Topics

[Публичный план 2024+][public-2024] ставил уникальные индексы, автоматический выбор индексов, ExecuteQuery, DEFAULT, LLVM JIT, необязательный DECLARE, CBO, VIEW, volatile transactions, статистики, incremental/PITR, репликацию, Topics и аналитику до 100 ТБ. Это исходные намерения с горизонтами Q4'23–Q1'25, а не список выполненного.

**Подтверждённые вехи:** базовые уникальные индексы — [KIKIMR-8192](https://st.yandex-team.ru/KIKIMR-8192), `fixed` 02.11.2024. Релиз **24.3.11.13 от 24.12.2024** добавил трассировку, асинхронную репликацию, VIEW под флагом, автосплит топиков под флагами, транзакции топик–таблица под флагами, изменение retention CDC, константные DEFAULT и RETURNING. Расширены федеративные источники MySQL/MSSQL/Greenplum. [Release notes][releases].

**Перенесённое:** включение ряда перечисленных возможностей по умолчанию, зрелая аналитика/CBO/spilling, ANN, incremental/PITR и последующие этапы SQL/PG-совместимости. Перенос следует из более поздних подтверждённых выпусков и действующих задач, а не из одного старого пустого чекбокса.

### 2025 — включение базовых возможностей и подготовка аналитической платформы

**15.04.2025, 24.4.4.2:** VIEW, автопартиционирование топиков, транзакции топик–строковая таблица, волатильные транзакции и автоматический выбор вторичных индексов включены по умолчанию. [Release notes][releases].

**14.07.2025, RC 25.1.2.7:** ANN-индекс, консистентная асинхронная репликация, Configuration V2, необязательный DECLARE; выгружаемые B-tree-индексы вместо полного удержания SST-индексов в памяти. **21.09.2025, RC 25.2.1.10:** аналитика с колоночными таблицами по умолчанию, CBO для аналитики, spilling, трансферы топик→таблица, расширение Kafka API. Эти две даты относятся к **релизам-кандидатам**; в 2025 они не названы здесь стабильными выпусками. Стабильные 25.1.4.18/25.2.1.26 датированы 12.05.2026. [Release notes][releases].

[Продуктовый план 2025][plan-2025] разделял **Commit** и **Best Effort**. В Commit входили аналитические витрины 100+ ТБ, CBO/spilling/векторизация, векторный поиск, incremental backup, Topics/Kafka, автопартиционирование CDC, трансферы, интеграции, Enterprise Manager, репликация меняющейся схемы, Configuration V2 и 2DC. PITR, полнотекстовый поиск, in-memory/охлаждение строковых таблиц, схематизация и message-level parallelism были в Best Effort. Полная PG-совместимость в том плане не была коммитом; в 2026 приоритет изменился.

**Сохранившиеся долги:** PITR, развитие incremental backup, schema-aware replication, общий rollout V2/2DC, промышленная эксплуатация аналитики, WM/память, схемы топиков и scale-down. [Планы 2026][helicopter]; [текущие работы Core][core]; [Topics H2][topics-monthly].

### 2026 — промышленная аналитика, PG/1С, поиск и эксплуатационная готовность

**Подтверждённые стабильные релизы:**

| Дата | Версия | Существенные результаты |
|---|---|---|
| 12.05.2026 | 25.1.4.18 и 25.2.1.26 | Стабильные выпуски веток с перечисленными выше возможностями и исправлениями; аналитика/CBO/spilling/трансферы и ANN уже не только планы 2023–2024. |
| 20.05.2026 | 25.3.1.27 | Среди изменений — работа с компактифицированными топиками через Kafka API. |
| 05.06.2026 | 25.4.1.15 | Развитие фильтруемых векторных индексов, overlap clusters и pushdown векторного поиска. |
| 02.07.2026 | 26.1.1.20 | ADD COLUMN DEFAULT, backup/restore репликаций, внешних источников/таблиц и трансферов; репликация автопартиционированных топиков CDC. |
| 16.09.2026 | 26.2.1.14 | Полнотекстовые индексы по умолчанию, расширенный Streaming и watermarks, Bloom/ngram-индексы, Arrow-результаты QueryService, ALTER COLUMN DEFAULT, TRUNCATE, автоматическая балансировка хранения между группами. |

Источник дат и объёма: [release notes][releases]. Упоминание исправлений в релизе не означает, что в нём впервые появилась вся функциональность ветки.

**18.09.2026 опубликован RC 26.3.1.16**: backup/export/import колоночных таблиц, dictionary encoding, min-max-индексы, виртуальные группы для декомиссии, OIDC. Гибридный поиск, SQS over Topics, JSON-индексы и часть полнотекстовых расширений в его описании выключены по умолчанию. В проверенном локальном changelog это **RC**, поэтому они не превращены в общий стабильный выпуск. Внутренний мастер-план репликации уже ссылается на более поздние теги 26.3.1.20–24; это другой контур доказательства, и статусы отдельных выкладок учитываются отдельно. [Release notes][releases]; [KIKIMR-27574](https://st.yandex-team.ru/KIKIMR-27574).

## Актуальный roadmap по направлениям

### Query Processing, Optimizer и Runtime

| Направление | Состояние и следующий результат | Источник / горизонт |
|---|---|---|
| Новый RBO и коррелированные подзапросы | 🧪 Декорреляция IN/EXISTS/скалярных подзапросов, WITH, произвольные JOIN ON реализованы; новый RBO выбран по умолчанию в PG Proxy по сентябрьскому отчёту. Полный переход всех DML/аналитических сценариев ещё продолжается. | [QP Monthly, с. 3–12][qp-monthly]; [текущее состояние][new-optimizer]. Ориентир отчёта — 26-4. |
| Корректность и качество нового оптимизатора | 🚧 В отчёте: TPC-H 22/22 по компиляции/корректности, 20/22 планов не хуже прежних; TPC-DS 94/99 по компиляции, общая корректность ещё проверяется. Цель 99/99 и полный паритет — план, не достигнутый результат. | [QP Monthly, с. 10][qp-monthly]; [Wiki-таблица][new-optimizer]. Цифры относятся к отчёту, не к новому прогону 15.11. |
| Ускорение компиляции | 🚧 В измерениях с отключённым CBO: TPCH 5,3→1,7 с; TPCDS 66→20 с; ClickBench 2,6→1,4 с. Цель 7–10× ещё не достигнута; следующие шаги — parsing/Peephole/правила и отдельный benchmark. | [QP Monthly, с. 5][qp-monthly]. |
| CBO для OLTP и сложной аналитики | 🚧 Развитие cost functions/cardinality estimation, выбор индексов, динамические фильтры, Shuffle Elimination, aggregates/predicate pushdown. Не следует смешивать это с уже выпущенной базовой аналитической версией CBO. | [Optimizer roadmap][optimizer]; [зависимости H2][dependencies]; [KIKIMR-25621](https://st.yandex-team.ru/KIKIMR-25621) уже `closed/fixed`. |
| Read Committed и PG/1С | 🧪 В main/PG Proxy используется Read Committed; остаются ретраи записей/коммита при split/merge/переездах и совместимость приложений. | [QP Monthly, с. 6–8][qp-monthly]; [YDB-2975](https://st.yandex-team.ru/YDB-2975) — `inProgress`; [план DataShard H2][datashard]. |
| StrictSerializableRW | 🧪 Реализация есть; нужны Jepsen, документация и включение. Карточка остаётся `inProgress`; наличие кода 26-3-1 не доказывает включение по умолчанию. | [YDBFEATURES-194](https://st.yandex-team.ru/YDBFEATURES-194); [QP Monthly, с. 15][qp-monthly], ориентир 26-4-1. |
| Вычисляемые колонки | 🧪 STORED/VIRTUAL в main под флагами; обе карточки `inProgress`. В отчёте блокер — SQL-представление схемы в export/import; ориентир готовности и документации — 27-1. | [YDBFEATURES-228](https://st.yandex-team.ru/YDBFEATURES-228), [229](https://st.yandex-team.ru/YDBFEATURES-229); [QP Monthly, с. 17][qp-monthly]. Это ориентир сентябрьского отчёта, не подтверждённый дедлайн. |
| Большие DML / QP 4.0 | 🚧 INSERT…SELECT и CTAS существуют, но полное снятие лимитов больших записей, потоковые записи в индексы/RETURNING и крупные UPDATE/DELETE остаются задачами. | [QP roadmap][qp]; [CS roadmap][cs]; [Helicopter H1][helicopter]. |
| WM и память | 🚧 Гарантии CPU, лимиты/гарантии ресурсов пулов, точный учёт KQP, Memory Quota 2.0 и интеграция операторов. По прочитанным карточкам нельзя объявлять общую проблему OOM решённой. | [YDB-3428](https://st.yandex-team.ru/YDB-3428) — `open`; [YDBFEATURES-253](https://st.yandex-team.ru/YDBFEATURES-253), [252](https://st.yandex-team.ru/YDBFEATURES-252), [54](https://st.yandex-team.ru/YDBFEATURES-54) — `inProgress`. |
| Runtime / spilling | 🚧 DqHashJoin/HashAggregate, нативные агрегации, оконные функции со spilling, каналы и Arrow memory accounting. Базовый spilling выпущен, полнота операторов и управление ресурсами продолжаются. | [Runtime roadmap][runtime]; [YDBFEATURES-255](https://st.yandex-team.ru/YDBFEATURES-255), [242](https://st.yandex-team.ru/YDBFEATURES-242), [158](https://st.yandex-team.ru/YDBFEATURES-158) — `inProgress`. |
| Диагностика запросов | 🧪 User-facing KQP spans и query_metrics_one_hour имеют `includedAsCodeReady`; это более точное описание, чем старый открытый зонтик трассировки. | [YDBFEATURES-238](https://st.yandex-team.ru/YDBFEATURES-238), [239](https://st.yandex-team.ru/YDBFEATURES-239); [YDB-3359](https://st.yandex-team.ru/YDB-3359) — `open`. |

### Строковые таблицы, транзакции и поисковые индексы

| Направление | Состояние / следующий результат | Источники |
|---|---|---|
| Транзакции | 🚧 Snapshot Isolation и Read Committed, удержание snapshot, многопоточный shared cache и чтения. | [DataShard H2 2026][datashard]. |
| Статистика для оптимизатора | 🚧 Базовые и многоколоночные статистики реализованы; остаются большие таблицы, полнота ANALYZE, стоимость сбора и применение в оптимизаторе. | [KIKIMR-25621](https://st.yandex-team.ru/KIKIMR-25621), [26267](https://st.yandex-team.ru/KIKIMR-26267), [26443](https://st.yandex-team.ru/KIKIMR-26443) — `closed/fixed`; [KIKIMR-25605](https://st.yandex-team.ru/KIKIMR-25605) — `open` в выгрузке эпиков; [Optimizer][optimizer]. |
| Уникальные индексы v2 | 🧪 Базовая уникальность выпущена; онлайн-построение в 26.2 выключено по умолчанию, [KIKIMR-25653](https://st.yandex-team.ru/KIKIMR-25653) — `testing`. | [Release notes][releases]; [QP roadmap][qp]. |
| Партиционирование / большие шарды | 📌 Директивное партиционирование, split под timeout-нагрузкой, external blobs, большие шарды, метрики и TLI. | [DataShard H2][datashard]; [KIKIMR-24191](https://st.yandex-team.ru/KIKIMR-24191), [25619](https://st.yandex-team.ru/KIKIMR-25619), [25659](https://st.yandex-team.ru/KIKIMR-25659) — `open`. |
| Охлаждение строковых данных | 📌 Действующее направление; не равно уже используемому tiering колоночных таблиц. | [KIKIMR-19432](https://st.yandex-team.ru/KIKIMR-19432) — `open` в выгрузке эпиков; [DataShard H2][datashard]. |
| Векторный поиск | ✅ Базовый ANN выпущен. 📌 Дальше — HNSW, бенчмарки и перестроение индекса. | [Release notes, 25.1/25.2/25.4][releases]; [DataShard H2][datashard]. |
| Полнотекстовый / гибридный / JSON-поиск | ✅ Базовый полнотекстовый индекс включён в 26.2. 🧪 JSON/гибридный поиск и фильтруемые полнотекстовые расширения имеют отдельные этапы доступности. 📌 В H2 — морфология, OpenSearch API, атрибуты/несколько колонок, эксперимент с графовым индексом. | [Release notes, 26.2/26.3 RC][releases]; [YDBFEATURES-139](https://st.yandex-team.ru/YDBFEATURES-139) — `includedAsCodeReady`; [DataShard H2][datashard]. |

### Колоночные таблицы и аналитика

Основа уже доступна; актуальный план — промышленная эксплуатация больших объёмов и нескольких конкурирующих запросов. Цели из [Analytics 2026][analytics-2026] про сотни ТБ, 500 ТБ, 100 параллельных запросов и большие количества таблиц — требования/план, а не измеренная ёмкость выпущенной версии.

| Направление | Состояние / следующий результат | Источники |
|---|---|---|
| Backup/restore CS | 🧪 В 26.2 выключено по умолчанию, включено в описание RC 26.3. Стабильный общий выпуск не установлен по проверенному changelog. | [Release notes][releases]; [CS roadmap][cs]. |
| Смешанные транзакции CS+DS | 🚧 Одновременное чтение поддержано; **смешанные пишущие транзакции остаются в разработке**. Read+Write внутри CS не доказывает смешанную запись CS+DS. | [Явное ограничение документации][mixed-tx]; [CS roadmap][cs]. |
| Compaction и эффективность хранения | 🚧 Скорость записи/чтения свежих данных, уменьшение write/space amplification, контроль маленьких блобов, квоты CPU/RAM. | [Analytics 2026][analytics-2026]; [CS H1][cs-h1]; [Release notes 26.2/26.3 RC][releases]. |
| Индексы и кодирование | ✅ Bloom/ngram в 26.2. 🧪 Min-max и dictionary encoding в RC 26.3; дальнейшее развитие pushdown и выборов стратегии. | [Release notes][releases]; [зависимости H2][dependencies]. |
| Tiering в S3 | 🚧 Внутренние сценарии существуют; публичный продукт, DDL/API, тестирование разных S3, эффективность чтения/записи — отдельная незавершённая работа. | [CS roadmap][cs]; [Analytics 2026][analytics-2026]. |
| Автошардирование | 📌 Не следует объявлять выполненным: документация прямо говорит об отсутствии автоматического репартиционирования CS. Split/merge и autosharding находятся в плане. | [CREATE TABLE][create-table]; [CS roadmap][cs]. |
| Загрузка / совместимость SQL / HTAP | 🚧 BulkUpsert, большие CTAS/INSERT/UPSERT, WITH/коррелированные подзапросы/inequality join, удержание snapshot. CDC из CS и OLTP→OLAP/HTAP — последующие направления. | [CS roadmap][cs]; [Analytics 2026][analytics-2026]; [QP Monthly][qp-monthly]. |

### Репликация, CDC и резервное копирование

| Направление | Состояние / следующий результат | Источники |
|---|---|---|
| Асинхронная репликация: первая версия | ✅ Выпущена, консистентная версия также описана в 25.1. Общая надёжность и полная матрица объектов не закрыты. | [Release notes][releases]; [KIKIMR-12336](https://st.yandex-team.ru/KIKIMR-12336) — `closed/fixed`; [KIKIMR-27574](https://st.yandex-team.ru/KIKIMR-27574) — `inProgress`. |
| Initial sync, миграции и смена ролей | 🚧 Initial sync через backup/restore, «Туда и Обратно», «репликация по кнопке», наблюдаемость и исправления ROW/GLOBAL. | [KIKIMR-27574](https://st.yandex-team.ru/KIKIMR-27574). |
| Изменения схемы и матрица объектов | 🚧 Серийные типы, индексы, Topics/CDC, Kesus, секреты и ошибки schema changes разбиты на отдельные этапы. Нельзя ставить одну галочку за «все объекты + вся схема». | [KIKIMR-25813](https://st.yandex-team.ru/KIKIMR-25813), [25730](https://st.yandex-team.ru/KIKIMR-25730) — `open`; [27545](https://st.yandex-team.ru/KIKIMR-27545) и мастер-план. |
| Incremental backup v0 | 🧪 Collections, атомарная точка backup/restore, последняя точка цепочки, локальное хранение внутри кластера и строковые таблицы. Существенные ограничения перечислены в roadmap. | [KIKIMR-27150](https://st.yandex-team.ru/KIKIMR-27150); [26.2 release notes][releases]. |
| Incremental backup v1+ | 📌 Schema-aware backup, корректный retention, compaction цепочек/расширение SQL и инструментария; окончательный полный объём не зафиксирован. | [KIKIMR-25747](https://st.yandex-team.ru/KIKIMR-25747) и [KIKIMR-12228](https://st.yandex-team.ru/KIKIMR-12228) — `open`; [KIKIMR-27150](https://st.yandex-team.ru/KIKIMR-27150). |
| PITR | 📌 Отдельная незавершённая возможность. Восстановление на последнюю точку incremental-цепочки не является PITR. | [KIKIMR-18552](https://st.yandex-team.ru/KIKIMR-18552) — `open`; v0 в [KIKIMR-27150](https://st.yandex-team.ru/KIKIMR-27150) прямо исключает PITR. |
| Backup/restore системных таблеток | 🚧 Отдельный проект восстановления служебного состояния и зависимость аварийных сценариев. | [KIKIMR-25735](https://st.yandex-team.ru/KIKIMR-25735) — `inProgress`. |
| Debezium / ALTER CHANGEFEED | 🚧 Минимальные Debezium-записи реализованы и документированы, доработки остаются. У ALTER CHANGEFEED подтверждено изменение retention; общий зонтик открыт. | [CDC docs][cdc]; [KIKIMR-19582](https://st.yandex-team.ru/KIKIMR-19582) и [20285](https://st.yandex-team.ru/KIKIMR-20285) — `open`. |

**Ближайший датированный хвост репликации.** В прочитанной версии мастер-плана (`updatedAt` 13.11.2026) есть плановые выкладки 15–24 ноября: heartbeat-оптимизации, индексы, Topics/CDC и Kesus. Например, 17.11 — KIKIMR-27694; 18–20.11 — часть индексов; 19/23.11 — Topics/CDC; 18/24.11 — этапы Kesus. Это **даты из плана**, не установленный факт выкладки. Просроченные синие строки от 13/15 ноября не перекрашены в «выполнено» по наступлению даты. [KIKIMR-27574](https://st.yandex-team.ru/KIKIMR-27574).

### Topics, Logbroker и потоковые/федеративные запросы

Текущая Wiki-страница Logbroker ссылается на [фильтр H2 2026](https://st.yandex-team.ru/issues/562856?_view=list). Его полная проверенная выдача — 37 карточек по тегам `logbroker-2026q3-goal` и `logbroker-2026q3-goal-candidate`. **Кандидат не приравнен к принятой цели**. Это полный результат конкретного фильтра, не весь backlog Logbroker.

| Направление | Состояние / следующий результат | Источники |
|---|---|---|
| Автопартиционирование | ✅ Автосплит выпущен. 📌 Merge/scale-down — кандидат (`neo`), доработки совместимости с retention и blind writes имеют отдельные задачи. | [LOGBROKER-10478](https://st.yandex-team.ru/LOGBROKER-10478); [release notes][releases]. |
| Схематизация | 📌 [LOGBROKER-7913](https://st.yandex-team.ru/LOGBROKER-7913) — `neo`, кандидат H2, а не подтверждённый выпуск. | [Фильтр H2](https://st.yandex-team.ru/issues/562856?_view=list); [план H1][topics-h1]. |
| Производительность Topic API | 🚧 Батчевание в SDK — `testing`; расширение чтения, квот и изоляции consumer/partition. | [LOGBROKER-10147](https://st.yandex-team.ru/LOGBROKER-10147); [Topics H1][topics-h1], [Topics Monthly][topics-monthly]. |
| Kafka API | ✅ Клиентская балансировка/компактификация/транзакции уже выпущены. 🚧 Сжатие — `awaitingDeployment`; e2e-тесты и serverless-исправления продолжаются. | [Release notes 25.2][releases]; [LOGBROKER-9609](https://st.yandex-team.ru/LOGBROKER-9609); фильтр H2. |
| Topics↔YT | 🚧 Двухфазный коммит — `inProgress`. Это отдельная задача от уже выпущенных транзакций Topics↔YDB tables. | [LOGBROKER-10183](https://st.yandex-team.ru/LOGBROKER-10183). |
| SQS over Topics | 🧪 API присутствует в RC 26.3 и выключен по умолчанию. Облачный релиз/митеринг и миграция старого SQS/YMQ остаются целями H2. | [Release notes][releases]; [LOGBROKER-10608](https://st.yandex-team.ru/LOGBROKER-10608) — `neo`; фильтр H2. |
| Message-level parallelism | 📌 Сохраняется как продуктовый запрос. В проверенных материалах нет достаточного подтверждения выполнения именно исходного пункта; SQS over Topics автоматически ему не приравнивается. | [План 2025, Best Effort][plan-2025]; [Helicopter H1 2026][helicopter]. |
| Streaming | ✅ В 26.2 расширены локальные Topics/tables, несколько INSERT и watermarks. 🚧 Далее — автономность, наблюдаемость, миграции RTMR/YaEM, эксплуатация и WM. | [Release notes][releases]; [FQ/Streaming H1][fq-h1]; [зависимости H2][dependencies]. |
| Federated Query | ✅ Первая версия выпущена. 🚧 Следующие задачи: импорт/экспорт S3 для строковых и широких CS, PushBulkData, Solomon/Monium, унификация партиционирования и эксплуатации коннекторов. | [Release notes][releases]; [FQ/Streaming roadmap][fq-h1]. |

### Distributed Storage, конфигурация и устойчивость

| Направление | Состояние / следующий результат | Источники |
|---|---|---|
| Configuration V2 / distconf | 🚧 Механизм уже есть; цель H2 — эксплуатация и миграция кластеров, управление конфигурацией и устранение блокеров. Не «ещё не начали» и не «весь флот переведён». | [Release notes 25.1][releases]; [Ops H2][ops-h2]; [процесс V1→V2][v2-process]. |
| State Storage / статическая группа | 🧪 Автоматическая реконфигурация и масштабирование State Storage/SchemeBoard/Board/static group отмечены в DS-таблице как код в 25-3 и включение в testing. Общий промышленный rollout из этой строки не следует. | [Distributed Storage roadmap][ds]. |
| Декомиссия групп / BlobDepot | 🚧 Механизм виртуальных групп есть в RC 26.3, но продуктовая декомиссия со всеми типами таблеток/UI/prod-процессами не завершена. | [KIKIMR-25733](https://st.yandex-team.ru/KIKIMR-25733) — `inProgress`; [release notes][releases]; [DS roadmap][ds]. |
| Работа без системных таблеток | 🚧 NodeBroker: подтверждён узкий результат — 72-часовая работоспособность Data Plane, задача и её тест закрыты. Hive/остальные системные зависимости остаются в общем проекте. | [KIKIMR-25710](https://st.yandex-team.ru/KIKIMR-25710), [26239](https://st.yandex-team.ru/KIKIMR-26239) — `closed/fixed`; [25732](https://st.yandex-team.ru/KIKIMR-25732) — `inProgress`. |
| Работа без BSC | 🚧 Есть реализованная peer-to-peer передача конфигураций и кэши; полный аварийный сценарий/подъём кластера не следует считать автоматически закрытым. | [KIKIMR-11324](https://st.yandex-team.ru/KIKIMR-11324) — `closed/fixed`; [DS roadmap][ds]. |
| Self-Heal degraded | 🚧 [KIKIMR-17991](https://st.yandex-team.ru/KIKIMR-17991) закрыт с `fixed`, но Wiki-строка всё ещё говорит о блокировке потерей блобов. Общая production-ready готовность сценария с двумя отказами по противоречивым источникам не установлена. | [DS roadmap][ds]; карточка KIKIMR-17991. |
| Эффективность и NBS 2.0 | 📌 CPU/память, disk bandwidth, RDMA IC, чексуммы и предсказуемая latency. Закрытый RFC экономической эффективности не означает реализацию всех NBS-требований. | [KIKIMR-26529](https://st.yandex-team.ru/KIKIMR-26529) — `inProgress`; [25741](https://st.yandex-team.ru/KIKIMR-25741), [25740](https://st.yandex-team.ru/KIKIMR-25740), [25739](https://st.yandex-team.ru/KIKIMR-25739), [25805](https://st.yandex-team.ru/KIKIMR-25805), [25806](https://st.yandex-team.ru/KIKIMR-25806) — `open`; [25807](https://st.yandex-team.ru/KIKIMR-25807) — `closed/fixed`. |
| 2DC / Bridge | 🚧 Реализация есть; H2 on-prem-план содержит внедрение у клиентов, а DS-план — миграции и оптимизацию меж-ДЦ трафика. Универсальная промышленная готовность всех инсталляций не объявляется. | [DS roadmap][ds]; [OnPrem H2][onprem-h2]; [Helicopter H1][helicopter]. |

### Security, SDK/CLI, UI и качество

| Направление | Состояние / следующий результат | Источники |
|---|---|---|
| Аутентификация | ✅ Базовый LDAP. 🧪 OIDC включён в RC 26.3. SCRAM-SHA-256 реализован для Kafka/PG; это не обещание поддержки всех SASL-механизмов. | [Release notes][releases]; [KIKIMR-24270](https://st.yandex-team.ru/KIKIMR-24270) — `closed/fixed`; [Security H2][security]. |
| Enterprise security | 🚧 Облачные роли, клиентские сертификаты динамических узлов, аудит/Audit Trails, IAM, IDM и SSO как продукт; наблюдаемость/конфигурирование и жизненный цикл секретов. | [Security H2][security]; [KIKIMR-27119](https://st.yandex-team.ru/KIKIMR-27119) — `inProgress` в выгрузке эпиков. |
| ФСТЭК | 🚧 Работы по версиям и комплектам проверок, включая Streaming/CS. Наличие в планах не выдано за получение сертификата. | [Helicopter H1][helicopter]; [OnPrem H2][onprem-h2]; эпики [KIKIMR-26003](https://st.yandex-team.ru/KIKIMR-26003), [24812](https://st.yandex-team.ru/KIKIMR-24812), [24813](https://st.yandex-team.ru/KIKIMR-24813), [24854](https://st.yandex-team.ru/KIKIMR-24854) — `inProgress` в выгрузке. |
| AppTeam Commit H2 | 📌 Java Topic SDK/API, Rust SDK, блокеры и тестирование PG/1С, ReadCommittedRW/StrictSerializableRW в SDK, рефакторинг ydbd CLI, SDK-приёмка и подготовка Serverless/DocAPI. | [AppTeam H2 и таблица «Берём в H2?»][app-h2]; [YDB-3063](https://st.yandex-team.ru/YDB-3063) — `inProgress`. |
| AppTeam за линией обязательств | 📌 Сквозные трассы клиент→YDB, OpenTelemetry metrics/logs, FDW в Managed PG, Trino/Cedrus, DataCatalog-интеграции и ряд ускорений есть в списке, но в таблице H2 не взяты. Их нельзя объявлять коммитами сезона по одному упоминанию в тексте. | [AppTeam H2][app-h2], checkbox «Берём в H2?» = false. |
| UI / Enterprise Manager | 📌 Самостоятельная диагностика: ресурсы, фоновые процессы, observability/AI, снижение нагрузки на поддержку; уточнение стратегии Enterprise Manager. Источник прямо является draft. | [UI plans 2026 H2][ui-h2]. |
| Engineering | 🚧 Автоматизированная приёмка тегов, OLTP/OLAP/Jepsen, Nemesis, perf-инфраструктура, приёмка NBS, ускорение PR-проверок, диагностика core dumps и релизная автоматизация. Проценты в таблице датированы 18.08.2026 и не являются актуальным измерением на 15.11. | [Engineering H2][engineering-h2]; [зависимости H2][dependencies]. |
| Operations | 🚧 Релизные процессы и дашборды, V2/distconf, диагностика OOM/verify, автоматизация эксплуатации и восстановление. Часть зависимостей имеет риск/нет ресурса: цвет/условия источника сохранены при трактовке. | [Ops H2][ops-h2]; [зависимости H2][dependencies]. |

## Сверка незавершённых пунктов исходного ROADMAP.md

Таблица охватывает все содержательные пункты без ✅/❌ и старые частично выполненные пункты. Изначально завершённые позиции не отменялись без доказательств регрессии. Там, где результат подтверждён лишь частично, оригинал не превращён в безусловное «released». Ссылки на источники для групп приведены в последнем столбце; версии — из release notes, а не выведены из года метки ㉓/㉔.

### Query Processor

| Исходный пункт | Состояние на дату среза | Основание / граница |
|---|---|---|
| Unique secondary indexes | ✅ Базовая функциональность; онлайн-построение v2 — 🧪 | [KIKIMR-8192](https://st.yandex-team.ru/KIKIMR-8192); [KIKIMR-25653](https://st.yandex-team.ru/KIKIMR-25653); [release notes][releases]. |
| Apply indexes automatically | ✅ | 24.4.4.2; [KIKIMR-10864](https://st.yandex-team.ru/KIKIMR-10864) закрыт. Cost-based выбор в новом CBO — дальнейшее развитие. |
| Default values | ✅ Константные DEFAULT | 24.3; расширения ADD/ALTER COLUMN в 26.1/26.2. Произвольные expressions остаются в [QP roadmap][qp]. |
| Asynchronous LLVM JIT | ❓ Полное выполнение исходного обещания не установлено | LLVM существует, есть исправления и отдельные открытые задачи; это не доказательство именно асинхронной JIT-компиляции. |
| Optional DECLARE | ✅ | 25.1.2.7 RC, далее стабильная 25.1.4.18. [Release notes][releases]. |
| Cost-based optimizer | ✅ Базовый аналитический CBO; 🚧 новые этапы | 25.2, [Optimizer roadmap][optimizer]. |
| INSERT…SELECT for large datasets | 🚧 | Операции поддержаны, но снятие лимитов больших записей остаётся в [QP][qp]/[CS][cs]. |
| Transactional writes row+column | 🚧 Не выпущено в исходном полном объёме | [Документированное ограничение смешанной записи][mixed-tx]. |
| Computed columns | 🧪 Под флагами, обе реализации inProgress | [YDBFEATURES-228](https://st.yandex-team.ru/YDBFEATURES-228), [229](https://st.yandex-team.ru/YDBFEATURES-229). |
| Temporary tables | 🧪 Подтверждена PG-реализация; общая YQL-доступность не установлена | [KIKIMR-17960](https://st.yandex-team.ru/KIKIMR-17960) закрыт; [QP Monthly, с. 5][qp-monthly]. |
| VIEW | ✅ | 24.3 под флагом, 24.4 по умолчанию; старый [KIKIMR-19567](https://st.yandex-team.ru/KIKIMR-19567) всё ещё `inProgress`. |
| Data spilling | ✅ Дисковый spilling; 🚧 развитие операторов/квот | 25.2; [Runtime][runtime]. |
| TPC-H и TPC-DS 100 TB | 🚧 Полный объединённый критерий не подтверждён | В [QP][qp] отмечена готовность TPCH 10/100 TB, но это не завершение TPCDS 100 TB. Не смешивать размер данных с числом корректных запросов. |
| Better KQP resource management | 🚧 | [YDBFEATURES-54](https://st.yandex-team.ru/YDBFEATURES-54), [252](https://st.yandex-team.ru/YDBFEATURES-252), [253](https://st.yandex-team.ru/YDBFEATURES-253). |
| PostgreSQL query execution | 🚧 | [YDB-2975](https://st.yandex-team.ru/YDB-2975), [QP Monthly][qp-monthly]. Wire protocol ≠ совместимость приложений. |
| Single database connection string | ❓ | Факт поддержки отдельных connection-string интерфейсов не доказывает завершение исходного универсального пункта для всех клиентов. |
| Constraints in query optimizer | 🚧/❓ Полный scope не определён | Новый оптимизатор реализует отдельные преобразования; общего критерия завершения исходной формулировки не найдено. [Optimizer][optimizer]. |
| Streaming Lookup Join | 🧪 Реализация/включение отмечены выполненными, 25.3.1 | [QP roadmap][qp]; общий rollout не восстановлен по релизам. |
| Universal ExecuteQuery | ✅ Базовый QueryService/ExecuteQuery; 🚧 дальнейшее объединение сценариев | В 24.1 описан QueryService для DML/DDL с неограниченными результатами. [Release notes][releases], [QP][qp]. Смешанный DDL+DML в одном запросе остаётся отдельной задачей. |
| RPC Deadline & Cancellation propagation | 🚧 | Для storage PUT/GET протяжка deadline отмечена в prod в [DS][ds]; end-to-end завершение всех API/операторов не установлено. |

### Core, Topics и системные механизмы

| Исходный пункт | Состояние на дату среза | Основание / граница |
|---|---|---|
| ANN vector search | ✅ | 25.1/25.2; новые виды индексов — последующие планы. [Release notes][releases]. |
| Volatile transactions | ✅ | По умолчанию в 24.4.4.2. Позднее закрытие [KIKIMR-16341](https://st.yandex-team.ru/KIKIMR-16341) не переносит дату выпуска в 2026. |
| Table statistics | ✅ Базовые статистики; 🚧 развитие | [ANALYZE/CREATE TABLE][create-table], [KIKIMR-25621](https://st.yandex-team.ru/KIKIMR-25621), [26267](https://st.yandex-team.ru/KIKIMR-26267). |
| Memory optimization row tables | ✅ Выгружаемые B-tree-индексы; 🚧 общая оптимизация памяти | 25.1; [release notes][releases]. |
| Minimum two cores per node | 🧪 Подтверждено для testing, не общий production sizing | Строка «Про работу на 2-х ядрах для тестинга», код в prod, в [DS][ds]. |
| Incremental backup + PITR | 🧪/📌 Разделено на разные этапы | [KIKIMR-27150](https://st.yandex-team.ru/KIKIMR-27150), [18552](https://st.yandex-team.ru/KIKIMR-18552); 26.2 под флагом. |
| ALTER CHANGEFEED | 🚧 Retention реализован, общий зонтик открыт | 24.3; [KIKIMR-20285](https://st.yandex-team.ru/KIKIMR-20285). |
| Async replication column tables/topics | ❓ Полный объём не подтверждён | Репликация служебного автопартиционированного CDC-топика не равна поддержке произвольных Topics/CS. [Матрица репликации](https://st.yandex-team.ru/KIKIMR-27545), [CS][cs]. |
| Async replication schema changes | 🚧 | [KIKIMR-25813](https://st.yandex-team.ru/KIKIMR-25813), [25730](https://st.yandex-team.ru/KIKIMR-25730), [27574](https://st.yandex-team.ru/KIKIMR-27574). |
| Debezium format | 🚧 Базовый формат существует | [CDC docs][cdc]; [KIKIMR-19582](https://st.yandex-team.ru/KIKIMR-19582) открыт. |
| Topics autoscaling increase/decrease | 🚧 Split выпущен, merge/scale-down — план | [Release notes][releases]; [LOGBROKER-10478](https://st.yandex-team.ru/LOGBROKER-10478). |
| Extended Kafka API | 🚧 Значительное расширение выпущено; полный исходный v19-критерий отдельно не подтверждён | [Release notes 25.1/25.2][releases]; текущие Kafka-задачи в [H2-фильтре](https://st.yandex-team.ru/issues/562856?_view=list). |
| Topic schemas | 📌 | [LOGBROKER-7913](https://st.yandex-team.ru/LOGBROKER-7913), кандидат H2. |
| Message-level parallelism | 📌/❓ Выпуск не подтверждён | [План 2025][plan-2025], [Helicopter 2026][helicopter]. |
| First async replication, row tables | ✅ | 24.3; [KIKIMR-12336](https://st.yandex-team.ru/KIKIMR-12336). |
| Extend State Storage online | 🧪 Код/этап testing подтверждены | [DS roadmap][ds]. Полный rollout без остановки всего флота не установлен. |
| Split/Merge DataShards BY LOAD by default | 🚧/❓ Автосплит реализован, универсальный default не установлен | [KIKIMR-3690](https://st.yandex-team.ru/KIKIMR-3690) закрыт; [DataShard H2][datashard] содержит дальнейшую работу. |
| Basic histogram | 🧪/🚧 Реализованы конкретные гистограммы; общий rollout не установлен | [KIKIMR-26443](https://st.yandex-team.ru/KIKIMR-26443) закрыт; [DataShard H2][datashard]. |
| Data ingestion topic→table | ✅ Трансферы | 25.2; [release notes][releases]. |
| Snapshot read over read replicas | ❓ Исходный согласованный snapshot-сценарий не подтверждён | Stale Read-Only на репликах не равен этому требованию. [Документация транзакций][transactions]. |
| Transactions topics↔tables | ✅ | По умолчанию в 24.4.4.2; [release notes][releases]. |
| Tracing capabilities | ✅ Базовая трассировка; 🧪 новые user-facing spans | 24.3; [YDBFEATURES-238](https://st.yandex-team.ru/YDBFEATURES-238). |
| Automatic tablet-channel balance across groups | 🚧 Полный исходный критерий не закрыт | Автобалансировка хранения в 26.2 не доказывает завершение всех механизмов Hive/channels: соответствующий пункт остаётся в [Core][core]. |
| Avoid NodeBroker SPF | 🚧 Узкий результат подтверждён | 72 часа Data Plane: [KIKIMR-25710](https://st.yandex-team.ru/KIKIMR-25710); общий аварийный режим [25732](https://st.yandex-team.ru/KIKIMR-25732) — в работе. NBS-специфическая гарантия отдельно не проверена. |
| SchemeBoard subscriptions | ❓ | Наличие подписок и задач по их оптимизации не доказывает закрытие исходного rollout-пункта. |

### Security, BlobStorage, аналитика и тесты

| Исходный пункт | Состояние на дату среза | Основание / граница |
|---|---|---|
| OIDC | 🧪 | RC 26.3; [release notes][releases]. |
| KeyCloak authentication | ❓ Полный продуктовый сценарий не установлен | Не приравнен к OIDC либо к запуску самого Keycloak поверх YDB. [Security H2][security]. |
| SASL framework | 🚧 Конкретный SCRAM-SHA-256 реализован | [KIKIMR-24270](https://st.yandex-team.ru/KIKIMR-24270); общий перечень механизмов не закрыт. |
| BlobStorage p999/CPU | 🚧 Постоянное развитие | [DS roadmap][ds]; [план H2](https://st.yandex-team.ru/KIKIMR-26529). |
| ActorSystem performance | 🚧 Постоянное развитие | [DS roadmap][ds]. |
| ActorSystem ARM | ❓ Общая production-ready готовность не установлена | В [QP][qp] ARM production-ready остаётся незавершённым пунктом; узкие оптимизации не равны завершению. |
| Effortless initial deployment | 🚧 V2/distconf существуют, продуктовый rollout продолжается | [DS][ds], [Ops H2][ops-h2], [V2 migration][v2-process]. |
| Reduce/remove BlobStorage groups | 🚧 | RC 26.3 и [KIKIMR-25733](https://st.yandex-team.ru/KIKIMR-25733) в работе. |
| One-leg migration without downtime | ❓ | Полный критерий исходного сценария переноса 1/3 mirror3-dc не подтверждён найденными источниками. |
| Self-Heal degraded groups | 🚧 Противоречивые источники | Закрытый [KIKIMR-17991](https://st.yandex-team.ru/KIKIMR-17991), но блокирующая пометка в [DS][ds]; безусловный ✅ не поставлен. |
| BlobDepot | 🚧 MVP и частные применения есть | [DS][ds]; [KIKIMR-25733](https://st.yandex-team.ru/KIKIMR-25733). |
| Avoid BSC SPF | 🚧 P2P/кэши реализованы, общий критерий шире | [KIKIMR-11324](https://st.yandex-team.ru/KIKIMR-11324); [DS][ds]. |
| BSC manages static group | 🚧/❓ Есть автоматика, но не доказана именно исходная архитектура через BSC | Distconf/автоматика статической группы в [DS][ds] не автоматически равны этому обещанию. |
| Hard disk-space separation | ❓ | Исходные гарантии изоляции VDisk/PDisk отдельно не подтверждены. |
| Reduce space amplification | 🚧 | Продолжаются оптимизации CS/DS, но нет единого критерия закрытия исходного пункта. [Analytics 2026][analytics-2026], [DS][ds]. |
| CS backup | 🧪 | 26.2 под флагом, 26.3 RC; [release notes][releases]. |
| CS autosharding | 📌 | Отсутствие автоматического репартиционирования явно описано в [CREATE TABLE][create-table]. |
| Log Store: 1+ million tables | 🚧/❓ Базовый механизм не доказывает заявленный масштаб | В [CS][cs] ColumnStore и дальнейшее масштабирование ещё задачи; миллион таблиц не верифицирован. |
| Column-oriented tables | ✅ Базовая аналитика выпущена | 25.2; зрелость отдельных сценариев остаётся предметом дальнейшего плана. [Release notes][releases]. |
| Tiered storage CS in S3 | 🚧 Внутреннее применение ≠ общий публичный продукт | [CS roadmap][cs], [Analytics 2026][analytics-2026]. |
| Built-in DataShard YCSB load | ❓ Точный исходный встроенный механизм не подтверждён | Наличие CLI/workload/CI-прогонов YCSB не приравнено к built-in нагрузке на DataShard. |

Пункты, уже помеченные ✅ в исходнике (включая Snapshot Readonly, New Engine, NOT NULL PK, MVCC/CDC, PostgreSQL types/wire, ActorSystem 1.5, ds_tool, storage-node decommission, первую FQ, интерактивный CLI, `ydb sql`, workload topics и Jepsen), оставлены как историческая база. Это **не повторная независимая сертификация каждого старого ✅**. Противоположных доказательств, достаточных для отмены этих отметок, в исследовании не установлено. Отказы TRope и RTMR-tablet сохранены как решения исходного файла.

## Зависимости и открытые вопросы планирования

1. **PG/1С** требует совместной работы DataShard, QP/Optimizer, SDK и испытаний приложений. Поддержка wire protocol не снимает эту зависимость. [AppTeam H2][app-h2]; [DataShard H2][datashard].
2. **Большая аналитика** зависит не только от CS: память/WM, каналы, spilling, статистики, планы запросов, загрузка и приёмочные perf-тесты составляют общий критический путь. [Analytics 2026][analytics-2026]; [зависимости H2][dependencies].
3. **Репликация и backup** пересекаются в initial sync и матрице схемных объектов, но имеют разные критерии готовности. PITR нельзя считать следствием появления backup collections. [KIKIMR-27574](https://st.yandex-team.ru/KIKIMR-27574); [KIKIMR-27150](https://st.yandex-team.ru/KIKIMR-27150).
4. **Декомиссия storage groups** требует поддержки конкретных типов таблеток, контроля истории каналов и эксплуатационного/UI-пути. Наличие BlobDepot или RC-реализации недостаточно для объявления всего продукта готовым. [KIKIMR-25733](https://st.yandex-team.ru/KIKIMR-25733).
5. **V2/2DC** имеют отдельные этапы: код, миграция, испытания, раскатка и принятие заказчиком. В [Ops H2][ops-h2] миграция V2 условна на снятие блокеров; эта условность не заменена выдуманным сроком.
6. **Сроки:** 26-4/27-1 из отчётов — ориентиры версий на момент планирования. При отсутствии актуального подтверждённого deadline календарная дата в этот документ не добавлялась. Таблицы с 30.0/30.1 в CS не трактуются как реальные обязательства на 2030 год.

## Охват и ограничения исследования

- Полностью получен индекс **152 потомков раздела roadmap**; получены 56 страниц Wiki и 12 отдельных гридов. Прочитаны выбранные годовые/командные планы, архивы, зависимости и актуальные материалы за пределами этого раздела, включая Analytics, AppTeam, Operations и Engineering. Сам индекс страниц не выдаётся за чтение их содержимого.
- Для Tracker получена полная выборка **403 доступных эпиков KIKIMR** с сокращёнными полями; дополнительно читались конкретные карточки и ограниченные поисковые выдачи, а для Topics — все 37 карточек заданного H2-фильтра. Поиск не гарантирует покрытие скрытых ACL или неиндексированных данных.
- Отдельно прочитан **KQP Monthly от 21.09.2026, 32 страницы PDF**. Его численные результаты и планы датированы сентябрём; текущие статусы выбранных задач проверены в Tracker. Табличные проценты Engineering от августа не перенесены в настоящее время.
- Проверены локальные `changelog-server.md`, описания транзакций, CDC, CREATE TABLE и другие относящиеся к пунктам страницы. Эти файлы не являются инвентаризацией включённых флагов на каждом production-кластере. Формулировка «по умолчанию в версии» не означает «установлено на всех кластерах».
- У страниц **Press Release 2023/2024** API вернул контейнеры SharePoint, у **Official Public Roadmap** — встраивание SharePoint. Содержание этих документов не извлечено и **не использовано как доказательство**. Вместо них использованы доступные текстовые планы, архивы, release notes и Tracker.
- История до 2020 года не восстановлена в этом проходе; полные ежегодные официальные итоговые документы за каждый год не найдены/не прочитаны. Ранние годы представлены подтверждёнными вехами и источниками соответствующего периода, без выдуманной полноты.
- Внешние системы не изменялись. В исходном `ROADMAP.md` обновлены только подтверждённые статусы/ограничения; существующие пользовательские заметки сохранены.

## Основные источники

### Документация текущего checkout

- [Release notes сервера][releases] — точные даты/версии, включение по умолчанию и отключённая функциональность.
- [Ограничение смешанных пишущих транзакций][mixed-tx], [режимы транзакций][transactions], [CDC/Debezium][cdc], [CREATE TABLE/CS partitioning][create-table].

### Исторические и общие планы

- [Общий roadmap по компонентам][roadmap], [2021 Q2–Q3 results][report-2021], [улучшения 2022][enhancements-2022], [Enterprise Features 2023][enterprise-2023], [публичный roadmap 2024+][public-2024], [план 2025 с Commit/Best Effort][plan-2025].
- [Архив июля 2020][archive-2020a], [октября 2020][archive-2020b], [Query Processing 2021][archive-2021], [таблеточные задачи мая 2022][archive-2022], [закрытые задачи апреля 2023][archive-2023].

### Актуальные направления

- [Helicopter View H1 2026][helicopter], [межкомандные зависимости H2][dependencies].
- [QP roadmap][qp], [Optimizer][optimizer], [новый оптимизатор: прогресс][new-optimizer], [Runtime][runtime], [QP Monthly PDF][qp-monthly].
- [DataShard H2][datashard], [Core][core], [Distributed Storage][ds], [Security H2][security].
- [Analytics 2026][analytics-2026], [CS roadmap][cs], [CS H1 2026][cs-h1], [FQ/Streaming H1][fq-h1].
- [Topics Monthly/H2 filter][topics-monthly], [Topics H1][topics-h1], [AppTeam H2][app-h2], [UI H2 draft][ui-h2], [Engineering H2][engineering-h2], [Operations H2][ops-h2], [OnPrem H2][onprem-h2], [процесс миграции V2][v2-process].

[releases]: ydb/docs/ru/core/changelog-server.md
[mixed-tx]: ydb/docs/ru/core/yql/reference/_includes/limitation-column-row-in-read-only-tx.md
[transactions]: ydb/docs/ru/core/concepts/_includes/transactions.md
[cdc]: ydb/docs/ru/core/concepts/cdc.md
[create-table]: ydb/docs/ru/core/yql/reference/syntax/create_table/index.md
[roadmap]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/
[report-2021]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/reports/2021-q23/
[archive-2020a]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/archive/2020-07-08-tablets/
[archive-2020b]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/archive/2020-10-20-tablets/
[archive-2021]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/archive/2021-04-13-yql/
[archive-2022]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/archive/2022-05-31-tablets/
[archive-2023]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/archive/2022-04-18-tablets/
[enhancements-2022]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/ydb-enhancements-2022/
[enterprise-2023]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/ydb-enterprise-features/
[public-2024]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/ydb-public-roadmap-2024/
[plan-2025]: https://wiki.yandex-team.ru/kikimr/managers/roadmap-2025/
[helicopter]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/helicopterview2026h1/
[dependencies]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/team-dependencies-2026-h2/
[qp]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/tbl-yql/
[optimizer]: https://wiki.yandex-team.ru/kikimr/query-optimizer/
[new-optimizer]: https://wiki.yandex-team.ru/kikimr/developers/optimizer/novyjj-optimizator-zachem-kakojj-progress/
[runtime]: https://wiki.yandex-team.ru/kikimr/query-runtime/
[qp-monthly]: https://wiki.yandex-team.ru/kikimr/developers/monthly/query-processor-and-optimizer/.files/kqp-monthly-2026-09-21.pdf
[datashard]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/datashard/
[core]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/tbl-core/
[ds]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/tbl-ds/
[security]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/security/
[analytics-2026]: https://wiki.yandex-team.ru/kikimr/developers/analytics/ydb-analytics-2026-roadmap/
[cs]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/tbl-analytical-tables/
[cs-h1]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/columnshards/plans-cs-2026h1/
[fq-h1]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/fq-h12026/
[topics-monthly]: https://wiki.yandex-team.ru/kikimr/developers/monthly/logbroker-yds-ymq-sqs/
[topics-h1]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/logbroker-h1-2026/
[app-h2]: https://wiki.yandex-team.ru/kikimr/developers/appteam/roadmap/roadmap-ydb-appteam-2026-h2/
[ui-h2]: https://wiki.yandex-team.ru/kikimr/developers/ydbui/2026h2-plans-draft/
[engineering-h2]: https://wiki.yandex-team.ru/kikimr/ydb-engineering/roadmap-ydb-engteam-q3-q426-wip/
[ops-h2]: https://wiki.yandex-team.ru/kikimr/developers/operations/planning/okr2026h2/
[onprem-h2]: https://wiki.yandex-team.ru/kikimr/developers/roadmap/onprem-2026-h2/
[v2-process]: https://wiki.yandex-team.ru/kikimr/developers/operations/ydb-v1-v2-migration/
