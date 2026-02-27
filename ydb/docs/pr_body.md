### Changelog entry <!-- a user-readable short description of the changes that goes to CHANGELOG.md and Release Notes -->

Обновлено описание гарантий доставки данных для потоковых запросов. Теперь явно указано использование At-least-once. Добавлена информация о текущем поведении и ограничениях для запросов с группировкой по окнам. Также добавлены ограничения по использованию «важного читателя» и автопартиционирования.

### Changelog category <!-- remove all except one -->
* Documentation (changelog entry is not required)

### Description for reviewers <!-- (optional) description for those who read this PR -->

- В описании гарантий доставки (guarantees) теперь указано At-least-once для всех сценариев (включая UPSERT), взамен разделения на at-least-once и best effort.
- Добавлено уточнение по поводу работы с группировкой по окнам: возможно временное запаздывание или неполнота агрегатов из-за задержки данных из различных партиций.
- Указана версия 26.1 для исправления этого ограничения.
- Добавлены ограничения на использование признака «важного читателя» (important consumer) и автопартиционирования для топиков, используемых в потоковых запросах.
