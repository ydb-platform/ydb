# Структура тематических каталогов документации {{ ydb-short-name }}

## Введение {#intro}

Статьи документации размещаются в иерархической структуре тематических директорий. Тематическая директория объединяет набор статей на определенную общую тематику.

Описание тематики включает в себя:
1. Целевую аудиторию, то есть для кого написана статья. Аудитория определяется ролью потенциального читателя при работе с {{ ydb-short-name }}, со следующими тремя базовыми аудиториями: 
   - Разработчик приложений на {{ ydb-short-name }}
   - Администратор {{ ydb-short-name }}
   - Разработчик/контрибьютор {{ ydb-short-name }}
2. Назначение -- задачу или проблему читателя, решение которой описывается в контенте.

В основном, структура директорий напрямую отражается на структуре оглавления документации. Типовыми исключениями являются:
- Промежуточные стадии создания документации. Новая директория формируется сразу, для сохранения ссылочной целостности в дальнейшем, но её контент еще недостаточен для того, чтобы быть оформленным отдельным подменю в оглавлении. В этом случае статьи могут быть временно включены в существующие подменю.
- Исторически сложившиеся директории, перенос которых нежелателен из-за потери ссылочной целостности.

## Структура 1-го уровня

На 1-м уровне размещены следующие тематические директории:

| Имя | Аудитория | Назначение |
| --- | ----------| ---------- |
| getting_started | Все | Предоставить быстрые решения типовых задач, которые возникают при начале работы с новой БД: что это, как поставить, как соединиться, как выполнить простейшие операции с данными, куда дальше по каким вопросам ходить в документации |
| concepts | Разработчик приложений | Ознакомить с основными структурными компонентами {{ ydb-short-name }}, с которыми придется иметь дело при разработке приложений под {{ ydb-short-name }}. Дать понимание роли каждого такого компонента при решении задач разработки приложений. Предоставить расширенную информацию по вариантам конфигурации компонентов, доступных разработчик приложений. |
| reference, yql | Разработчик приложений | Справочник для ежедневного использования по инструментам доступа к функциям {{ ydb-short-name }}: CLI, YQL, SDK |
| best_practices | Разработчик приложений | Типовые подходы к решению основных задач, возникающих при разработке приложений |
| troubleshooting | ? | Инструменты локализации причин проблем |
| how_to_edit_docs | Разработчики {{ ydb-short-name }} | Как дорабатывать документацию по {{ ydb-short-name }} |
| deploy | Все | Развертывание и конфигурирование баз данных и кластеров {{ ydb-short-name }}. Облачные, оркестрируемые, ручные установки. |
| maintenance | Все | Как обслуживать базы данных и кластера {{ ydb-short-name }}: резервные копии, мониторинг, логи, замена дисков. |
| faq | Все | Вопросы и ответы |

