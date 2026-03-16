# Material for MkDocs

Версия: 4.1.1  
Оригинал библиотеки: https://github.com/squidfunk/mkdocs-material/tree/4.1.1  

## При обновлении
- Удаляем папку `src/partials/integrations` и все упоминания о ней из кода
- Удаляем из `src/base.html` упоминания о `gstatic.com` и `googleapis.com`
- Пересобираем проект через команду `npm run build`
- Обновляем внутри ya.make `RESOURCE_FILES()`
- Подробности в CONTRIB-607

## Активация интранет поиска
Чтобы заменить стандартный поиск поиском по интранет, нужно внутри конфигурационного .yml файла указать:
```yml
extra:
  search:
    engine: intranet
    scope: [scope]
    product: [product]
```
О параметрах `scope` и `product` можно подробнее узнать из документации на [вики](https://wiki.yandex-team.ru/intranetpoisk/api/).
