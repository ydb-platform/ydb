TVM-API recipe
--

Этот рецепт позволяет в тестах поднять демон, который скрывается за `tvm-api.yandex.net`.
Демон слушает на порте из файла `tvmapi.port` - только http.

База у этого демона в read-only режиме, список доступных TVM-приложений с секретами лежит [здесь](clients/clients.json).

Публичные ключи этого демона позволяют проверять тикеты, сгенерированные через `tvmknife unittest`.

Примеры:
1. `ut_simple` - поднимается tvm-api

Примеры комбинирования с tvmtool можно найти в `library/recipes/tvmtool`

Вопросы можно писать в [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)
