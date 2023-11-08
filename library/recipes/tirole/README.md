Tirole recipe
--

Этот рецепт позволяет в тестах поднять демон, который скрывается за `tirole-api.yandex.net`.
Демон слушает на порте из файла `tirole.port` - только http.

База ролей в tirole - это каталог в Аркадии с файлами:
  * `<slug>.json` - роли, которые надо отдавать в API
  * `mapping.yaml` - соответствие между slug и tvmid

В рецепте API принимает service-тикеты с dst==1000001: их можно получать из `tvmapi`/`tvmtool`, запущеного в рецепте.

Примеры:
1. `ut_simple`

Вопросы можно писать в [PASSPORTDUTY](https://st.yandex-team.ru/createTicket?queue=PASSPORTDUTY&_form=77618)
