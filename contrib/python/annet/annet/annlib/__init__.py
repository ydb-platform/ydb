import os

import colorama


# отключить colorama.init, если стоит env-переменная. Нужно в тестах
if os.environ.get("ANN_FORCE_COLOR", None) not in [None, "", "0", "no"]:
    colorama.init = lambda *_, **__: None
