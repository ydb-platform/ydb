class ExecError(Exception):
    """Обработчик этого exception должен залоггировать ошибку и выйти с exit_code 1"""

    pass


class DeployCancelled(Exception):
    """Деплой на устройство был отменен"""

    pass
