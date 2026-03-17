from annet.generators import PartialGenerator


class InitialConfig(PartialGenerator):
    """
    Конфиги у свежих (еще ни разу не настраиваемых устройств)
    на самом деле НЕ пустые. В данном генераторе отображен
    такой набор команд, по крайней мере тех, которые могут
    изменяться в ходе первичной конфигурации.

    Acl для данного генератора не нужен, он будет генерировать
    конфиг целиком.
    """

    def __init__(self, storage, do_run: bool = False):
        super().__init__(storage=storage)
        self._do_run = do_run

    def run_huawei(self, device):
        if not self._do_run:
            return
        if device.hw.CE:
            yield """
            telnet server disable
            telnet ipv6 server disable
            diffserv domain default
            aaa
                authentication-scheme default
                authorization-scheme default
                accounting-scheme default
                domain default
                domain default_admin
            """
