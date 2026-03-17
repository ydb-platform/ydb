class Application:
    def __init__(self, device, package):
        self._device = device
        self._package = package

    def pid(self):
        pass

    def uid(self):
        pass

    @property
    def tcp_recv(self):
        return 0

    @property
    def tcp_send(self):
        return 0
