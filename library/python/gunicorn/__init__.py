import gunicorn.app.base


class StandaloneApplication(gunicorn.app.base.Application):
    def __init__(self, wsgiapp):
        self.wsgiapp = wsgiapp
        super(StandaloneApplication, self).__init__()

    def init(self, parser, opts, args):
        pass

    def load(self):
        return self.wsgiapp


class GunicornApp(gunicorn.app.base.BaseApplication):
    def __init__(self, wsgiapp, host, port, workers, reload_type, timeout):
        self.wsgiapp = wsgiapp
        self.host = host
        self.port = port
        self.workers = workers
        self.reload_type = reload_type
        self.timeout = timeout
        super(GunicornApp, self).__init__()

    def load_config(self):
        self.cfg.set('workers', self.workers)
        self.cfg.set('reload', self.reload_type == 'on')
        self.cfg.set('timeout', self.timeout)
        self.cfg.set('bind', '{}:{}'.format(self.host, self.port))

    def load(self):
        return self.wsgiapp


def run_standalone(wsgiapp):
    StandaloneApplication(wsgiapp).run()


def run(wsgiapp, host='[::]', port='80', workers=16, reload_type='off', timeout=90):
    GunicornApp(wsgiapp, host, port, workers, reload_type, timeout).run()
