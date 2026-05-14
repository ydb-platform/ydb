class Response:
    def __init__(self, status=200, payload=None, text="error", json_exception=None):
        self.status = status
        self.payload = payload or {}
        self.text_value = text
        self.json_exception = json_exception

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def text(self):
        return self.text_value

    async def json(self):
        if self.json_exception is not None:
            raise self.json_exception
        return self.payload


class ClientSession:
    def __init__(self, response):
        self.response = response
        self.post_calls = []
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def post(self, url, json):
        self.post_calls.append((url, json))
        return self.response

    def get(self, url):
        self.get_calls.append(url)
        return self.response


class Task:
    def __init__(self):
        self.updates = []

    async def update(self, **kwargs):
        self.updates.append(kwargs)


class ParentTask(Task):
    def __init__(self):
        super().__init__()
        self.subtasks = []

    async def add_subtask(self, *args, **kwargs):
        task = Task()
        self.subtasks.append((args, kwargs, task))
        return task
