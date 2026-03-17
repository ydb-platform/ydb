import library.python.testing.swag.ports
import os
import uuid
import yatest.common


class ConfigBase(object):
    def __init__(self, port=None, keep_work_dir=False, work_dir=None):
        self.host = 'localhost'
        self.instance_id = str(uuid.uuid4())
        self.keep_work_dir = keep_work_dir
        self.port = self.initialize_port(port)
        self.work_dir = self.initialize_work_dir(work_dir)
        self.db_path = os.path.join(self.work_dir, 'db')
        self.work_path = self.initialize_work_path()
        self.pid_file = os.path.join(self.work_dir, 'mongo.pid')
        self.log_path = os.path.join(self.work_dir, 'mongo.log')

    def initialize_port(self, port):
        return port

    def initialize_work_dir(self, work_dir):
        return work_dir

    def initialize_work_path(self):
        raise NotImplementedError

    @property
    def uri(self):
        return "{}:{}".format(self.host, self.port)


class ArcConfig(ConfigBase):
    def initialize_port(self, port):
        if port is None:
            return library.python.testing.swag.ports.find_free_port()
        return port

    def initialize_work_dir(self, work_dir):
        if work_dir is None:
            return yatest.common.output_path(self.instance_id)
        return work_dir

    def initialize_work_path(self):
        return yatest.common.work_path('')
