import requests

from ydb.tests.library.harness.kikimr_runner import KiKiMR


class TestHiveHttpInterface(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR()
        cls.cluster.start()
        host = cls.cluster.nodes[1].host
        mon_port = cls.cluster.nodes[1].mon_port
        cls.endpoint = f'http://{host}:{mon_port}'

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def test_error_code_on_get_request(self):
        url = self.endpoint + '/tablets/app'
        params = {'TabletID': 72057594037968897, 'page': 'SetDown', 'node': 7, 'down': 1}

        get_request = requests.get(url, params=params)
        assert 400 <= get_request.status_code < 500

        post_request = requests.post(url, data=params)
        assert 200 <= post_request.status_code < 300
