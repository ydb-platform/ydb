from mock import patch
from pretend import stub

from zeep import __main__, client


def test_main_no_args(monkeypatch):
    def mock_init(self, *args, **kwargs):
        self.wsdl = stub(dump=lambda: None)

    monkeypatch.setattr(client.Client, "__init__", mock_init)
    args = __main__.parse_arguments(["foo.wsdl"])
    __main__._main(args)


def test_main_extract_auth(monkeypatch):
    def mock_init(self, *args, **kwargs):
        self.wsdl = stub(dump=lambda: None)

    monkeypatch.setattr(client.Client, "__init__", mock_init)

    with patch.object(__main__, "Transport", autospec=True) as mock_transport:
        args = __main__.parse_arguments(
            ["http://user:secret@tests.python-zeep.org/foo.wsdl"]
        )

        __main__._main(args)

        assert mock_transport.call_count == 1

        args, kwargs = mock_transport.call_args
        assert kwargs["session"].auth == ("user", "secret")
