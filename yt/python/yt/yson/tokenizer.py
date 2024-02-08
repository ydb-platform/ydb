from .yson_token import YsonToken, TOKEN_START_OF_STREAM
from .lexer import YsonLexer
from .common import _ENCODING_SENTINEL


class YsonTokenizer(object):
    def __init__(self, input_str, encoding=_ENCODING_SENTINEL, output_buffer=None):
        self._token = YsonToken(type=TOKEN_START_OF_STREAM)
        self._lexer = YsonLexer(input_str, encoding=encoding, output_buffer=output_buffer)

    def parse_next(self):
        self._token = self._lexer.get_next_token()

    def get_current_token(self):
        return self._token

    def get_current_type(self):
        return self.get_current_token().get_type()

    def get_position_info(self):
        return self._lexer.get_position_info()
