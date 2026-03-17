from parse import Parser


class ExtendedParser(Parser):
    def _handle_field(self, field):
        # handle as path parameter field
        field = field[1:-1]
        path_parameter_field = "{%s:PathParameter}" % field
        return super(ExtendedParser, self)._handle_field(
            path_parameter_field)


def parse_path_parameter(text):
    return text


parse_path_parameter.pattern = r"[^\/]+"
parse_path_parameter.name = "PathParameter"


def search(path_pattern, full_url_pattern):
    extra_types = {parse_path_parameter.name: parse_path_parameter}
    p = ExtendedParser(path_pattern, extra_types)
    p._expression = p._expression + '$'
    return p.search(full_url_pattern)


def parse(server_url, server_url_pattern):
    extra_types = {parse_path_parameter.name: parse_path_parameter}
    p = ExtendedParser(server_url, extra_types)
    p._expression = '^' + p._expression
    return p.parse(server_url_pattern)
