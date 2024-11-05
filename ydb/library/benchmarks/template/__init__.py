from jinja2 import Environment, FileSystemLoader
from library.python import resource


class Loader(FileSystemLoader):
    def __init__(self, searchpath, encoding='utf-8', followlinks=False):
        super().__init__(searchpath, encoding, followlinks)
        self.static = {}
        self.links = {}
        self.loaded = {}

    def get_source(self, environment, template):
        if template in self.links:
            text, tmpl, flag = self.get_source(environment, self.links[template])
        elif template in self.static:
            text, tmpl, flag = self.static[template], template, lambda: True
        elif resource.find(template) is not None:
            x = resource.find(template)
            text, tmpl, flag = x.decode('utf-8'), template, lambda: True
        else:
            text, tmpl, flag = super().get_source(environment, template)

        self.loaded[tmpl] = 1
        return text, tmpl, flag

    def add_source(self, name, content):
        self.static[name] = content

    def add_link(self, f, to):
        self.links[f] = to


class Builder:
    def __init__(self, paths=[]):
        self.loader = Loader(paths)
        self.env = Environment(loader=self.loader)
        self.vars = {}

    def add(self, name: str, text: str):
        self.loader.add_source(name, text)

    def add_link(self, f: str, to: str):
        self.loader.add_link(f, to)

    def add_from_file(self, name: str, file: str):
        with open(file, "r") as f:
            return self.template(name, f.read())

    def add_vars(self, v):
        self.vars.update(v)

    def expose_vars(self, name: str):
        m = self.env.get_template(name).make_module(self.vars)
        for k, v in m.__dict__.items():
            if not k.startswith("_"):
                self.vars[k] = v

    def replace_vars(self, v):
        self.vars = v

    def build(self, name: str, expose_all=False):
        t = self.env.get_template(name)
        if expose_all:
            t.render(self.vars)
            for k in self.loader.loaded.keys():
                if k != name:
                    self.expose_vars(k)

        return t.render(self.vars)


class ResultFormatter:
    def __init__(self, fmt):
        self.fmt = fmt

    def _is_double(self, t):
        if (str(t[1]) == 'Double'):
            return True

        if (t[0] == 'OptionalType'):
            return self._is_double(t[1])

        return False

    def _is_date(self, t):
        if (str(t[1]) == 'Date'):
            return True

        if (t[0] == 'OptionalType'):
            return self._is_date(t[1])

        return False

    def _format_date(self, d):
        import datetime
        seconds = int(d) * 86400
        dd = datetime.datetime.fromtimestamp(seconds)
        return str(dd.date())

    def _format_double(self, d):
        t = self.fmt % (d)
        if 'e' not in t:
            t = t.rstrip('0').rstrip('.')
            if t == '-0':
                t = '0'
        return t

    def _format(self, r):
        cols = len(r["Type"][1][1])
        doubles = []
        dates = []
        for i in range(cols):
            t = r["Type"][1][1][i]
            if self._is_double(t[1]):
                doubles.append(i)

            if self._is_date(t[1]):
                dates.append(i)

        for row in r["Data"]:
            for i in range(len(row)):
                if isinstance(row[i], list):
                    if (len(row[i]) == 0):
                        row[i] = 'NULL'
                    else:
                        row[i] = row[i][0]

            for i in doubles:
                if row[i] != 'NULL' and row[i] is not None:
                    row[i] = self._format_double(float(row[i]))

            for i in dates:
                if row[i] != 'NULL' and row[i] is not None:
                    row[i] = self._format_date(float(row[i]))

    def format(self, res):
        # {'Write': [{'Type': ['ListType', ['StructType', [['cntrycode', ['DataType', 'String']], ['numcust', ['DataType', 'Uint64']], ['totacctbal', ['DataType', 'Double']]]]]
        for x in res:
            for y in x['Write']:
                self._format(y)
