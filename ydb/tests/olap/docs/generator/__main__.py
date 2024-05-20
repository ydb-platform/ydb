import ydb.tests.olap.scenario.helpers as sth
from ydb.tests.olap.docs.generator.parser import parse_docstring
import inspect
import types
import os
import yaml
import shutil
import sys
import typing
import re
from logging import error, getLogger
from optparse import OptionParser


class ModulePrinter:
    def __init__(self, options, module: types.ModuleType) -> None:
        self._options = options
        self._module = module
        self._output = None

    def _get_anchor(self, obj: any, with_module: bool) -> str | None:
        def _is_interest_module(module_name: str) -> bool:
            return module_name.startswith(self._options.name_prefix)

        def _get_module_anchor(module_name: str) -> str:
            return f'./{self._get_stricted_name(module_name)}.md' if with_module else ''

        if inspect.ismodule(obj) and _is_interest_module(obj.__name__):
            return _get_module_anchor(obj.__name__)
        if inspect.isclass(obj) and _is_interest_module(obj.__module__):
            return f'{_get_module_anchor(obj.__module__)}#{obj.__qualname__}'
        if (inspect.ismethod(obj) or inspect.isfunction(obj) or inspect.isdatadescriptor(obj)) and _is_interest_module(obj.__module__):
            return f'{_get_module_anchor(obj.__module__)}#{obj.__qualname__.replace("_", "")}'
        return None

    def _get_docs_link(self, obj: any, link_text: str, no_link_text: str | None = None) -> str:
        link = self._get_anchor(obj, with_module=True)
        return f'[{link_text}]({link})' if link is not None else no_link_text

    def get_doc(self, obj) -> dict:
        def _subst_links(descr: str) -> str:
            if isinstance(obj, type):
                # class
                obj_globals = sys.modules[obj.__module__].__dict__
                obj_locals = dict(vars(obj))
            elif isinstance(obj, types.ModuleType):
                # module
                obj_globals = obj.__dict__
                obj_locals = None
            elif callable(obj):
                obj_globals = obj.__globals__
                obj_locals = None
            pat = re.compile(r'\{[a-zA-Z_]+[a-zA-Z0-9_\.]*\}')
            tmp = []
            for m in pat.finditer(descr):
                start, end = m.span()
                name = m.group(0).strip('{}')
                module = sys.modules.get(name)
                if module is None:
                    try:
                        evaluted = eval(name, obj_globals, obj_locals)
                        link = self._get_docs_link(evaluted, '{#T}', name)
                    except BaseException as e:
                        error(f'cannot evalute {name}: {e}')
                        link = name
                else:
                    name = self._get_stricted_name(name)
                    link = self._get_docs_link(module, '{#T}', name)
                tmp.append((start, end, link))
            for start, end, link in reversed(tmp):
                descr = descr[:start] + link + descr[end:]
            return descr

        result = inspect.getdoc(obj)
        if result is not None:
            result = parse_docstring(result)
            if 'description' in result:
                result['description'] = _subst_links(result['description'])
            for p in result.get('parameters', []):
                if 'description' in p:
                    p['description'] = _subst_links(p['description'])
        return result

    def print_description(self, doc):
        if doc is not None:
            description = doc.get('description')
            if description is not None:
                self._output.write(f'{description}\n')

    def print_examples(self, doc):
        if doc is None:
            return
        examples = []
        for s in doc.get('sections', []):
            if s.get('section_header', '') in {'Example', 'Examples'}:
                if s.get('section_body', '') != '':
                    examples.append(s.get('section_body'))
        if len(examples) == 0:
            return
        self._output.write('{% list tabs %}\n')
        for i in range(len(examples)):
            example = '\n'.join([f'  {s}' for s in examples[i].splitlines()])
            self._output.write(f'- Example {i + 1}\n  ```python\n{example}\n  ```\n')
        self._output.write('{% endlist %}\n')

    def print_toc(self, *entities):
        for map in entities:
            for _, sub in map:
                self._output.write(f'**##{self._get_docs_link(sub, "{#T}", sub.__name__)}##**\n')
        self._output.write('\n')

    @staticmethod
    def _get_github_link(obj, text: str, header_level: int) -> str:
        file = inspect.getsourcefile(obj)
        if file is None:
            return text
        contrib_prefix = 'contrib/'
        if file.startswith(f'{contrib_prefix}ydb'):
            file = file[len(contrib_prefix):]
        _, line = inspect.findsource(obj)
        size = 27 - 3 * header_level
        return f'[![Code on Github](../_assets/github_icon.png "Github" ={size}x) {text}](https://github.com/ydb-platform/ydb/blob/main/{file}?#L{line + 1})'

    def print_header(self, header: str, level: int) -> None:
        a = '#' * level
        self._output.write(f'{a} {header}\n')

    @staticmethod
    def get_subclasses(in_obj, predicate=None):
        def _check(x):
            if not inspect.isclass(x):
                return False
            return predicate is None or predicate(x)

        classes = {cl[1]: cl[0] for cl in filter(lambda x: not x[0].startswith('_'), inspect.getmembers(in_obj, _check))}
        tree = inspect.getclasstree(classes, True)
        result = []

        def _tree_walk(element):
            if isinstance(element, list):
                for e in element:
                    _tree_walk(e)
            elif element[0] in classes:
                result.append((classes[element[0]], element[0]))

        _tree_walk(tree)
        return result

    def print_subclasses(self, subclasses, level: int):
        for _, sub in subclasses:
            self.print_class(sub, level)

    @staticmethod
    def get_functions(in_obj, predicate=None):
        def _check(x):
            if not inspect.ismethod(x) and not inspect.isfunction(x):
                return False
            return predicate is None or predicate(x)
        special_names = set({'__init__', '__call__'})
        return [fn for fn in filter(lambda x: not x[0].startswith('_') or x[0] in special_names, inspect.getmembers(in_obj, _check))]

    def print_functions(self, functions, level: int):
        for _, sub in functions:
            self.print_method(sub, level)

    def print_class(self, cl: type, level: int):
        self.print_header(f'{self._get_github_link(cl, "Class " + cl.__qualname__, level)} {{{self._get_anchor(cl, False)}}}', level)
        subclasses = self.get_subclasses(cl)
        functions = self.get_functions(cl)
        self.print_toc(subclasses, functions)
        if cl.__base__ is not None and cl.__base__.__module__.startswith(self._options.name_prefix):
            self.print_header('Inherits', 6)
            self._output.write(f'{self._get_docs_link(cl.__base__, "{#T}", f"{cl.__base__.__module__}.{cl.__base__.__qualname__}")}\n')
        doc = self.get_doc(cl)
        self.print_description(doc)
        self.print_examples(doc)
        self.print_subclasses(subclasses, level + 1)
        self.print_functions(functions, level + 1)

    def print_method(self, method: types.MethodType | types.FunctionType, level: int):
        def _types_with_links(annotation: any) -> str:
            def _get_link(a: any) -> str:
                if inspect.isclass(a):
                    name = a.__name__
                else:
                    name = str(a)
                name = name.replace('typing.', '')
                return self._get_docs_link(a, f'**`{name}`**', f'`{name}`') if a is not None else '`None`'

            if isinstance(annotation, str):
                return f'`{annotation}`'
            if typing.get_origin(annotation) is types.UnionType or typing.get_origin(annotation) is typing.Union: # noqa
                return ' | '.join([_get_link(a) for a in typing.get_args(annotation)])
            return _get_link(annotation)

        if inspect.ismethod(method):
            type = 'Class method'
        elif inspect.isfunction(method):
            type = 'Method'
        elif inspect.isdatadescriptor(method):
            type = 'Property'
        else:
            type = repr(method.__class__)

        header_text = type + ' ' + method.__qualname__.replace('_', '\\_')
        self.print_header(f'{self._get_github_link(method, header_text, level)} {{{self._get_anchor(method, False)}}}', level)
        doc = self.get_doc(method)
        self.print_description(doc)
        try:
            method_annotations = inspect.get_annotations(method, eval_str=True)
        except BaseException as e:
            error(f'get annotations for {method.__qualname__}: {e}')
            method_annotations = {}
        sig = inspect.signature(method)
        params_desc = {}
        ret_descr = ''
        if doc is not None:
            params_desc = {p['name']: p['description'].strip(' \n\r') for p in doc.get('parameters', [])}
            for s in doc.get('sections', []):
                if s.get('section_header', '') == 'Returns':
                    ret_descr = s.get('section_body')
        params = [p for p in filter(lambda x: x[0] not in {'self', 'cls'}, sig.parameters.items())]
        if len(params) > 0:
            self.print_header('Parameters', 6)
            for name, p in params:
                annotations = []
                if p.annotation != p.empty:
                    annotations.append(_types_with_links(method_annotations.get(name, p.annotation)))
                default = ''
                if p.default == p.empty:
                    annotations.append('`required`')
                else:
                    default = f' \n_Default value_\n `{repr(p.default)}`\n'
                ann_str = ' (' + ', '.join(annotations) + ')' if len(annotations) > 0 else ''
                param_desc = params_desc.get(name, '')
                self._output.write(f'`{name}`{ann_str}\n{param_desc}\n{default}' '')
        if ret_descr != '' or sig.return_annotation != sig.empty:
            ann_str = ''
            if sig.return_annotation != sig.empty:
                ann_str = f'Type: {_types_with_links(method_annotations.get("return", sig.return_annotation))}.\n'
            self.print_header('Returns', 6)
            self._output.write(f'{ann_str}{ret_descr}\n')
        self.print_examples(doc)

    def _get_stricted_name(self, module: types.ModuleType | str) -> str:
        if isinstance(module, types.ModuleType):
            return self._get_stricted_name(module.__name__)
        if module.startswith(self._options.name_prefix):
            return module[len(self._options.name_prefix) :]
        return module

    def print_module(self) -> None:
        name = self._get_stricted_name(self._module)
        submodules = inspect.getmembers(
            self._module, lambda x: inspect.ismodule(x) and x.__name__.startswith(self._options.name_prefix)
        )
        subclasses = self.get_subclasses(self._module, lambda x: inspect.getmodule(x) == self._module)
        functions = self.get_functions(self._module, lambda x: inspect.getmodule(x) == self._module)
        self._output = open(os.path.join(self._options.out_dir, 'scenario_tests', f'{name}.md'), 'w', encoding='utf-8')
        with self._output:
            self.print_header(f'{self._get_github_link(self._module, "Module " + name, 2)}', 2)
            self.print_toc(submodules, subclasses, functions)
            doc = self.get_doc(self._module)
            self.print_description(doc)
            self.print_examples(doc)
            self.print_subclasses(subclasses, 3)
            self.print_functions(functions, 3)
        for _, sub in submodules:
            ModulePrinter(module=sub, options=self._options).print_module()

    def generate_toc(self):
        name = self._get_stricted_name(self._module)
        submodules = inspect.getmembers(
            self._module, lambda x: inspect.ismodule(x) and x.__name__.startswith(self._options.name_prefix)
        )
        children = [ModulePrinter(module=sub, options=self._options).generate_toc() for _, sub in submodules]
        toc = {'name': name, 'href': f'scenario_tests/{name}.md'}
        if len(children) > 0:
            toc['items'] = children
        return toc


def main():
    getLogger().setLevel('INFO')
    parser = OptionParser(usage='usage: %prog [options]')
    parser.add_option('-o', '--output-dir', dest='out_dir', help='Output dir')
    parser.add_option('-p', '--name-prefix', dest='name_prefix', help='root module name prefix', default='ydb.tests.olap.')
    options, _ = parser.parse_args()
    if os.path.exists(options.out_dir):
        shutil.rmtree(options.out_dir)
    os.makedirs(os.path.join(options.out_dir, 'scenario_tests'), exist_ok=True)
    printer = ModulePrinter(options, sth)
    printer.print_module()
    toc = {
        'title': 'Scenario tests',
        'items': [
            {
                'name': 'Introduction',
                'href': 'scenario_tests/introduction.md'
            },
            printer.generate_toc()
        ]
    }
    with open(os.path.join(options.out_dir, 'toc.yaml'), 'w', encoding='utf-8') as f:
        yaml.dump(toc, f)


if __name__ == '__main__':
    main()
