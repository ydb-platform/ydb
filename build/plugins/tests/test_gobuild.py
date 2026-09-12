import pytest

import gobuild


class FakeUnit:
    def __init__(self, path='project/pkg', variables=None, flags=('_GO_FMT_ADD_CHECK',), sources=(), resolutions=None):
        self.module_path = path
        self.variables = dict(variables or {})
        self.flags = set(flags)
        self.checks = []
        self.resolutions = {source: self.path() + '/' + source for source in sources}
        self.resolutions.update(resolutions or {})
        self.calls = []

    def path(self):
        return '$S/' + self.module_path

    def get(self, name):
        if isinstance(name, list):
            name = name[0]
        return self.variables.get(name, '')

    def set(self, args):
        name, value = args
        self.variables[name] = value

    def enabled(self, name):
        return name in self.flags

    def resolve_arc_path(self, args):
        path = args[0] if isinstance(args, list) else args
        # TModuleWrapper::ResolveToArcPath returns $-prefixed input unchanged.
        # ResolveSourcePath(Default) returns an empty string when not found.
        if path.startswith('$'):
            return path
        return self.resolutions.get(path, '')

    def filename(self):
        return 'pkg.a'

    def __getattr__(self, name):
        if name not in {
            'on_go_gen_cover',
            'on_go_proto_cmd',
            'on_go_flatc_cmd',
            'onsrc',
            'on_go_compile_symabis',
            'oncopy_file_with_context',
            'onaddincl',
            'onpeerdir',
            'on_go_compile_cgo1',
            'on_go_compile_cgo2',
            'onrun_program',
        }:
            raise AttributeError(name)
        return lambda args: self.calls.append((name, args))

    def onadd_check(self, args):
        self.checks.append(args)


@pytest.mark.parametrize(
    'srcs, glob, expected',
    [
        pytest.param('', '', [], id='empty'),
        pytest.param('explicit.go', '', ['explicit.go'], id='explicit-only'),
        pytest.param('', 'first.go second.go', ['first.go', 'second.go'], id='glob-only'),
        pytest.param('explicit.go', 'first.go second.go', ['explicit.go', 'first.go', 'second.go'], id='mixed'),
        pytest.param('${BINDIR}/generated.go', 'first.go', ['first.go'], id='generated-and-glob'),
        pytest.param('first.go', 'first.go second.go', ['first.go', 'second.go'], id='overlap'),
        pytest.param(
            '${ARCADIA_ROOT}/project/pkg/first.go',
            'first.go second.go',
            ['first.go', 'second.go'],
            id='overlap-after-resolution',
        ),
    ],
)
def test_gofmt_source_files(srcs, glob, expected):
    unit = FakeUnit(
        variables={
            '_GO_SRCS_VALUE': srcs,
            '_ALL_GO_FILES': ' '.join('${ARCADIA_ROOT}/project/pkg/' + path for path in glob.split()),
        },
        sources=('explicit.go', 'first.go', 'second.go'),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == ([['gofmt'] + ['$S/project/pkg/' + path for path in expected]] if expected else [])
    # Collecting style inputs must not add globbed files to the compilation inputs here.
    assert unit.get('_GO_SRCS_VALUE') == srcs


@pytest.mark.parametrize('test_module', [False, True])
def test_gofmt_includes_test_sources(test_module):
    flags = ['_GO_FMT_ADD_CHECK']
    if test_module:
        flags.append('GO_TEST_MODULE')
    unit = FakeUnit(
        variables={
            '_ALL_GO_FILES': '${ARCADIA_ROOT}/project/pkg/main.go',
            '_GO_TEST_SRCS_VALUE': 'main_test.go',
            '_GO_XTEST_SRCS_VALUE': 'external_test.go',
        },
        flags=flags,
        sources=('main_test.go', 'external_test.go'),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [
        ['gofmt', '$S/project/pkg/main.go', '$S/project/pkg/main_test.go', '$S/project/pkg/external_test.go']
    ]


@pytest.mark.parametrize('glob_dir', ['project/pkg', 'project/pkg/gotest'])
def test_gofmt_test_for_skips_production_sources(glob_dir):
    unit = FakeUnit(
        path='project/pkg/gotest',
        variables={
            'GO_TEST_FOR_DIR': '$S/project/pkg',
            '_GO_SRCS_VALUE': '${ARCADIA_ROOT}/project/pkg/main.go',
            '_ALL_GO_FILES': '${ARCADIA_ROOT}/' + glob_dir + '/main.go',
            '_GO_XTEST_SRCS_VALUE': 'external_test.go',
        },
        flags=('_GO_FMT_ADD_CHECK', 'GO_TEST_MODULE'),
        sources=('external_test.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [['gofmt', '$S/project/pkg/gotest/external_test.go']]


@pytest.mark.parametrize('path', ['vendor/pkg', 'contrib/pkg'])
def test_gofmt_skips_third_party_sources(path):
    unit = FakeUnit(path=path, variables={'_ALL_GO_FILES': '${ARCADIA_ROOT}/' + path + '/main.go'})

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == []


def test_gofmt_respects_allowed_skip():
    unit = FakeUnit(
        variables={
            '_ALL_GO_FILES': '${ARCADIA_ROOT}/project/pkg/main.go',
            '_GO_FMT_ALLOW_SKIP': 'project',
        },
        flags=(),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == []


@pytest.mark.parametrize(
    'path',
    [
        'main.go',
        '$S/project/pkg/main.go',
        '${ARCADIA_ROOT}/project/pkg/main.go',
        '${CURDIR}/main.go',
        '$S/project/pkg/./main.go',
        '${CURDIR}/sub/../main.go',
        r'$S/project\pkg\main.go',
        r'${CURDIR}\main.go',
        '/checkout/project/pkg/main.go',
        'from_srcdir.go',
    ],
)
@pytest.mark.parametrize('variable', ['_GO_SRCS_VALUE', '_GO_TEST_SRCS_VALUE', '_GO_XTEST_SRCS_VALUE'])
def test_gofmt_accepts_source_paths(path, variable):
    if variable != '_GO_SRCS_VALUE':
        path = path.replace('.go', '_test.go')
        filename = 'main_test.go'
    else:
        filename = 'main.go'
    unit = FakeUnit(
        variables={variable: path},
        sources=(filename,),
        resolutions={
            '/checkout/project/pkg/' + filename: '$S/project/pkg/' + filename,
            'from_srcdir' + ('_test.go' if variable != '_GO_SRCS_VALUE' else '.go'): '$S/project/pkg/' + filename,
        },
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [['gofmt', '$S/project/pkg/' + filename]]


@pytest.mark.parametrize(
    'path',
    [
        'missing.go',
        'generated.go',
        '$B/project/pkg/generated.go',
        '${ARCADIA_BUILD_ROOT}/project/pkg/generated.go',
        '${BINDIR}/generated.go',
        '$U/missing.go',
        '$L/context/$S/project/pkg/main.go',
        '${UNKNOWN}/main.go',
        '${ARCADIA_ROOT}other/main.go',
        '$S/../../outside.go',
    ],
)
def test_gofmt_rejects_non_source_paths(path):
    unit = FakeUnit(
        variables={'_GO_SRCS_VALUE': path},
        resolutions={'generated.go': '$B/project/pkg/generated.go'},
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == []
    assert unit.get('_GO_SRCS_VALUE') == path


def test_gofmt_deduplicates_normalized_explicit_and_glob_paths():
    unit = FakeUnit(
        variables={
            '_GO_SRCS_VALUE': 'main.go $S/project/pkg/./main.go ${CURDIR}/sub/../main.go',
            '_ALL_GO_FILES': '$_ALL_GO_FILES ${ARCADIA_ROOT}/project/pkg/main.go',
        },
        sources=('main.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [['gofmt', '$S/project/pkg/main.go']]
    assert unit.get('_GO_SRCS_VALUE') == 'main.go $S/project/pkg/./main.go ${CURDIR}/sub/../main.go'


def test_gofmt_groups_sources_by_resolved_directory():
    unit = FakeUnit(
        variables={'_GO_SRCS_VALUE': 'local.go shared.go'},
        sources=('local.go',),
        resolutions={'shared.go': '$S/project/shared/shared.go'},
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [
        ['gofmt', '$S/project/pkg/local.go'],
        ['gofmt', '$S/project/shared/shared.go'],
    ]


@pytest.mark.parametrize(
    'path, expected',
    [
        ('$S/vendor/pkg/main.go', []),
        ('${ARCADIA_ROOT}/contrib/pkg/main.go', []),
        ('$S/project/../vendor/pkg/main.go', []),
        (r'$S/vendor\pkg/main.go', []),
        ('$S/vendorish/pkg/main.go', [['gofmt', '$S/vendorish/pkg/main.go']]),
        ('$S/contributor/pkg/main.go', [['gofmt', '$S/contributor/pkg/main.go']]),
    ],
)
def test_gofmt_filters_resolved_third_party_paths(path, expected):
    unit = FakeUnit(variables={'_GO_SRCS_VALUE': path})

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == expected


@pytest.fixture
def errors(monkeypatch):
    result = []
    monkeypatch.setattr(gobuild.ymake, 'report_configure_error', result.append, raising=False)
    return result


@pytest.mark.parametrize('allow_skip', ['', 'project/pk', 'project/pkg-other'])
def test_gofmt_reports_prohibited_skip_and_keeps_checks(allow_skip, errors):
    unit = FakeUnit(
        variables={'_GO_SRCS_VALUE': 'main.go', '_GO_FMT_ALLOW_SKIP': allow_skip},
        flags=(),
        sources=('main.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert errors == ['Disabling gofmt is prohibited, please contact devtools']
    assert unit.checks == [['gofmt', '$S/project/pkg/main.go']]


@pytest.mark.parametrize('allow_skip', ['project/pkg', 'project/', 'other project/pkg'])
def test_gofmt_allows_exact_and_parent_skip(allow_skip, errors):
    unit = FakeUnit(
        variables={'_GO_SRCS_VALUE': 'main.go', '_GO_FMT_ALLOW_SKIP': allow_skip},
        flags=(),
        sources=('main.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert errors == []
    assert unit.checks == []


@pytest.mark.parametrize(
    'variable, path, message',
    [
        ('_GO_SRCS_VALUE', 'main_test.go', 'must be listed in GO_TEST_SRCS() or GO_XTEST_SRCS()'),
        ('_GO_TEST_SRCS_VALUE', 'main.go', 'should not be listed in GO_TEST_SRCS() or GO_XTEST_SRCS()'),
        ('_GO_XTEST_SRCS_VALUE', 'main.go', 'should not be listed in GO_TEST_SRCS() or GO_XTEST_SRCS()'),
    ],
)
def test_go_source_classification_reports_invalid_test_files(variable, path, message, errors):
    unit = FakeUnit(variables={variable: path})

    gobuild._GO_PROCESS_SRCS(unit)

    assert len(errors) == 1
    assert message in errors[0]


def test_gofmt_keeps_original_sources_before_coverage():
    unit = FakeUnit(
        variables={
            '_GO_SRCS_VALUE': 'main.go',
            '_ALL_GO_FILES': '${ARCADIA_ROOT}/project/pkg/other.go',
            '_GO_PACKAGE_VALUE': 'pkg',
        },
        flags=('_GO_FMT_ADD_CHECK', 'GO_TEST_MODULE', 'GO_TEST_COVER'),
        sources=('main.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [['gofmt', '$S/project/pkg/main.go', '$S/project/pkg/other.go']]
    assert unit.calls == [('on_go_gen_cover', ['pkg', 'main.go'])]
    assert unit.get('GO_COVER_MODE') == 'set'
    assert unit.get('_GO_SRCS_VALUE') == ''


@pytest.mark.parametrize('module', ['project/pkg', 'vendor/pkg', 'contrib/pkg'])
def test_gofmt_preserves_govet_check(module):
    unit = FakeUnit(
        path=module,
        variables={'_GO_SRCS_VALUE': 'main.go', 'GO_VET_REPORT_EXT': '.vet.txt'},
        flags=('_GO_FMT_ADD_CHECK', '_GO_VET_ADD_CHECK'),
        sources=('main.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    expected = (
        []
        if module != 'project/pkg'
        else [
            ['gofmt', '$S/project/pkg/main.go'],
            ['govet', '$(BUILD_ROOT)/project/pkg/pkg.a.vet.txt'],
        ]
    )
    assert unit.checks == expected


def test_go_non_go_sources_keep_their_processing():
    unit = FakeUnit(
        variables={
            '_GO_SRCS_VALUE': 'main.go api.proto event.ev schema.fbs config.in code.s object.syso',
            '_GO_PACKAGE_VALUE': 'pkg',
            'GOSTD_VERSION': '1.16',
            'GOSTD': 'contrib/go/_std',
            'GO_ARCADIA_PROJECT_PREFIX': 'a.yandex-team.ru/',
            'GO_CONTRIB_PROJECT_PREFIX': 'vendor/',
        },
        sources=('main.go',),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [['gofmt', '$S/project/pkg/main.go']]
    assert unit.get('_GO_SRCS_VALUE') == 'main.go code.s object.syso'
    assert unit.calls == [
        ('oncopy_file_with_context', ['event.ev', 'event.ev.proto']),
        ('on_go_proto_cmd', 'api.proto'),
        ('on_go_proto_cmd', 'event.ev.proto'),
        ('on_go_flatc_cmd', ['schema.fbs', 'pkg']),
        ('onsrc', 'config.in'),
        ('on_go_compile_symabis', ['code.s', 'FLAGS', '-p', 'a.yandex-team.ru/project/pkg']),
    ]


def test_go_cgo_processing_does_not_add_generated_files_to_gofmt():
    unit = FakeUnit(
        variables={
            '_GO_SRCS_VALUE': 'main.go CGO_EXPORT export.c helper.cc asm.S object.syso',
            '_CGO_SRCS_VALUE': 'cgo.go',
            'MODDIR': 'project/pkg',
            'CGO_CFLAGS_VALUE': '-O2',
            'CGO2_CFLAGS_VALUE': '-Wextra',
            'GOSTD': 'contrib/go/_std',
            'GO_ARCADIA_PROJECT_PREFIX': 'a.yandex-team.ru/',
            'GO_CONTRIB_PROJECT_PREFIX': 'vendor/',
            '_GO_PACKAGE_VALUE': 'pkg',
        },
        flags=('_GO_FMT_ADD_CHECK', 'CGO_ENABLED'),
        sources=('main.go', 'cgo.go'),
    )

    gobuild._GO_PROCESS_SRCS(unit)

    assert unit.checks == [['gofmt', '$S/project/pkg/main.go']]
    assert unit.get('_GO_SRCS_VALUE') == 'main.go object.syso'
    assert unit.calls == [
        ('oncopy_file_with_context', ['export.c', 'export.c', 'OUTPUT_INCLUDES', '${BINDIR}/_cgo_export.h']),
        ('onaddincl', ['FOR', 'c', 'project/pkg']),
        ('onsrc', ['${BINDIR}/export.c', '-O2']),
        ('onsrc', ['helper.cc', '-O2']),
        ('onsrc', ['asm.S', '-O2']),
        ('onpeerdir', 'contrib/go/_std/runtime/cgo'),
        (
            'on_go_compile_cgo1',
            ['a.yandex-team.ru/project/pkg', 'cgo.go', 'FLAGS', '-import_runtime_cgo=true', '-import_syscall=true'],
        ),
        ('onsrc', ['cgo.cgo2.c', '-O2', '-Wextra']),
        (
            'on_go_compile_cgo2',
            ['pkg', 'cgo.go', 'C_FILES', '${BINDIR}/export.c', 'S_FILES', 'asm.S', 'OBJ_FILES', 'object.syso'],
        ),
    ]


@pytest.mark.parametrize('srcs', ['CGO_EXPORT', 'CGO_EXPORT main.go'])
def test_go_reports_unmatched_cgo_export(srcs, errors):
    unit = FakeUnit(variables={'_GO_SRCS_VALUE': srcs})

    gobuild._GO_PROCESS_SRCS(unit)

    assert errors == ['Unmatched CGO_EXPORT keyword in SRCS() macro']
