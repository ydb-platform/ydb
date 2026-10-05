import pytest

import _requirements as requirements
import lib.test_const as consts
import ytest

VALID_YAV_REQUIREMENTS = [
    'TOKEN=value:sec-01abc:token',
    'TOKEN_FILE=file:sec-02def:key.with-dots-and-dashes=part',
]

VALID_SB_VAULT_REQUIREMENTS = [
    'SB_TOKEN=value:YATOOL:token',
    'SB_TOKEN_FILE=file:OWNER-WITH-DASH:key.with-dots-and-dashes=part',
]


class FakeUnit:
    def path(self):
        return '$S/project/path'


def validate_test(requirement_values):
    return ytest.validate_test(
        FakeUnit(),
        {
            'SCRIPT-REL-PATH': 'py.test',
            'SOURCE-FOLDER-PATH': 'project/path',
            'TEST-NAME': 'test',
            'REQUIREMENTS': ytest.serialize_list(requirement_values),
        },
    )


class TestRequirements(object):
    @pytest.mark.parametrize('test_size', consts.TestSize.sizes())
    def test_cpu(self, test_size):
        max_cpu = consts.TestSize.get_max_requirements(test_size).get(consts.TestRequirements.Cpu)
        min_cpu = consts.TestRequirementsConstants.MinCpu
        assert requirements.check_cpu(-1, test_size)
        assert requirements.check_cpu(min_cpu - 1, test_size)
        assert requirements.check_cpu("unknown", test_size)
        assert not requirements.check_cpu(1, test_size)
        assert not requirements.check_cpu(3, test_size)
        assert requirements.check_cpu(1000, test_size)
        if max_cpu != consts.TestRequirementsConstants.All:
            assert requirements.check_cpu(max_cpu + 1, test_size)
            assert requirements.check_cpu(max_cpu + 4, test_size)
            assert requirements.check_cpu(consts.TestRequirementsConstants.All, test_size)
        else:
            assert not requirements.check_cpu(consts.TestRequirementsConstants.All, test_size)

    @pytest.mark.parametrize('test_size', consts.TestSize.sizes())
    def test_ram(self, test_size):
        max_ram = consts.TestSize.get_max_requirements(test_size).get(consts.TestRequirements.Ram)
        min_ram = consts.TestRequirementsConstants.MinRam
        assert requirements.check_ram(-1, test_size)
        assert requirements.check_ram(min_ram - 1, test_size)
        assert requirements.check_ram(max_ram + 1, test_size)
        assert not requirements.check_ram(1, test_size)
        assert not requirements.check_ram(4, test_size)
        assert not requirements.check_ram(5, test_size)
        assert not requirements.check_ram(32, consts.TestSize.Large)
        assert requirements.check_ram(48, consts.TestSize.Large)

        assert not requirements.check_ram(1, test_size, is_kvm=True)
        assert not requirements.check_ram(4, test_size, is_kvm=True)
        assert not requirements.check_ram(16, test_size, is_kvm=True)
        assert requirements.check_ram(32, test_size, is_kvm=True)

    @pytest.mark.parametrize('test_size', consts.TestSize.sizes())
    def test_ram_disk(self, test_size):
        max_ram_disk = consts.TestSize.get_max_requirements(test_size).get(consts.TestRequirements.RamDisk)
        min_ram_disk = consts.TestRequirementsConstants.MinRamDisk
        assert requirements.check_ram_disk(-1, test_size)
        assert requirements.check_ram_disk(min_ram_disk - 1, test_size)
        assert requirements.check_ram_disk(max_ram_disk + 1, test_size)
        assert requirements.check_ram_disk(33, test_size)
        assert not requirements.check_ram_disk(32, test_size)
        assert not requirements.check_ram_disk(1, test_size)
        assert not requirements.check_ram_disk(4, test_size)
        assert not requirements.validate_ram_disk_requirement(
            'ram_disk', '0', test_size, False, True, False, False, False, 1
        )
        assert not requirements.validate_ram_disk_requirement(
            'ram_disk', '1', test_size, False, True, False, False, False, 1
        )
        assert not requirements.validate_ram_disk_requirement(
            'ram_disk', '1', test_size, True, True, False, False, False, 0
        )
        assert not requirements.validate_ram_disk_requirement(
            'ram_disk', '1', test_size, False, False, False, False, False, 0
        )
        if test_size != consts.TestSize.Large:
            assert requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, False, False, False, 0
            )
            assert requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, True, False, False, 0
            )
            assert requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, False, True, False, 0
            )
            assert requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, False, False, True, 0
            )
        else:
            assert not requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, False, False, False, 0
            )
            assert not requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, True, False, False, 0
            )
            assert not requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, False, True, False, 0
            )
            assert not requirements.validate_ram_disk_requirement(
                'ram_disk', '1', test_size, False, True, False, False, True, 0
            )


@pytest.mark.parametrize(
    ('validator', 'values'),
    [
        (requirements.validate_yav_vault, VALID_YAV_REQUIREMENTS),
        (requirements.validate_sb_vault, VALID_SB_VAULT_REQUIREMENTS),
    ],
)
def test_secret_requirement_validator_accepts_single_and_serialized_values(validator, values):
    assert validator('unused', values[0]) is None
    assert validator('unused', ','.join(values)) is None


@pytest.mark.parametrize(
    ('validator', 'value'),
    [
        (requirements.validate_yav_vault, ''),
        (requirements.validate_yav_vault, ','.join((VALID_YAV_REQUIREMENTS[0], 'malformed'))),
        (requirements.validate_yav_vault, ','.join(('malformed', VALID_YAV_REQUIREMENTS[0]))),
        (requirements.validate_yav_vault, ',,'.join(VALID_YAV_REQUIREMENTS)),
        (requirements.validate_yav_vault, VALID_YAV_REQUIREMENTS[0] + ','),
        (requirements.validate_yav_vault, '=value:sec-01abc:key'),
        (requirements.validate_yav_vault, '1TOKEN=value:sec-01abc:key'),
        (requirements.validate_yav_vault, 'ТОКЕН=value:sec-01abc:key'),
        (requirements.validate_yav_vault, 'TOKEN='),
        (requirements.validate_yav_vault, 'TOKEN=value::key'),
        (requirements.validate_yav_vault, 'TOKEN=value:sec-01abc:'),
        (requirements.validate_yav_vault, 'TOKEN=value:sec-01abc:key:extra'),
        (requirements.validate_yav_vault, 'TOKEN=value:not-a-secret:key'),
        (requirements.validate_yav_vault, VALID_YAV_REQUIREMENTS[0] + ')'),
        (requirements.validate_yav_vault, VALID_YAV_REQUIREMENTS[0] + ' key'),
        (requirements.validate_yav_vault, VALID_YAV_REQUIREMENTS[0] + '$('),
        (requirements.validate_yav_vault, VALID_YAV_REQUIREMENTS[0] + ',part'),
        (requirements.validate_sb_vault, ''),
        (requirements.validate_sb_vault, ','.join((VALID_SB_VAULT_REQUIREMENTS[0], 'malformed'))),
        (requirements.validate_sb_vault, ','.join(('malformed', VALID_SB_VAULT_REQUIREMENTS[0]))),
        (requirements.validate_sb_vault, ',,'.join(VALID_SB_VAULT_REQUIREMENTS)),
        (requirements.validate_sb_vault, VALID_SB_VAULT_REQUIREMENTS[0] + ','),
        (requirements.validate_sb_vault, '=value:YATOOL:key'),
        (requirements.validate_sb_vault, '1TOKEN=value:YATOOL:key'),
        (requirements.validate_sb_vault, 'ТОКЕН=value:YATOOL:key'),
        (requirements.validate_sb_vault, 'TOKEN='),
        (requirements.validate_sb_vault, 'TOKEN=value::key'),
        (requirements.validate_sb_vault, 'TOKEN=value:YATOOL:'),
        (requirements.validate_sb_vault, 'TOKEN=value:YATOOL:key:extra'),
        (requirements.validate_sb_vault, 'TOKEN=unknown:YATOOL:key'),
        (requirements.validate_sb_vault, VALID_SB_VAULT_REQUIREMENTS[0] + ')'),
        (requirements.validate_sb_vault, VALID_SB_VAULT_REQUIREMENTS[0] + ' key'),
        (requirements.validate_sb_vault, VALID_SB_VAULT_REQUIREMENTS[0] + '$('),
        (requirements.validate_sb_vault, VALID_SB_VAULT_REQUIREMENTS[0] + ',part'),
    ],
)
def test_secret_requirement_validator_rejects_malformed_values(validator, value):
    assert validator('unused', value)


def test_validate_test_groups_secret_requirements_by_store_and_preserves_order_within_each_store():
    valid_kw, warnings, errors = validate_test(
        [
            'yav:' + VALID_YAV_REQUIREMENTS[0],
            'sb_vault:' + VALID_SB_VAULT_REQUIREMENTS[0],
            'yav:' + VALID_YAV_REQUIREMENTS[1],
            'sb_vault:' + VALID_SB_VAULT_REQUIREMENTS[1],
        ]
    )

    assert warnings == []
    assert errors == []
    assert ytest.deserialize_list(valid_kw['REQUIREMENTS']) == [
        'sb_vault:' + ','.join(VALID_SB_VAULT_REQUIREMENTS),
        'yav:' + ','.join(VALID_YAV_REQUIREMENTS),
    ]
    assert consts.YaTestTags.External in ytest.deserialize_list(valid_kw['TAG'])


@pytest.mark.parametrize(
    ('requirement_name', 'values'),
    [
        ('yav', ['', VALID_YAV_REQUIREMENTS[0]]),
        ('yav', [VALID_YAV_REQUIREMENTS[0], '']),
        ('yav', [VALID_YAV_REQUIREMENTS[0], '', VALID_YAV_REQUIREMENTS[1]]),
        ('sb_vault', ['', VALID_SB_VAULT_REQUIREMENTS[0]]),
        ('sb_vault', [VALID_SB_VAULT_REQUIREMENTS[0], '']),
        ('sb_vault', [VALID_SB_VAULT_REQUIREMENTS[0], '', VALID_SB_VAULT_REQUIREMENTS[1]]),
    ],
)
def test_validate_test_does_not_drop_empty_repeated_secret_requirement(requirement_name, values):
    valid_kw, warnings, errors = validate_test(['{}:{}'.format(requirement_name, value) for value in values])

    assert valid_kw is None
    assert warnings == []
    assert len(errors) == 1
    assert errors[0].startswith(requirement_name + ' value ')


@pytest.mark.parametrize(
    ('requirement_name', 'value'),
    [
        ('yav', VALID_YAV_REQUIREMENTS[0]),
        ('sb_vault', VALID_SB_VAULT_REQUIREMENTS[0]),
    ],
)
def test_validate_test_deduplicates_identical_secret_requirements(requirement_name, value):
    valid_kw, warnings, errors = validate_test(
        [
            '{}:{}'.format(requirement_name, value),
            '{}:{}'.format(requirement_name, value),
        ]
    )

    assert warnings == []
    assert errors == []
    assert ytest.deserialize_list(valid_kw['REQUIREMENTS']) == ['{}:{}'.format(requirement_name, value)]


@pytest.mark.parametrize(
    'requirement_values',
    [
        [
            'yav:TOKEN=value:sec-01abc:first',
            'yav:TOKEN=value:sec-02def:second',
        ],
        [
            'sb_vault:TOKEN=value:YATOOL:token',
            'yav:TOKEN=value:sec-01abc:token',
        ],
    ],
)
def test_validate_test_rejects_conflicting_secret_requirements_for_same_environment_variable(requirement_values):
    valid_kw, warnings, errors = validate_test(requirement_values)

    assert valid_kw is None
    assert warnings == []
    assert errors == ["Environment variable 'TOKEN' has conflicting secret requirements"]


@pytest.mark.parametrize('requirement_name', ['yav', 'sb_vault'])
def test_validate_test_rejects_malformed_repeated_secret_requirement(requirement_name):
    valid_value = {
        'yav': VALID_YAV_REQUIREMENTS[0],
        'sb_vault': VALID_SB_VAULT_REQUIREMENTS[0],
    }[requirement_name]

    valid_kw, warnings, errors = validate_test(
        [
            '{}:{}'.format(requirement_name, valid_value),
            '{}:malformed'.format(requirement_name),
        ]
    )

    assert valid_kw is None
    assert warnings == []
    assert len(errors) == 1
    assert errors[0].startswith(requirement_name + ' value ')
