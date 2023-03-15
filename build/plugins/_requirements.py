import lib.test_const as consts
import re
import lib._metric_resolvers as mr

CANON_SB_VAULT_REGEX = re.compile(r"\w+=(value|file):[-\w]+:\w+")
CANON_YAV_REGEX = re.compile(r"\w+=(value|file):sec-[a-z0-9]+:\w+")
VALID_DNS_REQUIREMENTS = ("default", "local", "dns64")
VALID_NETWORK_REQUIREMENTS = ("full", "restricted")


def check_cpu(suite_cpu_requirements, test_size, is_kvm=False):
    min_cpu_requirements = consts.TestRequirementsConstants.MinCpu
    max_cpu_requirements = consts.TestSize.get_max_requirements(test_size).get(consts.TestRequirements.Cpu)
    if isinstance(suite_cpu_requirements, str):
        if all(
            consts.TestRequirementsConstants.is_all_cpu(req) for req in (max_cpu_requirements, suite_cpu_requirements)
        ):
            return None
        return "Wrong 'cpu' requirements: {}, should be in [{}..{}] for {}-size tests".format(
            suite_cpu_requirements, min_cpu_requirements, max_cpu_requirements, test_size
        )

    if not isinstance(suite_cpu_requirements, int):
        return "Wrong 'cpu' requirements: {}, should be integer".format(suite_cpu_requirements)

    if (
        suite_cpu_requirements < min_cpu_requirements
        or suite_cpu_requirements > consts.TestRequirementsConstants.get_cpu_value(max_cpu_requirements)
    ):
        return "Wrong 'cpu' requirement: {}, should be in [{}..{}] for {}-size tests".format(
            suite_cpu_requirements, min_cpu_requirements, max_cpu_requirements, test_size
        )

    return None


# TODO: Remove is_kvm param when there will be guarantees on RAM
def check_ram(suite_ram_requirements, test_size, is_kvm=False):
    if not isinstance(suite_ram_requirements, int):
        return "Wrong 'ram' requirements: {}, should be integer".format(suite_ram_requirements)
    min_ram_requirements = consts.TestRequirementsConstants.MinRam
    max_ram_requirements = (
        consts.MAX_RAM_REQUIREMENTS_FOR_KVM
        if is_kvm
        else consts.TestSize.get_max_requirements(test_size).get(consts.TestRequirements.Ram)
    )
    if suite_ram_requirements < min_ram_requirements or suite_ram_requirements > max_ram_requirements:
        err_msg = "Wrong 'ram' requirements: {}, should be in [{}..{}] for {}-size tests".format(
            suite_ram_requirements, min_ram_requirements, max_ram_requirements, test_size
        )
        if is_kvm:
            err_msg += ' with kvm requirements'
        return err_msg
    return None


def check_ram_disk(suite_ram_disk, test_size, is_kvm=False):
    min_ram_disk = consts.TestRequirementsConstants.MinRamDisk
    max_ram_disk = consts.TestSize.get_max_requirements(test_size).get(consts.TestRequirements.RamDisk)
    if isinstance(suite_ram_disk, str):
        if all(consts.TestRequirementsConstants.is_all_ram_disk(req) for req in (max_ram_disk, suite_ram_disk)):
            return None
        return "Wrong 'ram_disk' requirements: {}, should be in [{}..{}] for {}-size tests".format(
            suite_ram_disk, 0, max_ram_disk, test_size
        )

    if not isinstance(suite_ram_disk, int):
        return "Wrong 'ram_disk' requirements: {}, should be integer".format(suite_ram_disk)

    if suite_ram_disk < min_ram_disk or suite_ram_disk > consts.TestRequirementsConstants.get_ram_disk_value(
        max_ram_disk
    ):
        return "Wrong 'ram_disk' requirement: {}, should be in [{}..{}] for {}-size tests".format(
            suite_ram_disk, min_ram_disk, max_ram_disk, test_size
        )

    return None


def validate_sb_vault(name, value):
    if not CANON_SB_VAULT_REGEX.match(value):
        return "sb_vault value '{}' should follow pattern <ENV_NAME>=<value|file>:<owner>:<vault key>".format(value)


def validate_yav_vault(name, value):
    if not CANON_YAV_REGEX.match(value):
        return "yav value '{}' should follow pattern <ENV_NAME>=<value|file>:<sec-id>:<key>".format(value)


def validate_numerical_requirement(name, value):
    if mr.resolve_value(value) is None:
        return "Cannot convert [[imp]]{}[[rst]] to the proper [[imp]]{}[[rst]] requirement value".format(value, name)


def validate_choice_requirement(name, val, valid):
    if val not in valid:
        return "Unknown [[imp]]{}[[rst]] requirement: [[imp]]{}[[rst]], choose from [[imp]]{}[[rst]]".format(
            name, val, ", ".join(valid)
        )


def validate_force_sandbox_requirement(
    name, value, test_size, is_force_sandbox, in_autocheck, is_fuzzing, is_kvm, is_ytexec_run, check_func
):
    if is_force_sandbox or not in_autocheck or is_fuzzing or is_ytexec_run:
        if value == 'all':
            return
        return validate_numerical_requirement(name, value)
    error_msg = validate_numerical_requirement(name, value)
    if error_msg:
        return error_msg
    return check_func(mr.resolve_value(value), test_size, is_kvm)


def validate_ram_disk_requirement(
    name, value, test_size, is_force_sandbox, in_autocheck, is_fuzzing, is_kvm, is_ytexec_run, ram
):
    error_msg = validate_force_sandbox_requirement(
        name, value, test_size, is_force_sandbox, in_autocheck, is_fuzzing, is_kvm, is_ytexec_run, check_ram_disk
    )
    if error_msg:
        return error_msg
    if is_force_sandbox or not in_autocheck or test_size == consts.TestSize.Large:
        return
    if int(value) > int(ram):
        return "Wrong 'ram_disk' value, 'ram_disk':{} should be no more than 'ram':{}".format(value, ram)
    return None


# TODO: Remove is_kvm param when there will be guarantees on RAM
def validate_requirement(
    req_name, value, test_size, is_force_sandbox, in_autocheck, is_fuzzing, is_kvm, is_ytexec_run, requirements
):
    req_checks = {
        'container': validate_numerical_requirement,
        'cpu': lambda n, v: validate_force_sandbox_requirement(
            n, v, test_size, is_force_sandbox, in_autocheck, is_fuzzing, is_kvm, is_ytexec_run, check_cpu
        ),
        'disk_usage': validate_numerical_requirement,
        'dns': lambda n, v: validate_choice_requirement(n, v, VALID_DNS_REQUIREMENTS),
        'kvm': None,
        'network': lambda n, v: validate_choice_requirement(n, v, VALID_NETWORK_REQUIREMENTS),
        'ram': lambda n, v: validate_force_sandbox_requirement(
            n, v, test_size, is_force_sandbox, in_autocheck, is_fuzzing, is_kvm, is_ytexec_run, check_ram
        ),
        'ram_disk': lambda n, v: validate_ram_disk_requirement(
            n,
            v,
            test_size,
            is_force_sandbox,
            in_autocheck,
            is_fuzzing,
            is_kvm,
            is_ytexec_run,
            requirements.get(
                'ram', consts.TestSize.get_default_requirements(test_size).get(consts.TestRequirements.Ram)
            ),
        ),
        'sb': None,
        'sb_vault': validate_sb_vault,
        'yav': validate_yav_vault,
    }

    if req_name not in req_checks:
        return "Unknown requirement: [[imp]]{}[[rst]], choose from [[imp]]{}[[rst]]".format(
            req_name, ", ".join(sorted(req_checks))
        )

    if req_name in ('container', 'disk') and not is_force_sandbox:
        return "Only [[imp]]LARGE[[rst]] tests without [[imp]]ya:force_distbuild[[rst]] tag can have [[imp]]{}[[rst]] requirement".format(
            req_name
        )

    check_func = req_checks[req_name]
    if check_func:
        return check_func(req_name, value)
