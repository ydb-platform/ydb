import re

from gpu_codegen import environment_resource_name
import ymake

from ymake import macro, Unit


@macro
def _CUTE_DSL_KERNEL_IMPL(unit: Unit, *args: str):
    keywords = {"SHARDS", "PY_DIRS"}
    if len(args) < 3 or any(arg in keywords for arg in args[:3]):
        ymake.report_configure_error("CUTE_DSL_KERNEL requires Driver ClassName Environment")
        return

    options = {}
    current = None
    for arg in args[3:]:
        if arg in keywords:
            if arg in options:
                ymake.report_configure_error(f"Duplicate CUTE_DSL_KERNEL option: {arg}")
                return
            current = arg
            options[current] = []
        elif current is None:
            ymake.report_configure_error(f"Unexpected CUTE_DSL_KERNEL argument: {arg}")
            return
        else:
            options[current].append(arg)

    shards = options.pop("SHARDS", None)
    if shards is not None and (len(shards) != 1 or not re.fullmatch(r"[0-9]+", shards[0]) or int(shards[0]) < 1):
        ymake.report_configure_error("CUTE_DSL_KERNEL SHARDS requires exactly one positive integer")
        return

    unit.onpeerdir([args[2]])
    resource_name = environment_resource_name(unit, args[2])
    unit.set(["_GPU_CODEGEN_ENV_ROOT", "${" + resource_name + "_RESOURCE_GLOBAL}"])

    setup = list(args[:3])
    for name, values in options.items():
        setup.extend([name, *values])
    unit.on_cute_dsl_setup(setup)

    count = 32 if shards is None else int(shards[0])
    if count == 1 or unit.get("CUTE_DSL_NO_SHARDING") == "yes":
        unit.on_gpu_codegen_generate([])
    else:
        archives = []
        for index in range(count):
            archive = f"generated_cpp/_codegen/{args[1]}_shard_{index}_of_{count}.tar"
            unit.on_gpu_codegen_compile_shard([str(index), str(count), archive])
            archives.append(archive)
        unit.on_gpu_codegen_finalize([str(count), *archives])
    unit.on_gpu_codegen_publish([])
