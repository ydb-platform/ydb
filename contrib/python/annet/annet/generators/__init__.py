from __future__ import annotations

import dataclasses
import importlib
import importlib.machinery
import os
import re
import textwrap
from collections import OrderedDict as odict
from typing import FrozenSet, Iterable, List, Optional, Union

from contextlog import get_logger

from annet import patching, tracing
from annet.annlib.rbparser.acl import compile_acl_text
from annet.cli_args import GenSelectOptions, ShowGeneratorsOptions
from annet.lib import get_context
from annet.storage import Device
from annet.tracing import tracing_connector
from annet.types import (
    GeneratorEntireResult,
    GeneratorJSONFragmentResult,
    GeneratorPartialResult,
    GeneratorPartialRunArgs,
    GeneratorResult,
)
from annet.vendors import registry_connector, tabparser

from .annotate import AbstractAnnotateFormatter, annotate_formatter_connector
from .base import BaseGenerator
from .base import ParamsList as ParamsList
from .base import TextGenerator as TextGenerator
from .entire import Entire
from .exceptions import GeneratorError, InvalidValueFromGenerator, NotSupportedDevice
from .jsonfragment import JSONFragment
from .partial import PartialGenerator
from .perf import GeneratorPerfMesurer
from .ref import RefGenerator
from .result import RunGeneratorResult


# =====
DISABLED_TAG = "disable"


# =====
def get_list(args: ShowGeneratorsOptions):
    if args.generators_context is not None:
        os.environ["ANN_GENERATORS_CONTEXT"] = args.generators_context
    return {
        cls.__class__.__name__: {
            "type": cls.TYPE,
            "tags": set(cls.TAGS),
            "description": get_description(cls.__class__),
        }
        for cls in _get_generators(get_context()["generators"], None)
    }


def get_description(gen_cls) -> str:
    return textwrap.dedent(
        " ".join(
            [
                (gen_cls.__doc__ or ""),
                ("Disabled. Use '-g %s' to enable" % gen_cls.__name__ if DISABLED_TAG in gen_cls.TAGS else ""),
            ]
        )
    ).strip()


def validate_genselect(gens: GenSelectOptions, all_classes):
    logger = get_logger()
    unknown_err = "Unknown generator alias %s"
    all_aliases = {alias for cls in all_classes for alias in cls.get_aliases()}
    for gen_set in (gens.allowed_gens, gens.force_enabled):
        for alias in set(gen_set or ()) - all_aliases:
            logger.error(unknown_err, alias)
            raise Exception(unknown_err % alias)


@dataclasses.dataclass
class Generators:
    """Collection of various types of generators."""

    partial: List[PartialGenerator] = dataclasses.field(default_factory=list)
    entire: List[Entire] = dataclasses.field(default_factory=list)
    ref: List[RefGenerator] = dataclasses.field(default_factory=list)
    json_fragment: List[JSONFragment] = dataclasses.field(default_factory=list)


def build_generators(storage, gens: GenSelectOptions, device: Optional[Device] = None) -> Generators:
    """Return generators that meet the gens filter conditions."""
    if gens.generators_context is not None:
        os.environ["ANN_GENERATORS_CONTEXT"] = gens.generators_context
    all_generators = _get_generators(get_context()["generators"], storage, device)
    ref_generators = _get_ref_generators(get_context()["generators"], storage, device)
    validate_genselect(gens, all_generators)
    classes = list(select_generators(gens, all_generators))
    partial = [obj for obj in classes if obj.TYPE == "PARTIAL"]
    entire = [obj for obj in classes if obj.TYPE == "ENTIRE"]
    entire = list(sorted(entire, key=lambda x: x.prio, reverse=True))
    json_fragment = [obj for obj in classes if obj.TYPE == "JSON_FRAGMENT"]
    return Generators(
        partial=partial,
        entire=entire,
        json_fragment=json_fragment,
        ref=ref_generators,
    )


@tracing.function
def run_partial_initial(device):
    from .common.initial import InitialConfig

    tracing_connector.get().set_device_attributes(tracing_connector.get().get_current_span(), device)

    run_args = GeneratorPartialRunArgs(device)
    return run_partial_generators([InitialConfig(storage=device.storage, do_run=True)], [], run_args)


@tracing.function
def run_partial_generators(
    gens: List["PartialGenerator"],
    ref_gens: List["RefGenerator"],
    run_args: GeneratorPartialRunArgs,
):
    logger = get_logger(host=run_args.device.hostname)
    tracing_connector.get().set_device_attributes(tracing_connector.get().get_current_span(), run_args.device)

    ret = RunGeneratorResult()
    if run_args.generators_context is not None:
        os.environ["ANN_GENERATORS_CONTEXT"] = run_args.generators_context

    for gen in ref_gens:
        ret.ref_matcher.add(gen.ref(run_args.device), gen)

    logger.debug("Generating selected PARTIALs ...")

    for gen in gens:
        try:
            result = _run_partial_generator(gen, run_args)
        except NotSupportedDevice as exc:
            logger.info("generator %s raised unsupported error: %r", gen, exc)
            continue

        if not result:
            continue

        ref_match = ret.ref_matcher.match(result.config)
        for ref_gen, groups in ref_match:
            gens.append(ref_gen.with_groups(groups))
            ret.ref_track.add(gen.__class__, ref_gen.__class__)

        ret.ref_track.config(gen.__class__, result.config)
        ret.add_partial(result)

    return ret


@tracing.function(name="run_partial_generator")
def _run_partial_generator(gen: "PartialGenerator", run_args: GeneratorPartialRunArgs) -> GeneratorPartialResult | None:
    logger = get_logger(generator=_make_generator_ctx(gen))
    device = run_args.device
    output = ""
    config = odict()
    safe_config = odict()

    if not gen.supports_device(device):
        logger.debug("generator %s is not supported for device %s", gen, device.hostname)
        return None

    if span := tracing_connector.get().get_current_span():
        tracing_connector.get().set_device_attributes(span, run_args.device)
        tracing_connector.get().set_dimensions_attributes(span, gen, run_args.device)
        span.set_attributes(
            {
                "use_acl": run_args.use_acl,
                "use_acl_safe": run_args.use_acl_safe,
                "generators_context": str(run_args.generators_context),
            }
        )

    with GeneratorPerfMesurer(gen, run_args=run_args) as pm:
        if not run_args.no_new:
            if gen.get_user_runner(device):
                logger.info("Generating PARTIAL ...")
            try:
                output = gen(device, run_args.annotate)
            except NotSupportedDevice:  # это исключение нужно передать выше в оригинальном виде
                raise
            except Exception as err:
                filename, lineno = gen.get_running_line()
                logger.error("Generator error in file '%s:%i'", filename, lineno)
                raise GeneratorError(f"{gen} on {device}") from err

            fmtr = registry_connector.get().match(device.hw).make_formatter()

            try:
                config = tabparser.parse_to_tree(text=output, splitter=fmtr.split)
            except tabparser.ParserError as err:
                logger.exception("Parser error")
                raise GeneratorError from err

    acl = gen.acl(device) or ""
    rules = compile_acl_text(textwrap.dedent(acl), device.hw.vendor)
    acl_safe = gen.acl_safe(device) or ""
    safe_rules = compile_acl_text(textwrap.dedent(acl_safe), device.hw.vendor)

    if run_args.use_acl:
        try:
            with tracing_connector.get().start_as_current_span(
                "apply_acl", tracer_name=__name__, min_duration="0.01"
            ) as acl_span:
                tracing_connector.get().set_device_attributes(acl_span, run_args.device)
                config = patching.apply_acl(
                    config=config,
                    rules=rules,
                    fatal_acl=True,
                    with_annotations=run_args.annotate,
                )
            if run_args.use_acl_safe:
                with tracing_connector.get().start_as_current_span(
                    "apply_acl_safe", tracer_name=__name__, min_duration="0.01"
                ) as acl_safe_span:
                    tracing_connector.get().set_device_attributes(acl_safe_span, run_args.device)
                    safe_config = patching.apply_acl(
                        config=config,
                        rules=safe_rules,
                        fatal_acl=False,
                        with_annotations=run_args.annotate,
                    )
        except patching.AclError as err:
            logger.error("ACL error: generator is not allowed to yield this command: %s", err)
            raise GeneratorError from err
        except NotImplementedError as err:
            logger.error(str(err))
            raise GeneratorError from err

    return GeneratorPartialResult(
        name=gen.__class__.__name__,
        tags=gen.TAGS,
        output=output,
        acl=acl,
        acl_rules=rules,
        acl_safe=acl_safe,
        acl_safe_rules=safe_rules,
        config=config,
        safe_config=safe_config,
        perf=pm.last_result,
    )


@tracing.function
def check_entire_generators_required_packages(gens, device_packages: FrozenSet[str]) -> List[str]:
    errors: List[str] = []
    for gen in gens:
        if not gen.REQUIRED_PACKAGES.issubset(device_packages):
            missing = gen.REQUIRED_PACKAGES - device_packages
            missing_str = ", ".join("`{}'".format(pkg) for pkg in sorted(missing))
            if len(missing) == 1:
                errors.append("missing package {} required for {}".format(missing_str, gen))
            else:
                errors.append("missing packages {} required for {}".format(missing_str, gen))
    return errors


@tracing.function
def run_file_generators(
    gens: Iterable[Union["JSONFragment", "Entire"]],
    device: "Device",
) -> RunGeneratorResult:
    """Run generators that generate files or file parts."""
    ret = RunGeneratorResult()
    logger = get_logger(host=device.hostname)
    logger.debug("Generating selected ENTIREs and JSON_FRAGMENTs ...")
    for gen in gens:
        if gen.__class__.TYPE == "ENTIRE":
            run_generator_fn = _run_entire_generator
            add_result_fn = ret.add_entire
        elif gen.__class__.TYPE == "JSON_FRAGMENT":
            run_generator_fn = _run_json_fragment_generator
            add_result_fn = ret.add_json_fragment
        else:
            raise RuntimeError(f"Unknown generator class type: cls={gen.__class__} TYPE={gen.__class__.TYPE}")
        try:
            result = run_generator_fn(gen, device)
        except NotSupportedDevice as exc:
            logger.info("generator %s raised unsupported error: %r", gen, exc)
            continue
        if result:
            add_result_fn(result)

    return ret


@tracing.function(min_duration="0.5")
def _run_entire_generator(gen: "Entire", device: "Device") -> Optional[GeneratorResult]:
    logger = get_logger(generator=_make_generator_ctx(gen))
    span = tracing_connector.get().get_current_span()
    if span:
        tracing_connector.get().set_device_attributes(span, device)
        tracing_connector.get().set_dimensions_attributes(span, gen, device)

    with GeneratorPerfMesurer(gen, trace_min_duration="0.5") as pm:
        if not gen.supports_device(device):
            logger.debug("generator %s is not supported for device %s", gen, device.hostname)
            return

        path = gen.path(device)
        if not path:
            raise RuntimeError("entire generator should return non-empty path")

        logger.info("Generating ENTIRE ...")

        gen_output = gen(device)
        gen_reload = gen.get_reload_cmds(device)
        gen_is_safe = gen.is_safe(device)
        gen_prio = gen.prio

    return GeneratorEntireResult(
        name=gen.__class__.__name__,
        tags=gen.TAGS,
        path=path,
        output=gen_output,
        reload=gen_reload,
        prio=gen_prio,
        perf=pm.last_result,
        is_safe=gen_is_safe,
    )


def _make_generator_ctx(gen):
    return "%s.[%s]" % (gen.__module__, gen.__class__.__name__)


def _run_json_fragment_generator(
    gen: "JSONFragment",
    device: "Device",
) -> Optional[GeneratorResult]:
    logger = get_logger(generator=_make_generator_ctx(gen))

    with GeneratorPerfMesurer(gen) as pm:
        if not gen.supports_device(device):
            logger.info("generator %s is not supported for device %s", gen, device.hostname)
            return

        path = gen.path(device)
        if not path:
            raise RuntimeError("json fragment generator should return non-empty path")

        acl_item_or_list_of_items = gen.acl(device)
        safe_acl_item_or_list_of_items = gen.acl_safe(device)
        if not acl_item_or_list_of_items:
            raise RuntimeError("json fragment generator should return non-empty acl")
        if isinstance(acl_item_or_list_of_items, list):
            acl = acl_item_or_list_of_items
        else:
            acl = [acl_item_or_list_of_items]
        if isinstance(safe_acl_item_or_list_of_items, list):
            acl_safe = safe_acl_item_or_list_of_items
        else:
            acl_safe = [safe_acl_item_or_list_of_items]

        logger.info("Generating JSON_FRAGMENT ...")

        config = gen(device)
        reload_cmds = gen.get_reload_cmds(device)

    return GeneratorJSONFragmentResult(
        name=gen.__class__.__name__,
        tags=gen.TAGS,
        path=path,
        acl=acl,
        acl_safe=acl_safe,
        config=config,
        reload=reload_cmds,
        perf=pm.last_result,
        reload_prio=gen.reload_prio,
    )


def _get_generators(module_paths: Union[List[str], dict], storage, device=None):
    if isinstance(module_paths, dict):
        if device is None:
            module_paths = module_paths.get("default", [])
        else:
            modules = []
            seen = set()
            matched_property = None
            for prop, prop_modules in module_paths.get("per_device_property", {}).items():
                if getattr(device, prop, False) is True:
                    if matched_property is not None:
                        raise RuntimeError(
                            f"Device {device.hostname} is matched by more than one "
                            f"per_device_property: {matched_property} and {prop}"
                        )
                    matched_property = prop
                    for module in prop_modules:
                        if module not in seen:
                            modules.append(module)
                            seen.add(module)
            module_paths = modules or module_paths.get("default", [])
    res_generators = []
    for module_path in module_paths:
        module = _load_gen_module(module_path)
        if hasattr(module, "get_generators"):
            generators: List[BaseGenerator] = module.get_generators(storage)
            res_generators += generators
    return res_generators


def _load_gen_module(module_path: str):
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as e:
        try:  # maybe it's a path to module
            module_abs_path = os.path.abspath(module_path)
            module = importlib.machinery.SourceFileLoader(
                re.sub(r"[./]", "_", module_abs_path).strip("_"), module_abs_path
            ).load_module()
        except ModuleNotFoundError:
            raise e
    return module


def _get_ref_generators(module_paths: List[str], storage, device):
    if isinstance(module_paths, dict):
        module_paths = module_paths.get("default")
    res_generators = []
    for module_path in module_paths:
        module = _load_gen_module(module_path)
        if hasattr(module, "get_ref_generators"):
            res_generators += module.get_ref_generators(storage)
    return res_generators


def select_generators(gens: GenSelectOptions, classes: Iterable[BaseGenerator]):
    def contains(obj, where):
        if where:
            return obj.get_aliases().intersection(where)
        return False

    def has(cls, what):
        return what in cls.TAGS

    flts = [lambda c: not isinstance(c, RefGenerator)]
    if gens.allowed_gens:
        flts.append(lambda c: contains(c, gens.allowed_gens))
    elif gens.force_enabled:
        flts.append(lambda c: not has(c, DISABLED_TAG) or contains(c, gens.force_enabled))
    elif not gens.ignore_disabled:
        flts.append(lambda c: not has(c, DISABLED_TAG))

    if gens.excluded_gens:
        flts.append(lambda c: not contains(c, gens.excluded_gens))

    return filter(lambda x: all(f(x) for f in flts), classes)
