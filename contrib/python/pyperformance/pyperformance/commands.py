import logging
import os.path
import sys


def cmd_list(options, benchmarks):
    print("%r benchmarks:" % options.benchmarks)
    for bench in sorted(benchmarks):
        print("- %s" % bench.name)
    print()
    print("Total: %s benchmarks" % len(benchmarks))


def cmd_list_groups(manifest, *, showtags=True):
    all_benchmarks = set(manifest.benchmarks)

    groups = sorted(manifest.groups - {"all", "default"})
    groups[0:0] = ["all", "default"]
    for group in groups:
        specs = list(manifest.resolve_group(group))
        known = set(specs) & all_benchmarks
        if not known:
            # skip empty groups
            continue

        print("%s (%s):" % (group, len(specs)))
        for spec in sorted(specs):
            print("- %s" % spec.name)
        print()

    if showtags:
        print("=============================")
        print()
        print("tags:")
        print()
        tags = sorted(manifest.tags or ())
        if not tags:
            print("(no tags)")
        else:
            for tag in tags:
                specs = list(manifest.resolve_group(tag))
                known = set(specs) & all_benchmarks
                if not known:
                    # skip empty groups
                    continue

                print("%s (%s):" % (tag, len(specs)))
                for spec in sorted(specs):
                    print("- %s" % spec.name)
                print()


def cmd_venv_create(options, root, python, benchmarks):
    from . import _venv
    from .venv import Requirements, VenvForBenchmarks

    if _venv.venv_exists(root):
        sys.exit(f"ERROR: the virtual environment already exists at {root}")

    requirements = Requirements.from_benchmarks(benchmarks)
    venv = VenvForBenchmarks.ensure(
        root,
        python or sys.executable,
        inherit_environ=options.inherit_environ,
    )
    venv.ensure_pip()
    try:
        venv.install_pyperformance()
        venv.ensure_reqs(requirements)
    except _venv.RequirementsInstallationFailedError:
        sys.exit(1)
    print("The virtual environment %s has been created" % root)


def cmd_venv_recreate(options, root, python, benchmarks):
    from . import _utils, _venv
    from .venv import Requirements, VenvForBenchmarks

    requirements = Requirements.from_benchmarks(benchmarks)
    if _venv.venv_exists(root):
        venv_python = _venv.resolve_venv_python(root)
        if venv_python == sys.executable:
            print("The virtual environment %s already exists" % root)
            print("(it matches the currently running Python executable)")
            venv = VenvForBenchmarks(
                root,
                inherit_environ=options.inherit_environ,
            )
            venv.ensure_pip()
            try:
                venv.ensure_reqs(requirements)
            except _venv.RequirementsInstallationFailedError:
                sys.exit(1)
        else:
            print("The virtual environment %s already exists" % root)
            _utils.safe_rmtree(root)
            print("The old virtual environment %s has been removed" % root)
            print()
            venv = VenvForBenchmarks.ensure(
                root,
                python or sys.executable,
                inherit_environ=options.inherit_environ,
            )
            venv.ensure_pip()
            try:
                venv.install_pyperformance()
                venv.ensure_reqs(requirements)
            except _venv.RequirementsInstallationFailedError:
                sys.exit(1)
            print("The virtual environment %s has been recreated" % root)
    else:
        venv = VenvForBenchmarks.create(
            root,
            python or sys.executable,
            inherit_environ=options.inherit_environ,
        )
        venv.ensure_pip()
        try:
            venv.install_pyperformance()
            venv.ensure_reqs(requirements)
        except _venv.RequirementsInstallationFailedError:
            sys.exit(1)
        print("The virtual environment %s has been created" % root)


def cmd_venv_remove(options, root):
    from . import _utils

    if _utils.safe_rmtree(root):
        print("The virtual environment %s has been removed" % root)
    else:
        print("The virtual environment %s does not exist" % root)


def cmd_venv_show(options, root):
    from . import _venv

    exists = _venv.venv_exists(root)

    text = "Virtual environment path: %s" % root
    if exists:
        text += " (already created)"
    else:
        text += " (not created yet)"
    print(text)

    if not exists:
        print()
        print("Command to create it:")
        cmd = "%s -m pyperformance venv create" % options.python
        if options.venv:
            cmd += " --venv=%s" % options.venv
        print(cmd)


def cmd_run(options, benchmarks):
    import pyperf

    import pyperformance

    from .compare import display_benchmark_suite
    from .run import run_benchmarks

    logging.basicConfig(level=logging.INFO)

    print("Python benchmark suite %s" % pyperformance.__version__)
    print()

    if options.output and os.path.exists(options.output):
        print("ERROR: the output file %s already exists!" % options.output)
        sys.exit(1)

    if hasattr(options, "python"):
        executable = options.python
    else:
        executable = sys.executable
    if not os.path.isabs(executable):
        print('ERROR: "%s" is not an absolute path' % executable)
        sys.exit(1)

    suite, errors = run_benchmarks(benchmarks, executable, options)

    if not suite:
        print("ERROR: No benchmark was run")
        sys.exit(1)

    if options.output:
        suite.dump(options.output)
    if options.append:
        pyperf.add_runs(options.append, suite)
    display_benchmark_suite(suite)

    if errors:
        print("%s benchmarks failed:" % len(errors))
        for name, reason in errors:
            print("- %s (%s)" % (name, reason))
        print()
        sys.exit(1)


def cmd_compile(options):
    from .compile import BenchmarkRevision, parse_config

    conf = parse_config(options.config_file, "compile")
    if options is not None:
        if options.no_update:
            conf.update = False
        if options.no_tune:
            conf.system_tune = False
    bench = BenchmarkRevision(
        conf, options.revision, options.branch, patch=options.patch, options=options
    )
    bench.main()


def cmd_compile_all(options):
    from .compile import BenchmarkAll

    bench = BenchmarkAll(options.config_file, options=options)
    bench.main()


def cmd_upload(options):
    import pyperf

    from .compile import BenchmarkRevision, parse_config, parse_date

    conf = parse_config(options.config_file, "upload")

    filename = options.json_file
    bench = pyperf.BenchmarkSuite.load(filename)
    metadata = bench.get_metadata()
    revision = metadata["commit_id"]
    branch = metadata["commit_branch"]
    commit_date = parse_date(metadata["commit_date"])

    bench = BenchmarkRevision(
        conf,
        revision,
        branch,
        filename=filename,
        commit_date=commit_date,
        setup_log=False,
        options=options,
    )
    bench.upload()


def cmd_show(options):
    import pyperf

    from .compare import display_benchmark_suite

    suite = pyperf.BenchmarkSuite.load(options.filename)
    display_benchmark_suite(suite)


def cmd_compare(options):
    from .compare import VersionMismatchError, compare_results, write_csv

    try:
        results = compare_results(options)
    except VersionMismatchError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    if options.csv:
        write_csv(results, options.csv)
