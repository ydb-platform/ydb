import os


def onbundle(unit, *args):
    """
    @usage BUNDLE(<Dir [SUFFIX Suffix] [NAME Name]>...)

    Brings build artefact from module Dir under optional Name to the current module (e.g. UNION)
    If NAME is not specified, the name of the Dir's build artefact will be preserved
    Optional SUFFIX allows to use secondary module output. The suffix is appended to the primary output name, so the applicability is limited.
    It makes little sense to specify BUNDLE on non-final targets and so this may stop working without prior notice.
    Bundle on multimodule will select final target among multimodule variants and will fail if there are none or more than one.
    """
    i = 0
    while i < len(args):
        target = args[i]
        i += 1

        if i + 1 < len(args) and args[i] == "SUFFIX":
            suffix = args[i + 1]
            i += 2
        else:
            suffix = ""

        if i + 1 < len(args) and args[i] == "NAME":
            name = args[i + 1]
            i += 2
        else:
            name = os.path.basename(target) + suffix

        unit.on_bundle_target([target, name, suffix])
