import os

from ymake import macro, Unit


@macro
def COPY(
    unit: Unit,
    *args: tuple[str, ...],
    RESULT: str = '',
    FROM: str = '',
    DESTINATION: str = '',
    OUTPUT_INCLUDES: tuple[str, ...] = (),
    KEEP_DIR_STRUCT: bool = False,
    AUTO: bool = False,
    WITH_CONTEXT: bool = False,
):
    save_in_var = RESULT != ''
    targets = []

    for source in args:
        rel_path = ''
        path_list = source.split(os.sep)
        filename = path_list[-1]
        if KEEP_DIR_STRUCT:
            if path_list[:-1]:
                rel_path = os.path.join(*path_list[:-1])
        source_path = os.path.join(FROM, rel_path, filename)
        target_path = os.path.join(DESTINATION, rel_path, filename)
        if save_in_var:
            targets.append(target_path)
        unit.oncopy_file(
            ([] if WITH_CONTEXT else ['TEXT'])
            + [source_path, target_path]
            + (['OUTPUT_INCLUDES'] + list(OUTPUT_INCLUDES))
            + (['OUTPUT_INCLUDES', source_path] if WITH_CONTEXT else [])
            + (['AUTO'] if AUTO else [])
        )

    if save_in_var:
        unit.set([RESULT, " ".join(targets)])
