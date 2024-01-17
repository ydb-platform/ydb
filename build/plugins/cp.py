import os

from _common import sort_by_keywords


def oncopy(unit, *args):
    keywords = {
        'RESULT': 1,
        'KEEP_DIR_STRUCT': 0,
        'DESTINATION': 1,
        'FROM': 1,
        'OUTPUT_INCLUDES': -1,
        'AUTO': 0,
        'WITH_CONTEXT': 0,
    }

    flat_args, spec_args = sort_by_keywords(keywords, args)

    dest_dir = spec_args['DESTINATION'][0] if 'DESTINATION' in spec_args else ''
    from_dir = spec_args['FROM'][0] if 'FROM' in spec_args else ''
    output_includes = spec_args['OUTPUT_INCLUDES'] if 'OUTPUT_INCLUDES' in spec_args else None
    keep_struct = 'KEEP_DIR_STRUCT' in spec_args
    save_in_var = 'RESULT' in spec_args
    auto = 'AUTO' in spec_args
    with_context = 'WITH_CONTEXT' in spec_args
    targets = []

    for source in flat_args:
        rel_path = ''
        path_list = source.split(os.sep)
        filename = path_list[-1]
        if keep_struct:
            if path_list[:-1]:
                rel_path = os.path.join(*path_list[:-1])
        source_path = os.path.join(from_dir, rel_path, filename)
        target_path = os.path.join(dest_dir, rel_path, filename)
        if save_in_var:
            targets.append(target_path)
        unit.oncopy_file(
            [source_path, target_path]
            + (['OUTPUT_INCLUDES'] + output_includes if output_includes else [])
            + (['OUTPUT_INCLUDES', source_path] if with_context else [])
            + (['AUTO'] if auto else [])
        )

    if save_in_var:
        unit.set([spec_args["RESULT"][0], " ".join(targets)])
