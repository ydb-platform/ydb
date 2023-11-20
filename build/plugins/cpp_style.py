def on_style(unit, *args):
    def it():
        yield 'DONT_PARSE'

        for f in args:
            f = f[len('${ARCADIA_ROOT}') + 1 :]

            if '/generated/' in f:
                continue

            yield f
            yield '/cpp_style/files/' + f

    unit.onresource(list(it()))
