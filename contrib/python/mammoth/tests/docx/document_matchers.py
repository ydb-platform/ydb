from precisely import all_of, has_attrs, instance_of

from mammoth import documents


def create_element_matcher(element_type):
    def matcher(**kwargs):
        return all_of(
            instance_of(element_type),
            has_attrs(**kwargs),
        )

    return matcher


is_paragraph = create_element_matcher(documents.Paragraph)
is_run = create_element_matcher(documents.Run)
is_hyperlink = create_element_matcher(documents.Hyperlink)
is_checkbox = create_element_matcher(documents.Checkbox)
is_table = create_element_matcher(documents.Table)
is_row = create_element_matcher(documents.TableRow)


is_empty_run = is_run(children=[])


def is_text(value):
    return all_of(
        instance_of(documents.Text),
        has_attrs(value=value),
    )
