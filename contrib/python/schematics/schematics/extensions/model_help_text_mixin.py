# -*- coding: utf-8 -*-

from textwrap import fill, dedent

from ..iteration import atoms


def help_text_metadata(label=None, description=None, example=None):
    """
    Standard interface to help specify the required metadata fields for helptext to
    work correctly for a model.

    :param str label: Alternative name for the model.
    :param str description: Long description of the model.
    :param example: A concrete example usage of the model.
    :return dict: Dictionary of the help text metadata
    """
    return {
        'label': label,
        'description': description,
        'example': example
    }


api_docstring_format = '''"""
{docstring}

Example:

{example}


{parameter_description}
"""'''


class ModelHelpTextMixin(object):
    """
    A mixin for Models that add helptext functionality.

    Example usage is:

        class MyModel(schematics.models.Model, ModelHelpTextMixin):
            my_field = StringType(
                metadata=help_text_metadata('Name', 'A Persons name', 'Joe Stummer')
            )
    """

    @classmethod
    def _all_metadata(cls):
        """
        Collate all metadata from fields defined on this Model for simpler usage elsewhere
        in this class.
        """
        metadata = {}
        for type_instance in atoms(cls._schema, None):
            name = type_instance.name
            label = type_instance.field.metadata.get('label', type_instance.name)
            value = type_instance.value or type_instance.field.metadata.get('example', None)
            description = type_instance.field.metadata.get('description', None)
            metadata[name] = dict(
                name=name,
                label=label,
                value=value,
                description=description,
                field=type_instance.field
            )
        return metadata

    @classmethod
    def get_helptext(cls):
        """
        Generate user friendly description of this Model.
        """
        docstring = dedent(cls.__doc__).lstrip().rstrip()
        lines = [docstring]
        for metadata in cls._all_metadata().values():
            if metadata['label']:
                lines.append('    {name} ({label})'.format(**metadata))
            else:
                lines.append('    {name}'.format(**metadata))

            if metadata['value'] is not None:
                lines.append('        Example: {value}'.format(**metadata))

            if metadata['description']:
                lines.append('        {description}'.format(**metadata))

            if not(metadata['value'] is not None or metadata['description']):
                lines.append('        No helptext provided.')

        return '\n'.join(lines)

    @classmethod
    def get_example_usage(cls):
        """
        Generate example python code to use this Model.
        """
        lines = ['%s({' % cls.__name__]
        for metadata in cls._all_metadata().values():
            lines.append("    '{name}': {value},".format(**metadata))

        lines.append('})')
        return '\n'.join(lines)

    @classmethod
    def get_parameter_descriptions(cls):
        parameter_lines = []
        for metadata in cls._all_metadata().values():
            line = ":param{native_type} {name}:".format(
                name=metadata['name'],
                native_type=' {}'.format(metadata['field'].native_type.__name__) if metadata['field'] else ''
            )
            if metadata['description']:
                line += ' {}'.format(metadata['description'])
            parameter_lines.append(fill(line, subsequent_indent='    '))

        return '\n'.join(parameter_lines)

    @classmethod
    def get_api_docstring(cls):
        """
        Generate a sphinx apidoc compatible docstring for use in generating documentation for this model.
        """
        parameter_description = cls.get_parameter_descriptions()

        docstring = dedent(cls.__doc__).lstrip().rstrip()

        # Indent the example usage lines
        example_usage_indented = ''.join(['    ' + line for line in cls.get_example_usage().splitlines(True)])

        return api_docstring_format.format(
            docstring=docstring,
            example=example_usage_indented,
            parameter_description=parameter_description
        )
