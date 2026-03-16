class ChoicesMeta(type):

    def __new__(meta, name, bases, attrs):
        original_choices = {k: v for k, v in attrs.items() if not k.startswith('_')}
        choices = []
        value_mapper = {}
        error_msg = "All choices must be of type 'tuple' and have the collowing format: (<database value>, <value for user>)"
        for var, choice in original_choices.items():
            assert isinstance(choice, tuple) and len(choice) == 2, error_msg
            db_value, user_value = choice
            attrs[var] = db_value
            choices.append((db_value, user_value))
            value_mapper[db_value] = var
        attrs['CHOICES'] = tuple(choices)
        attrs['VALUE_MAPPER'] = value_mapper
        return super().__new__(meta, name, bases, attrs)


class Choices(metaclass=ChoicesMeta):
    CHOICES: tuple
    VALUE_MAPPER: dict