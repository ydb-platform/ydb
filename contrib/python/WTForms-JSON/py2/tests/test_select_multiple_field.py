from wtforms import (
    Form,
    SelectField,
    SelectFieldBase,
    SelectMultipleField,
    StringField,
    validators,
    widgets,
)


def test_select_field():
    fixtures = [
        {'name': 'Scarlet Witch', 'childs': 3, '__result': True},
        {'name': 'Black Cat', 'childs': 0, '__result': False},
        {'name': 'Tigra', 'childs': 1, '__result': True},
    ]

    class MomForm(Form):
        name = StringField()
        childs = SelectField(choices=((1, 1), (2, 2), (3, 3)), coerce=int)

    for fixture in fixtures:
        result = fixture.pop('__result')
        assert MomForm.from_json(fixture).validate() == result


def test_select_multiple_field():
    fixtures = [
        {'name': 'Juggernaut', 'gadgets': [1, 2, 3, 4], '__result': True},
        {'name': 'Wolverine', 'gadgets': [], '__result': False},
        {'name': 'Beast', 'gadgets': [4], '__result': True},
    ]

    class AppleFanBoyForm(Form):
        name = StringField()
        gadgets = SelectMultipleField(
            choices=(
                (1, 'Macbook Pro'),
                (2, 'Macbook Air'),
                (3, 'iPhone'),
                (4, 'iPad')
            ),
            validators=[validators.DataRequired()],
            coerce=int
        )

    for fixture in fixtures:
        result = fixture.pop('__result')
        assert AppleFanBoyForm.from_json(fixture).validate() == result


def test_custom_field():
    # a custom field that returns a list
    # it doesn't inherits from SelectMultipleField
    class SuperPowersField(SelectFieldBase):
        POWERS = [
            ('fly', ''),
            ('super strength', ''),
            ('regeneration', ''),
            ('stamina', ''),
            ('agility', ''),
        ]

        widget = widgets.Select(multiple=True)

        def iter_choices(self):
            if self.allow_blank:
                yield (u'__None', self.blank_text, self.data is None)
            for item in self.POWERS:
                selected = item[0] in self.data
                yield (item[0], item[1], selected)

        def process_formdata(self, valuelist):
            if valuelist:
                if valuelist[0] == '__None':
                    self.data = None
                else:
                    self.data = [
                        item[0] for item in self.POWERS
                        if str(item[0]) in valuelist]
                    if not len(self.data):
                        self.data = None

        def _is_selected(self, item):
            return item in self.data

    class SuperHeroForm(Form):
        name = StringField()
        powers = SuperPowersField(
            validators=[validators.DataRequired()]
        )

    fixtures = [
        {'name': 'Juggernaut', 'powers': ['super strength'], '__result': True},
        {
            'name': 'Wolverine',
            'powers': ['stamina', 'agility', 'regeneration'],
            '__result': True
        },
        {'name': 'Beast', 'powers': ['agility'], '__result': True},
        {'name': 'Rocket Rackoon', 'powers': [], '__result': False}
    ]

    for fixture in fixtures:
        result = fixture.pop('__result')
        assert SuperHeroForm.from_json(fixture).validate() == result
