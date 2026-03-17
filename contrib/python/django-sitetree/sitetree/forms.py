from django import forms

from .fields import TreeItemChoiceField


class TreeItemForm(forms.Form):
    """Generic sitetree form.

    Accepts the following kwargs:

        - `tree`: tree model or alias
        - `tree_item`: ID of an initial tree item

    """
    choice_field_class = TreeItemChoiceField

    def __init__(self, *args, **kwargs):
        tree = kwargs.pop('tree', None)
        tree_item = kwargs.pop('tree_item', None)
        super().__init__(*args, **kwargs)

        # autocomplete off - deals with Firefox form caching
        # https://bugzilla.mozilla.org/show_bug.cgi?id=46845
        self.fields['tree_item'] = self.choice_field_class(
            tree, initial=tree_item, widget=forms.Select(attrs={'autocomplete': 'off'}))
