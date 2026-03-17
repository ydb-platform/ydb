from flask_wtf import FlaskForm
from flask_wtf.form import _Auto


class ModelForm(FlaskForm):
    """A WTForms mongoengine model form"""

    def __init__(self, formdata=_Auto, **kwargs):
        self.instance = kwargs.pop("instance", None) or kwargs.get("obj")
        if self.instance and not formdata:
            kwargs["obj"] = self.instance
        self.formdata = formdata
        super(ModelForm, self).__init__(formdata, **kwargs)

    def save(self, commit=True, **kwargs):
        if self.instance:
            self.populate_obj(self.instance)
        else:
            self.instance = self.model_class(**self.data)

        if commit:
            self.instance.save(**kwargs)
        return self.instance
