# wtf-peewee

this project, based on the code found in ``wtforms.ext``, provides a bridge
between peewee models and wtforms, mapping model fields to form fields.

## example usage:

first, create a couple basic models and then use the model_form class factory
to create a form for the Entry model:
```python
from peewee import *
from wtfpeewee.orm import model_form
import wtforms

class PasswordField(TextField):
    """ Custom-defined field example. """
    def wtf_field(self, model, **kwargs):
        return wtforms.PasswordField(**kwargs)

class Blog(Model):
    name = CharField()

    def __unicode__(self):
        return self.name

class Entry(Model):
    blog = ForeignKeyField(Blog)
    title = CharField()
    body = TextField()
    protected = PasswordField()

    def __unicode__(self):
        return self.title

# create a form class for use with the Entry model
EntryForm = model_form(Entry)
```

Example implementation for an "edit" view using Flask:

```python
@app.route('/entries/<int:entry_id>/', methods=['GET', 'POST'])
def edit_entry(entry_id):
    try:
        entry = Entry.get(id=entry_id)
    except Entry.DoesNotExist:
        abort(404)

    if request.method == 'POST':
        form = EntryForm(request.form, obj=entry)
        if form.validate():
            form.populate_obj(entry)
            entry.save()
            flash('Your entry has been saved')
    else:
        form = EntryForm(obj=entry)

    return render_template('blog/entry_edit.html', form=form, entry=entry)
```
Example template for above view:
```jinja
{% extends "layout.html" %}
{% block body %}
  <h2>Edit {{ entry.title }}</h2>

  <form method="post" action="">
    {% for field in form %}
      <p>{{ field.label }} {{ field }}</p>
    {% endfor %}
    <p><button type="submit">Submit</button></p>
  </form>
{% endblock %}
```
