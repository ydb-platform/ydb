"""
This file exists so that imports of widgets and fields can be done in an
analogous manner to imports from the widgets and fields of django.forms, e.g.:

from django import forms
from select2 import forms as select2forms

class MyForm(forms.Form):
    regular_select = forms.Select()
    select2_select = select2forms.Select()
"""
from . import fields
from .fields import *

from . import widgets
from .widgets import *
