[![Build Status](https://travis-ci.org/MattBroach/DjangoRestMultipleModels.svg?branch=master)](https://travis-ci.org/MattBroach/DjangoRestMultipleModels)
[![Coverage Status](https://coveralls.io/repos/github/Axiologue/DjangoRestMultipleModels/badge.svg?branch=master)](https://coveralls.io/github/Axiologue/DjangoRestMultipleModels?branch=master)
[![PyPI version](https://badge.fury.io/py/django-rest-multiple-models.svg)](https://badge.fury.io/py/django-rest-multiple-models)

# Multiple Model View

[Django Rest Framework](https://github.com/tomchristie/django-rest-framework) provides some incredible tools for serializing data, but sometimes you need to combine many serializers and/or models into a single API call.  **drf-multiple-model** is an app designed to do just that.

* Full Documentation: [https://django-rest-multiple-models.readthedocs.org/en/latest/](https://django-rest-multiple-models.readthedocs.io/en/latest/)
* Source Code: [https://github.com/Axiologue/DjangoRestMultipleModels](https://github.com/Axiologue/DjangoRestMultipleModels)
* PyPI: [https://pypi.python.org/pypi/django-rest-multiple-models](https://pypi.python.org/pypi/django-rest-multiple-models)
* License: MIT

# Installation

Install the package from pip:

```
pip install django-rest-multiple-models
```

Make sure to add 'drf_multiple_model' to your INSTALLED_APPS:

```python
INSTALLED_APPS = (
    ....
    'drf_multiple_model',
)
```

Then simply import the view into any views.py in which you'd want to use it:

```python
from drf_multiple_model.views import ObjectMultipleModelAPIView
```

**Note:** This package is built on top of Django Rest Framework's generic views and serializers, so it presupposes that Django Rest Framework is installed and added to your project as well.

# Features

* Send multiple serialized models as separate arrays, one merged list, or a single JSON object
* Sort different serialized models using shared fields
* pagination
* Filtering -- either per queryset or on all querysets
* custom model labeling

For full configuration options, filtering tools, and more, see [the documentation](https://django-rest-multiple-models.readthedocs.org/en/latest/).

# Basic Usage

**drf-multiple-model** comes with two generic class-based-view for serializing multiple models: the `ObjectMultipleModelAPIView` and the `FlatMultipleModelAPIView`.  Both views require a `querylist` attribute, which is a list or tuple of dicts containing (at minimum) a `queryset` key and a `serializer_class` key; the main difference between the views is the format of the response data.  For example, let's say you have the following models and serializers:

```python
# Models
class Play(models.Model):
    genre = models.CharField(max_length=100)
    title = models.CharField(max_length=200)
    pages = models.IntegerField()

class Poem(models.Model):
    title = models.CharField(max_length=200)
    style = models.CharField(max_length=100)
    lines = models.IntegerField()
    stanzas = models.IntegerField()

# Serializers
class PlaySerializer(serializers.ModelSerializer):
    class Meta:
        model = Play
        fields = ('genre','title','pages')

class PoemSerializer(serializers.ModelSerializer):
    class Meta:
        model = Poem
        fields = ('title','stanzas')
```

Then you might use the `ObjectMultipleModelAPIView` as follows:


```python
from drf_multiple_model.views import ObjectMultipleModelAPIView

class TextAPIView(ObjectMultipleModelAPIView):
    querylist = [
        {'queryset': Play.objects.all(), 'serializer_class': PlaySerializer},
        {'queryset': Poem.objects.filter(style='Sonnet'), 'serializer_class': PoemSerializer},
        ....
    ]
```

which would return:

```python
{
    "Play" : [
        {"genre": "Comedy", "title": "A Midsummer Night"s Dream", "pages": 350},
        {"genre": "Tragedy", "title": "Romeo and Juliet", "pages": 300},
        ....
    ],
    "Poem" : [
        {"title": "Shall I compare thee to a summer"s day", "stanzas": 1},
        {"title": "As a decrepit father takes delight", "stanzas": 1},
        ....
    ],
}
```

Or you coulde use the `FlatMultipleModelAPIView` as follows:

```python
from drf_multiple_model.views import FlatMultipleModelAPIView

class TextAPIView(FlatMultipleModelAPIView):
    querylist = [
        {'queryset': Play.objects.all(), 'serializer_class': PlaySerializer},
        {'queryset': Poem.objects.filter(style='Sonnet'), 'serializer_class': PoemSerializer},
        ....
    ]
```

which would return::

```python
[
    {"genre": "Comedy", "title": "A Midsummer Night"s Dream", "pages": 350, "type": "Play"},
    {"genre": "Tragedy", "title": "Romeo and Juliet", "pages": 300, "type": "Play"},
    ....
    {"title": "Shall I compare thee to a summer"s day", "stanzas": 1, "type": "Poem"},
    {"title": "As a decrepit father takes delight", "stanzas": 1, "type": "Poem"},
    ....
]
```
