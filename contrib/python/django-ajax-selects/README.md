# Edit `ForeignKey`, `ManyToManyField` and `CharField` in Django Admin using jQuery UI AutoComplete

[![Build Status](https://travis-ci.org/crucialfelix/django-ajax-selects.svg?branch=master)](https://travis-ci.org/crucialfelix/django-ajax-selects) [![PyPI version](https://badge.fury.io/py/django-ajax-selects.svg)](https://badge.fury.io/py/django-ajax-selects)

This Django app glues Django Admin, jQuery UI together to enable searching and managing ForeignKey  and ManyToMany relationships.

At the time it was created Django did not have any way to do this, and this solution glued together some technologies of the day.

If you are building a new project then you should not use this.

Django has built in support now:
https://docs.djangoproject.com/en/3.2/ref/contrib/admin/#django.contrib.admin.ModelAdmin.autocomplete_fields


---

![selecting](/docs/source/_static/kiss.png?raw=true)

![selected](/docs/source/_static/kiss-all.png?raw=true)

## Documentation

http://django-ajax-selects.readthedocs.org/en/latest/

## Installation

`pip install django-ajax-selects`

Add the app:

```py
# settings.py
INSTALLED_APPS = (
    ...
    'ajax_select',  # <-   add the app
    ...
)
```

Include the urls in your project:

```py
# urls.py
from django.urls import path
from django.conf.urls import include

from django.conf.urls.static import static
from django.contrib import admin
from django.conf import settings
from ajax_select import urls as ajax_select_urls

admin.autodiscover()

urlpatterns = [
    # This is the api endpoint that django-ajax-selects will call
    # to lookup your model ids by name
    path("admin/lookups/", include(ajax_select_urls)),
    path("admin/", admin.site.urls),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
```

## Quick Usage

Define a lookup channel:

```python
# yourapp/lookups.py
from ajax_select import register, LookupChannel
from .models import Tag

@register('tags')
class TagsLookup(LookupChannel):

    model = Tag

    def get_query(self, q, request):
        return self.model.objects.filter(name__icontains=q).order_by('name')[:50]

    def format_item_display(self, item):
        return u"<span class='tag'>%s</span>" % item.name
```

Add field to a form:

```python
# yourapp/forms.py
from ajax_select.fields import AutoCompleteSelectMultipleField

class DocumentForm(ModelForm):

    class Meta:
        model = Document

    tags = AutoCompleteSelectMultipleField('tags')
```

This will now work in the Django Admin.

To use a form outside, be sure to include `form.media` on the template where you place the form:

```html
{{ form.media }}
{{ form }}
```

Read the full documention here: [outside of the admin](http://django-ajax-selects.readthedocs.io/en/latest/Outside-of-Admin.html)

## Fully customizable

* Customize search query
* Query other resources besides Django ORM
* Format results with HTML
* Customize styling
* Customize security policy
* Add additional custom UI alongside widget
* Integrate with other UI elements elsewhere on the page using the javascript API
* Works in Admin as well as in normal views

## Assets included by default

https://jquery.com/ 3.7.1
https://jqueryui.com/ 1.13.2

## Customize jquery

To use a custom jQuery UI theme you can set:

```python
# settings.py
AJAX_SELECT_JQUERYUI_THEME = "/static/path-to-your-theme/jquery-ui-min.css"
```

https://jqueryui.com/themeroller/

If you need to use a different jQuery or jQuery UI then turn off the default assets:

```python
# settings.py
AJAX_SELECT_BOOTSTRAP = False
```

and include jquery and jquery-ui yourself, making sure they are loaded before the Django admin loads.


## Compatibility

* Django >=3.2
* Python >=3.10

## Contributors

Many thanks to all contributors and pull requesters !

<https://github.com/crucialfelix/django-ajax-selects/graphs/contributors/>

## License

Dual licensed under the MIT and GPL licenses:

* <http://www.opensource.org/licenses/mit-license.php/>
* <http://www.gnu.org/licenses/gpl.html/>
