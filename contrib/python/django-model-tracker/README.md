# django-model-tracker

Track model object changes over time so that you know who done what.

[![PyPI version](https://badge.fury.io/py/django-model-tracker.svg)](https://badge.fury.io/py/django-model-tracker)
[![Downloads](https://pepy.tech/badge/django-model-tracker)](https://pepy.tech/project/django-model-tracker)
 
## Installation

* Install the package
    * For Django<4.0
       ```sh
        pip install 'django-model-tracker jsonfield'
      ```
    * For Django>=4.0
      ```sh
       pip install 'django-model-tracker'
      ```

* Add Application to your project's INSTALLED_APPs
```python
INSTALLED_APPS = (
     '....',
    'ModelTracker',
    )
```    
* Add the following line to your urls.py
```python
import ModelTracker
urlpatterns = patterns('',
...
url(r'^track/', include('ModelTracker.urls')),
...
)
```
* Run Migrations
```sh
   python manage.py migrate ModelTracker 
```

* Add the following line to your models.py file
```python
from ModelTracker import Tracker
```
*  Convert each Model you want to track to inhert from `Tracker.ModelTracker` instead of `models.Model`
   
**Old Code**

```python
   class Employee(models.Model):
     name=models.CharField(max_length=255)
     address=models.CharField(max_length=255)
     age=models.IntegerField()
 ``` 
  **New Code**
 
   ```python
    class Employee(Tracker.ModelTracker):
      name=models.CharField(max_length=255)
      address=models.CharField(max_length=255)
      age=models.IntegerField()
```
* For each save() call, add the user the username
    * Old Code
 ```python
    emp=Employee()
    emp.save()
 ``` 
 
     * New Code
 ```python
        emp=Employee()
        emp.save(request.user.username)
 ```
* Starting from version of 0.5, you can pass a event_name parameter to mark change as an event
 
     * New Code
 ```python
        emp=Employee()
        emp.save(request.user.username,event_name="Created the user")
 ```

# Using The Middleware

You can add `ModelTracker.middleware.ModelTrackerMiddleware` to your Middleware classes to get the username automatically from the request.

```python
MIDDLEWARE_CLASSES = (
     '....',
    'ModelTracker.middleware.ModelTrackerMiddleware',
    )
```   

**Note:** If you pass username as `None` then the change won't be saved.

# Showing Record History

There are 3 ways to see the history of a record
 1. go to `ModelTracker` url and select `Table` and enter `id`.
 2. call `showModelChanges` by POST and send `csrftokenmiddleware` to return history as html.
 3. call `getModelChanges` which returns history as Json.

# Django Admin

There is 2 ways to update an object by django admin
1. Handle save and delete in ModelAdmin as follows
   ```python
   def save_model(self, request, obj, form, change):
        obj.save(request.user.username,"Editing From admin interface")

   def delete_model(self, request, obj):
        obj.delete(username=request.user.username, event_name="Deleting From admin interface")
   ```
2. Inhert from TrackerAdmin rather ModelAdmin
   ```python
   from ModelTracker.Tracker import TrackerAdmin 
   admin.site.register(employee, TrackerAdmin)
``` 
