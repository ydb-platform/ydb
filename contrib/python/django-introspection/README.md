# Django Introspection

Introspection tools for Django

## Install

`pip install django-introspection`

## Usage

   ```python
   from introspection import AppInspector
   
   app = AppInspector("myapp_label") # note you can also use a path: django.contrib.auth
   # get a list of app's models':
   app.get_models()
   print(app.models)
   # get a list of fields for a model
   fields = app.models[0].fields
   print(fields)
   ```

## Management command

Print details about a model or app:

   ```python
   # inspect an app
   python3 manage.py inspectapp auth
   # or python3 manage.py inspectapp django.contrib.auth
   
   # for a model
   python3 manage.py inspectmodel auth.User
   # or python3 manage.py inspectmodel django.contrib.auth.User
   ```
   
Output:

   ```
========================================================
                     Fields
========================================================
# Found 14 fields: 
profile OneToOneField with related name user 
id AutoField 
password CharField 
last_login DateTimeField 
is_superuser BooleanField 
username CharField 
first_name CharField 
last_name CharField 
email CharField 
is_staff BooleanField 
is_active BooleanField 
date_joined DateTimeField 
groups ManyToManyField with related name user 
user_permissions ManyToManyField with related name user 
========================================================
                     Relations
========================================================
# Found 5 external relations : 
admin.LogEntry.user from auth.User.id ManyToOneRel  
account.EmailAddress.user from auth.User.id ManyToOneRel  
socialaccount.SocialAccount.user from auth.User.id ManyToOneRel  
reversion.Revision.user from auth.User.id ManyToOneRel  
polls.Vote.user from auth.User.id ManyToOneRel  
========================================================
                     Instances
========================================================
# Found 558 instances of User
   ```

## Run the tests

Clone then cd in the django-introspection directory and run:

```
make install
make test-initial
```