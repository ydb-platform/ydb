# Django Export Action
Generic export action for Django's Admin for Python 3.6 and Django 2.x
## Quickstart
Install Django Export Action:
```
pip install django-export-action-py3
```
Include it on INSTALLED_APPS:
```
'export_action',
```
Add to urls:
```
path(r'^export_action/', include("export_action.urls", namespace="export_action")),
```
## Usage
Go to any admin page, select fields, then select the export to xls action. Then check off any fields you want to export.

## Features
- Generic action to enable export data from Admin.
- Automatic traversal of model relations.
- Selection of fields to export.
- Can export to XSLx, CSV and HTML.

## Reference
This is a fork of another repository to keep the compatibility with Django 2.x and Python 3.6.

Original repository:
https://github.com/fgmacedo/django-export-action
