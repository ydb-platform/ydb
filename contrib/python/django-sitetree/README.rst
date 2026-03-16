django-sitetree
===============
http://github.com/idlesign/django-sitetree

|release| |lic| |coverage|

.. |release| image:: https://img.shields.io/pypi/v/django-sitetree.svg
    :target: https://pypi.python.org/pypi/django-sitetree

.. |lic| image:: https://img.shields.io/pypi/l/django-sitetree.svg
    :target: https://pypi.python.org/pypi/django-sitetree

.. |coverage| image:: https://img.shields.io/coveralls/idlesign/django-sitetree/master.svg
    :target: https://coveralls.io/r/idlesign/django-sitetree


What's that
-----------

*django-sitetree is a reusable application for Django, introducing site tree, menu and breadcrumbs navigation elements.*

Site structure in django-sitetree is described through Django admin interface in a so called site trees.
Every item of such a tree describes a page or a set of pages through the relation of URI or URL to human-friendly title. Eg. using site tree editor in Django admin::

  URI             Title
    /             - Site Root
    |_users/      - Site Users
      |_users/13/ - Definite User


Alas the example above makes a little sense if you have more than just a few users, that's why django-sitetree supports Django template tags in item titles and Django named URLs in item URIs.
If we define a named URL for user personal page in urls.py, for example, 'users-personal', we could change a scheme in the following way::

  URI                           Title
    /                           - Site Root
    |_users/                    - Site Users
      |_users-personal user.id  - User Called {{ user.first_name }}

After setting up site structure as a sitetree you should be able to use convenient and highly customizable site navigation means (menus, breadcrumbs and full site trees).

User access to certain sitetree items can be restricted to authenticated users or more accurately with the help of Django permissions system (Auth contrib package).

Sitetree also allows you to define dynamic trees in your code instead of Admin interface. And even more: you can combine those two types of trees in more sophisticated ways.


Documentation
-------------

http://django-sitetree.readthedocs.org/
