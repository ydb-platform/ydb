django-sitemessage
==================
https://github.com/idlesign/django-sitemessage

.. image:: https://img.shields.io/pypi/v/django-sitemessage.svg
    :target: https://pypi.python.org/pypi/django-sitemessage

.. image:: https://img.shields.io/pypi/l/django-sitemessage.svg
    :target: https://pypi.python.org/pypi/django-sitemessage

.. image:: https://img.shields.io/coveralls/idlesign/django-sitemessage/master.svg
    :target: https://coveralls.io/r/idlesign/django-sitemessage

Description
-----------

*Reusable application for Django introducing a message delivery framework.*

Features:

* **Message Types** - message classes exposing message composition logic (plain text, html, etc.).
* **Messengers** - clients for various protocols (smtp, jabber, twitter, telegram, facebook, vkontakte, etc.);
* Support for user defined message types.
* Support for user defined messenger types.
* Message prioritization.
* Message subscription/unsubscription system.
* Message grouping to prevent flooding.
* Message 'read' indication.
* Means for background message delivery and cleanup.
* Means to debug integration: test requisites, delivery log.
* Django Admin integration.


1. Configure messengers for your project (create ``sitemessages.py`` in one of your apps):

.. code-block:: python

    from sitemessage.toolbox import register_messenger_objects, register_message_types
    from sitemessage.messengers.smtp import SMTPMessenger

    register_messenger_objects(
        # Here we register one messenger to deliver emails.
        # By default it uses mailing related settings from Django settings file.
        SMTPMessenger()
    )


2. Schedule messages for delivery when and where needed (e.g. in a view):

.. code-block:: python

    from sitemessage.shortcuts import schedule_email

    def send_mail_view(request):
        ...

        # Suppose `user_model` is a recipient Django User model instance.
        user1_model = ...

        # We pass `request.user` into `sender` to keep track of senders.
        schedule_email('Message from sitemessage.', [user1_model, 'user2@host.com'], sender=request.user)

        ...


3. Periodically run Django management command from wherever you like (cli, cron, Celery, uWSGI, etc.):

    ./manage.py sitemessage_send_scheduled


And that's only the tip of ``sitemessage`` iceberg, read the docs %)


Documentation
-------------

http://django-sitemessage.readthedocs.org/
