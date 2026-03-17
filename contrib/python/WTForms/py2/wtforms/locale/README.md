Translation Submission Guidelines
=================================

To create a translation, the easiest way to start is to run:

    $ python setup.py init_catalog --locale <your locale>

Which will copy the template to the right location. To run that setup.py sub-command, you need Babel and setuptools/distribute installed.

.po files:
 - must be a valid utf-8 text file
 - should have the header filled out appropriately
 - should translate all messages

You probably want to try setup.py compile_catalog and try loading your translations up to verify you did it all right.

Submitting
----------

The best ways to submit your translation are as a pull request on github, or an email to james+i18n@simplecodes.com, with the file included as an attachment.

utf-8 text may not format nicely in an email body, so please refrain from pasting the translations into an email body, and include them as an attachment instead. Also do not post translation files in the issue tracker text box, or onto the mailing list either, because again formatting may be broken.