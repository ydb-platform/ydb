from django.core.files.base import ContentFile
from django.core.exceptions import ValidationError

from django.test import TestCase
from django.test.utils import override_settings

from ..models import Email, STATUS, PRIORITY, EmailTemplate, Attachment
from ..utils import create_attachments, get_email_template, parse_emails, parse_priority, send_mail, split_emails
from ..validators import validate_email_with_name, validate_comma_separated_emails


@override_settings(EMAIL_BACKEND='django.core.mail.backends.locmem.EmailBackend')
class UtilsTest(TestCase):
    def test_mail_status(self):
        """
        Check that send_mail assigns the right status field to Email instances
        """
        send_mail('subject', 'message', 'from@example.com', ['to@example.com'], priority=PRIORITY.medium)
        email = Email.objects.latest('id')
        self.assertEqual(email.status, STATUS.queued)

        # Emails sent with "now" priority is sent right away
        send_mail('subject', 'message', 'from@example.com', ['to@example.com'], priority=PRIORITY.now)
        email = Email.objects.latest('id')
        self.assertEqual(email.status, STATUS.sent)

    def test_email_validator(self):
        # These should validate
        validate_email_with_name('email@example.com')
        validate_email_with_name('Alice Bob <email@example.com>')
        Email.objects.create(
            to=['to@example.com'],
            from_email='Alice <from@example.com>',
            subject='Test',
            message='Message',
            status=STATUS.sent,
        )

        # Should also support international domains
        validate_email_with_name('Alice Bob <email@example.co.id>')

        # These should raise ValidationError
        self.assertRaises(ValidationError, validate_email_with_name, 'invalid')
        self.assertRaises(ValidationError, validate_email_with_name, 'Al <ab>')
        self.assertRaises(ValidationError, validate_email_with_name, 'Al <>')

    def test_comma_separated_email_list_validator(self):
        # These should validate
        validate_comma_separated_emails(['email@example.com'])
        validate_comma_separated_emails(['email@example.com', 'email2@example.com', 'email3@example.com'])
        validate_comma_separated_emails(['Alice Bob <email@example.com>'])

        # Should also support international domains
        validate_comma_separated_emails(['email@example.co.id'])

        # These should raise ValidationError
        self.assertRaises(
            ValidationError, validate_comma_separated_emails, ['email@example.com', 'invalid_mail', 'email@example.com']
        )

    def test_get_template_email(self):
        # Sanity Check
        name = 'customer/happy-holidays'
        self.assertRaises(EmailTemplate.DoesNotExist, get_email_template, name)
        template = EmailTemplate.objects.create(name=name, content='test')

        # First query should hit database
        self.assertNumQueries(1, lambda: get_email_template(name))
        # Second query should hit cache instead
        self.assertNumQueries(0, lambda: get_email_template(name))

        # It should return the correct template
        self.assertEqual(template, get_email_template(name))

        # Repeat with language support
        template = EmailTemplate.objects.create(name=name, content='test', language='en')
        # First query should hit database
        self.assertNumQueries(1, lambda: get_email_template(name, 'en'))
        # Second query should hit cache instead
        self.assertNumQueries(0, lambda: get_email_template(name, 'en'))

        # It should return the correct template
        self.assertEqual(template, get_email_template(name, 'en'))

    def test_template_caching_settings(self):
        """Check if POST_OFFICE_CACHE and POST_OFFICE_TEMPLATE_CACHE understood
        correctly
        """

        def is_cache_used(suffix='', desired_cache=False):
            """Raise exception if real cache usage not equal to desired_cache value"""
            # to avoid cache cleaning - just create new template
            name = 'can_i/suport_cache_settings%s' % suffix
            self.assertRaises(EmailTemplate.DoesNotExist, get_email_template, name)
            EmailTemplate.objects.create(name=name, content='test')

            # First query should hit database anyway
            self.assertNumQueries(1, lambda: get_email_template(name))
            # Second query should hit cache instead only if we want it
            self.assertNumQueries(0 if desired_cache else 1, lambda: get_email_template(name))
            return

        # default - use cache
        is_cache_used(suffix='with_default_cache', desired_cache=True)

        # disable cache
        with self.settings(POST_OFFICE_CACHE=False):
            is_cache_used(suffix='cache_disabled_global', desired_cache=False)
        with self.settings(POST_OFFICE_TEMPLATE_CACHE=False):
            is_cache_used(suffix='cache_disabled_for_templates', desired_cache=False)
        with self.settings(POST_OFFICE_CACHE=True, POST_OFFICE_TEMPLATE_CACHE=False):
            is_cache_used(suffix='cache_disabled_for_templates_but_enabled_global', desired_cache=False)
        return

    def test_split_emails(self):
        """
        Check that split emails correctly divide email lists for multiprocessing
        """
        for i in range(225):
            Email.objects.create(from_email='from@example.com', to=['to@example.com'])
        expected_size = [57, 56, 56, 56]
        email_list = split_emails(Email.objects.all(), 4)
        self.assertEqual(expected_size, [len(emails) for emails in email_list])

    def test_create_attachments(self):
        attachments = create_attachments(
            {
                'attachment_file1.txt': ContentFile('content'),
                'attachment_file2.txt': ContentFile('content'),
            }
        )

        self.assertEqual(len(attachments), 2)
        self.assertIsInstance(attachments[0], Attachment)
        self.assertTrue(attachments[0].pk)
        self.assertEqual(attachments[0].file.read(), b'content')
        self.assertTrue(attachments[0].name.startswith('attachment_file'))
        self.assertEqual(attachments[0].mimetype, '')

    def test_create_attachments_with_mimetype(self):
        attachments = create_attachments(
            {
                'attachment_file1.txt': {'file': ContentFile('content'), 'mimetype': 'text/plain'},
                'attachment_file2.jpg': {'file': ContentFile('content'), 'mimetype': 'text/plain'},
            }
        )

        self.assertEqual(len(attachments), 2)
        self.assertIsInstance(attachments[0], Attachment)
        self.assertTrue(attachments[0].pk)
        self.assertEqual(attachments[0].file.read(), b'content')
        self.assertTrue(attachments[0].name.startswith('attachment_file'))
        self.assertEqual(attachments[0].mimetype, 'text/plain')

    def test_create_attachments_open_file(self):
        attachments = create_attachments({'attachment_file.py': __file__})

        self.assertEqual(len(attachments), 1)
        self.assertIsInstance(attachments[0], Attachment)
        self.assertTrue(attachments[0].pk)
        self.assertTrue(attachments[0].file.read())
        self.assertEqual(attachments[0].name, 'attachment_file.py')
        self.assertEqual(attachments[0].mimetype, '')

    def test_parse_priority(self):
        self.assertEqual(parse_priority('now'), PRIORITY.now)
        self.assertEqual(parse_priority('high'), PRIORITY.high)
        self.assertEqual(parse_priority('medium'), PRIORITY.medium)
        self.assertEqual(parse_priority('low'), PRIORITY.low)

    def test_parse_emails(self):
        # Converts a single email to list of email
        self.assertEqual(parse_emails('test@example.com'), ['test@example.com'])

        # None is converted into an empty list
        self.assertEqual(parse_emails(None), [])

        # Raises ValidationError if email is invalid
        self.assertRaises(ValidationError, parse_emails, 'invalid_email')
        self.assertRaises(ValidationError, parse_emails, ['invalid_email', 'test@example.com'])
