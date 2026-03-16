import multiprocessing
from collections.abc import Sequence
from email.utils import make_msgid
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any, Optional

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import connection as db_connection
from django.db.models import Q, QuerySet
from django.template import Context, Template
from django.utils import timezone

from .connections import connections
from .lockfile import FileLock, FileLocked, default_lockfile
from .logutils import setup_loghandlers
from .models import PRIORITY, STATUS, Email, EmailTemplate, Log
from .settings import (
    get_available_backends,
    get_batch_delivery_timeout,
    get_batch_size,
    get_log_level,
    get_max_retries,
    get_message_id_enabled,
    get_message_id_fqdn,
    get_retry_timedelta,
    get_sending_order,
    get_threads_per_process,
)
from .signals import email_queued
from .utils import (
    create_attachments,
    get_email_template,
    parse_emails,
    parse_priority,
    split_emails,
)

logger = setup_loghandlers('INFO')


def _send_email(email: Email, log_level: int) -> tuple[bool, Optional[Exception]]:
    try:
        email.dispatch(log_level=log_level, commit=False, disconnect_after_delivery=False)
        logger.debug('Successfully sent email #%d' % email.id)
        return True, None
    except Exception as e:
        logger.exception('Failed to send email #%d' % email.id)
        return False, e


def create(
    sender,
    recipients=None,
    cc=None,
    bcc=None,
    subject='',
    message='',
    html_message='',
    context=None,
    scheduled_time=None,
    expires_at=None,
    headers=None,
    template=None,
    priority=None,
    render_on_delivery=False,
    commit=True,
    backend='',
):
    """
    Creates an email from supplied keyword arguments. If template is
    specified, email subject and content will be rendered during delivery.
    """
    priority = parse_priority(priority)
    status = None if priority == PRIORITY.now else STATUS.queued

    if recipients is None:
        recipients = []
    if cc is None:
        cc = []
    if bcc is None:
        bcc = []
    if context is None:
        context = ''
    message_id = make_msgid(domain=get_message_id_fqdn()) if get_message_id_enabled() else None

    # If email is to be rendered during delivery, save all necessary
    # information
    if render_on_delivery:
        email = Email(
            from_email=sender,
            to=recipients,
            cc=cc,
            bcc=bcc,
            scheduled_time=scheduled_time,
            expires_at=expires_at,
            message_id=message_id,
            headers=headers,
            priority=priority,
            status=status,
            context=context,
            template=template,
            backend_alias=backend,
        )

    else:
        if template:
            subject = template.subject
            message = template.content
            html_message = template.html_content

        _context = Context(context or {})
        subject = Template(subject).render(_context)
        message = Template(message).render(_context)
        html_message = Template(html_message).render(_context)

        email = Email(
            from_email=sender,
            to=recipients,
            cc=cc,
            bcc=bcc,
            subject=subject,
            message=message,
            html_message=html_message,
            scheduled_time=scheduled_time,
            expires_at=expires_at,
            message_id=message_id,
            headers=headers,
            priority=priority,
            status=status,
            backend_alias=backend,
            template=template,
        )

    if commit:
        email.save()

    return email


def send(
    recipients=None,
    sender=None,
    template=None,
    context=None,
    subject='',
    message='',
    html_message='',
    scheduled_time=None,
    expires_at=None,
    headers=None,
    priority=None,
    attachments=None,
    render_on_delivery=False,
    log_level=None,
    commit=True,
    cc=None,
    bcc=None,
    language='',
    backend='',
):
    try:
        recipients = parse_emails(recipients)
    except ValidationError as e:
        raise ValidationError('recipients: %s' % e.message)

    try:
        cc = parse_emails(cc)
    except ValidationError as e:
        raise ValidationError('c: %s' % e.message)

    try:
        bcc = parse_emails(bcc)
    except ValidationError as e:
        raise ValidationError('bcc: %s' % e.message)

    if sender is None:
        sender = settings.DEFAULT_FROM_EMAIL

    priority = parse_priority(priority)

    if log_level is None:
        log_level = get_log_level()

    if not commit:
        if priority == PRIORITY.now:
            raise ValueError("send_many() can't be used with priority = 'now'")
        if attachments:
            raise ValueError("Can't add attachments with send_many()")

    if template:
        if subject:
            raise ValueError('You can\'t specify both "template" and "subject" arguments')
        if message:
            raise ValueError('You can\'t specify both "template" and "message" arguments')
        if html_message:
            raise ValueError('You can\'t specify both "template" and "html_message" arguments')

        # template can be an EmailTemplate instance or name
        if isinstance(template, EmailTemplate):
            template = template
            # If language is specified, ensure template uses the right language
            if language and template.language != language:
                template = template.translated_templates.get(language=language)
        else:
            template = get_email_template(template, language)

    if backend and backend not in get_available_backends().keys():
        raise ValueError('%s is not a valid backend alias' % backend)

    email = create(
        sender,
        recipients,
        cc,
        bcc,
        subject,
        message,
        html_message,
        context,
        scheduled_time,
        expires_at,
        headers,
        template,
        priority,
        render_on_delivery,
        commit=commit,
        backend=backend,
    )

    if attachments:
        attachments = create_attachments(attachments)
        email.attachments.add(*attachments)

    if priority == PRIORITY.now:
        email.dispatch(log_level=log_level)
    elif commit:
        email_queued.send(sender=Email, emails=[email])

    return email


def send_many(kwargs_list: list[dict[str, Any]]) -> list[Email]:
    """
    Similar to mail.send(), but this function accepts a list of kwargs.
    Internally, it uses Django's bulk_create command for efficiency reasons.
    Currently send_many() can't be used to send emails with priority = 'now'.
    """
    emails = [send(commit=False, **kwargs) for kwargs in kwargs_list]
    if emails:
        Email.objects.bulk_create(emails)
        email_queued.send(sender=Email, emails=emails)

    return emails


def get_queued() -> QuerySet[Email]:
    """
    Returns the queryset of emails eligible for sending – fulfilling these conditions:
     - Status is queued or requeued
     - Has scheduled_time before the current time or is None
     - Has expires_at after the current time or is None
    """
    now = timezone.now()
    query = (Q(scheduled_time__lte=now) | Q(scheduled_time=None)) & (Q(expires_at__gt=now) | Q(expires_at=None))
    return (
        Email.objects.filter(query, status__in=[STATUS.queued, STATUS.requeued])
        .order_by(*get_sending_order())
        .prefetch_related('attachments')[: get_batch_size()]
    )


def attach_templates(emails: list[Email]) -> None:
    """
    Efficiently attach template objects to emails using a single query
    for all unique templates instead of loading duplicates.
    """
    template_ids = {email.template_id for email in emails if email.template_id is not None}

    if not template_ids:
        return

    templates = EmailTemplate.objects.filter(id__in=template_ids)
    template_map = {template.id: template for template in templates}

    for email in emails:
        if email.template_id is not None:
            email.template = template_map.get(email.template_id)


def send_queued(processes: int = 1, log_level: Optional[int] = None) -> tuple[int, int, int]:
    """
    Sends out all queued mails that has scheduled_time less than now or None
    """
    queued_emails = list(get_queued())
    attach_templates(queued_emails)
    total_sent, total_failed, total_requeued = 0, 0, 0
    total_email = len(queued_emails)

    logger.info('Started sending %s emails with %s processes.' % (total_email, processes))

    if log_level is None:
        log_level = get_log_level()

    if queued_emails:
        # Don't use more processes than number of emails
        if total_email < processes:
            processes = total_email

        if processes == 1:
            total_sent, total_failed, total_requeued = _send_bulk(
                emails=queued_emails,
                uses_multiprocessing=False,
                log_level=log_level,
            )
        else:
            email_lists = split_emails(queued_emails, processes)

            # Use 'fork' context to ensure child processes inherit Django setup.
            # This is required for Python 3.14+ where the default start method is 'forkserver' on Linux.
            ctx = multiprocessing.get_context('fork')
            with ctx.Pool(processes) as pool:
                tasks = []
                for email_list in email_lists:
                    tasks.append(pool.apply_async(_send_bulk, args=(email_list,)))

                timeout = get_batch_delivery_timeout()
                results = []

                # Wait for all tasks to complete with a timeout
                # The get method is used with a timeout to wait for each result
                for task in tasks:
                    results.append(task.get(timeout=timeout))

            total_sent = sum(result[0] for result in results)
            total_failed = sum(result[1] for result in results)
            total_requeued = sum(result[2] for result in results)

    logger.info(
        '%s emails attempted, %s sent, %s failed, %s requeued',
        total_email,
        total_sent,
        total_failed,
        total_requeued,
    )

    return total_sent, total_failed, total_requeued


def _send_bulk(
    emails: Sequence[Email], uses_multiprocessing: bool = True, log_level: Optional[int] = None
) -> tuple[int, int, int]:
    # Multiprocessing does not play well with database connection
    # Fix: Close connections on forking process
    # https://groups.google.com/forum/#!topic/django-users/eCAIY9DAfG0
    if uses_multiprocessing:
        db_connection.close()

    if log_level is None:
        log_level = get_log_level()
    assert log_level is not None

    sent_emails = []
    failed_emails = []  # This is a list of two tuples (email, exception)
    email_count = len(emails)

    logger.info('Process started, sending %s emails' % email_count)

    emails_to_send = []

    # Prepare emails before we send these to threads for sending
    # So we don't need to access the DB from within threads
    for email in emails:
        # Sometimes this can fail, for example when trying to render
        # email from a faulty Django template
        try:
            email.prepare_email_message()
            emails_to_send.append(email)
        except Exception as e:
            logger.exception('Failed to prepare email #%d' % email.id)
            failed_emails.append((email, e))

    number_of_threads = min(get_threads_per_process(), email_count)
    with ThreadPool(number_of_threads) as pool:
        results = []
        for email in emails_to_send:
            results.append((email, pool.apply_async(_send_email, args=(email, log_level))))

        timeout = get_batch_delivery_timeout()

        # Wait for all tasks to complete with a timeout
        # The get method is used with a timeout to wait for each result
        for email, result in results:
            success, exception = result.get(timeout=timeout)
            if success:
                sent_emails.append(email)
            else:
                failed_emails.append((email, exception))

    connections.close()

    # Update statuses of sent emails
    email_ids = [email.id for email in sent_emails]
    Email.objects.filter(id__in=email_ids).update(status=STATUS.sent, last_updated=timezone.now())

    # Update statuses and conditionally requeue failed emails
    num_failed, num_requeued = 0, 0
    max_retries = get_max_retries()
    scheduled_time = timezone.now() + get_retry_timedelta()
    emails_failed = [email for email, _ in failed_emails]

    for email in emails_failed:
        if email.number_of_retries is None:
            email.number_of_retries = 0
        if email.number_of_retries < max_retries:
            email.number_of_retries += 1
            email.status = STATUS.requeued
            email.scheduled_time = scheduled_time
            num_requeued += 1
        else:
            email.status = STATUS.failed
            num_failed += 1
        email.last_updated = timezone.now()

    Email.objects.bulk_update(emails_failed, ['status', 'scheduled_time', 'number_of_retries', 'last_updated'])

    # If log level is 0, log nothing, 1 logs only sending failures
    # and 2 means log both successes and failures
    if log_level >= 1:
        logs = []
        for email, exception in failed_emails:
            logs.append(
                Log(
                    email=email,
                    status=STATUS.failed,
                    message=str(exception),
                    exception_type=type(exception).__name__,
                )
            )

        if logs:
            Log.objects.bulk_create(logs)

    if log_level == 2:
        logs = []
        for email in sent_emails:
            logs.append(Log(email=email, status=STATUS.sent))

        if logs:
            Log.objects.bulk_create(logs)

    logger.info(
        'Process finished, %s attempted, %s sent, %s failed, %s requeued',
        email_count,
        len(sent_emails),
        num_failed,
        num_requeued,
    )

    return len(sent_emails), num_failed, num_requeued


def send_queued_mail_until_done(
    lockfile: str = default_lockfile, processes: int = 1, log_level: Optional[int] = None
) -> None:
    """
    Send mail in queue batch by batch, until all emails have been processed.
    """
    try:
        with FileLock(lockfile):
            logger.info('Acquired lock for sending queued emails at %s.lock', lockfile)
            while True:
                try:
                    send_queued(processes, log_level)
                except Exception as e:
                    logger.exception(e, extra={'status_code': 500})
                    raise

                # Close DB connection to avoid multiprocessing errors
                db_connection.close()

                if not get_queued().exists():
                    break
    except FileLocked:
        logger.info('Failed to acquire lock, terminating now.')
