from django.dispatch import Signal

email_queued = Signal()
"""
This signal is triggered whenever Post Office pushes one or more emails into its queue.
The Emails objects added to the queue are passed as list to the callback handler. 
It can be connected to any handler function using this signature:

Example:
    from django.dispatch import receiver
    from post_office.signal import email_queued

    @receiver(email_queued)
    def my_callback(sender, emails, **kwargs):
        print("Just added {} mails to the sending queue".format(len(emails)))
"""
