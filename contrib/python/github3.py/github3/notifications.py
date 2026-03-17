"""
This module contains the classes relating to notifications.

See also: http://developer.github.com/v3/activity/notifications/
"""
from json import dumps

from . import models


class Thread(models.GitHubCore):
    """Object representing a notification thread.

    .. versionchanged:: 1.0.0

        The ``comment``, ``thread``, and ``url`` attributes are no longer
        present because GitHub stopped returning the comment that caused
        the notification.

        The ``is_unread`` method was removed since it just returned the
        ``unread`` attribute.

    This object has the following attributes:

    .. attribute:: id

        The unique identifier for this notification across all GitHub
        notifications.

    .. attribute:: last_read_at

        A :class:`~datetime.datetime` object representing the date and time
        when the authenticated user last read this thread.

    .. attribute:: reason

        The reason the authenticated user is receiving this notification.

    .. attribute:: repository

        A :class:`~github3.repos.ShortRepository` this thread originated on.

    .. attribute:: subject

        A dictionary with the subject of the notification, for example, which
        issue, pull request, or diff this is in relation to.

    .. attribute:: unread

        A boolean attribute indicating whether this thread has been read or
        not.

    .. attribute:: updated_at

        A :class:`~datetime.datetime` representing the date and time when this
        thread was last updated.

    See also:
    http://developer.github.com/v3/activity/notifications/
    """

    def _update_attributes(self, thread):
        from . import repos

        self._api = thread["url"]
        self.id = thread["id"]
        self.last_read_at = self._strptime(thread["last_read_at"])
        self.reason = thread["reason"]
        self.repository = repos.ShortRepository(thread["repository"], self)
        self.subject = thread["subject"]
        self.unread = thread["unread"]
        self.updated_at = self._strptime(thread["updated_at"])

    def _repr(self):
        return "<Thread [{}]>".format(self.subject.get("title"))

    def delete_subscription(self):
        """Delete subscription for this thread.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        url = self._build_url("subscription", base_url=self._api)
        return self._boolean(self._delete(url), 204, 404)

    def mark(self):
        """Mark the thread as read.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._patch(self._api), 205, 404)

    def set_subscription(self, subscribed, ignored):
        """Set the user's subscription for this thread.

        :param bool subscribed:
            (required), determines if notifications should be received from
            this thread.
        :param bool ignored:
            (required), determines if notifications should be ignored from this
            thread.
        :returns:
            new subscription
        :rtype:
            :class:`~github3.notifications.ThreadSubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        sub = {"subscribed": subscribed, "ignored": ignored}
        json = self._json(self._put(url, data=dumps(sub)), 200)
        return self._instance_or_null(ThreadSubscription, json)

    def subscription(self):
        """Check the status of the user's subscription to this thread.

        :returns:
            the subscription for this thread
        :rtype:
            :class:`~github3.notifications.ThreadSubscription`
        """
        url = self._build_url("subscription", base_url=self._api)
        json = self._json(self._get(url), 200)
        return self._instance_or_null(ThreadSubscription, json)


class _Subscription(models.GitHubCore):
    """This object wraps thread and repository subscription information.

    See also:
    developer.github.com/v3/activity/notifications/#get-a-thread-subscription

    .. versionchanged:: 1.0.0

        The ``is_ignored`` and ``is_subscribed`` methods were removed. Use the
        :attr`ignored` and :attr:`subscribed` attributes instead.

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        the user was subscribed to the thread.

    .. attribute:: ignored

        A boolean attribute indicating whether the user ignored this.

    .. attribute:: reason

        The reason the user is subscribed to the thread.

    .. attribute:: subscribed

        A boolean attribute indicating whether the user is subscribed or not.

    .. attribute:: thread_url

        The URL of the thread resource in the GitHub API.
    """

    class_name = "_Subscription"

    def _update_attributes(self, sub):
        self._api = sub["url"]
        self.created_at = self._strptime(sub["created_at"])
        self.ignored = sub["ignored"]
        self.reason = sub["reason"]
        self.subscribed = sub["subscribed"]

    def _repr(self):
        return f"<{self.class_name} [{self.subscribed}]>"

    def delete(self):
        """Delete this subscription.

        :returns:
            True if successful, False otherwise
        :rtype:
            bool
        """
        return self._boolean(self._delete(self._api), 204, 404)

    def set(self, subscribed, ignored):
        """Set the user's subscription for this subscription.

        :param bool subscribed:
            (required), determines if notifications should be received from
            this thread.
        :param bool ignored:
            (required), determines if notifications should be ignored from this
            thread.
        """
        sub = {"subscribed": subscribed, "ignored": ignored}
        json = self._json(self._put(self._api, data=dumps(sub)), 200)
        self._update_attributes(json)


class ThreadSubscription(_Subscription):
    """This object provides a representation of a thread subscription.

    See also:
    developer.github.com/v3/activity/notifications/#get-a-thread-subscription

    .. versionchanged:: 1.0.0

        The ``is_ignored`` and ``is_subscribed`` methods were removed. Use the
        :attr`ignored` and :attr:`subscribed` attributes instead.

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        the user was subscribed to the thread.

    .. attribute:: ignored

        A boolean attribute indicating whether the user ignored this.

    .. attribute:: reason

        The reason the user is subscribed to the thread.

    .. attribute:: subscribed

        A boolean attribute indicating whether the user is subscribed or not.

    .. attribute:: thread_url

        The URL of the thread resource in the GitHub API.
    """

    class_name = "ThreadSubscription"

    def _update_subscription(self, sub):
        super()._update_attributes(sub)
        self.thread_url = sub["thread_url"]


class RepositorySubscription(_Subscription):
    """This object provides a representation of a thread subscription.

    See also:
    developer.github.com/v3/activity/notifications/#get-a-thread-subscription

    .. versionchanged:: 1.0.0

        The ``is_ignored`` and ``is_subscribed`` methods were removed. Use the
        :attr`ignored` and :attr:`subscribed` attributes instead.

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the date and time
        the user was subscribed to the thread.

    .. attribute:: ignored

        A boolean attribute indicating whether the user ignored this.

    .. attribute:: reason

        The reason the user is subscribed to the thread.

    .. attribute:: repository_url

        The URL of the repository resource in the GitHub API.

    .. attribute:: subscribed

        A boolean attribute indicating whether the user is subscribed or not.
    """

    class_name = "RepositorySubscription"

    def _update_subscription(self, sub):
        super()._update_attributes(sub)
        self.repository_url = sub.get("repository_url")
