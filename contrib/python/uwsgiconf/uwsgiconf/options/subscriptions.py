from ..base import OptionsGroup
from ..exceptions import ConfigurationError
from ..utils import KeyValue, filter_locals
from .subscriptions_algos import *


class Subscriptions(OptionsGroup):
    """
    This allows some uWSGI instances to announce their presence to subscriptions managing server,
    which in its turn can address those nodes (e.g. delegate request processing to them)
    and automatically remove dead nodes from the pool.

    Some routers provide subscription server functionality. See `.routing.routers`.

    .. note:: Subscription system in many ways relies on Master Process.

    .. warning:: The subscription system is meant for "trusted" networks.
        All of the nodes in your network can potentially make a total mess with it.

    * http://uwsgi.readthedocs.io/en/latest/SubscriptionServer.html

    """

    class algorithms:
        """Balancing algorithms available to use with ``subscribe``."""

        ip_hash = IpHash
        least_reference_count = LeastReferenceCount
        weighted_least_reference_count = WeightedLeastReferenceCount
        weighted_round_robin = WeightedRoundRobin

    def set_server_params(
            self, *, client_notify_address=None, mountpoints_depth=None, require_vassal=None,
            tolerance=None, tolerance_inactive=None, key_dot_split=None):
        """Sets subscription server related params.

        :param str client_notify_address: Set the notification socket for subscriptions.
            When you subscribe to a server, you can ask it to "acknowledge" the acceptance of your request.
            pointing address (Unix socket or UDP), on which your instance will bind and
            the subscription server will send acknowledgements to.

        :param int mountpoints_depth: Enable support of mountpoints of certain depth for subscription system.

            * http://uwsgi-docs.readthedocs.io/en/latest/SubscriptionServer.html#mountpoints-uwsgi-2-1

        :param bool require_vassal: Require a vassal field (see ``subscribe``) from each subscription.

        :param int tolerance: Subscription reclaim tolerance (seconds).

        :param int tolerance_inactive: Subscription inactivity tolerance (seconds).

        :param bool key_dot_split: Try to fallback to the next part in (dot based) subscription key.
            Used, for example, in SNI.

        """
        # todo notify-socket (fallback) relation
        self._set('subscription-notify-socket', client_notify_address)
        self._set('subscription-mountpoint', mountpoints_depth)
        self._set('subscription-vassal-required', require_vassal, cast=bool)
        self._set('subscription-tolerance', tolerance)
        self._set('subscription-tolerance-inactive', tolerance_inactive)
        self._set('subscription-dotsplit', key_dot_split, cast=bool)

        return self._section

    def set_server_verification_params(
            self, *, digest_algo=None, dir_cert=None, tolerance=None, no_check_uid=None,
            dir_credentials=None, pass_unix_credentials=None):
        """Sets peer verification params for subscription server.

        These are for secured subscriptions.

        :param str digest_algo: Digest algorithm. Example: SHA1

            .. note:: Also requires ``dir_cert`` to be set.

        :param str dir_cert: Certificate directory.

            .. note:: Also requires ``digest_algo`` to be set.

        :param int tolerance: Maximum tolerance (in seconds) of clock skew for secured subscription system.
            Default: 24h.

        :param str|int|list[str|int] no_check_uid: Skip signature check for the specified uids
            when using unix sockets credentials.

        :param str|list[str] dir_credentials: Directories to search for subscriptions
            key credentials.

        :param bool pass_unix_credentials: Enable management of SCM_CREDENTIALS in subscriptions UNIX sockets.

        """
        if digest_algo and dir_cert:
            self._set('subscriptions-sign-check', f'{digest_algo}:{dir_cert}')

        self._set('subscriptions-sign-check-tolerance', tolerance)
        self._set('subscriptions-sign-skip-uid', no_check_uid, multi=True)
        self._set('subscriptions-credentials-check', dir_credentials, multi=True)
        self._set('subscriptions-use-credentials', pass_unix_credentials, cast=bool)

        return self._section

    def set_client_params(
            self, *, start_unsubscribed=None, clear_on_exit=None, unsubscribe_on_reload=None,
            announce_interval=None):
        """Sets subscribers related params.

        :param bool start_unsubscribed: Configure subscriptions but do not send them.
            .. note:: Useful with master FIFO.

        :param bool clear_on_exit: Force clear instead of unsubscribe during shutdown.

        :param bool unsubscribe_on_reload: Force unsubscribe request even during graceful reload.

        :param int announce_interval: Send subscription announce at the specified interval. Default: 10 master cycles.

        """
        self._set('start-unsubscribed', start_unsubscribed, cast=bool)
        self._set('subscription-clear-on-shutdown', clear_on_exit, cast=bool)
        self._set('unsubscribe-on-graceful-reload', unsubscribe_on_reload, cast=bool)
        self._set('subscribe-freq', announce_interval)

        return self._section

    def subscribe(
            self, server=None, *, key=None, address=None, address_vassal=None,
            balancing_weight=None, balancing_algo=None, modifier=None, signing=None, check_file=None, protocol=None,
            sni_cert=None, sni_key=None, sni_client_ca=None):
        """Registers a subscription intent.

        :param str server: Subscription server address (UDP or UNIX socket).

            Examples:
                * 127.0.0.1:7171

        :param str key: Key to subscribe. Generally the domain name (+ optional '/< mountpoint>').
            Examples:
                * mydomain.it/foo
                * mydomain.it/foo/bar (requires ``mountpoints_depth=2``)
                * mydomain.it
                * ubuntu64.local:9090

        :param str|int address: Address to subscribe (the value for the key)
            or zero-based internal socket number (integer).

        :param str address: Vassal node address.

        :param int balancing_weight: Load balancing value. Default: 1.

        :param balancing_algo: Load balancing algorithm to use. See ``balancing_algorithms``
            .. note:: Since 2.1

        :param Modifier modifier: Routing modifier object. See ``.routing.modifiers``

        :param list|tuple signing:  Signing basics, expects two elements list/tuple:
            (signing_algorithm, key).

            Examples:
                * SHA1:idlessh001

        :param str check_file: If this file exists the subscription packet is sent,
            otherwise it is skipped.

        :param str protocol: the protocol to use, by default it is ``uwsgi``.
            See ``.networking.socket_types``.

            .. note:: Since 2.1

        :param str sni_cert: Certificate file to use for SNI proxy management.
            * http://uwsgi.readthedocs.io/en/latest/SNI.html#subscription-system-and-sni

        :param str sni_key: sni_key Key file to use for SNI proxy management.
            * http://uwsgi.readthedocs.io/en/latest/SNI.html#subscription-system-and-sni

        :param str sni_client_ca: Ca file to use for SNI proxy management.
            * http://uwsgi.readthedocs.io/en/latest/SNI.html#subscription-system-and-sni

        """
        # todo params: inactive (inactive slot activation)

        if not any((server, key)):
            raise ConfigurationError('Subscription requires `server` or `key` to be set.')

        address_key = 'addr'
        if isinstance(address, int):
            address_key = 'socket'

        if balancing_algo:
            backup = getattr(balancing_algo, 'backup_level', None)

        if signing:
            signing = ':'.join(signing)

        if modifier:
            modifier1 = modifier
            if modifier.submod:
                modifier2 = modifier.submod

        rule = KeyValue(
            filter_locals(locals(), drop=['address_key', 'modifier']),
            aliases={
                'address': address_key,
                'address_vassal': 'vassal',
                'signing': 'sign',
                'check_file': 'check',
                'balancing_weight': 'weight',
                'balancing_algo': 'algo',
                'protocol': 'proto',
                'sni_cert': 'sni_crt',
                'sni_client_ca': 'sni_ca',
            },
        )

        self._set('subscribe2', rule)

        return self._section
