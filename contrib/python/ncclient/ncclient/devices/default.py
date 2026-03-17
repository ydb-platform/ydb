"""
Handler for default device information.

Some devices require very specific information and action during client interaction.

The "device handlers" provide a number of callbacks that return the necessary
information. This allows the ncclient code to merely call upon this device handler -
once configured - instead of cluttering its code with if-statements.

Initially, not much is dealt with by the handler. However, in the future, as more
devices with specific handling are added, more handlers and more functions should be
implememted here, so that the ncclient code can use these callbacks to fill in the
device specific information.

Note that for proper import, the classname has to be:

    "<Devicename>DeviceHandler"

...where <Devicename> is something like "Default", "Nexus", etc.

All device-specific handlers derive from the DefaultDeviceHandler, which implements the
generic information needed for interaction with a Netconf server.

"""

from ncclient.transport.parser import DefaultXMLParser


class DefaultDeviceHandler:
    """
    Default handler for device specific information.

    """
    # Define the exempt error messages (those that shouldn't cause an exception).
    # Wild cards are possible: Start and/or end with a '*' to indicate that the text
    # can appear at the start, the end or the middle of the error message to still
    # match. All comparisons are case insensitive.
    _EXEMPT_ERRORS = []

    _BASE_CAPABILITIES = [
            "urn:ietf:params:netconf:base:1.0",
            "urn:ietf:params:netconf:base:1.1",
            "urn:ietf:params:netconf:capability:writable-running:1.0",
            "urn:ietf:params:netconf:capability:candidate:1.0",
            "urn:ietf:params:netconf:capability:confirmed-commit:1.0",
            "urn:ietf:params:netconf:capability:rollback-on-error:1.0",
            "urn:ietf:params:netconf:capability:startup:1.0",
            "urn:ietf:params:netconf:capability:url:1.0?scheme=http,ftp,file,https,sftp",
            "urn:ietf:params:netconf:capability:validate:1.0",
            "urn:ietf:params:netconf:capability:xpath:1.0",
            "urn:ietf:params:netconf:capability:notification:1.0",
            "urn:ietf:params:netconf:capability:interleave:1.0",
            "urn:ietf:params:netconf:capability:with-defaults:1.0"
    ]

    def __init__(self, device_params=None, ignore_errors=None):
        self.device_params = device_params
        self.capabilities = []
        self._EXEMPT_ERRORS = ignore_errors or self._EXEMPT_ERRORS
        # Turn all exempt errors into lower case, since we don't want those comparisons
        # to be case sensitive later on. Sort them into exact match, wildcard start,
        # wildcard end, and full wildcard categories, depending on whether they start
        # and/or end with a '*'.
        self._exempt_errors_exact_match = []
        self._exempt_errors_startwith_wildcard_match = []
        self._exempt_errors_endwith_wildcard_match = []
        self._exempt_errors_full_wildcard_match = []
        for i in range(len(self._EXEMPT_ERRORS)):
            e = self._EXEMPT_ERRORS[i].lower()
            if e.startswith("*"):
                if e.endswith("*"):
                    self._exempt_errors_full_wildcard_match.append(e[1:-1])
                else:
                    self._exempt_errors_startwith_wildcard_match.append(e[1:])
            elif e.endswith("*"):
                self._exempt_errors_endwith_wildcard_match.append(e[:-1])
            else:
                self._exempt_errors_exact_match.append(e)


    def add_additional_ssh_connect_params(self, kwargs):
        """
        Add device specific parameters for the SSH connect.

        Pass in the keyword-argument dictionary for the SSH connect call. The
        dictionary will be modified (!) with the additional device-specific parameters.

        """
        pass

    def add_additional_netconf_params(self, kwargs):
        """Add additional NETCONF parameters

        Accept a keyword-argument dictionary to add additional NETCONF
        parameters that may be in addition to those specified by the
        default and device specific handlers.

        Currently, only additional client specified capabilities are
        supported and will be appended to default and device specific
        capabilities.

        Args:
            kwargs: A dictionary of specific NETCONF parameters to
                apply in addition to those derived by default and
                device specific handlers.
        """
        self.capabilities = kwargs.pop("capabilities", [])

    def get_capabilities(self):
        """
        Return the capability list.

        A list of URI's representing the client's capabilities. This is used during
        the initial capability exchange. Modify (in a new device-handler subclass)
        as needed.

        """
        return self._BASE_CAPABILITIES + self.capabilities

    def get_xml_base_namespace_dict(self):
        """
        A dictionary containing the base namespace.

        For lxml's nsmap, the base namespace should have a 'None' key.

            {
                None: "... base namespace... "
            }

        If no base namespace is needed, an empty dictionary should be
        returned.

        """
        return {}

    def get_xml_extra_prefix_kwargs(self):
        """
        Return any extra prefix that should be sent with each RPC request.

        Since these are used as kwargs, the function should return
        either an empty dictionary if there are no additional arguments,
        or a dictionary with keyword parameters suitable fo the Element()
        function. Mostly, this is the "nsmap" argument.

            {
                "nsmap" : {
                    ... namespace definitions ...
                }
            }

        """
        return {}

    def get_ssh_subsystem_names(self):
        """
        Return a list of names to try for the SSH subsystems.

        This always returns a list, even if only a single subsystem name is used.

        If the returned list contains multiple names then the various subsystems are
        tried in order, until one of them can successfully connect.

        """
        return [ "netconf" ]

    def is_rpc_error_exempt(self, error_text):
        """
        Check whether an RPC error message is excempt, thus NOT causing an exception.

        On some devices the RPC operations may indicate an error response, even though
        the operation actually succeeded. This may be in cases where a warning would be
        more appropriate. In that case, the client may be better advised to simply
        ignore that error and not raise an exception.

        Note that there is also the "raise_mode", set on session and manager, which
        controls the exception-raising behaviour in case of returned errors. This error
        filter here is independent of that: No matter what the raise_mode says, if the
        error message matches one of the exempt errors returned here, an exception
        will not be raised.

        The exempt error messages are defined in the _EXEMPT_ERRORS field of the device
        handler object and can be overwritten by child classes.  Wild cards are
        possible: Start and/or end with a '*' to indicate that the text can appear at
        the start, the end or the middle of the error message to still match. All
        comparisons are case insensitive.

        Return True/False depending on found match.

        """
        if error_text is not None:
            error_text = error_text.lower().strip()
        else:
            error_text = 'no error given'

        # Compare the error text against all the exempt errors.
        for ex in self._exempt_errors_exact_match:
            if error_text == ex:
                return True

        for ex in self._exempt_errors_startwith_wildcard_match:
            if error_text.endswith(ex):
                return True

        for ex in self._exempt_errors_endwith_wildcard_match:
            if error_text.startswith(ex):
                return True

        for ex in self._exempt_errors_full_wildcard_match:
            if ex in error_text:
                return True

        return False


    def perform_qualify_check(self):
        """
        During RPC operations, we perform some initial sanity checks on the responses.

        This check will fail for some devices, in which case this function here should
        return False in order to skip the test.

        """
        return True


    def add_additional_operations(self):
        """
        Add device/vendor specific operations.

        """
        return {}


    def handle_raw_dispatch(self, raw):
        return False


    def handle_connection_exceptions(self, sshsession):
        return False

    def reply_parsing_error_transform(self, reply_cls):
        """
        Hook for working around bugs in replies from devices (the root element can be "fixed")

        :param reply_cls: the RPCReply class that is parsing the reply 'root' xml element

        :return: transform function for the 'root' xml element of the RPC reply in case the normal parsing fails
        """
        # No transformation by default
        return None

    def transform_reply(self):
        return False

    def transform_edit_config(self, node):
        """
        Hook for working around bugs in devices that cannot deal with
        standard config payloads for edits. This will be called
        in EditConfig.request just before the request is submitted,
        meaning it will get an XML tree rooted at edit-config.

        :param node: the XML tree for edit-config

        :return: either the original XML tree if no changes made or a modified XML tree
        """
        return node

    def get_xml_parser(self, session):
        """
        vendor can chose which parser to use for RPC reply response.
        Default being DOM

        :param session: ssh session object
        :return: default DOM parser
        """
        return DefaultXMLParser(session)
