# fmt: off
"""
This file serves as a database of different compatibility issues we've
encountered while working on the caldav library, and descriptions on
how the well-known servers behave.

TODO: it should probably be split with the "feature definitions",
"server implementation details" and "feature database logic" in three separate files.
"""
import copy

## NEW STYLE
## (we're gradually moving stuff from the good old
## "incompatibility_description" below over to
## "compatibility_features")

class FeatureSet:
    """Work in progress ... TODO: write a better class description.

    This class holds the description of different behaviour observed in
    a class constant.

    An object of this class describes the feature set of a server.

    TODO: use enums?  TODO: describe the different types  TODO: think more through the different types, consolidate?
      type -> "client-feature", "client-hints", "server-peculiarity", "tests-behaviour", "server-observation", "server-feature" (last is default)
      support -> "full" (default), "unsupported", "fragile", "quirk", "broken", "ungraceful"

    unsupported means that attempts to use the feature will be silently ignored (this may actually be the worst option, as it may cause data loss).  quirk means that the feature is suppored, but special handling needs to be done towards the server.  fragile means that it sometimes works and sometimes not - either it's arbitrary, or we didn't spend enough time doing research into the patterns.  My idea behind broken was that the server should do completely unexpected things.  Probably a lot of things classified as "unsupported" today should rather be classified as "broken".  Some AI-generated code is using"broken".  TODO: look through and clean up.  "ungraceful" means the server will throw some error (this may indeed be the most graceful, as the client may catch the error and handle it in the best possible way).

    types:
     * client-feature means the client is supposed to do special things (like, rate-limiting).  While the need for rate-limiting may be set by the server, it may not be possible to reliably establish it by probling the server, and the value may differ for different clients.
     * server-peculiarity - weird behaviour detected at the server side, behaviour that is too odd to be described as "missing support for a feature".  Example: there is some cache working, causing a delay from some object is sent to the server and until it can be retrieved.  The difference between an "unsupported server-feature" and a "server-peculiarity" may be a bit floating - like, arguably "instant updates" may be considered a feature.
     * tests-behaviour - configuration for the tests.  Like, it's OK to wipe everyhting from the test calendar, location of test calendar, rate-limiting that only should apply to test runs, etc.
     * server-observation - not features, but other facts found about the server
     * server-feature - some feature (preferably rooted with a pointer to some specific section of the RFC)
       * "support" -> "quirk" if we have a server-peculiarity where it's needed with special care to get the request through.
    """
    FEATURES = {
        "auto-connect": {
            ## Nothing here - everything is under auto-connect.url as for now.
            ## Other connection details - like what auth method to use - could also
            ## be under the auto-connect umbrella
            "type": "client-hints",
        },
        "auto-connect.url": {
            "description": "Instruction for how to access DAV.  I.e. `/remote.php/dav` - see also https://github.com/python-caldav/caldav/issues/463.  To be used in the get_davclient method if the URL only contains a domain",
            "type": "client-hints",
            "extra_keys": {
                "basepath": "The path to append to the domain",
                "domain": "Domain name may be given through the features - useful for well-known cloud solutions",
                "scheme": "The scheme to prepend to the domain.  Defaults to https",
                ## TODO: in the future, templates for the principal URL, calendar URLs etc may also be added.
            }
        },
        "get-current-user-principal": {
            "description": "Support for RFC5397, current principal extension.  Most CalDAV servers have this, but it is an extension to the DAV standard"},
        "get-current-user-principal.has-calendar": {
            "type": "server-observation",
            "description": "Principal has one or more calendars.  Some servers and providers comes with a pre-defined calendar for each user, for other servers a calendar has to be explicitly created (supported means there exists a calendar - it may be because the calendar was already provisioned together with the principal, or it may be because a calendar was created manually, the checks can't see the difference)"},
        "rate-limit": {
            "type": "client-feature",
            "description": "client (or test code) must not send requests too fast",
            "extra_keys": {
                "interval": "Rate limiting window, in seconds",
                "count": "Max number of requests to send within the interval",
            }},
        "search-cache": {
            "type": "server-peculiarity",
            "description": "The server delivers search results from a cache which is not immediately updated when an object is changed.  Hence recent changes may not be reflected in search results",
            "extra_keys": {
                "delay": "after this number of seconds, we may be reasonably sure that the search results are updated",
            }
        },
        "tests-cleanup-calendar": {
            "type": "tests-behaviour",
            "description": "Deleting a calendar does not delete the objects, or perhaps create/delete of calendars does not work at all.  For each test run, every calendar resource object should be deleted for every test run",
        },
        "create-calendar": {
            "description": "RFC4791 says that \"support for MKCALENDAR on the server is only RECOMMENDED and not REQUIRED because some calendar stores only support one calendar per user (or principal), and those are typically pre-created for each account\".  Hence a conformant server may opt to not support creating calendars, this is often seen for cloud services (some services allows extra calendars to be made, but not through the CalDAV protocol).  (RFC4791 also says that the server MAY support MKCOL in section 8.5.2.  I do read it as MKCOL may be used for creating calendars - which is weird, since section 8.5.2 is titled \"external attachments\".  We should consider testing this as well)",
        },
        "create-calendar.auto": {
            "default": { "support": "unsupported" },
            "description": "Accessing a calendar which does not exist automatically creates it",
        },
        "create-calendar.set-displayname": {
            "description": "It's possible to set the displayname on a calendar upon creation"
        },
        "delete-calendar": {
            "description": "RFC4791 says nothing about deletion of calendars, so the server implementation is free to choose weather this should be supported or not.  Section 3.2.3.2 in RFC 6638 says that if a calendar is deleted, all the calendarobjectresources on the calendar should also be deleted - but it's a bit unclear if this only applies to scheduling objects or not.  Some calendar servers moves the object to a trashcan rather than deleting it"
        },
        "delete-calendar.free-namespace": {
            "description": "The delete operations clears the namespace, so that another calendar with the same ID/name can be created"
        },
        "http": { },
        "http.multiplexing": {
            "description": "chulka/baikal:nginx is having Problems with using HTTP/2 with multiplexing, ref https://github.com/python-caldav/caldav/issues/564.  I haven't (yet) been able to reproduce this locally, so no check for this yet.  We'll define it as fragile in the radicale config as for now"
        },
        "save-load": {
            "description": "it's possible to save and load objects to the calendar"
        },
        "save-load.event": {"description": "it's possible to save and load events to the calendar"},
        "save-load.event.recurrences": {"description": "it's possible to save and load recurring events to the calendar - events with an RRULE property set, including recurrence sets"},
        "save-load.event.recurrences.count": {"description": "The server will receive and store a recurring event with a count set in the RRULE"},
        "save-load.todo": {"description": "it's possible to save and load tasks to the calendar"},
        "save-load.todo.recurrences": {"description": "it's possible to save and load recurring tasks to the calendar"},
        "save-load.todo.recurrences.count": {"description": "The server will receive and store a recurring task with a count set in the RRULE"},
        "save-load.todo.mixed-calendar": {"description": "The same calendar may contain both events and tasks (Zimbra only allows tasks to be placed on special task lists)"},
        "save-load.journal": {"description": "The server will even accept journals"},
        "save-load.event.timezone": {
            "description": "The server accepts events with non-UTC timezone information. When unsupported or broken, the server may reject events with timezone data (e.g., return 403 Forbidden). Related to GitHub issue https://github.com/python-caldav/caldav/issues/372."
        },
        "search": {
            "description": "calendar MUST support searching for objects using the REPORT method, as specified in RFC4791, section 7"
        },
        "search.comp-type-optional": {
            "description": "In all the search examples in the RFC, comptype is given during a search, the client specifies if it's event or tasks or journals that is wanted.  However, as I read the RFC this is not required.  If omitted, the server should deliver all objects.  Many servers will not return anything if the COMPTYPE filter is not set.  Other servers will return 404"
        },
        "search.comp-type": {
            "description": "Server correctly filters calendar-query results by component type. When 'broken', server may misclassify component types (e.g., returning TODOs when VEVENTs are requested). The library will perform client-side filtering to work around this issue",
            "default": {"support": "full"}
        },
        ## TODO - there is still quite a lot of search-related
        ## stuff that hasn't been moved from the old "quirk list"
        "search.time-range": {
            "description": "Search for time or date ranges should work.  This is specified in RFC4791, section 7.4 and section 9.9"},
        "search.time-range.accurate": {
            "description": "Time-range searches should only return events/todos that actually fall within the requested time range. Some servers incorrectly return recurring events whose recurrences fall outside (after) the search interval, or events with no recurrences in the requested time range at all. RFC4791 section 9.9 specifies that a VEVENT component overlaps a time range if the condition (start < search_end AND end > search_start) is true.",
            "links": ["https://datatracker.ietf.org/doc/html/rfc4791#section-9.9"],
        },
        "search.time-range.todo": {"description": "basic time range searches for tasks works"},
        "search.time-range.event": {"description": "basic time range searches for event works"},
        "search.time-range.journal": {"description": "basic time range searches for journal works"},
        "search.time-range.alarm": {"description": "Time range searches for alarms work. The server supports searching for events based on when their alarms trigger, as specified in RFC4791 section 9.9"},
        "search.is-not-defined": {
            "description": "Supports searching for objects where properties is-not-defined according to rfc4791 section 9.7.4"
        },
        "search.text": {
            "description": "Search for text attributes should work"
        },
        "search.text.case-sensitive": {
            "description": "In RFC4791, section-9.7.5, a text-match may pass a collation, and i;ascii-casemap MUST be the default, this is not checked (yet - TODO) by the caldav-server-checker project.  Section 7.5 describes that the servers also are REQUIRED to support i;octet.  The definitions of those collations are given in RFC4790, i;octet is a case-sensitive byte-by-byte comparition (fastest).  search.text.case-sensitive is supported if passing the i;octet collation to search causes the search to be case-sensitive."
        },
        "search.text.case-insensitive": {
            "description": "The i;ascii-casemap requires ascii-characters to be case-insensitive, while non-ascii characters are compared byte-by-byte (case-sensitive).  Proper unicode case-insensitive searches may be supported by the server, but it's not a requirement in the RFC.  As for now, we consider case-insensitive searches to be supported if the i;ascii-casemap collation does what it's supposed to do..  In the future we may consider adding a search.text.case-insensitive.unicode. (i;unicode-casemap is defined in RFC5051)"
        },
        "search.text.substring": {
            "description": "According to RFC4791 the search done should be a substring search.  The search.text.substring feature is set if the calendar server does this (as opposed to only return full matches).  Substring matches does not always make sense, but it's mandated by the RFC.  When a server does a substring match on some properties but an exact match on others, the support should be marked as fragile.  Except for categories, which are handled in search.text.category.substring"
        },
        "search.text.category": {
            "description": "Search for category should work.  This is not explicitly specified in RFC4791, but covered in section 9.7.5.  No examples targets categories explicitly, but there are some text match examples in section 7.8.6 and following sections"},
        "search.text.category.substring": {
            "description": "Substring search for category should work according to the RFC.  I.e., search for mil should match family,finance",
        },
        "search.text.by-uid": {
            "description": "The server supports searching for objects by UID property. When unsupported, calendar.object_by_uid(uid) will not work.  This may be removed in the feature - the checker-script is not checking the right thing (check TODO-comments), probably search by uid is no special case for any server implementations"
        },
        "search.recurrences": {
            "description": "Support for recurrences in search"
        },
        "search.recurrences.includes-implicit": {
            "description": "RFC 4791, section 7.4 says that the server MUST expand recurring components to determine whether any recurrence instances overlap the specified time range.  Considered supported i.e. if a search for 2005 yields a yearly event happening first time in 2004.",
            "links": ["https://datatracker.ietf.org/doc/html/rfc4791#section-7.4"],
        },
        "search.recurrences.includes-implicit.todo": {
            "description": "tasks can also be recurring"
        },
        "search.recurrences.includes-implicit.todo.pending": {
            "description": "a future recurrence of a pending task should always be pending and appear in searches for pending tasks"
        },
        "search.recurrences.includes-implicit.event": {
            "description": "support for events"
        },
        "search.recurrences.includes-implicit.infinite-scope": {
            "description": "Needless to say, search on any future date range, no matter how far out in the future, should yield the recurring object"
        },
        "search.combined-is-logical-and": {
            "description": "Multiple search filters should yield only those that passes all filters"
            ## For "unsupported", we could also add a "behaviour" (returns everything, returns nothing, returns logical OR, etc).
        },
        "search.recurrences.expanded": {
            "description": "According to RFC 4791, the server MUST expand recurrence objects if asked for it - but many server doesn't do that.  Some servers don't do expand at all, others deliver broken data, typically missing RECURRENCE-ID.  The python caldav client library (from 2.0) does the expand-operation client-side no matter if it's supported or not",
            "links": ["https://datatracker.ietf.org/doc/html/rfc4791#section-9.6.5"],
        },
        "search.recurrences.expanded.todo": {
            "description": "expanding tasks"
        },
        "search.recurrences.expanded.event": {
            "description": "exanding events"
        },
        "search.recurrences.expanded.exception": {
            "description": "Server expand should work correctly also if a recurrence set with exceptions is given"
        },
        "sync-token": {
            "description": "RFC6578 sync-collection reports are supported. Server provides sync tokens that can be used to efficiently retrieve only changed objects since last sync. Support can be 'full', 'fragile' (occasionally returns more content than expected), or 'unsupported'. Behaviour 'time-based' indicates second-precision tokens requiring sleep(1) between operations"
        },
        "sync-token.delete": {
            "description": "Server correctly handles sync-collection reports after objects have been deleted from the calendar (solved in Nextcloud in https://github.com/nextcloud/server/pull/44130)"
        },
        'freebusy-query': {'description': "freebusy queries come in two flavors, one query can be done towards a CalDAV server as defined in RFC4791, another query can be done through the scheduling framework, RFC 6638.  Only RFC4791 is tested for as today"},
        "freebusy-query.rfc4791": {
            "description": "Server supports free/busy-query REPORT as specified in RFC4791 section 7.10. The REPORT allows clients to query for free/busy time information for a time range. Servers without this support will typically return an error (often 500 Internal Server Error or 501 Not Implemented). Note: RFC6638 defines a different freebusy mechanism for scheduling",
            "links": ["https://datatracker.ietf.org/doc/html/rfc4791#section-7.10"],
        },
        "principal-search": {
            "description": "Server supports searching for principals (CalDAV users). Principal search may be restricted for privacy/security reasons on many servers.  (not to be confused with get-current-user-principal)"
        },
        "principal-search.by-name": {
            "description": "Server supports searching for principals by display name. Testing this properly requires setting up another user with a known name, so this check is not yet implemented"
        },
        "principal-search.by-name.self": {
            "description": "Server allows searching for own principal by display name. Some servers block this for privacy reasons even when general principal search works"
        },
        "principal-search.list-all": {
            "description": "Server allows listing all principals without a name filter. Often blocked for privacy/security reasons"
        },
        "save": {},
        "save.duplicate-uid": {},
        "save.duplicate-uid.cross-calendar": {
            "description": "Server allows events with the same UID to exist in different calendars and treats them as separate entities. Support can be 'full' (allowed), 'ungraceful' (rejected with error), or 'unsupported' (silently ignored or moved). Behaviour 'silently-ignored' means the duplicate is not saved but no error is thrown. Behaviour 'moved-instead-of-copied' means the event is moved from the original calendar to the new calendar (Zimbra behavior)"
        },
        ## TODO: as for now, the tests will run towards the first calendar it will find, and most of the tests will assume the calendar is empty.  This is bad.
        "test-calendar": {
            "type": "tests-behaviour",
            "description": "if the server does not allow creating new calendars, then use the calendar with the given name for running tests (NOT SUPPORTED YET!), wipe the calendar between each test run (alternative for calendars not supporting the creation of new calendars is a very expensive delete objects one-by-one by uid)",
            "extra_keys": { "name": "calendar name", "cleanup-regime": "thorough|pre|post|light|wipe-calendar" }
        },
        "test-calendar.compatibility-tests": {
            "type": "tests-behaviour",
            "description": "if the server does not allow creating new calendars, then use the calendar with the given name for running the compatibility tests",
            "extra_keys": { "name": "calendar name", "cleanup": "Set to True to clean up the calendar after compatibility run" } ## if needed, pad up with cal_id, url, etc
        } ## if needed we may pad up with test-calendar.compatibility-tests.events, etc, etc
    }

    def __init__(self, feature_set_dict=None):
        """
        TODO: describe the feature_set better.

        Should be a dict on the same style as self.FEATURES, but different.

        Shortcuts accepted in the dict, like:

        {
            "recurrences.search-includes-implicit-recurrences.infinite-scope":
                "unsupported" }

        is equivalent with

        {
           "recurrences": {
               "features": {
                   "search-includes-inplicit-recurrences": {
                       "infinite-scope":
                           "support": "unsupported" }}}}

        (TODO: is this sane?  Am I reinventing a configuration language?)
        """
        if isinstance(feature_set_dict, FeatureSet):
            self._server_features = copy.deepcopy(feature_set_dict._server_features)

        ## TODO: copy the FEATURES dict, or just the feature_set dict?
        ## (anyways, that is an internal design decision that may be
        ## changed ... but we need test code in place)
        self.backward_compatibility_mode = feature_set_dict is None
        self._server_features = {}
        ## TODO: remove this when it can be removed
        self._old_flags = []
        if feature_set_dict:
            self.copyFeatureSet(feature_set_dict, collapse=False)


    def set_feature(self, feature, value=True):
        if isinstance(value, dict):
            fc = {feature: value}
        elif isinstance(value, str):
            fc = {feature: {"support": value}}
        elif value is True:
            fc = {feature: {"support": "full"}}
        elif value is False:
            fc = {feature: {"support": "unsupported"}}
        elif value is None:
            fc = {feature: {"support": "unknown"}}
        else:
            assert False
        self.copyFeatureSet(fc, collapse=False)
        feat_def = self.find_feature(feature)
        feat_type = feat_def.get('type', 'server-feature')
        sup = fc[feature].get('support', feat_def.get('default', 'full'))


    ## TODO: Why is this camelCase while every other method is with under_score?  rename ...
    def copyFeatureSet(self, feature_set, collapse=True):
        for feature in feature_set:
            ## TODO: temp - should be removed
            if feature == 'old_flags':
                self._old_flags = feature_set[feature]
                continue
            feature_info = self.find_feature(feature)
            value = feature_set[feature]
            if not feature in self._server_features:
                self._server_features[feature] = {}
            server_node = self._server_features[feature]
            if isinstance(value, bool):
                server_node['support'] = "full" if value else "unsupported"
            elif isinstance(value, str) and not 'support' in server_node:
                server_node['support'] = value
            elif isinstance(value, dict):
                server_node.update(value)
            else:
                assert False
        if collapse:
            self.collapse()

    def _collapse_key(self, feature_dict):
        """
        Extract the key part of a feature dictionary for comparison during collapse.

        For collapse purposes, we compare the 'support' level (or 'enable', 'behaviour', 'observed')
        but ignore differences in detailed behaviour messages, as those are often implementation-specific
        error messages that shouldn't prevent collapsing.
        """
        if not isinstance(feature_dict, dict):
            return feature_dict

        # Return a tuple of the main status fields, ignoring detailed messages
        return (
            feature_dict.get('support'),
            feature_dict.get('enable'),
            feature_dict.get('observed'),
        )

    def collapse(self):
        """
        If all subfeatures are the same, it should be collapsed into the parent

        Messy and complex logic :-(
        """
        features = list(self._server_features.keys())
        parents = set()
        for feature in features:
            if '.' in feature:
                parents.add(feature[:feature.rfind('.')])
        parents = list(parents)
        ## Parents needs to be ordered by the number of dots.  We proceed those with most dots first.
        parents.sort(key = lambda x: (-x.count('.'), x))
        for parent in parents:
            parent_info = self.find_feature(parent)

            if len(parent_info['subfeatures']):
                foo = self.is_supported(parent, return_type=dict, return_defaults=False)
                if len(parent_info['subfeatures']) > 1 or foo is not None:
                    dont_collapse = False
                    foo_key = self._collapse_key(foo) if foo is not None else None
                    for sub in parent_info['subfeatures']:
                        bar = self._server_features.get(f"{parent}.{sub}")
                        if bar is None:
                            dont_collapse = True
                            break
                        bar_key = self._collapse_key(bar)
                        if foo is None:
                            foo = bar
                            foo_key = bar_key
                        elif bar_key != foo_key:
                            dont_collapse = True
                            break
                    if not dont_collapse:
                        if not parent in self._server_features:
                            self._server_features[parent] = {}
                        for sub in parent_info['subfeatures']:
                            self._server_features.pop(f"{parent}.{sub}")
                        self.copyFeatureSet({parent: foo})

    def _default(self, feature_info):
        if isinstance(feature_info, str):
            feature_info = self.find_feature(feature_info)
        if 'default' in feature_info:
            return feature_info['default']
        feature_type = feature_info.get('type', 'server-feature')
        ## TODO: move the default values up to some constant dict probably, like self.DEFAULTS = { "server-feature": {...}}
        if feature_type == 'server-feature':
            return { "support": "full" }
        elif feature_type == 'client-feature':
            return { "enable": False }
        elif feature_type == 'server-peculiarity':
            return { "behaviour": "normal" }
        elif feature_type == 'server-observation':
            return { "observed": True }
        elif feature_type in ('tests-behaviour', 'client-hints'):
            return { }
        else:
            breakpoint()

    def is_supported(self, feature, return_type=bool, return_defaults=True, accept_fragile=False):
        """Work in progress

        TODO: write a better docstring

        The dotted features is essentially a tree.  If feature foo
        is unsupported it basically means that feature foo.bar is also
        unsupported.  Hence the extra logic visiting "nodes".
        """
        feature_info = self.find_feature(feature)
        feature_ = feature
        while True:
            if feature_ in self._server_features:
                return self._convert_node(self._server_features[feature_], feature_info, return_type, accept_fragile)
            if not '.' in feature_:
                if not return_defaults:
                    return None
                return self._convert_node(self._default(feature_info), feature_info, return_type, accept_fragile)
            feature_ = feature_[:feature_.rfind('.')]

    def _convert_node(self, node, feature_info, return_type, accept_fragile=False):
        """
        Return the information in a "node" given the wished return_type

        (The dotted feature format was an afterthought, the first
        iteration of this code the feature tree was actually a
        hierarchical dict, hence the naming of the method.  I
        considered it too complicated though)
        """
        if return_type == str:
            ## TODO: consider feature_info['type'], be smarter about it
            return node.get('support', node.get('enable', node.get('behaviour')))
        elif return_type == dict:
            return node
        elif return_type == bool:
            ## TODO: consider feature_info['type'], be smarter about this
            support = node.get('support', 'full')
            if support == 'quirk':
                return True
            if accept_fragile and support == 'fragile':
                support = 'full'
            if feature_info.get('type', 'server-feature') == 'server-feature':
                return support == 'full'
            else:
                ## TODO: this may be improved
                return not node.get('enable') and not node.get('behaviour') and not node.get('observed')
        else:
            assert False

    @classmethod
    def find_feature(cls, feature: str) -> dict:
        """
        Feature should be a string like feature.subfeature.subsubfeature.

        Looks through the FEATURES list and returns the relevant section.

        Will raise an Error if feature is not found

        (this is very simple now - used to be a hierarchy dict to be traversed)
        """
        assert feature in cls.FEATURES ## A feature in the configured feature-list does not exist.  TODO ... raise a better exception?
        if not 'name' in cls.FEATURES[feature]:
            cls.FEATURES[feature]['name'] = feature
        if '.' in feature and not 'parent' in cls.FEATURES[feature]:
            cls.FEATURES[feature]['parent'] = cls.find_feature(feature[:feature.rfind('.')])
        if not 'subfeatures' in cls.FEATURES[feature]:
            tree = cls.feature_tree()
            for x in feature.split('.'):
                tree = tree[x]
            cls.FEATURES[feature]['subfeatures'] = tree
        return cls.FEATURES[feature]

    @classmethod
    def _dots_to_tree(cls, target, source):
        for feat in source:
            node = target
            path = feat.split('.')
            for part in path:
                if not part in node:
                    node[part] = {}
                node = node[part]
        return target

    @classmethod
    def feature_tree(cls) -> dict:
        """TODO: is this in use at all?  Can it be deprecated already?

        TODO: the description may be outdated as I decided to refactor
        things from "overly complex" to "just sufficiently complex".
        Or maybe it's still a bit too complex.

        A "path" may have several "subpaths" in self.FEATURES
        (i.e. feat.subfeat.A, feat.subfeat.B, feat.subfeat.C)

        This method will return `{'feat': { 'subfeat': {'A': {}, ...}}}`
        making it possible to traverse the feature tree

        """
        ## I'm an old fart, grown up in an age where CPU-cycles was considered
        ## expensive ... so I always cache things when possible ...
        if hasattr(cls, '_feature_tree'):
            return cls._feature_tree
        cls._feature_tree = {}
        cls._dots_to_tree(cls._feature_tree, cls.FEATURES)
        return cls._feature_tree

    def dotted_feature_set_list(self, compact=False):
        ret = {}
        if compact:
            self.collapse()
        for x in self._server_features:
            feature = self._server_features[x]
            if compact and feature == self._default(x):
                continue
            ret[x] = feature.copy()
        return ret

#### OLD STYLE

## THE LIST BELOW IS TO BE REMOVED COMPLETELY.  DO NOT USE IT.

## It's not considered to be part of the public API (though, it should
## have been prefixed with _ to make it clear).  The list is being
## removed little-by-little, without regards of SemVer.

## The lists below are specifying what tests should be skipped or
## modified to accept non-conforming resultsets from the different
## calendar servers.  In addition there are some hacks in the library
## code itself to work around some known compatibility issues, like
## the caldav.lib.vcal.fix function.
## Here is a list of all observed (in)compatibility issues the test framework needs to know about
## TODO:
## * references to the relevant parts of the RFC would be nice.
## * Research should be done to triple-check that the issue is on the server side, and not on the client side
## * Some of the things below should be possible to probe the server for.
## * Perhaps some more readable format should be considered (yaml?).
## * Consider how to get this into the documentation
incompatibility_description = {
    'no_current-user-principal':
        """Current user principal not supported by the server (flag is ignored by the tests as for now - pass the principal URL as the testing URL and it will work, albeit with one warning""",

    'no_scheduling':
        """RFC6833 is not supported""",

    'no_scheduling_mailbox':
        """Parts of RFC6833 is supported, but not the existence of inbox/mailbox""",

    'no_scheduling_calendar_user_address_set':
        """Parts of RFC6833 is supported, but not getting the calendar users addresses""",

    'no_default_calendar':
        """The given user starts without an assigned default calendar """
        """(or without pre-defined calendars at all)""",

    'no_freebusy_rfc6638':
        """Server does not support a freebusy-request as per RFC6638""",

    'calendar_order':
        """Server supports (nonstandard) calendar ordering property""",

    'calendar_color':
        """Server supports (nonstandard) calendar color property""",

    'duplicates_not_allowed':
        """Duplication of an event in the same calendar not allowed """
        """(even with different uid)""",


    'event_by_url_is_broken':
        """A GET towards a valid calendar object resource URL will yield 404 (wtf?)""",

    'no_delete_event':
        """Zimbra does not support deleting an event, probably because event_by_url is broken""",


    'propfind_allprop_failure':
        """The propfind test fails ... """
        """it asserts DAV:allprop response contains the text 'resourcetype', """
        """possibly this assert is wrong""",

    'vtodo_datesearch_nodtstart_task_is_skipped':
        """date searches for todo-items will not find tasks without a dtstart""",

    'vtodo_datesearch_nodtstart_task_is_skipped_in_closed_date_range':
        """only open-ended date searches for todo-items will find tasks without a dtstart""",

    'vtodo_datesearch_notime_task_is_skipped':
        """date searches for todo-items will (only) find tasks that has either """
        """a dtstart or due set""",

    'vtodo_datesearch_nostart_future_tasks_delivered':
        """Future tasks are yielded when doing a date search with some end timestamp and without start timestamp and the task contains both dtstart and due, but not duration (xandikos 0.2.12)""",

    'vtodo_no_due_infinite_duration':
        """date search will find todo-items without due if dtstart is """
        """before the date search interval.  This is in breach of rfc4791"""
        """section 9.9""",

    'vtodo_no_dtstart_infinite_duration':
        """date search will find todo-items without dtstart if due is """
        """after the date search interval.  This is in breach of rfc4791"""
        """section 9.9""",

    'vtodo_no_dtstart_search_weirdness':
       """Zimbra is weird""",

    'vtodo_no_duration_search_weirdness':
       """Zimbra is weird""",

    'vtodo_with_due_weirdness':
       """Zimbra is weird""",

    'vtodo-cannot-be-uncompleted':
        """If a VTODO object has been set with STATUS:COMPLETE, it's not possible to delete the COMPLTEDED attribute and change back to STATUS:IN-ACTION""",

    'unique_calendar_ids':
        """For every test, generate a new and unique calendar id""",

    'sticky_events':
        """Events should be deleted before the calendar is deleted, """
        """and/or deleting a calendar may not have immediate effect""",

    'no_overwrite':
        """events cannot be edited""",

    'dav_not_supported':
        """when asked, the server may claim it doesn't support the DAV protocol.  Observed by one baikal server, should be investigated more (TODO) and robur""",

    'text_search_is_case_insensitive':
        """Probably not supporting the collation used by the caldav library""",

    'date_search_ignores_duration':
        """Date search with search interval overlapping event interval works on events with dtstart and dtend, but not on events with dtstart and due""",

    'date_todo_search_ignores_duration':
        """Same as above, but specifically for tasks""",

   'fastmail_buggy_noexpand_date_search':
        """The 'blissful anniversary' recurrent example event is returned when asked for a no-expand date search for some timestamps covering a completely different date""",

    'non_existing_raises_other':
        """Robur raises AuthorizationError when trying to access a non-existing resource (while 404 is expected).  Probably so one shouldn't probe a public name space?""",

    'no_supported_components_support':
        """The supported components prop query does not work""",

    'no_relships':
        """The calendar server does not support child/parent relationships between calendar components""",

    'robur_rrule_freq_yearly_expands_monthly':
        """Robur expands a yearly event into a monthly event.  I believe I've reported this one upstream at some point, but can't find back to it""",

    'no_search_openended':
        """An open-ended search will not work""",

}

## This is for Xandikos 0.2.12.
## Lots of development going on as of summer 2025, so expect the list to become shorter soon!
xandikos_v0_2_12 = {
    ## this only applies for very simple installations
    "auto-connect.url": {"domain": "localhost", "scheme": "http", "basepath": "/"},
    'search.recurrences.includes-implicit': {'support': 'unsupported'},
    'search.recurrences.expanded': {'support': 'unsupported'},
    'search.time-range.todo': {'support': 'unsupported'},
    'search.time-range.alarm': {'support': 'ungraceful', 'behaviour': '500 internal server error'},
    'search.comp-type-optional': {'support': 'ungraceful'},
    "search.text.substring": {"support": "unsupported"},
    "search.text.category.substring": {"support": "unsupported"},
    'principal-search': {'support': 'unsupported'},
    'freebusy-query.rfc4791': {'support': 'ungraceful', 'behaviour': '500 internal server error'},
    "old_flags":  [
    ## https://github.com/jelmer/xandikos/issues/8
    'date_todo_search_ignores_duration',
    'vtodo_datesearch_nostart_future_tasks_delivered',

    ## scheduling is not supported
    "no_scheduling",

    ## The test with an rrule and an overridden event passes as
    ## long as it's with timestamps.  With dates, xandikos gets
    ## into troubles.  I've chosen to edit the test to use timestamp
    ## rather than date, just to have the test exercised ... but we
    ## should report this upstream
    #'broken_expand_on_exceptions',

    ]
}

xandikos_v0_3 = {
    ## this only applies for very simple installations
    "auto-connect.url": {"domain": "localhost", "scheme": "http", "basepath": "/"},
    'search.comp-type-optional': {'support': 'unsupported'},
    ## This suddenly disappeared.  Should probably look more into the checks ...
    #"search.recurrences.includes-implicit.todo.pending": {"support": "unsupported"},
    'search.recurrences.expanded.todo': {'support': 'unsupported'},
    'search.recurrences.expanded.exception': {'support': 'unsupported'},
    'principal-search': {'support': 'ungraceful'},
    'freebusy-query.rfc4791': {'support': 'ungraceful', 'behaviour': '500 internal server error'},
    "old_flags":  [
    ## https://github.com/jelmer/xandikos/issues/8
    'date_todo_search_ignores_duration',
    'vtodo_datesearch_nostart_future_tasks_delivered',

    ## scheduling is not supported
    "no_scheduling",

    ## The test with an rrule and an overridden event passes as
    ## long as it's with timestamps.  With dates, xandikos gets
    ## into troubles.  I've chosen to edit the test to use timestamp
    ## rather than date, just to have the test exercised ... but we
    ## should report this upstream
    #'broken_expand_on_exceptions',

    ]
}

xandikos=xandikos_v0_3

## This seems to work as of version 3.5.4 of Radicale.
## There is much development going on at Radicale as of summar 2025,
## so I'm expecting this list to shrink a lot soon.
radicale = {
    "search.text.case-sensitive":  {"support": "unsupported"},
    "search.is-not-defined": {"support": "fragile", "behaviour": "seems to work for categories but not for dtend"},
    "search.recurrences.includes-implicit.todo.pending": {"support": "unsupported"},
    "search.recurrences.expanded.todo": {"support": "unsupported"},
    "search.recurrences.expanded.exception": {"support": "unsupported"},
    'principal-search': {'support': 'unknown', 'behaviour': 'No display name available - cannot test'},
    'principal-search.list-all': {'support': 'unsupported'},
    ## this only applies for very simple installations
    "auto-connect.url": {"domain": "localhost", "scheme": "http", "basepath": "/"},
    ## freebusy is not supported yet, but on the long-term road map
    'old_flags': [
    ## calendar listings and calendar creation works a bit
    ## "weird" on radicale

    'no_scheduling',
    'no_search_openended',

    #'text_search_is_exact_match_sometimes',

    ## extra features not specified in RFC5545
    "calendar_order",
    "calendar_color"
    ]
}

## Be aware that nextcloud by default have different rate limits, including how often a user is allowed to create a new calendar.  This may break test runs badly.
nextcloud = {
    'auto-connect.url': {
        'basepath': '/remote.php/dav',
    },
    'search.combined-is-logical-and': {'support': 'unsupported'},
    'search.comp-type-optional': {'support': 'ungraceful'},
    'search.recurrences.expanded.todo': {'support': 'unsupported'},
    'search.recurrences.expanded.exception': {'support': 'unsupported'}, ## TODO: verify
    'delete-calendar': {
        'support': 'fragile',
        'behaviour': 'Deleting a recently created calendar fails'},
    'delete-calendar.free-namespace': { ## TODO: not caught by server-tester
        'behaviour': "deleting a calendar moves it to a trashbin, thrashbin has to be manually 'emptied' from the web-ui before the namespace is freed up",
        'support': 'fragile',
    },
    'search.comp-type-optional': {
        'support': 'ungraceful',
    },
    "search.combined-is-logical-and": {"support": "unsupported"},
    'search.recurrences.includes-implicit.todo': {'support': 'unsupported'},
    #'save-load.todo.mixed-calendar': {'support': 'unsupported'}, ## Why?  It started complaining about this just recently.
    'principal-search.by-name': {'support': 'unsupported'},
    'principal-search.list-all': {'support': 'ungraceful'},
    'old_flags': ['unique_calendar_ids'],
}

## TODO: Latest - mismatch between config and test script in delete-calendar.free-namespace ... and create-calendar.set-displayname?
ecloud = nextcloud | {
    ## TODO: this applies only to test runs, not to ordinary usage
    'rate-limit': {
        'enable': True,
        'interval': 10,
        'count': 1,
        'description': "It's needed to manually empty trashbin frequently when running tests.  Since this oepration takes some time and/or there are some caches, it's needed to run tests slowly, even when hammering the 'empty thrashbin' frequently",
    },
    'auto-connect.url': {
        'basepath': '/remote.php/dav',
        'domain': 'ecloud.global',
        'scheme': 'https',
    },
}

## Zimbra is not very good at it's caldav support
zimbra = {
    'auto-connect.url': {'basepath': '/dav/'},
    'search.recurrences.expanded.exception': {'support': 'unsupported'}, ## TODO: verify
    'create-calendar.set-displayname': {'support': 'unsupported'},
    'save-load.todo.mixed-calendar': {'support': 'unsupported'},
    'save-load.todo.recurrences.count': {'support': 'unsupported'}, ## This is a new problem?
    'save-load.journal': "ungraceful",
    'search.is-not-defined': {'support': 'unsupported'},
    #'search.text': 'unsupported', ## weeeird ... it wasn't like this before
    'search.text.substring': {'support': 'unsupported'},
    'search.text.category': {'support': 'ungraceful'},
    'search.is-not-defined':  {'support': 'unsupported'},
    'search.recurrences.expanded.todo': { "support": "unsupported" },
    'search.comp-type-optional': {'support': 'fragile'}, ## TODO: more research on this, looks like a bug in the checker,
    'search.time-range.alarm': {'support': 'unsupported'},
    'sync-token': {'support': 'ungraceful'},
    'principal-search': "ungraceful",
    'save.duplicate-uid.cross-calendar': {'support': 'unsupported', "behaviour": "moved-instead-of-copied" },

    "old_flags": [
    ## apparently, zimbra has no journal support

    ## setting display name in zimbra does not work (display name,
    ## calendar-ID and URL is the same, the display name cannot be
    ## changed, it can only be given if no calendar-ID is given.  In
    ## earlier versions of Zimbra display-name could be changed, but
    ## then the calendar would not be available on the old URL
    ## anymore)
    'event_by_url_is_broken',
    'no_delete_event',
    'vtodo_datesearch_notime_task_is_skipped',
    'no_relships',

    ## TODO: I just discovered that when searching for a date some
    ## years after a recurring daily event was made, the event does
    ## not appear.

    ## extra features not specified in RFC5545
    "calendar_order",
    "calendar_color"
    ]
    ## TODO: there may be more, it should be organized and moved here.
    ## Search for 'zimbra' in the code repository!
}

bedework = {
    'search.comp-type': {'support': 'broken', 'behaviour': 'Server returns everything when searching for events and nothing when searching for todos'},
    #"search.combined-is-logical-and": { "support": "unsupported" },
    ## TODO: play with this and see if it's needed
    'search-cache': {'behaviour': 'delay', 'delay': 1.5},
    ## TODO: play with this and see if it's needed
    'old_flags': [
    'propfind_allprop_failure',
    'duplicates_not_allowed',
    ],
    'auto-connect.url': {'basepath': '/ucaldav/'},
    "save-load.journal": {
        "support": "ungraceful"
    },
    "search.time-range.alarm": {
        "support": "unsupported"
    },
    ## Huh?  Non-deterministic behaviour of the checking script?
    #"save.duplicate-uid.cross-calendar": {
    #    "support": "unsupported",
    #    "behaviour": "silently-ignored"
    #},
    "freebusy-query.rfc4791": {
        "support": "full"
    },
    "search.time-range.todo": {
        "support": "unsupported"
    },
    "search.text": {
        "support": "unsupported"
    },
    "search.is-not-defined": {
        "support": "fragile"
    },
    "search.text.by-uid": {
        "support": "fragile",
        "behaviour": "sometimes the text search delivers everything, other times it doesn't deliver anything.  When the text search delivers everything, then the post-filtering will save the day"
    },
    "search.time-range.accurate": {
        "support": "unsupported"
    },
    "search.recurrences.includes-implicit.todo": {
        "support": "unsupported"
    },
    "search.recurrences.includes-implicit.infinite-scope": {
        "support": "unsupported"
    },
    "sync-token": {
        "support": "fragile"
    },
    ## Check results are non-deterministic!?
    "search.recurrences.expanded.exception": {
        "support": "unsupported"
    },
    "search.recurrences.expanded.event": {
        "support": "unsupported"
    },
    ## It doesn't support expanding events, but it supports exapnding tasks!?
    ## Or maybe there is a problem in the checker script?
    ## TODO: look into this
    #"search.recurrences.expanded.todo": True,
    "principal-search": {
        "support": "ungraceful",
    }
}

synology = {
    'principal-search': False,
    'search.time-range.alarm': False,
    'sync-token': 'fragile',
    'delete-calendar': False,
    'search.comp-type-optional': 'fragile',
    "search.recurrences.expanded.exception": False,
     'old_flags': ['vtodo_datesearch_nodtstart_task_is_skipped'],
}

baikal =  { ## version 0.10.1
    "http.multiplexing": "fragile", ## ref https://github.com/python-caldav/caldav/issues/564
    "save-load.journal": {'support': 'ungraceful'},
    #'search.comp-type-optional': {'support': 'ungraceful'}, ## Possibly this has been fixed?
    'search.recurrences.expanded.todo': {'support': 'unsupported'},
    'search.recurrences.expanded.exception': {'support': 'unsupported'},
    'search.recurrences.includes-implicit.todo': {'support': 'unsupported'},
    "search.combined-is-logical-and": {"support": "unsupported"},
    'principal-search.by-name': {'support': 'unsupported'}, ## This is weird - I'm quite sure the tests were passing without this one some few days ago.
    'principal-search.list-all': {'support': 'ungraceful'}, ## This is weird - I'm quite sure the tests were passing without this one some few days ago.
    #'sync-token.delete': {'support': 'unsupported'}, ## Perhaps on some older servers?
    'old_flags': [
        ## extra features not specified in RFC5545
        "calendar_order",
        "calendar_color"
    ]
} ## TODO: testPrincipals, testWrongAuthType, testTodoDatesearch fails

## Some unknown version of baikal has this
baikal_old = baikal | {
    'create-calendar': {'support': 'quirk', 'behaviour': 'mkcol-required'},
    'create-calendar.auto': {'support': 'unsupported'}, ## this is the default, but the "quirk" from create-calendar overwrites it.  Hm.

}

cyrus = {
    "search.comp-type-optional": {"support": "ungraceful"},
    "search.recurrences.expanded.exception": {"support": "unsupported"},
    'search.time-range.alarm': {'support': 'unsupported'},
    'principal-search': {'support': 'ungraceful'},
    "test-calendar": {"cleanup-regime": "pre"},
    'delete-calendar': {
        'support': 'fragile',
        'behaviour': 'Deleting a recently created calendar fails'},
    'save.duplicate-uid.cross-calendar': {'support': 'ungraceful'},
    'old_flags': []
}

## See comments on https://github.com/python-caldav/caldav/issues/3
#icloud = [
#    'unique_calendar_ids',
#    'duplicate_in_other_calendar_with_same_uid_breaks',
#    'sticky_events',
#    'no_journal', ## it threw a 500 internal server error!
#    'no_todo',
#    "no_freebusy_rfc4791",
#    'no_recurring',
#    'propfind_allprop_failure',
#    'object_by_uid_is_broken'
#]

davical = {

    "search.comp-type-optional": { "support": "fragile" },
    "search.recurrences.expanded.todo": { "support": "unsupported" },
    "search.recurrences.expanded.exception": { "support": "unsupported" },
    'search.time-range.alarm': {'support': 'unsupported'},
    'sync-token': {'support': 'fragile'},
    'principal-search': {'support': 'unsupported'},
    'principal-search.list-all': {'support': 'unsupported'},
    "old_flags": [
        #'no_journal', ## it threw a 500 internal server error! ## for old versions
        #'nofreebusy', ## for old versions
        'fragile_sync_tokens', ## no issue raised yet
        'vtodo_datesearch_nodtstart_task_is_skipped', ## no issue raised yet
        'date_todo_search_ignores_duration',
        'calendar_color',
        'calendar_order',
        'vtodo_datesearch_notime_task_is_skipped',
    ]
}

sogo = {
    "save-load.journal": { "support": "ungraceful" },
    'freebusy-query.rfc4791': {'support': 'ungraceful'},
    "search.time-range.accurate": {
        "support": "unsupported",
        "description": "SOGo returns events/todos that fall outside the requested time range. For recurring events, it may return recurrences that start after the search interval ends, or events with no recurrences in the requested range at all."
    },
    "search.time-range.alarm": {
        "support": "unsupported"
    },
    "search.time-range.event": {
        "support": "unsupported"
    },
    "search.time-range.todo": {
        "support": "unsupported"
    },
    "search.text": {
        "support": "unsupported"
    },
    "search.text.by-uid": True,
    "search.is-not-defined": {
        "support": "unsupported"
    },
    "search.comp-type-optional": {
        "support": "unsupported"
    },
    "search.recurrences.includes-implicit.todo": {
        "support": "unsupported"
    },
    ## TODO: do some research into this, I think this is a bug in the checker script
    "search.recurrences.includes-implicit.todo.pending": {
        "support": "fragile"
    },
    "search.recurrences.includes-implicit.infinite-scope": {
        "support": "unsupported"
    },
    "sync-token": {
        "support": "fragile"
    },
    "search.recurrences.expanded": {
        "support": "unsupported"
    },
    "principal-search": {
        "support": "ungraceful",
        "behaviour": "Search by name failed: ReportError at '501 Not Implemented - <?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n<body><h3>An error occurred during object publishing</h3><p>did not find the specified REPORT</p></body>\n</html>\n', reason no reason",
    },

}
## Old notes for sogo (todo - incorporate them in the structure above)
## https://www.sogo.nu/bugs/view.php?id=3065
## left a note about time-based sync tokens on https://www.sogo.nu/bugs/view.php?id=5163
## https://www.sogo.nu/bugs/view.php?id=5282
## https://bugs.sogo.nu/view.php?id=5693
## https://bugs.sogo.nu/view.php?id=5694
#sogo = [ ## and in addition ... the requests are efficiently rate limited, as it spawns lots of postgresql connections all until it hits a limit, after that it's 501 errors ...
#    "time_based_sync_tokens",
#    "search_needs_comptype",
#    "fastmail_buggy_noexpand_date_search",
#    "text_search_not_working",
#    "isnotdefined_not_working",
#    'no_journal',
#    'no_freebusoy_rfc4791'
#]



#google = [
#    'no_mkcalendar',
#    'no_overwrite',
#    'no_todo',
#]

#fastmail = [
#    'duplicates_not_allowed',
#    'duplicate_in_other_calendar_with_same_uid_breaks',
#    'no_todo',
#    'sticky_events',
#    'fastmail_buggy_noexpand_date_search',
#    'combined_search_not_working',
#    'text_search_is_exact_match_sometimes',
#    'rrule_takes_no_count',
#    'isnotdefined_not_working',
#]

robur = {
    "auto-connect.url": {
        'domain': 'calendar.robur.coop',
        'basepath': '/principals/', # TODO: this seems fishy
    },
    "save-load.journal": { "support": "ungraceful" },
    "delete-calendar": { "support": "fragile" },
    "search.is-not-defined": { "support": "unsupported" },
    "search.time-range.todo": { "support": "unsupported" },
    "search.time-range.alarm": {'support': 'unsupported'},
    "search.text": { "support": "unsupported", "behaviour": "a text search ignores the filter and returns all elements" },
    "search.text.by-uid": { "support": "fragile", "behaviour": "Probably not supported, but my caldav-server-checker tool has issues with it at the moment" },
    "search.comp-type-optional": { "support": "ungraceful" },
    "search.recurrences.expanded.todo": { "support": "unsupported" },
    "search.recurrences.expanded.event": { "support": "fragile" },
    "search.recurrences.expanded.exception": { "support": "unsupported" },
    'search.recurrences.includes-implicit.todo': {'support': 'unsupported'},
    'principal-search': {'support': 'ungraceful'},
    'freebusy-query.rfc4791': {'support': 'ungraceful'},
    'old_flags': [
        'non_existing_raises_other', ## AuthorizationError instead of NotFoundError
        'no_scheduling',
        'no_supported_components_support',
        'no_relships',
        'unique_calendar_ids',
    ],
    "sync-token": False,
}

posteo = {
    'auto-connect.url': {
        'scheme': 'https',
        'domain': 'posteo.de:8443',
        'basepath': '/',
    },
    'create-calendar': {'support': 'unsupported'},
    'save-load.journal': { "support": "ungraceful" },
    ## TODO1: we should ignore cases where observations are unknown while configuration is known
    ## TODO2: there are more calendars available at the posteo account, so it should be possible to check this.
    "save.duplicate-uid.cross-calendar": { "support": "unknown" },
    'search.comp-type-optional': {'support': 'ungraceful'},
    'search.recurrences.expanded.todo': {'support': 'unsupported'},
    'search.recurrences.expanded.exception': {'support': 'unsupported'},
    'search.recurrences.includes-implicit.todo': {'support': 'unsupported'},
    "search.combined-is-logical-and": {"support": "unsupported"},
    'sync-token': {'support': 'ungraceful'},
    'principal-search': {'support': 'unsupported'},
    'old_flags': [
        'no_scheduling',
        #'no_recurring_todo', ## todo
    ]
}

#calendar_mail_ru = [
#    'no_mkcalendar', ## weird.  It was working in early June 2024, then it stopped working in mid-June 2024.
#    'no_current-user-principal',
#    'no_todo',
#    'no_journal',
#    'search_always_needs_comptype',
#    'no_sync_token', ## don't know if sync tokens are supported or not - the sync-token-code needs some workarounds ref https://github.com/python-caldav/caldav/issues/401
#    'text_search_not_working',
#    'isnotdefined_not_working',
#    'no_scheduling_mailbox',
#    'no_freebusy_rfc4791',
#    'no_relships', ## mail.ru recreates the icalendar content, and strips everything it doesn't know anyhting about, including relationship info
#]

purelymail = {
    ## Purelymail claims that the search indexes are "lazily" populated,
    ## so search works some minutes after the event was created/edited.
    'search-cache': {'behaviour': 'delay', 'delay': 160},
    "create-calendar.auto": {"support": "full"},
    'search.time-range.alarm': {'support': 'unsupported'},
    'principal-search': {'support': 'unsupported'},
    'auto-connect.url': {
        'basepath': '/webdav/',
        'domain': 'purelymail.com',
    },
    'old_flags': [
        ## Known, work in progress
        'no_scheduling',

        ## Known, not a breach of standard
        'no_supported_components_support',

        ## I haven't raised this one with them yet
    ]
}

gmx = {
    'auto-connect.url': {
        'scheme': 'https',
        'domain': 'caldav.gmx.net',
        ## This won't work yet.  I'm not able to connect with gmx at all now,
        ## so unable to create a verified fix for it now
        'basepath': '/begenda/dav/{username}/calendar', ## TODO: foobar
    },
    'create-calendar': {'support': 'unsupported'},
    'search.comp-type-optional': {'support': 'fragile', 'description': 'unexpected results from date-search without comp-type - but only sometimes - TODO: research more'},
    'search.recurrences.expanded': {'support': 'unsupported'},
    'search.time-range.alarm': {'support': 'unsupported'},
    'sync-token': {'support': 'unsupported'},
    'principal-search': {'support': 'unsupported'},
    'freebusy-query.rfc4791': {'support': 'unsupported'},
    "old_flags":  [
        "no_scheduling_mailbox",
        #"text_search_is_case_insensitive",
        "no_search_openended",
        "no_scheduling_calendar_user_address_set",
        "vtodo-cannot-be-uncompleted",
    ]
}

# fmt: on
