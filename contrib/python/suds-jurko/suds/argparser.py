# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jurko Gospodnetić jurko.gospodnetic@pke.hr )

"""
Suds web service operation invocation function argument parser.

See the parse_args() function description for more detailed information.

"""

__all__ = ["parse_args"]


def parse_args(method_name, param_defs, args, kwargs, external_param_processor,
    extra_parameter_errors):
    """
    Parse arguments for suds web service operation invocation functions.

    Suds prepares Python function objects for invoking web service operations.
    This function implements generic binding agnostic part of processing the
    arguments passed when calling those function objects.

    Argument parsing rules:
      * Each input parameter element should be represented by single regular
        Python function argument.
      * At most one input parameter belonging to a single choice parameter
        structure may have its value specified as something other than None.
      * Positional arguments are mapped to choice group input parameters the
        same as is done for a simple all/sequence group - each in turn.

    Expects to be passed the web service operation's parameter definitions
    (parameter name, type & optional ancestry information) in order and, based
    on that, extracts the values for those parameter from the arguments
    provided in the web service operation invocation call.

    Ancestry information describes parameters constructed based on suds
    library's automatic input parameter structure unwrapping. It is expected to
    include the parameter's XSD schema 'ancestry' context, i.e. a list of all
    the parent XSD schema tags containing the parameter's <element> tag. Such
    ancestry context provides detailed information about how the parameter's
    value is expected to be used, especially in relation to other input
    parameters, e.g. at most one parameter value may be specified for
    parameters directly belonging to the same choice input group.

    Rules on acceptable ancestry items:
      * Ancestry item's choice() method must return whether the item
        represents a <choice> XSD schema tag.
      * Passed ancestry items are used 'by address' internally and the same XSD
        schema tag is expected to be identified by the exact same ancestry item
        object during the whole argument processing.

    During processing, each parameter's definition and value, together with any
    additional pertinent information collected from the encountered parameter
    definition structure, is passed on to the provided external parameter
    processor function. There that information is expected to be used to
    construct the actual binding specific web service operation invocation
    request.

    Raises a TypeError exception in case any argument related errors are
    detected. The exceptions raised have been constructed to make them as
    similar as possible to their respective exceptions raised during regular
    Python function argument checking.

    Does not support multiple same-named input parameters.

    """
    arg_parser = _ArgParser(method_name, param_defs, external_param_processor)
    return arg_parser(args, kwargs, extra_parameter_errors)


class _ArgParser:
    """Internal argument parser implementation function object."""

    def __init__(self, method_name, param_defs, external_param_processor):
        self.__method_name = method_name
        self.__param_defs = param_defs
        self.__external_param_processor = external_param_processor
        self.__stack = []

    def __call__(self, args, kwargs, extra_parameter_errors):
        """
        Runs the main argument parsing operation.

        Passed args & kwargs objects are not modified during parsing.

        Returns an informative 2-tuple containing the number of required &
        allowed arguments.

        """
        assert not self.active(), "recursive argument parsing not allowed"
        self.__init_run(args, kwargs, extra_parameter_errors)
        try:
            self.__process_parameters()
            return self.__all_parameters_processed()
        finally:
            self.__cleanup_run()
            assert not self.active()

    def active(self):
        """
        Return whether this object is currently running argument processing.

        Used to avoid recursively entering argument processing from within an
        external parameter processor.

        """
        return bool(self.__stack)

    def __all_parameters_processed(self):
        """
        Finish the argument processing.

        Should be called after all the web service operation's parameters have
        been successfully processed and, afterwards, no further parameter
        processing is allowed.

        Returns a 2-tuple containing the number of required & allowed
        arguments.

        See the _ArgParser class description for more detailed information.

        """
        assert self.active()
        sentinel_frame = self.__stack[0]
        self.__pop_frames_above(sentinel_frame)
        assert len(self.__stack) == 1
        self.__pop_top_frame()
        assert not self.active()
        args_required = sentinel_frame.args_required()
        args_allowed = sentinel_frame.args_allowed()
        self.__check_for_extra_arguments(args_required, args_allowed)
        return args_required, args_allowed

    def __check_for_extra_arguments(self, args_required, args_allowed):
        """
        Report an error in case any extra arguments are detected.

        Does nothing if reporting extra arguments as exceptions has not been
        enabled.

        May only be called after the argument processing has been completed.

        """
        assert not self.active()
        if not self.__extra_parameter_errors:
            return

        if self.__kwargs:
            param_name = list(self.__kwargs.keys())[0]
            if param_name in self.__params_with_arguments:
                msg = "got multiple values for parameter '%s'"
            else:
                msg = "got an unexpected keyword argument '%s'"
            self.__error(msg % (param_name,))

        if self.__args:
            def plural_suffix(count):
                if count == 1:
                    return ""
                return "s"
            def plural_was_were(count):
                if count == 1:
                    return "was"
                return "were"
            expected = args_required
            if args_required != args_allowed:
                expected = "%d to %d" % (args_required, args_allowed)
            given = self.__args_count
            msg_parts = ["takes %s positional argument" % (expected,),
                plural_suffix(expected), " but %d " % (given,),
                plural_was_were(given), " given"]
            self.__error("".join(msg_parts))

    def __cleanup_run(self):
        """Cleans up after a completed argument parsing run."""
        self.__stack = []
        assert not self.active()

    def __error(self, message):
        """Report an argument processing error."""
        raise TypeError("%s() %s" % (self.__method_name, message))

    def __frame_factory(self, ancestry_item):
        """Construct a new frame representing the given ancestry item."""
        frame_class = Frame
        if ancestry_item is not None and ancestry_item.choice():
            frame_class = ChoiceFrame
        return frame_class(ancestry_item, self.__error,
            self.__extra_parameter_errors)

    def __get_param_value(self, name):
        """
        Extract a parameter value from the remaining given arguments.

        Returns a 2-tuple consisting of the following:
          * Boolean indicating whether an argument has been specified for the
            requested input parameter.
          * Parameter value.

        """
        if self.__args:
            return True, self.__args.pop(0)
        try:
            value = self.__kwargs.pop(name)
        except KeyError:
            return False, None
        return True, value

    def __in_choice_context(self):
        """
        Whether we are currently processing a choice parameter group.

        This includes processing a parameter defined directly or indirectly
        within such a group.

        May only be called during parameter processing or the result will be
        calculated based on the context left behind by the previous parameter
        processing if any.

        """
        for x in self.__stack:
            if x.__class__ is ChoiceFrame:
                return True
        return False

    def __init_run(self, args, kwargs, extra_parameter_errors):
        """Initializes data for a new argument parsing run."""
        assert not self.active()
        self.__args = list(args)
        self.__kwargs = dict(kwargs)
        self.__extra_parameter_errors = extra_parameter_errors
        self.__args_count = len(args) + len(kwargs)
        self.__params_with_arguments = set()
        self.__stack = []
        self.__push_frame(None)

    def __match_ancestry(self, ancestry):
        """
        Find frames matching the given ancestry.

        Returns a tuple containing the following:
          * Topmost frame matching the given ancestry or the bottom-most sentry
            frame if no frame matches.
          * Unmatched ancestry part.

        """
        stack = self.__stack
        if len(stack) == 1:
            return stack[0], ancestry
        previous = stack[0]
        for frame, n in zip(stack[1:], range(len(ancestry))):
            if frame.id() is not ancestry[n]:
                return previous, ancestry[n:]
            previous = frame
        return frame, ancestry[n + 1:]

    def __pop_frames_above(self, frame):
        """Pops all the frames above, but not including the given frame."""
        while self.__stack[-1] is not frame:
            self.__pop_top_frame()
        assert self.__stack

    def __pop_top_frame(self):
        """Pops the top frame off the frame stack."""
        popped = self.__stack.pop()
        if self.__stack:
            self.__stack[-1].process_subframe(popped)

    def __process_parameter(self, param_name, param_type, ancestry=None):
        """Collect values for a given web service operation input parameter."""
        assert self.active()
        param_optional = param_type.optional()
        has_argument, value = self.__get_param_value(param_name)
        if has_argument:
            self.__params_with_arguments.add(param_name)
        self.__update_context(ancestry)
        self.__stack[-1].process_parameter(param_optional, value is not None)
        self.__external_param_processor(param_name, param_type,
            self.__in_choice_context(), value)

    def __process_parameters(self):
        """Collect values for given web service operation input parameters."""
        for pdef in self.__param_defs:
            self.__process_parameter(*pdef)

    def __push_frame(self, ancestry_item):
        """Push a new frame on top of the frame stack."""
        frame = self.__frame_factory(ancestry_item)
        self.__stack.append(frame)

    def __push_frames(self, ancestry):
        """
        Push new frames representing given ancestry items.

        May only be given ancestry items other than None. Ancestry item None
        represents the internal sentinel item and should never appear in a
        given parameter's ancestry information.

        """
        for x in ancestry:
            assert x is not None
            self.__push_frame(x)

    def __update_context(self, ancestry):
        if not ancestry:
            return
        match_result = self.__match_ancestry(ancestry)
        last_matching_frame, unmatched_ancestry = match_result
        self.__pop_frames_above(last_matching_frame)
        self.__push_frames(unmatched_ancestry)


class Frame:
    """
    Base _ArgParser context frame.

    When used directly, as opposed to using a derived class, may represent any
    input parameter context/ancestry item except a choice order indicator.

    """

    def __init__(self, id, error, extra_parameter_errors):
        """
        Construct a new Frame instance.

        Passed error function is used to report any argument checking errors.

        """
        assert self.__class__ != Frame or not id or not id.choice()
        self.__id = id
        self._error = error
        self._extra_parameter_errors = extra_parameter_errors
        self._args_allowed = 0
        self._args_required = 0
        self._has_value = False

    def args_allowed(self):
        return self._args_allowed

    def args_required(self):
        return self._args_required

    def has_value(self):
        return self._has_value

    def id(self):
        return self.__id

    def process_parameter(self, optional, has_value):
        args_required = 1
        if optional:
            args_required = 0
        self._process_item(has_value, 1, args_required)

    def process_subframe(self, subframe):
        self._process_item(
            subframe.has_value(),
            subframe.args_allowed(),
            subframe.args_required())

    def _process_item(self, has_value, args_allowed, args_required):
        self._args_allowed += args_allowed
        self._args_required += args_required
        if has_value:
            self._has_value = True


class ChoiceFrame(Frame):
    """
    _ArgParser context frame representing a choice order indicator.

    A choice requires as many input arguments as are needed to satisfy the
    least requiring of its items. For example, if we use I(n) to identify an
    item requiring n parameter, then a choice containing I(2), I(3) & I(7)
    requires 2 arguments while a choice containing I(5) & I(4) requires 4.

    Accepts an argument for each of its contained elements but allows at most
    one of its directly contained items to have a defined value.

    """

    def __init__(self, id, error, extra_parameter_errors):
        assert id.choice()
        Frame.__init__(self, id, error, extra_parameter_errors)
        self.__has_item = False

    def _process_item(self, has_value, args_allowed, args_required):
        self._args_allowed += args_allowed
        self.__update_args_required_for_item(args_required)
        self.__update_has_value_for_item(has_value)

    def __update_args_required_for_item(self, item_args_required):
        if not self.__has_item:
            self.__has_item = True
            self._args_required = item_args_required
            return
        self._args_required = min(self.args_required(), item_args_required)

    def __update_has_value_for_item(self, item_has_value):
        if item_has_value:
            if self.has_value() and self._extra_parameter_errors:
                self._error("got multiple values for a single choice "
                    "parameter")
            self._has_value = True
