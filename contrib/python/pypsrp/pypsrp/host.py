# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import logging
import typing
import uuid
import xml.etree.ElementTree as ET

from pypsrp.complex_objects import (
    Array,
    BufferCell,
    Color,
    Coordinates,
    CultureInfo,
    GenericComplexObject,
    HostMethodIdentifier,
    KeyInfo,
    ObjectMeta,
    PSCredential,
    Size,
)
from pypsrp.powershell import PowerShell, RunspacePool

log = logging.getLogger(__name__)


class PSHost(object):
    def __init__(
        self,
        current_culture: typing.Optional[CultureInfo],
        current_ui_culture: typing.Optional[CultureInfo],
        debugger_enabled: bool,
        name: typing.Optional[str],
        private_data: typing.Optional[typing.Dict],
        ui: typing.Optional["PSHostUserInterface"],
        version: str,
    ) -> None:
        """
        Defines the properties and facilities provided by an application
        hosting a RunspacePool.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost

        This is a basic implementation some methods being noop or not
        implemented.

        :param current_culture: pypsrp.complex_objects.CultureInfo, the host's
            culture
        :param current_ui_culture: pypsrp.complex_objects.CultureInfo, the
            host's UI culture
        :param debugger_enabled: This property enables and disables the host
            debugger if debugging is supported
        :param name: Gets the hosting application identification in some user-
            friendly fashion.
        :param private_data: Used to allow the host to pass private data
            through a Runspace to cmdlets inside that Runspace
        :param ui: The hosts implementation of PSHostUserInterface. Should be
            None if the host that does not want to support user interaction
        :param version: The version of the hosting application
        """
        self.ui = ui
        self.debugger_enabled = debugger_enabled
        self.private_data = private_data
        self.rc: typing.Optional[int] = None

        self.name = name
        self.version = version
        self.instance_id = uuid.uuid4()
        self.current_culture = current_culture
        self.current_ui_culture = current_ui_culture

    def run_method(
        self,
        method_identifier: HostMethodIdentifier,
        args: typing.List,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell] = None,
    ) -> typing.Any:
        """
        Run a host call method requested by the server and return the response
        from this method to send back to the server.
        https://msdn.microsoft.com/en-us/library/dd306624.aspx

        Each method will have access to the current runspace and pipeline (if
        applicable) during the method call as well as any args sent from the
        server.

        :param method_identifier: pypsrp.complex_objects.HostMethodIdentifier
            in the host call message.
        :param args: The list of arguments for the host call function.
        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: The response (if any) to send back to the server
        """
        response = None
        mi = method_identifier.value or 0
        if mi < 11:
            func = getattr(self, str(method_identifier))
            response = func(runspace, pipeline, *args)
        elif mi < 27:
            func = getattr(self.ui, str(method_identifier))
            response = func(runspace, pipeline, *args)
        elif mi < 52:
            func = getattr(getattr(self.ui, "raw_ui", None), str(method_identifier))
            response = func(runspace, pipeline, *args)
        else:
            log.warning("Received unexpected/unsupported host method identifier: %d" % mi)

        return response

    # Start of Host Methods, the names of these functions are important as
    # they line up to the names defined by MS and are sent in the host call
    # messages
    def GetName(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> typing.Optional[str]:
        """
        MI: 1
        SHOULD return a string identifying the hosting application in a user
        friendly way.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.name

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: String of the user-friendly name of the hosting application
        """
        return self.name

    def GetVersion(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> typing.Optional[ET.Element]:
        """
        MI: 2
        SHOULD return the version number of the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.version

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: Version number of the hosting application
        """
        meta = ObjectMeta("Version")
        value = runspace.serialize(self.version, meta)
        return value

    def GetInstanceId(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> uuid.UUID:
        """
        MI: 3
        SHOULD return a GUID that uniquely identifies the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.instanceid

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: GUID of the hosting application
        """
        return self.instance_id

    def GetCurrentCulture(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> typing.Optional[CultureInfo]:
        """
        MI: 4
        SHOULD return the host's culture.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.currentculture

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: pypsrp.complex_objects.CultureInfo of the host's culture
        """
        return self.current_culture

    def GetCurrentUICulture(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> typing.Optional[CultureInfo]:
        """
        MI: 5
        MUST return the host's UI culture.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.currentuiculture

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: pypsrp.complex_objects.CultureInfo of the host's UI culture
        """
        return self.current_ui_culture

    def SetShouldExit(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        exit_code: int,
    ) -> None:
        """
        MI: 6
        SHOULD shut down the hosting application and close the current
        runspace. The default implementation just sets the rc on the host
        object and doesn't shutdown the runspace.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.setshouldexit

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param exit_code: The exit code accompanying the exit keyword.
            Typically after exiting a runspace, a host will also terminate
        """
        self.rc = exit_code

    def EnterNestedPrompt(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> None:
        """
        MI: 7
        SHOULD interrupt the current pipeline and start a nested pipeline.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.enternestedprompt

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        """
        raise NotImplementedError()

    def ExitNestedPrompt(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> None:
        """
        MI: 8
        SHOULD stop the nested pipeline and resume the current pipeline.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.exitnestedprompt

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        """
        raise NotImplementedError()

    def NotifyBeginApplication(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> None:
        """
        MI: 9
        Called by an application to indicate that it is executing a command
        line application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.notifybeginapplication

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        """
        pass

    def NotifyEndApplication(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> None:
        """
        MI: 10
        Called by an application to indicate that it has finished executing a
        command line application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshost.notifyendapplication

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        """
        pass


class PSHostUserInterface(object):
    def __init__(
        self,
        raw_ui: typing.Optional["PSHostRawUserInterface"] = None,
    ) -> None:
        """
        Defines the properties and facilities provided by a hosting application
        deriving from PSHost that offers dialog-oriented and line-oriented
        interactive features.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface

        This is a basic implementation some methods being noop or not
        implemented.

        :param raw_ui: Implementation of PSHostRawUserInterface, set to None
            if there is no raw user interface
        """
        self.raw_ui = raw_ui

        # the below properties don't need to be used, they are just here for
        # the default implementation
        self.stdout: typing.List[str] = []
        self.stderr: typing.List[str] = []

    def ReadLine(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> str:
        """
        MI: 11
        SHOULD read a line of characters from a user.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.readline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: A string of characters to return to the read line call
        """
        raise NotImplementedError()

    def ReadLineAsSecureString(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> ET.Element:
        """
        MI: 12
        SHOULD read a line of characters from a user, with the user input not
        echoed.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.readlineassecurestring

        Because the return value is meant to be a SecureString, the user must
        either have called or will call runspace.exchange_keys() in this
        implementation so that the serializer can create the string.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: The characters types by the user in an encrypted form
        """
        raise NotImplementedError()

    def Write1(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        value: str,
    ) -> None:
        """
        MI: 13
        SHOULD write specified characters on the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.write

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param value: The string of characters to be written
        """
        self.stdout.append(value)

    def Write2(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        foreground_color: int,
        background_color: int,
        value: str,
    ) -> None:
        """
        MI: 14
        SHOULD write the specified characters with the specified foreground and
        background color on the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.write

        This implementation just adds this result to the stdout list and
        ignores the colors, create your own method implementation if you wish
        to utilise this correctly

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param foreground_color: The int value of pypsrp.complex_objects.Color
            of the foreground color to display the text with
        :param background_color: The int value of pypsrp.complex_objects.Color
            of the background color to display the text with
        :param value: The string of characters to be written
        """
        self.stdout.append(value)

    def WriteLine1(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> None:
        """
        MI: 15
        SHOULD write a carriage return on the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writeline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        """
        self.stdout.append("\r\n")

    def WriteLine2(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        value: str,
    ) -> None:
        """
        MI: 16
        SHOULD write the specified line on the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writeline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param value: The string of characters to be written
        """
        self.stdout.append(value + "\r\n")

    def WriteLine3(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        foreground_color: int,
        background_color: int,
        value: str,
    ) -> None:
        """
        MI: 17
        SHOULD write the specified line with the specified foreground and
        background color on the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writeline

        This implementation just adds this result to the stdout list and
        ignores the colors, create your own method implementation if you wish
        to utilise this correctly

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param foreground_color: The int value of pypsrp.complex_objects.Color
            of the foreground color to display the text with
        :param background_color: The int value of pypsrp.complex_objects.Color
            of the background color to display the text with
        :param value: The string of characters to be written
        """
        self.stdout.append(value + "\r\n")

    def WriteErrorLine(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        message: str,
    ) -> None:
        """
        MI: 18
        SHOULD write a line to the error display of the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writeerrorline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param message: The message to display
        """
        self.stderr.append(message + "\r\n")

    def WriteDebugLine(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        message: str,
    ) -> None:
        """
        MI: 19
        SHOULD write a line to the debug display of the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writedebugline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param message: The message to display
        """
        self.stdout.append("DEBUG: %s\r\n" % message)

    def WriteProgress(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        source_id: int,
        record: str,
    ) -> None:
        """
        MI: 20
        SHOULD display a progress record on the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writeprogress

        Because of the way MS serializes the record in the method call args,
        the value for record is the serialized XML string for a ProgressRecord
        message. You can manually parse it like;

            from pypsrp.messages import ProgressRecord

            meta = ObjectMeta("Obj", object=ProgressRecord)
            rec = runspace._serializer.deserialize(record, meta)

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param source_id: Unique identifier of the source of the record
        :param record: A ProgressRecord serialized as XML
        """
        pass

    def WriteVerboseLine(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        message: str,
    ) -> None:
        """
        MI: 21
        SHOULD write a line on the verbose display of the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writeverboseline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param message: The verbose message to display
        """
        self.stdout.append("VERBOSE: %s\r\n" % message)

    def WriteWarningLine(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        message: str,
    ) -> None:
        """
        MI: 22
        SHOULD write a line on the warning display of the hosting application.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.writewarningline

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param message: The warning message to display
        """
        self.stdout.append("WARNING: %s\r\n" % message)

    def Prompt(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        caption: str,
        message: str,
        description: typing.List[GenericComplexObject],
    ) -> typing.Dict[str, typing.Any]:
        """
        MI: 23
        SHOULD prompt the user with a set of choices.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.prompt

        The descriptions arg is a list of GenericComplexObjects with the
        following extended attributes (correlates to FieldDescription in .NET):
            attributes
            defaultValue
            helpMessage
            isMandatory
            label
            name
            parameterAssemblyFullName
            parameterTypeFullName
            parameterTypeName

        For example you can access the prompt name from `Read-Host -Prompt`
        with descriptions[i].extended_properties['name'].

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param caption: Caption to precede or title the prompt
        :param message: A text description of the set of fields to be prompted
        :param descriptions: list of serialized FieldDescriptions that contain
            information about each field to be prompted for
        :return: Dict with results of prompting. Key are the field names from
            the FieldDescriptions, the values are objects representing the
            values of the corresponding fields as collected from the user.
        """
        raise NotImplementedError()

    def PromptForCredential1(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        caption: str,
        message: str,
        user_name: str,
        target_name: str,
    ) -> PSCredential:
        """
        MI: 24
        SHOULD prompt the user for entering credentials with the specified
        caption, message, user name and target name.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.promptforcredential

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param caption: Caption for the message
        :param message: Text description for the credential to be prompted
        :param user_name: Name of the user whose credential is to be prompted
            for. If set to null or empty string, the function will prompt for
            the user name first
        :param target_name: Name of the target for which the credential is
            being collected
        :return: PSCredential object of the user input credential
        """
        raise NotImplementedError()

    def PromptForCredential2(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        caption: str,
        message: str,
        user_name: str,
        target_name: str,
        allowed_credential_types: int,
        options: int,
    ) -> PSCredential:
        """
        MI: 25
        SHOULD prompt the user for entering credentials with the specified
        caption, message, username, target name, allowed credential types and
        options.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.promptforcredential

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param caption: Caption for the message
        :param message: Text description for the credential to be prompted
        :param user_name: Name of the user whose credential is to be prompted
            for. If set to null or empty string, the function will prompt for
            the user name first
        :param target_name: Name of the target for which the credential is
            being collected
        :param allowed_credential_types: the int value for PSCredentialTypes,
            types of credentials that can be supplied by the user
        :param options: the int value for PSCredentialUIOptions, options that
            control the credential gathering UI behavior
        :return: PSCredential object of the user input credential
        """
        raise NotImplementedError()

    def PromptForChoice(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        caption: str,
        message: str,
        choices: typing.List[GenericComplexObject],
        default_choice: int,
    ) -> int:
        """
        MI: 26
        SHOULD display a list of choices to the user and MUST return the index
        of the selected option.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostuserinterface.promptforchoice

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param caption: The caption to precede or title the prompt
        :param message: A message that describes what the choice is for
        :param choices: A list of serialized (GenericComplexObject)
            ChoiceDescription objects that describe each choice
        :param default_choice: The index of the label in the choices collection
            element to be present to the user as the default choice, -1 means
            no default
        :return: The index of the choices element that corresponds to the
            option selected
        """
        raise NotImplementedError()


class PSHostRawUserInterface(object):
    def __init__(
        self,
        window_title: str,
        cursor_size: int,
        foreground_color: Color,
        background_color: Color,
        cursor_position: Coordinates,
        window_position: Coordinates,
        buffer_size: Size,
        max_physical_window_size: Size,
        max_window_size: Size,
        window_size: Size,
    ) -> None:
        """
        Defines the lowest-level user interface functions that an interactive
        application hosting a Runspace can choose to implement if it wants
        to support any cmdlet that does character-mode interaction with the
        user.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface

        This is a basic framework implementation with the majority of the
        methods not implemented or tested.

        :param window_title: The titlebar text of the current view window
        :param cursor_size: The size of the cursor as a percentage (0 to 100)
        :param foreground_color: The pypsrp.complex_objects.Color used to
            render characters on the screen buffer
        :param background_color: The pypsrp.complex_objects.Color used to
            render the backfround behind characters on the screen buffer
        :param cursor_position: The pypsrp.complex_objects.Coordinates of the
            position of the cursor in the screen buffer
        :param window_position: The pypsrp.complex_objects.Coordinates of the
            position of the window relative to the screen buffer, (0, 0) is the
            upper left of the screen buffer
        :param buffer_size: The pypsrp.complex_objects.Size of the screen
            buffer
        :param max_physical_window_size: The pypsrp.complex_objects.Size of the
            largest windows possible for the display hardward
        :param max_window_size: The pypsrp.complex_objects.Size of the window
            possible for the current buffer
        :param window_size: The pypsrp.complex_objects.Size of the current
            window, cannot be larger than max_physical_window_size
        """
        self.key_available = False

        self.window_title = window_title
        self.cursor_size = cursor_size
        self.foreground_color = foreground_color
        self.background_color = background_color
        self.cursor_position = cursor_position
        self.window_position = window_position
        self.buffer_size = buffer_size
        self.max_physical_window_size = max_physical_window_size
        self.max_window_size = max_window_size
        self.window_size = window_size

    def GetForegroundColor(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Color:
        """
        MI: 27
        SHOULD return the foreground color of the hosting application.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: A pypsrp.complex_objects.Color to return to the server
        """
        return self.foreground_color

    def SetForegroundColor(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        color: Color,
    ) -> None:
        """
        MI: 28
        SHOULD set the foreground color of the hosting application.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param color: The int value for pypsrp.complex_objects.Color to set
        """
        self.foreground_color = Color(value=color)

    def GetBackgroundColor(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Color:
        """
        MI: 29
        SHOULD return the background color of the hosting application.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: A pypsrp.complex_objects.Color to return to the server
        """
        return self.background_color

    def SetBackgroundColor(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        color: Color,
    ) -> None:
        """
        MI: 30
        SHOULD set the background color of the hosting application.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param color: The int value for pypsrp.complex_objects.Color to set
        """
        self.background_color = Color(value=color)

    def GetCursorPosition(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Coordinates:
        """
        MI: 31
        SHOULD return the current cursor position in the hosting application.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return:  A pypsrp.complex_objects.Coordinates to return to the server
        """
        return self.cursor_position

    def SetCursorPosition(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        coordinates: GenericComplexObject,
    ) -> None:
        """
        MI: 32
        SHOULD return the current cursor position in the hosting application.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param coordinates: A GenericComplexObject that contains the extended
            properties for the coordinates
        """
        pos = Coordinates(x=coordinates.extended_properties["x"], y=coordinates.extended_properties["y"])
        self.cursor_position = pos

    def GetWindowPosition(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Coordinates:
        """
        MI: 33
        SHOULD return the position of the view window relative to the screen
        buffer.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: A pypsrp.complex_objects.Coordinates to return to the server
        """
        return self.window_position

    def SetWindowPosition(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        coordinates: GenericComplexObject,
    ) -> None:
        """
        MI: 34
        SHOULD set the position of the view window relative to the screen
        buffer.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param coordinates: A GenericComplexObject that contains the extended
            properties for the coordinates
        """
        pos = Coordinates(x=coordinates.extended_properties["x"], y=coordinates.extended_properties["y"])
        self.window_position = pos

    def GetCursorSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> int:
        """
        MI: 35
        SHOULD return the cursor size as a percentage.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: The int value for the cursor size
        """
        return self.cursor_size

    def SetCursorSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        percentage: int,
    ) -> None:
        """
        MI: 36
        SHOULD set the cursor size based on the percentage value specified.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param percentage: The int value representing the cursor size
        """
        self.cursor_size = percentage

    def GetBufferSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Size:
        """
        MI: 37
        SHOULD return the current size of the screen buffer, measured in
        character cells.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: pypsrp.complex_object.Size of the screen buffer, measured in
            character cells
        """
        return self.buffer_size

    def SetBufferSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        size: GenericComplexObject,
    ) -> None:
        """
        MI: 38
        SHOULD set the size of the screen buffer with the specified size in
        character cells.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param size: A GenericComplexObject that contains the extended
            properties for the size
        """
        obj = Size(height=size.extended_properties["height"], width=size.extended_properties["width"])
        self.buffer_size = obj

    def GetWindowSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Size:
        """
        MI: 39
        SHOULD return the current view window size.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: pypsrp.complex_objects.Size of the current window
        """
        return self.window_size

    def SetWindowSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        size: GenericComplexObject,
    ) -> None:
        """
        MI: 40
        SHOULD set the view window size based on the size specified.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param size: A GenericComplexObject that contains the extended
            properties for the size
        """
        obj = Size(height=size.extended_properties["height"], width=size.extended_properties["width"])
        self.window_size = obj

    def GetWindowTitle(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> str:
        """
        MI: 41
        SHOULD return the title of the hosting application's window.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: The window title of the hosting application
        """
        return self.window_title

    def SetWindowTitle(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        title: str,
    ) -> None:
        """
        MI: 42
        SHOULD set the view window size based on the size specified.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param title: The string for the window title to set
        """
        self.window_title = title

    def GetMaxWindowSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Size:
        """
        MI: 43
        SHOULD return the maximum window size possible for the current buffer,
        current font, and current display hardware.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: pypsrp.complex_objects.Size of the max possible window size
        """
        return self.max_window_size

    def GetMaxPhysicalWindowSize(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> Size:
        """
        MI: 44
        SHOULD return the maximum window size possible for the current font and
        current display hardware, ignoring the current buffer size.

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: pypsrp.complex_objects.Size of the max physically possible
            window size
        """
        return self.max_physical_window_size

    def GetKeyAvailable(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> bool:
        """
        MI: 45
        SHOULD examine if a keystroke is waiting on the input, returning TRUE
        if so and FALSE otherwise

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :return: bool if a keystroke is waiting on the input
        """
        return self.key_available

    def ReadKey(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        options: int = 4,
    ) -> KeyInfo:
        """
        MI: 46
        SHOULD read a key stroke from the keyboard, blocking until a key is
        typed.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface.readkey

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param options: Bit mask combo of ReadKeyOptions, default is
            ReadKeyOptions.IncludeKeyDown
        :return: KeyInfo - key stroke depending on the value of options
        """
        raise NotImplementedError()

    def FlushInputBuffer(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
    ) -> None:
        """
        MI: 47
        SHOULD reset the keyboard input buffer.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface.flushinputbuffer

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        """
        pass

    def SetBufferContents1(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        rectangle: GenericComplexObject,
        fill: GenericComplexObject,
    ) -> None:
        """
        MI: 49
        SHOULD copy the specified buffer cell into all the cells within the
        specified rectangle.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface.setbuffercontents

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param rectange: A GenericComplexObject that represents the rectangle
            in which to fill. If all element are -1, the entire screen buffer
            will be copied with fill. Contains the following extended
            properties: left, top, right, bottom
        :param fill: A GenericComplexObject of the characters and attributes
            used to fill the rectangle
        """
        pass

    def SetBufferContents2(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        origin: GenericComplexObject,
        contents: GenericComplexObject,
    ) -> None:
        """
        MI: 48
        SHOULD copy the specified buffer cell array into the screen buffer at
        the specified coordinates (as specified in section 2.2.3.1).
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface.setbuffercontents

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param origin: A GenericComplexObject that represents the coordinates,
            x and y are extended properties of this object
                origin.extended_properties['x']
                origin.extended_properties['y']
        :param contents: A rectangle of BufferCell objects to be copied to the
            screen buffer. This is also a GenericComplexObject which is a multi
            dimensional array.
            https://msdn.microsoft.com/en-us/library/dd340684.aspx
                # number of elements in each row
                contents.extended_properties['mal']

                # each BufferCell is in this list, use mal to determine what
                # rows they are in
                contents.extended_properties['mae']
        """
        pass

    def GetBufferContents(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        rectangle: GenericComplexObject,
    ) -> Array:
        """
        MI: 50
        SHOULD return the contents in a specified rectangular region of the
        hosting application's window and MUST return an array of buffer cells.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface.getbuffercontents

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param rectangle: The rectangle on the screen buffer to extract
        :return: A pypsrp.complex_objects.Array of BufferCell objects extracted
            from the rectangular region of the screen buffer specified by
            rectangle
        """
        raise NotImplementedError()

    def ScrollBufferContents(
        self,
        runspace: RunspacePool,
        pipeline: typing.Optional[PowerShell],
        source: GenericComplexObject,
        destination: Coordinates,
        clip: GenericComplexObject,
        fill: BufferCell,
    ) -> None:
        """
        MI: 51
        SHOULD scroll a region on the screen buffer.
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.host.pshostrawuserinterface.scrollbuffercontents

        :param runspace: The runspace the host call relates to
        :param pipeline: The pipeline (if any) that the call relates to
        :param source: Rectangle - Indicates the region of the screen to be
            scrolled
        :param destination: Coordinates - Indicates the upper left coordinates
            of the region of the screen to receive the source region contents.
            That target region is the same size as the source region
        :param clip: Rectangle - Indicates the region of the screen to include
            in the operation. If a cell would be changed by the operation but
            does not fall within the clip region, it will be unchanged
        :param fill: BufferCell - The character and attributes to be used to
            fill any cells within the intersection of the source rectangle and
            clipping rectangle that are left "empty" by the move
        """
        pass
