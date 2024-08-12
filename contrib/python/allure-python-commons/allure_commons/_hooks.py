from pluggy import HookspecMarker, HookimplMarker

hookspec = HookspecMarker("allure")
hookimpl = HookimplMarker("allure")


class AllureUserHooks:

    @hookspec
    def decorate_as_title(self, test_title):
        """ title """

    @hookspec
    def add_title(self, test_title):
        """ title """

    @hookspec
    def decorate_as_description(self, test_description):
        """ description """

    @hookspec
    def add_description(self, test_description):
        """ description """

    @hookspec
    def decorate_as_description_html(self, test_description_html):
        """ description html"""

    @hookspec
    def add_description_html(self, test_description_html):
        """ description html"""

    @hookspec
    def decorate_as_label(self, label_type, labels):
        """ label """

    @hookspec
    def add_label(self, label_type, labels):
        """ label """

    @hookspec
    def decorate_as_link(self, url, link_type, name):
        """ url """

    @hookspec
    def add_link(self, url, link_type, name):
        """ url """

    @hookspec
    def add_parameter(self, name, value, excluded, mode):
        """ parameter """

    @hookspec
    def start_step(self, uuid, title, params):
        """ step """

    @hookspec
    def stop_step(self, uuid, exc_type, exc_val, exc_tb):
        """ step """

    @hookspec
    def attach_data(self, body, name, attachment_type, extension):
        """ attach data """

    @hookspec
    def attach_file(self, source, name, attachment_type, extension):
        """ attach file """


class AllureDeveloperHooks:

    @hookspec
    def start_fixture(self, parent_uuid, uuid, name, parameters):
        """ start fixture"""

    @hookspec
    def stop_fixture(self, parent_uuid, uuid, name, exc_type, exc_val, exc_tb):
        """ stop fixture """

    @hookspec
    def start_test(self, parent_uuid, uuid, name, parameters, context):
        """ start test"""

    @hookspec
    def stop_test(self, parent_uuid, uuid, name, context, exc_type, exc_val, exc_tb):
        """ stop test """

    @hookspec
    def report_result(self, result):
        """ reporting """

    @hookspec
    def report_container(self, container):
        """ reporting """

    @hookspec
    def report_attached_file(self, source, file_name):
        """ reporting """

    @hookspec
    def report_attached_data(self, body, file_name):
        """ reporting """
