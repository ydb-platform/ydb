# coding=utf-8
from .couchdbkit import *


class BuildSpec(DocumentSchema):
    version = StringProperty()
    build_number = IntegerProperty(required=False)
    latest = BooleanProperty()


class BuildRecord(BuildSpec):
    signed = BooleanProperty(default=True)
    datetime = DateTimeProperty(required=False)


class TranslationMixin(Document):
    translations = DictProperty()


class HQMediaMapItem(DocumentSchema):
    multimedia_id = StringProperty()
    media_type = StringProperty()
    output_size = DictProperty()
    version = IntegerProperty()


class HQMediaMixin(Document):
    multimedia_map = SchemaDictProperty(HQMediaMapItem)


class SnapshotMixin(DocumentSchema):
    copy_history = StringListProperty()


class FormActionCondition(DocumentSchema):
    type = StringProperty(choices=["if", "always", "never"], default="never")
    question = StringProperty()
    answer = StringProperty()

class FormAction(DocumentSchema):
    condition = SchemaProperty(FormActionCondition)


class UpdateCaseAction(FormAction):
    update = DictProperty()

class PreloadAction(FormAction):
    preload = DictProperty()

class UpdateReferralAction(FormAction):
    followup_date = StringProperty()


class OpenReferralAction(UpdateReferralAction):
    name_path = StringProperty()


class OpenCaseAction(FormAction):
    name_path = StringProperty()
    external_id = StringProperty()


class OpenSubCaseAction(FormAction):
    case_type = StringProperty()
    case_name = StringProperty()
    case_properties = DictProperty()
    repeat_context = StringProperty()


class FormActions(DocumentSchema):
    open_case = SchemaProperty(OpenCaseAction)
    update_case = SchemaProperty(UpdateCaseAction)
    close_case = SchemaProperty(FormAction)
    open_referral = SchemaProperty(OpenReferralAction)
    update_referral = SchemaProperty(UpdateReferralAction)
    close_referral = SchemaProperty(FormAction)

    case_preload = SchemaProperty(PreloadAction)
    referral_preload = SchemaProperty(PreloadAction)

    subcases = SchemaListProperty(OpenSubCaseAction)


class FormBase(DocumentSchema):
    """
    Part of a Managed Application; configuration for a form.
    Translates to a second-level menu on the phone

    """

    name = DictProperty()
    unique_id = StringProperty()
    requires = StringProperty(choices=["case", "referral", "none"], default="none")
    actions = SchemaProperty(FormActions)
    show_count = BooleanProperty(default=False)
    xmlns = StringProperty()
    version = IntegerProperty()


class JRResourceProperty(StringProperty):
    pass


class NavMenuItemMediaMixin(DocumentSchema):
    media_image = JRResourceProperty(required=False)
    media_audio = JRResourceProperty(required=False)


class Form(FormBase, NavMenuItemMediaMixin):
    form_filter = StringProperty()


class UserRegistrationForm(FormBase):
    username_path = StringProperty(default='username')
    password_path = StringProperty(default='password')
    data_paths = DictProperty()


class DetailColumn(DocumentSchema):
    header = DictProperty()
    model = StringProperty()
    field = StringProperty()
    format = StringProperty()

    enum = DictProperty()
    late_flag = IntegerProperty(default=30)
    advanced = StringProperty(default="")
    filter_xpath = StringProperty(default="")
    time_ago_interval = FloatProperty(default=365.25)


class SortElement(DocumentSchema):
    field = StringProperty()
    type = StringProperty()
    direction = StringProperty()


class Detail(DocumentSchema):
    type = StringProperty(choices=['case_short', 'case_long', 'ref_short', 'ref_long'])

    columns = SchemaListProperty(DetailColumn)
    sort_elements = SchemaListProperty(SortElement)


class CaseList(DocumentSchema):
    label = DictProperty()
    show = BooleanProperty(default=False)


class ParentSelect(DocumentSchema):
    active = BooleanProperty(default=False)
    relationship = StringProperty(default='parent')
    module_id = StringProperty()


class Module(NavMenuItemMediaMixin):
    name = DictProperty()
    case_label = DictProperty()
    referral_label = DictProperty()
    forms = SchemaListProperty(Form)
    details = SchemaListProperty(Detail)
    case_type = StringProperty()
    put_in_root = BooleanProperty(default=False)
    case_list = SchemaProperty(CaseList)
    referral_list = SchemaProperty(CaseList)
    task_list = SchemaProperty(CaseList)
    parent_select = SchemaProperty(ParentSelect)
    unique_id = StringProperty()


class VersionedDoc(DocumentSchema):
    """
    A document that keeps an auto-incrementing version number, knows how to make copies of itself,
    delete a copy of itself, and revert back to an earlier copy of itself.

    """
    domain = StringProperty()
    copy_of = StringProperty()
    version = IntegerProperty()
    short_url = StringProperty()
    short_odk_url = StringProperty()


class ApplicationBase(VersionedDoc, SnapshotMixin):
    """
    Abstract base class for Application and RemoteApp.
    Contains methods for generating the various files and zipping them into CommCare.jar

    """

    recipients = StringProperty(default="")

    # this is the supported way of specifying which commcare build to use
    build_spec = SchemaProperty(BuildSpec)
    platform = StringProperty(
        choices=["nokia/s40", "nokia/s60", "winmo", "generic"],
        default="nokia/s40"
    )
    text_input = StringProperty(
        choices=['roman', 'native', 'custom-keys', 'qwerty'],
        default="roman"
    )
    success_message = DictProperty()

    # The following properties should only appear on saved builds
    # built_with stores a record of CommCare build used in a saved app
    built_with = SchemaProperty(BuildRecord)
    build_signed = BooleanProperty(default=True)
    built_on = DateTimeProperty(required=False)
    build_comment = StringProperty()
    comment_from = StringProperty()
    build_broken = BooleanProperty(default=False)

    # watch out for a past bug:
    # when reverting to a build that happens to be released
    # that got copied into into the new app doc, and when new releases were made,
    # they were automatically starred
    # AFAIK this is fixed in code, but my rear its ugly head in an as-yet-not-understood
    # way for apps that already had this problem. Just keep an eye out
    is_released = BooleanProperty(default=False)

    # django-style salted hash of the admin password
    admin_password = StringProperty()
    # a=Alphanumeric, n=Numeric, x=Neither (not allowed)
    admin_password_charset = StringProperty(choices=['a', 'n', 'x'], default='n')

    # This is here instead of in Application because it needs to be available in stub representation
    application_version = StringProperty(default='1.0', choices=['1.0', '2.0'], required=False)

    langs = StringListProperty()
    # only the languages that go in the build
    build_langs = StringListProperty()

    # exchange properties
    cached_properties = DictProperty()
    description = StringProperty()
    deployment_date = DateTimeProperty()
    phone_model = StringProperty()
    user_type = StringProperty()
    attribution_notes = StringProperty()

    # always false for RemoteApp
    case_sharing = BooleanProperty(default=False)


class Profile(DocumentSchema):
   features = DictProperty()
   _properties = DictProperty(name='properties_')


class Application(ApplicationBase, TranslationMixin, HQMediaMixin):
    user_registration = SchemaProperty(UserRegistrationForm)
    show_user_registration = BooleanProperty(default=False, required=True)
    modules = SchemaListProperty(Module)
    name = StringProperty()
    profile = SchemaProperty(Profile)
    use_custom_suite = BooleanProperty(default=False)
    force_http = BooleanProperty(default=False)
    cloudcare_enabled = BooleanProperty(default=False)


class RemoteApp(ApplicationBase):
    profile_url = StringProperty(default="http://")
    name = StringProperty()
    manage_urls = BooleanProperty(default=False)

    questions_map = DictProperty(required=False)
