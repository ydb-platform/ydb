import sys
from typing import Optional
import warnings

from typing_extensions import Self

from tableauserverclient.config import config
from tableauserverclient.models.property_decorators import property_is_int
import logging

from tableauserverclient.helpers.logging import logger


class RequestOptionsBase:
    # This method is used if server api version is below 3.7 (2020.1)
    def apply_query_params(self, url):
        try:
            params = self.get_query_params()
            params_list = [f"{k}={v}" for (k, v) in params.items()]

            logger.debug("Applying options to request: <%s(%s)>", self.__class__.__name__, ",".join(params_list))

            if "?" in url:
                url, existing_params = url.split("?")
                params_list.append(existing_params)

            return "{}?{}".format(url, "&".join(params_list))
        except NotImplementedError:
            raise


# If it wasn't a breaking change, I'd rename it to QueryOptions
"""
This class manages options can be used when querying content on the server
"""


class RequestOptions(RequestOptionsBase):
    """
    This class is used to manage the options that can be used when querying content on the server.
    Optionally initialize with a page number and page size to control the number of items returned.

    Additionally, you can add sorting and filtering options to the request.

    The `sort` and `filter` options are set-like objects, so you can only add a field once. If you add the same field
    multiple times, only the last one will be used.

    The list of fields that can be sorted on or filtered by can be found in the `Field`
    class contained within this class.

    Parameters
    ----------
    pagenumber: int, optional
        The page number to start the query on. Default is 1.

    pagesize: int, optional
        The number of items to return per page. Default is 100. Can also read
        from the environment variable `TSC_PAGE_SIZE`
    """

    def __init__(self, pagenumber=1, pagesize=None):
        self.pagenumber = pagenumber
        self.pagesize = pagesize or config.PAGE_SIZE
        self.sort = set()
        self.filter = set()
        self.fields = set()
        # This is private until we expand all of our parsers to handle the extra fields
        self.all_fields = False

    @property
    def _all_fields(self) -> bool:
        return self.all_fields

    @_all_fields.setter
    def _all_fields(self, value):
        warnings.warn(
            "Directly setting _all_fields is deprecated, please use the all_fields property instead.",
            DeprecationWarning,
        )
        self.all_fields = value

    def get_query_params(self) -> dict:
        params = {}
        if self.sort and len(self.sort) > 0:
            sort_options = (str(sort_item) for sort_item in self.sort)
            ordered_sort_options = sorted(sort_options)
            params["sort"] = ",".join(ordered_sort_options)
        if len(self.filter) > 0:
            filter_options = (str(filter_item) for filter_item in self.filter)
            ordered_filter_options = sorted(filter_options)
            params["filter"] = ",".join(ordered_filter_options)
        if self.all_fields:
            params["fields"] = "_all_"
        if self.pagenumber:
            params["pageNumber"] = self.pagenumber
        if self.pagesize:
            params["pageSize"] = self.pagesize
        if self.fields:
            if "_all_" in self.fields:
                params["fields"] = "_all_"
            else:
                params["fields"] = ",".join(sorted(self.fields))
        return params

    def page_size(self, page_size):
        self.pagesize = page_size
        return self

    def page_number(self, page_number):
        self.pagenumber = page_number
        return self

    class Operator:
        Equals = "eq"
        GreaterThan = "gt"
        GreaterThanOrEqual = "gte"
        LessThan = "lt"
        LessThanOrEqual = "lte"
        In = "in"
        Has = "has"
        CaseInsensitiveEquals = "cieq"

    # These are fields in the REST API
    class Field:
        Args = "args"
        AuthenticationType = "authenticationType"
        Caption = "caption"
        Channel = "channel"
        CompletedAt = "completedAt"
        ConnectedWorkbookType = "connectedWorkbookType"
        ConnectionTo = "connectionTo"
        ConnectionType = "connectionType"
        ContentUrl = "contentUrl"
        CreatedAt = "createdAt"
        DatabaseName = "databaseName"
        DatabaseUserName = "databaseUserName"
        Description = "description"
        DisplayTabs = "displayTabs"
        DomainName = "domainName"
        DomainNickname = "domainNickname"
        FavoritesTotal = "favoritesTotal"
        Fields = "fields"
        FlowId = "flowId"
        FriendlyName = "friendlyName"
        HasAlert = "hasAlert"
        HasAlerts = "hasAlerts"
        HasEmbeddedPassword = "hasEmbeddedPassword"
        HasExtracts = "hasExtracts"
        HitsTotal = "hitsTotal"
        Id = "id"
        IsCertified = "isCertified"
        IsConnectable = "isConnectable"
        IsDefaultPort = "isDefaultPort"
        IsHierarchical = "isHierarchical"
        IsLocal = "isLocal"
        IsPublished = "isPublished"
        JobType = "jobType"
        LastLogin = "lastLogin"
        Luid = "luid"
        MinimumSiteRole = "minimumSiteRole"
        Name = "name"
        Notes = "notes"
        NotificationType = "notificationType"
        OwnerDomain = "ownerDomain"
        OwnerEmail = "ownerEmail"
        OwnerId = "ownerId"
        OwnerName = "ownerName"
        ParentProjectId = "parentProjectId"
        Priority = "priority"
        Progress = "progress"
        ProjectId = "projectId"
        ProjectName = "projectName"
        PublishSamples = "publishSamples"
        ServerName = "serverName"
        ServerPort = "serverPort"
        SheetCount = "sheetCount"
        SheetNumber = "sheetNumber"
        SheetType = "sheetType"
        SiteRole = "siteRole"
        Size = "size"
        StartedAt = "startedAt"
        Status = "status"
        SubscriptionsTotal = "subscriptionsTotal"
        Subtitle = "subtitle"
        TableName = "tableName"
        Tags = "tags"
        Title = "title"
        TopLevelProject = "topLevelProject"
        Type = "type"
        UpdatedAt = "updatedAt"
        UserCount = "userCount"
        UserId = "userId"
        ViewId = "viewId"
        ViewUrlName = "viewUrlName"
        WorkbookDescription = "workbookDescription"
        WorkbookId = "workbookId"
        WorkbookName = "workbookName"

    class Direction:
        Desc = "desc"
        Asc = "asc"

    class SelectFields:
        class Common:
            All = "_all_"
            Default = "_default_"

        class ContentsCounts:
            ProjectCount = "contentsCounts.projectCount"
            ViewCount = "contentsCounts.viewCount"
            DatasourceCount = "contentsCounts.datasourceCount"
            WorkbookCount = "contentsCounts.workbookCount"

        class Datasource:
            ContentUrl = "datasource.contentUrl"
            ID = "datasource.id"
            Name = "datasource.name"
            Type = "datasource.type"
            Description = "datasource.description"
            CreatedAt = "datasource.createdAt"
            UpdatedAt = "datasource.updatedAt"
            EncryptExtracts = "datasource.encryptExtracts"
            IsCertified = "datasource.isCertified"
            UseRemoteQueryAgent = "datasource.useRemoteQueryAgent"
            WebPageURL = "datasource.webpageUrl"
            Size = "datasource.size"
            Tag = "datasource.tag"
            FavoritesTotal = "datasource.favoritesTotal"
            DatabaseName = "datasource.databaseName"
            ConnectedWorkbooksCount = "datasource.connectedWorkbooksCount"
            HasAlert = "datasource.hasAlert"
            HasExtracts = "datasource.hasExtracts"
            IsPublished = "datasource.isPublished"
            ServerName = "datasource.serverName"

        class Favorite:
            Label = "favorite.label"
            ParentProjectName = "favorite.parentProjectName"
            TargetOwnerName = "favorite.targetOwnerName"

        class Group:
            ID = "group.id"
            Name = "group.name"
            DomainName = "group.domainName"
            UserCount = "group.userCount"
            MinimumSiteRole = "group.minimumSiteRole"

        class Job:
            ID = "job.id"
            Status = "job.status"
            CreatedAt = "job.createdAt"
            StartedAt = "job.startedAt"
            EndedAt = "job.endedAt"
            Priority = "job.priority"
            JobType = "job.jobType"
            Title = "job.title"
            Subtitle = "job.subtitle"

        class Owner:
            ID = "owner.id"
            Name = "owner.name"
            FullName = "owner.fullName"
            SiteRole = "owner.siteRole"
            LastLogin = "owner.lastLogin"
            Email = "owner.email"

        class Project:
            ID = "project.id"
            Name = "project.name"
            Description = "project.description"
            CreatedAt = "project.createdAt"
            UpdatedAt = "project.updatedAt"
            ContentPermissions = "project.contentPermissions"
            ParentProjectID = "project.parentProjectId"
            TopLevelProject = "project.topLevelProject"
            Writeable = "project.writeable"

        class User:
            ExternalAuthUserId = "user.externalAuthUserId"
            ID = "user.id"
            Name = "user.name"
            SiteRole = "user.siteRole"
            LastLogin = "user.lastLogin"
            FullName = "user.fullName"
            Email = "user.email"
            AuthSetting = "user.authSetting"

        class View:
            ID = "view.id"
            Name = "view.name"
            ContentUrl = "view.contentUrl"
            CreatedAt = "view.createdAt"
            UpdatedAt = "view.updatedAt"
            Tags = "view.tags"
            SheetType = "view.sheetType"
            Usage = "view.usage"

        class Workbook:
            ID = "workbook.id"
            Description = "workbook.description"
            Name = "workbook.name"
            ContentUrl = "workbook.contentUrl"
            ShowTabs = "workbook.showTabs"
            Size = "workbook.size"
            CreatedAt = "workbook.createdAt"
            UpdatedAt = "workbook.updatedAt"
            SheetCount = "workbook.sheetCount"
            HasExtracts = "workbook.hasExtracts"
            Tags = "workbook.tags"
            WebpageUrl = "workbook.webpageUrl"
            DefaultViewId = "workbook.defaultViewId"


"""
These options can be used by methods that are fetching data exported from a specific content item
"""


class _DataExportOptions(RequestOptionsBase):
    def __init__(self, maxage: int = -1):
        super().__init__()
        self.view_filters: list[tuple[str, str]] = []
        self.view_parameters: list[tuple[str, str]] = []
        self.max_age: Optional[int] = maxage
        """
        This setting will affect the contents of the workbook as they are exported.
        Valid language values are tableau-supported languages like de, es, en
        If no locale is specified, the default locale for that language will be used
        """
        self.language: Optional[str] = None

    @property
    def max_age(self) -> int:
        return self._max_age

    @max_age.setter
    @property_is_int(range=(0, 240), allowed=[-1])
    def max_age(self, value):
        self._max_age = value

    def get_query_params(self):
        params = {}
        if self.max_age != -1:
            params["maxAge"] = self.max_age
        if self.language:
            params["language"] = self.language

        self._append_view_filters(params)
        return params

    def vf(self, name: str, value: str) -> Self:
        """Apply a filter based on a column within the view.
        Note that when filtering on a boolean type field, the only valid values are 'true' and 'false'

        For more detail see: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_filtering_and_sorting.htm#Filter-query-views

        Parameters
        ----------
        name: str
            The name of the column to filter on

        value: str
            The value to filter on

        Returns
        -------
        Self
            The current object
        """
        self.view_filters.append((name, value))
        return self

    def parameter(self, name: str, value: str) -> Self:
        """Apply a filter based on a parameter within the workbook.
        Note that when filtering on a boolean type field, the only valid values are 'true' and 'false'

        Parameters
        ----------
        name: str
            The name of the parameter to filter on

        value: str
            The value to filter on

        Returns
        -------
        Self
            The current object
        """
        prefix = "vf_Parameters."
        if name.startswith(prefix):
            proper_name = name
        elif name.startswith("Parameters."):
            proper_name = f"vf_{name}"
        else:
            proper_name = f"{prefix}{name}"
        self.view_parameters.append((proper_name, value))
        return self

    def _append_view_filters(self, params) -> None:
        for name, value in self.view_filters:
            params["vf_" + name] = value
        for name, value in self.view_parameters:
            params[name] = value


class _ImagePDFCommonExportOptions(_DataExportOptions):
    def __init__(self, maxage=-1, viz_height=None, viz_width=None):
        super().__init__(maxage=maxage)
        self.viz_height = viz_height
        self.viz_width = viz_width

    @property
    def viz_height(self):
        return self._viz_height

    @viz_height.setter
    @property_is_int(range=(0, sys.maxsize), allowed=(None,))
    def viz_height(self, value):
        self._viz_height = value

    @property
    def viz_width(self):
        return self._viz_width

    @viz_width.setter
    @property_is_int(range=(0, sys.maxsize), allowed=(None,))
    def viz_width(self, value):
        self._viz_width = value

    def get_query_params(self) -> dict:
        params = super().get_query_params()

        # XOR. Either both are None or both are not None.
        if (self.viz_height is None) ^ (self.viz_width is None):
            raise ValueError("viz_height and viz_width must be specified together")

        if self.viz_height is not None:
            params["vizHeight"] = self.viz_height

        if self.viz_width is not None:
            params["vizWidth"] = self.viz_width

        return params


class CSVRequestOptions(_DataExportOptions):
    """
    Options that can be used when exporting a view to CSV. Set the maxage to control the age of the data exported.
    Filters to the underlying data can be applied using the `vf` and `parameter` methods.

    Parameters
    ----------
    maxage: int, optional
        The maximum age of the data to export. Shortest possible duration is 1
        minute. No upper limit. Default is -1, which means no limit.
    """

    extension = "csv"


class ExcelRequestOptions(_DataExportOptions):
    """
    Options that can be used when exporting a view to Excel. Set the maxage to control the age of the data exported.
    Filters to the underlying data can be applied using the `vf` and `parameter` methods.

    Parameters
    ----------
    maxage: int, optional
        The maximum age of the data to export. Shortest possible duration is 1
        minute. No upper limit. Default is -1, which means no limit.
    """

    extension = "xlsx"


class ImageRequestOptions(_ImagePDFCommonExportOptions):
    """
    Options that can be used when exporting a view to an image. Set the maxage to control the age of the data exported.
    Filters to the underlying data can be applied using the `vf` and `parameter` methods.

    Parameters
    ----------
    imageresolution: str, optional
        The resolution of the image to export. Valid values are "high" or None. Default is None.
        Image width and actual pixel density are determined by the display context
        of the image. Aspect ratio is always preserved. Set the value to "high" to
        ensure maximum pixel density.

    maxage: int, optional
        The maximum age of the data to export. Shortest possible duration is 1
        minute. No upper limit. Default is -1, which means no limit.

    viz_height: int, optional
        The height of the viz in pixels. If specified, viz_width must also be specified.

    viz_width: int, optional
        The width of the viz in pixels. If specified, viz_height must also be specified.

    """

    extension = "png"

    # if 'high' isn't specified, the REST API endpoint returns an image with standard resolution
    class Resolution:
        High = "high"

    def __init__(self, imageresolution=None, maxage=-1, viz_height=None, viz_width=None):
        super().__init__(maxage=maxage, viz_height=viz_height, viz_width=viz_width)
        self.image_resolution = imageresolution

    def get_query_params(self):
        params = super().get_query_params()
        if self.image_resolution:
            params["resolution"] = self.image_resolution
        return params


class PDFRequestOptions(_ImagePDFCommonExportOptions):
    """
    Options that can be used when exporting a view to PDF. Set the maxage to control the age of the data exported.
    Filters to the underlying data can be applied using the `vf` and `parameter` methods.

    vf and parameter filters are only supported in API version 3.23 and later.

    Parameters
    ----------
    page_type: str, optional
        The page type of the PDF to export. Valid values are accessible via the `PageType` class.

    orientation: str, optional
        The orientation of the PDF to export. Valid values are accessible via the `Orientation` class.

    maxage: int, optional
        The maximum age of the data to export. Shortest possible duration is 1
        minute. No upper limit. Default is -1, which means no limit.

    viz_height: int, optional
        The height of the viz in pixels. If specified, viz_width must also be specified.

    viz_width: int, optional
        The width of the viz in pixels. If specified, viz_height must also be specified.
    """

    class PageType:
        A3 = "a3"
        A4 = "a4"
        A5 = "a5"
        B4 = "b4"
        B5 = "b5"
        Executive = "executive"
        Folio = "folio"
        Ledger = "ledger"
        Legal = "legal"
        Letter = "letter"
        Note = "note"
        Quarto = "quarto"
        Tabloid = "tabloid"
        Unspecified = "unspecified"

    class Orientation:
        Portrait = "portrait"
        Landscape = "landscape"

    def __init__(self, page_type=None, orientation=None, maxage=-1, viz_height=None, viz_width=None):
        super().__init__(maxage=maxage, viz_height=viz_height, viz_width=viz_width)
        self.page_type = page_type
        self.orientation = orientation

    def get_query_params(self) -> dict:
        params = super().get_query_params()
        if self.page_type:
            params["type"] = self.page_type

        if self.orientation:
            params["orientation"] = self.orientation

        return params


class PPTXRequestOptions(RequestOptionsBase):
    """
    Options that can be used when exporting a view to PPTX. Set the maxage to control the age of the data exported.

    Parameters
    ----------
    maxage: int, optional
        The maximum age of the data to export. Shortest possible duration is 1
        minute. No upper limit. Default is -1, which means no limit.
    """

    def __init__(self, maxage=-1):
        super().__init__()
        self.max_age = maxage

    @property
    def max_age(self) -> int:
        return self._max_age

    @max_age.setter
    @property_is_int(range=(0, 240), allowed=[-1])
    def max_age(self, value):
        self._max_age = value

    def get_query_params(self):
        params = {}
        if self.max_age != -1:
            params["maxAge"] = self.max_age

        return params
