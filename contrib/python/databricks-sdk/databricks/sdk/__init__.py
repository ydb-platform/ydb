# Code generated from OpenAPI specs by Databricks SDK Generator. DO NOT EDIT.

import json
import logging
from typing import List, Optional

import databricks.sdk.core as client
import databricks.sdk.dbutils as dbutils
from databricks.sdk import azure
from databricks.sdk.credentials_provider import CredentialsStrategy
from databricks.sdk.data_plane import DataPlaneTokenSource
from databricks.sdk.mixins.compute import ClustersExt
from databricks.sdk.mixins.files import DbfsExt, FilesExt
from databricks.sdk.mixins.jobs import JobsExt
from databricks.sdk.mixins.open_ai_client import ServingEndpointsExt
from databricks.sdk.mixins.workspace import WorkspaceExt
from databricks.sdk.oauth import AuthorizationDetail
from databricks.sdk.service import agentbricks as pkg_agentbricks
from databricks.sdk.service import apps as pkg_apps
from databricks.sdk.service import billing as pkg_billing
from databricks.sdk.service import catalog as pkg_catalog
from databricks.sdk.service import cleanrooms as pkg_cleanrooms
from databricks.sdk.service import compute as pkg_compute
from databricks.sdk.service import dashboards as pkg_dashboards
from databricks.sdk.service import database as pkg_database
from databricks.sdk.service import dataquality as pkg_dataquality
from databricks.sdk.service import files as pkg_files
from databricks.sdk.service import iam as pkg_iam
from databricks.sdk.service import iamv2 as pkg_iamv2
from databricks.sdk.service import jobs as pkg_jobs
from databricks.sdk.service import marketplace as pkg_marketplace
from databricks.sdk.service import ml as pkg_ml
from databricks.sdk.service import networking as pkg_networking
from databricks.sdk.service import oauth2 as pkg_oauth2
from databricks.sdk.service import pipelines as pkg_pipelines
from databricks.sdk.service import postgres as pkg_postgres
from databricks.sdk.service import provisioning as pkg_provisioning
from databricks.sdk.service import qualitymonitorv2 as pkg_qualitymonitorv2
from databricks.sdk.service import serving as pkg_serving
from databricks.sdk.service import settings as pkg_settings
from databricks.sdk.service import settingsv2 as pkg_settingsv2
from databricks.sdk.service import sharing as pkg_sharing
from databricks.sdk.service import sql as pkg_sql
from databricks.sdk.service import tags as pkg_tags
from databricks.sdk.service import vectorsearch as pkg_vectorsearch
from databricks.sdk.service import workspace as pkg_workspace
from databricks.sdk.service.agentbricks import AgentBricksAPI
from databricks.sdk.service.apps import AppsAPI, AppsSettingsAPI
from databricks.sdk.service.billing import (BillableUsageAPI, BudgetPolicyAPI,
                                            BudgetsAPI, LogDeliveryAPI,
                                            UsageDashboardsAPI)
from databricks.sdk.service.catalog import (AccountMetastoreAssignmentsAPI,
                                            AccountMetastoresAPI,
                                            AccountStorageCredentialsAPI,
                                            ArtifactAllowlistsAPI, CatalogsAPI,
                                            ConnectionsAPI, CredentialsAPI,
                                            EntityTagAssignmentsAPI,
                                            ExternalLineageAPI,
                                            ExternalLocationsAPI,
                                            ExternalMetadataAPI, FunctionsAPI,
                                            GrantsAPI, MetastoresAPI,
                                            ModelVersionsAPI, OnlineTablesAPI,
                                            PoliciesAPI, QualityMonitorsAPI,
                                            RegisteredModelsAPI,
                                            ResourceQuotasAPI, RfaAPI,
                                            SchemasAPI, StorageCredentialsAPI,
                                            SystemSchemasAPI,
                                            TableConstraintsAPI, TablesAPI,
                                            TemporaryPathCredentialsAPI,
                                            TemporaryTableCredentialsAPI,
                                            VolumesAPI, WorkspaceBindingsAPI)
from databricks.sdk.service.cleanrooms import (CleanRoomAssetRevisionsAPI,
                                               CleanRoomAssetsAPI,
                                               CleanRoomAutoApprovalRulesAPI,
                                               CleanRoomsAPI,
                                               CleanRoomTaskRunsAPI)
from databricks.sdk.service.compute import (ClusterPoliciesAPI, ClustersAPI,
                                            CommandExecutionAPI,
                                            GlobalInitScriptsAPI,
                                            InstancePoolsAPI,
                                            InstanceProfilesAPI, LibrariesAPI,
                                            PolicyComplianceForClustersAPI,
                                            PolicyFamiliesAPI)
from databricks.sdk.service.dashboards import (GenieAPI, LakeviewAPI,
                                               LakeviewEmbeddedAPI)
from databricks.sdk.service.database import DatabaseAPI
from databricks.sdk.service.dataquality import DataQualityAPI
from databricks.sdk.service.files import DbfsAPI, FilesAPI
from databricks.sdk.service.iam import (AccessControlAPI,
                                        AccountAccessControlAPI,
                                        AccountAccessControlProxyAPI,
                                        AccountGroupsAPI, AccountGroupsV2API,
                                        AccountServicePrincipalsAPI,
                                        AccountServicePrincipalsV2API,
                                        AccountUsersAPI, AccountUsersV2API,
                                        CurrentUserAPI, GroupsAPI, GroupsV2API,
                                        PermissionMigrationAPI, PermissionsAPI,
                                        ServicePrincipalsAPI,
                                        ServicePrincipalsV2API, UsersAPI,
                                        UsersV2API, WorkspaceAssignmentAPI)
from databricks.sdk.service.iamv2 import AccountIamV2API, WorkspaceIamV2API
from databricks.sdk.service.jobs import JobsAPI, PolicyComplianceForJobsAPI
from databricks.sdk.service.marketplace import (
    ConsumerFulfillmentsAPI, ConsumerInstallationsAPI, ConsumerListingsAPI,
    ConsumerPersonalizationRequestsAPI, ConsumerProvidersAPI,
    ProviderExchangeFiltersAPI, ProviderExchangesAPI, ProviderFilesAPI,
    ProviderListingsAPI, ProviderPersonalizationRequestsAPI,
    ProviderProviderAnalyticsDashboardsAPI, ProviderProvidersAPI)
from databricks.sdk.service.ml import (ExperimentsAPI, FeatureEngineeringAPI,
                                       FeatureStoreAPI, ForecastingAPI,
                                       MaterializedFeaturesAPI,
                                       ModelRegistryAPI)
from databricks.sdk.service.networking import EndpointsAPI
from databricks.sdk.service.oauth2 import (AccountFederationPolicyAPI,
                                           CustomAppIntegrationAPI,
                                           OAuthPublishedAppsAPI,
                                           PublishedAppIntegrationAPI,
                                           ServicePrincipalFederationPolicyAPI,
                                           ServicePrincipalSecretsAPI,
                                           ServicePrincipalSecretsProxyAPI)
from databricks.sdk.service.pipelines import PipelinesAPI
from databricks.sdk.service.postgres import PostgresAPI
from databricks.sdk.service.provisioning import (CredentialsAPI,
                                                 EncryptionKeysAPI,
                                                 NetworksAPI, PrivateAccessAPI,
                                                 StorageAPI, VpcEndpointsAPI,
                                                 Workspace, WorkspacesAPI)
from databricks.sdk.service.qualitymonitorv2 import QualityMonitorV2API
from databricks.sdk.service.serving import (ServingEndpointsAPI,
                                            ServingEndpointsDataPlaneAPI)
from databricks.sdk.service.settings import (
    AccountIpAccessListsAPI, AccountSettingsAPI,
    AibiDashboardEmbeddingAccessPolicyAPI,
    AibiDashboardEmbeddingApprovedDomainsAPI, AutomaticClusterUpdateAPI,
    ComplianceSecurityProfileAPI, CredentialsManagerAPI,
    CspEnablementAccountAPI, DashboardEmailSubscriptionsAPI,
    DefaultNamespaceAPI, DefaultWarehouseIdAPI, DisableLegacyAccessAPI,
    DisableLegacyDbfsAPI, DisableLegacyFeaturesAPI, EnableExportNotebookAPI,
    EnableIpAccessListsAPI, EnableNotebookTableClipboardAPI,
    EnableResultsDownloadingAPI, EnhancedSecurityMonitoringAPI,
    EsmEnablementAccountAPI, IpAccessListsAPI,
    LlmProxyPartnerPoweredAccountAPI, LlmProxyPartnerPoweredEnforceAPI,
    LlmProxyPartnerPoweredWorkspaceAPI, NetworkConnectivityAPI,
    NetworkPoliciesAPI, NotificationDestinationsAPI, PersonalComputeAPI,
    RestrictWorkspaceAdminsAPI, SettingsAPI, SqlResultsDownloadAPI,
    TokenManagementAPI, TokensAPI, WorkspaceConfAPI,
    WorkspaceNetworkConfigurationAPI)
from databricks.sdk.service.settingsv2 import (AccountSettingsV2API,
                                               WorkspaceSettingsV2API)
from databricks.sdk.service.sharing import (ProvidersAPI,
                                            RecipientActivationAPI,
                                            RecipientFederationPoliciesAPI,
                                            RecipientsAPI, SharesAPI)
from databricks.sdk.service.sql import (AlertsAPI, AlertsLegacyAPI,
                                        AlertsV2API, DashboardsAPI,
                                        DashboardWidgetsAPI, DataSourcesAPI,
                                        DbsqlPermissionsAPI, QueriesAPI,
                                        QueriesLegacyAPI, QueryHistoryAPI,
                                        QueryVisualizationsAPI,
                                        QueryVisualizationsLegacyAPI,
                                        RedashConfigAPI, StatementExecutionAPI,
                                        WarehousesAPI)
from databricks.sdk.service.tags import (TagPoliciesAPI,
                                         WorkspaceEntityTagAssignmentsAPI)
from databricks.sdk.service.vectorsearch import (VectorSearchEndpointsAPI,
                                                 VectorSearchIndexesAPI)
from databricks.sdk.service.workspace import (GitCredentialsAPI, ReposAPI,
                                              SecretsAPI, WorkspaceAPI)

_LOG = logging.getLogger(__name__)


def _make_dbutils(config: client.Config):
    # We try to directly check if we are in runtime, instead of
    # trying to import from databricks.sdk.runtime. This is to prevent
    # remote dbutils from being created without the config, which is both
    # expensive (will need to check all credential providers) and can
    # throw errors (when no env vars are set).
    try:
        from dbruntime import UserNamespaceInitializer
    except ImportError:
        return dbutils.RemoteDbUtils(config)

    # We are in runtime, so we can use the runtime dbutils
    from databricks.sdk.runtime import dbutils as runtime_dbutils

    return runtime_dbutils


def _make_files_client(apiClient: client.ApiClient, config: client.Config):
    return FilesExt(apiClient, config)


class WorkspaceClient:
    """
    The WorkspaceClient is a client for the workspace-level Databricks REST API.
    """

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        account_id: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token: Optional[str] = None,
        profile: Optional[str] = None,
        config_file: Optional[str] = None,
        azure_workspace_resource_id: Optional[str] = None,
        azure_client_secret: Optional[str] = None,
        azure_client_id: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
        azure_environment: Optional[str] = None,
        auth_type: Optional[str] = None,
        cluster_id: Optional[str] = None,
        google_credentials: Optional[str] = None,
        google_service_account: Optional[str] = None,
        debug_truncate_bytes: Optional[int] = None,
        debug_headers: Optional[bool] = None,
        product="unknown",
        product_version="0.0.0",
        credentials_strategy: Optional[CredentialsStrategy] = None,
        credentials_provider: Optional[CredentialsStrategy] = None,
        token_audience: Optional[str] = None,
        config: Optional[client.Config] = None,
        scopes: Optional[List[str]] = None,
        authorization_details: Optional[List[AuthorizationDetail]] = None,
        custom_headers: Optional[dict] = None,
    ):
        if not config:
            config = client.Config(
                host=host,
                account_id=account_id,
                username=username,
                password=password,
                client_id=client_id,
                client_secret=client_secret,
                token=token,
                profile=profile,
                config_file=config_file,
                azure_workspace_resource_id=azure_workspace_resource_id,
                azure_client_secret=azure_client_secret,
                azure_client_id=azure_client_id,
                azure_tenant_id=azure_tenant_id,
                azure_environment=azure_environment,
                auth_type=auth_type,
                cluster_id=cluster_id,
                google_credentials=google_credentials,
                google_service_account=google_service_account,
                credentials_strategy=credentials_strategy,
                credentials_provider=credentials_provider,
                debug_truncate_bytes=debug_truncate_bytes,
                debug_headers=debug_headers,
                product=product,
                product_version=product_version,
                token_audience=token_audience,
                scopes=scopes,
                authorization_details=(
                    json.dumps([detail.as_dict() for detail in authorization_details])
                    if authorization_details
                    else None
                ),
                custom_headers=custom_headers,
            )
        self._config = config.copy()
        self._dbutils = _make_dbutils(self._config)
        self._api_client = client.ApiClient(self._config)
        serving_endpoints = ServingEndpointsExt(self._api_client)
        self._access_control = pkg_iam.AccessControlAPI(self._api_client)
        self._account_access_control_proxy = pkg_iam.AccountAccessControlProxyAPI(self._api_client)
        self._agent_bricks = pkg_agentbricks.AgentBricksAPI(self._api_client)
        self._alerts = pkg_sql.AlertsAPI(self._api_client)
        self._alerts_legacy = pkg_sql.AlertsLegacyAPI(self._api_client)
        self._alerts_v2 = pkg_sql.AlertsV2API(self._api_client)
        self._apps = pkg_apps.AppsAPI(self._api_client)
        self._apps_settings = pkg_apps.AppsSettingsAPI(self._api_client)
        self._artifact_allowlists = pkg_catalog.ArtifactAllowlistsAPI(self._api_client)
        self._catalogs = pkg_catalog.CatalogsAPI(self._api_client)
        self._clean_room_asset_revisions = pkg_cleanrooms.CleanRoomAssetRevisionsAPI(self._api_client)
        self._clean_room_assets = pkg_cleanrooms.CleanRoomAssetsAPI(self._api_client)
        self._clean_room_auto_approval_rules = pkg_cleanrooms.CleanRoomAutoApprovalRulesAPI(self._api_client)
        self._clean_room_task_runs = pkg_cleanrooms.CleanRoomTaskRunsAPI(self._api_client)
        self._clean_rooms = pkg_cleanrooms.CleanRoomsAPI(self._api_client)
        self._cluster_policies = pkg_compute.ClusterPoliciesAPI(self._api_client)
        self._clusters = ClustersExt(self._api_client)
        self._command_execution = pkg_compute.CommandExecutionAPI(self._api_client)
        self._connections = pkg_catalog.ConnectionsAPI(self._api_client)
        self._consumer_fulfillments = pkg_marketplace.ConsumerFulfillmentsAPI(self._api_client)
        self._consumer_installations = pkg_marketplace.ConsumerInstallationsAPI(self._api_client)
        self._consumer_listings = pkg_marketplace.ConsumerListingsAPI(self._api_client)
        self._consumer_personalization_requests = pkg_marketplace.ConsumerPersonalizationRequestsAPI(self._api_client)
        self._consumer_providers = pkg_marketplace.ConsumerProvidersAPI(self._api_client)
        self._credentials = pkg_catalog.CredentialsAPI(self._api_client)
        self._credentials_manager = pkg_settings.CredentialsManagerAPI(self._api_client)
        self._current_user = pkg_iam.CurrentUserAPI(self._api_client)
        self._dashboard_widgets = pkg_sql.DashboardWidgetsAPI(self._api_client)
        self._dashboards = pkg_sql.DashboardsAPI(self._api_client)
        self._data_quality = pkg_dataquality.DataQualityAPI(self._api_client)
        self._data_sources = pkg_sql.DataSourcesAPI(self._api_client)
        self._database = pkg_database.DatabaseAPI(self._api_client)
        self._dbfs = DbfsExt(self._api_client)
        self._dbsql_permissions = pkg_sql.DbsqlPermissionsAPI(self._api_client)
        self._entity_tag_assignments = pkg_catalog.EntityTagAssignmentsAPI(self._api_client)
        self._experiments = pkg_ml.ExperimentsAPI(self._api_client)
        self._external_lineage = pkg_catalog.ExternalLineageAPI(self._api_client)
        self._external_locations = pkg_catalog.ExternalLocationsAPI(self._api_client)
        self._external_metadata = pkg_catalog.ExternalMetadataAPI(self._api_client)
        self._feature_engineering = pkg_ml.FeatureEngineeringAPI(self._api_client)
        self._feature_store = pkg_ml.FeatureStoreAPI(self._api_client)
        self._files = _make_files_client(self._api_client, self._config)
        self._forecasting = pkg_ml.ForecastingAPI(self._api_client)
        self._functions = pkg_catalog.FunctionsAPI(self._api_client)
        self._genie = pkg_dashboards.GenieAPI(self._api_client)
        self._git_credentials = pkg_workspace.GitCredentialsAPI(self._api_client)
        self._global_init_scripts = pkg_compute.GlobalInitScriptsAPI(self._api_client)
        self._grants = pkg_catalog.GrantsAPI(self._api_client)
        self._groups_v2 = pkg_iam.GroupsV2API(self._api_client)
        self._instance_pools = pkg_compute.InstancePoolsAPI(self._api_client)
        self._instance_profiles = pkg_compute.InstanceProfilesAPI(self._api_client)
        self._ip_access_lists = pkg_settings.IpAccessListsAPI(self._api_client)
        self._jobs = JobsExt(self._api_client)
        self._lakeview = pkg_dashboards.LakeviewAPI(self._api_client)
        self._lakeview_embedded = pkg_dashboards.LakeviewEmbeddedAPI(self._api_client)
        self._libraries = pkg_compute.LibrariesAPI(self._api_client)
        self._materialized_features = pkg_ml.MaterializedFeaturesAPI(self._api_client)
        self._metastores = pkg_catalog.MetastoresAPI(self._api_client)
        self._model_registry = pkg_ml.ModelRegistryAPI(self._api_client)
        self._model_versions = pkg_catalog.ModelVersionsAPI(self._api_client)
        self._notification_destinations = pkg_settings.NotificationDestinationsAPI(self._api_client)
        self._online_tables = pkg_catalog.OnlineTablesAPI(self._api_client)
        self._permission_migration = pkg_iam.PermissionMigrationAPI(self._api_client)
        self._permissions = pkg_iam.PermissionsAPI(self._api_client)
        self._pipelines = pkg_pipelines.PipelinesAPI(self._api_client)
        self._policies = pkg_catalog.PoliciesAPI(self._api_client)
        self._policy_compliance_for_clusters = pkg_compute.PolicyComplianceForClustersAPI(self._api_client)
        self._policy_compliance_for_jobs = pkg_jobs.PolicyComplianceForJobsAPI(self._api_client)
        self._policy_families = pkg_compute.PolicyFamiliesAPI(self._api_client)
        self._postgres = pkg_postgres.PostgresAPI(self._api_client)
        self._provider_exchange_filters = pkg_marketplace.ProviderExchangeFiltersAPI(self._api_client)
        self._provider_exchanges = pkg_marketplace.ProviderExchangesAPI(self._api_client)
        self._provider_files = pkg_marketplace.ProviderFilesAPI(self._api_client)
        self._provider_listings = pkg_marketplace.ProviderListingsAPI(self._api_client)
        self._provider_personalization_requests = pkg_marketplace.ProviderPersonalizationRequestsAPI(self._api_client)
        self._provider_provider_analytics_dashboards = pkg_marketplace.ProviderProviderAnalyticsDashboardsAPI(
            self._api_client
        )
        self._provider_providers = pkg_marketplace.ProviderProvidersAPI(self._api_client)
        self._providers = pkg_sharing.ProvidersAPI(self._api_client)
        self._quality_monitor_v2 = pkg_qualitymonitorv2.QualityMonitorV2API(self._api_client)
        self._quality_monitors = pkg_catalog.QualityMonitorsAPI(self._api_client)
        self._queries = pkg_sql.QueriesAPI(self._api_client)
        self._queries_legacy = pkg_sql.QueriesLegacyAPI(self._api_client)
        self._query_history = pkg_sql.QueryHistoryAPI(self._api_client)
        self._query_visualizations = pkg_sql.QueryVisualizationsAPI(self._api_client)
        self._query_visualizations_legacy = pkg_sql.QueryVisualizationsLegacyAPI(self._api_client)
        self._recipient_activation = pkg_sharing.RecipientActivationAPI(self._api_client)
        self._recipient_federation_policies = pkg_sharing.RecipientFederationPoliciesAPI(self._api_client)
        self._recipients = pkg_sharing.RecipientsAPI(self._api_client)
        self._redash_config = pkg_sql.RedashConfigAPI(self._api_client)
        self._registered_models = pkg_catalog.RegisteredModelsAPI(self._api_client)
        self._repos = pkg_workspace.ReposAPI(self._api_client)
        self._resource_quotas = pkg_catalog.ResourceQuotasAPI(self._api_client)
        self._rfa = pkg_catalog.RfaAPI(self._api_client)
        self._schemas = pkg_catalog.SchemasAPI(self._api_client)
        self._secrets = pkg_workspace.SecretsAPI(self._api_client)
        self._service_principal_secrets_proxy = pkg_oauth2.ServicePrincipalSecretsProxyAPI(self._api_client)
        self._service_principals_v2 = pkg_iam.ServicePrincipalsV2API(self._api_client)
        self._serving_endpoints = serving_endpoints
        serving_endpoints_data_plane_token_source = DataPlaneTokenSource(
            self._config.host, self._config.oauth_token, self._config.disable_async_token_refresh
        )
        self._serving_endpoints_data_plane = pkg_serving.ServingEndpointsDataPlaneAPI(
            self._api_client, serving_endpoints, serving_endpoints_data_plane_token_source
        )
        self._settings = pkg_settings.SettingsAPI(self._api_client)
        self._shares = pkg_sharing.SharesAPI(self._api_client)
        self._statement_execution = pkg_sql.StatementExecutionAPI(self._api_client)
        self._storage_credentials = pkg_catalog.StorageCredentialsAPI(self._api_client)
        self._system_schemas = pkg_catalog.SystemSchemasAPI(self._api_client)
        self._table_constraints = pkg_catalog.TableConstraintsAPI(self._api_client)
        self._tables = pkg_catalog.TablesAPI(self._api_client)
        self._tag_policies = pkg_tags.TagPoliciesAPI(self._api_client)
        self._temporary_path_credentials = pkg_catalog.TemporaryPathCredentialsAPI(self._api_client)
        self._temporary_table_credentials = pkg_catalog.TemporaryTableCredentialsAPI(self._api_client)
        self._token_management = pkg_settings.TokenManagementAPI(self._api_client)
        self._tokens = pkg_settings.TokensAPI(self._api_client)
        self._users_v2 = pkg_iam.UsersV2API(self._api_client)
        self._vector_search_endpoints = pkg_vectorsearch.VectorSearchEndpointsAPI(self._api_client)
        self._vector_search_indexes = pkg_vectorsearch.VectorSearchIndexesAPI(self._api_client)
        self._volumes = pkg_catalog.VolumesAPI(self._api_client)
        self._warehouses = pkg_sql.WarehousesAPI(self._api_client)
        self._workspace = WorkspaceExt(self._api_client)
        self._workspace_bindings = pkg_catalog.WorkspaceBindingsAPI(self._api_client)
        self._workspace_conf = pkg_settings.WorkspaceConfAPI(self._api_client)
        self._workspace_entity_tag_assignments = pkg_tags.WorkspaceEntityTagAssignmentsAPI(self._api_client)
        self._workspace_iam_v2 = pkg_iamv2.WorkspaceIamV2API(self._api_client)
        self._workspace_settings_v2 = pkg_settingsv2.WorkspaceSettingsV2API(self._api_client)
        self._groups = pkg_iam.GroupsAPI(self._api_client)
        self._service_principals = pkg_iam.ServicePrincipalsAPI(self._api_client)
        self._users = pkg_iam.UsersAPI(self._api_client)

    @property
    def config(self) -> client.Config:
        return self._config

    @property
    def api_client(self) -> client.ApiClient:
        return self._api_client

    @property
    def dbutils(self) -> dbutils.RemoteDbUtils:
        return self._dbutils

    @property
    def access_control(self) -> pkg_iam.AccessControlAPI:
        """Rule based Access Control for Databricks Resources."""
        return self._access_control

    @property
    def account_access_control_proxy(self) -> pkg_iam.AccountAccessControlProxyAPI:
        """These APIs manage access rules on resources in an account."""
        return self._account_access_control_proxy

    @property
    def agent_bricks(self) -> pkg_agentbricks.AgentBricksAPI:
        """The Custom LLMs service manages state and powers the UI for the Custom LLM product."""
        return self._agent_bricks

    @property
    def alerts(self) -> pkg_sql.AlertsAPI:
        """The alerts API can be used to perform CRUD operations on alerts."""
        return self._alerts

    @property
    def alerts_legacy(self) -> pkg_sql.AlertsLegacyAPI:
        """The alerts API can be used to perform CRUD operations on alerts."""
        return self._alerts_legacy

    @property
    def alerts_v2(self) -> pkg_sql.AlertsV2API:
        """New version of SQL Alerts."""
        return self._alerts_v2

    @property
    def apps(self) -> pkg_apps.AppsAPI:
        """Apps run directly on a customer's Databricks instance, integrate with their data, use and extend Databricks services, and enable users to interact through single sign-on."""
        return self._apps

    @property
    def apps_settings(self) -> pkg_apps.AppsSettingsAPI:
        """Apps Settings manage the settings for the Apps service on a customer's Databricks instance."""
        return self._apps_settings

    @property
    def artifact_allowlists(self) -> pkg_catalog.ArtifactAllowlistsAPI:
        """In Databricks Runtime 13.3 and above, you can add libraries and init scripts to the `allowlist` in UC so that users can leverage these artifacts on compute configured with shared access mode."""
        return self._artifact_allowlists

    @property
    def catalogs(self) -> pkg_catalog.CatalogsAPI:
        """A catalog is the first layer of Unity Catalog’s three-level namespace."""
        return self._catalogs

    @property
    def clean_room_asset_revisions(self) -> pkg_cleanrooms.CleanRoomAssetRevisionsAPI:
        """Clean Room Asset Revisions denote new versions of uploaded assets (e.g."""
        return self._clean_room_asset_revisions

    @property
    def clean_room_assets(self) -> pkg_cleanrooms.CleanRoomAssetsAPI:
        """Clean room assets are data and code objects — Tables, volumes, and notebooks that are shared with the clean room."""
        return self._clean_room_assets

    @property
    def clean_room_auto_approval_rules(self) -> pkg_cleanrooms.CleanRoomAutoApprovalRulesAPI:
        """Clean room auto-approval rules automatically create an approval on your behalf when an asset (e.g."""
        return self._clean_room_auto_approval_rules

    @property
    def clean_room_task_runs(self) -> pkg_cleanrooms.CleanRoomTaskRunsAPI:
        """Clean room task runs are the executions of notebooks in a clean room."""
        return self._clean_room_task_runs

    @property
    def clean_rooms(self) -> pkg_cleanrooms.CleanRoomsAPI:
        """A clean room uses Delta Sharing and serverless compute to provide a secure and privacy-protecting environment where multiple parties can work together on sensitive enterprise data without direct access to each other's data."""
        return self._clean_rooms

    @property
    def cluster_policies(self) -> pkg_compute.ClusterPoliciesAPI:
        """You can use cluster policies to control users' ability to configure clusters based on a set of rules."""
        return self._cluster_policies

    @property
    def clusters(self) -> ClustersExt:
        """The Clusters API allows you to create, start, edit, list, terminate, and delete clusters."""
        return self._clusters

    @property
    def command_execution(self) -> pkg_compute.CommandExecutionAPI:
        """This API allows execution of Python, Scala, SQL, or R commands on running Databricks Clusters."""
        return self._command_execution

    @property
    def connections(self) -> pkg_catalog.ConnectionsAPI:
        """Connections allow for creating a connection to an external data source."""
        return self._connections

    @property
    def consumer_fulfillments(self) -> pkg_marketplace.ConsumerFulfillmentsAPI:
        """Fulfillments are entities that allow consumers to preview installations."""
        return self._consumer_fulfillments

    @property
    def consumer_installations(self) -> pkg_marketplace.ConsumerInstallationsAPI:
        """Installations are entities that allow consumers to interact with Databricks Marketplace listings."""
        return self._consumer_installations

    @property
    def consumer_listings(self) -> pkg_marketplace.ConsumerListingsAPI:
        """Listings are the core entities in the Marketplace."""
        return self._consumer_listings

    @property
    def consumer_personalization_requests(self) -> pkg_marketplace.ConsumerPersonalizationRequestsAPI:
        """Personalization Requests allow customers to interact with the individualized Marketplace listing flow."""
        return self._consumer_personalization_requests

    @property
    def consumer_providers(self) -> pkg_marketplace.ConsumerProvidersAPI:
        """Providers are the entities that publish listings to the Marketplace."""
        return self._consumer_providers

    @property
    def credentials(self) -> pkg_catalog.CredentialsAPI:
        """A credential represents an authentication and authorization mechanism for accessing services on your cloud tenant."""
        return self._credentials

    @property
    def credentials_manager(self) -> pkg_settings.CredentialsManagerAPI:
        """Credentials manager interacts with with Identity Providers to to perform token exchanges using stored credentials and refresh tokens."""
        return self._credentials_manager

    @property
    def current_user(self) -> pkg_iam.CurrentUserAPI:
        """This API allows retrieving information about currently authenticated user or service principal."""
        return self._current_user

    @property
    def dashboard_widgets(self) -> pkg_sql.DashboardWidgetsAPI:
        """This is an evolving API that facilitates the addition and removal of widgets from existing dashboards within the Databricks Workspace."""
        return self._dashboard_widgets

    @property
    def dashboards(self) -> pkg_sql.DashboardsAPI:
        """In general, there is little need to modify dashboards using the API."""
        return self._dashboards

    @property
    def data_quality(self) -> pkg_dataquality.DataQualityAPI:
        """Manage the data quality of Unity Catalog objects (currently support `schema` and `table`)."""
        return self._data_quality

    @property
    def data_sources(self) -> pkg_sql.DataSourcesAPI:
        """This API is provided to assist you in making new query objects."""
        return self._data_sources

    @property
    def database(self) -> pkg_database.DatabaseAPI:
        """Database Instances provide access to a database via REST API or direct SQL."""
        return self._database

    @property
    def dbfs(self) -> DbfsExt:
        """DBFS API makes it simple to interact with various data sources without having to include a users credentials every time to read a file."""
        return self._dbfs

    @property
    def dbsql_permissions(self) -> pkg_sql.DbsqlPermissionsAPI:
        """The SQL Permissions API is similar to the endpoints of the :method:permissions/set."""
        return self._dbsql_permissions

    @property
    def entity_tag_assignments(self) -> pkg_catalog.EntityTagAssignmentsAPI:
        """Tags are attributes that include keys and optional values that you can use to organize and categorize entities in Unity Catalog."""
        return self._entity_tag_assignments

    @property
    def experiments(self) -> pkg_ml.ExperimentsAPI:
        """Experiments are the primary unit of organization in MLflow; all MLflow runs belong to an experiment."""
        return self._experiments

    @property
    def external_lineage(self) -> pkg_catalog.ExternalLineageAPI:
        """External Lineage APIs enable defining and managing lineage relationships between Databricks objects and external systems."""
        return self._external_lineage

    @property
    def external_locations(self) -> pkg_catalog.ExternalLocationsAPI:
        """An external location is an object that combines a cloud storage path with a storage credential that authorizes access to the cloud storage path."""
        return self._external_locations

    @property
    def external_metadata(self) -> pkg_catalog.ExternalMetadataAPI:
        """External Metadata objects enable customers to register and manage metadata about external systems within Unity Catalog."""
        return self._external_metadata

    @property
    def feature_engineering(self) -> pkg_ml.FeatureEngineeringAPI:
        """[description]."""
        return self._feature_engineering

    @property
    def feature_store(self) -> pkg_ml.FeatureStoreAPI:
        """A feature store is a centralized repository that enables data scientists to find and share features."""
        return self._feature_store

    @property
    def forecasting(self) -> pkg_ml.ForecastingAPI:
        """The Forecasting API allows you to create and get serverless forecasting experiments."""
        return self._forecasting

    @property
    def functions(self) -> pkg_catalog.FunctionsAPI:
        """Functions implement User-Defined Functions (UDFs) in Unity Catalog."""
        return self._functions

    @property
    def genie(self) -> pkg_dashboards.GenieAPI:
        """Genie provides a no-code experience for business users, powered by AI/BI."""
        return self._genie

    @property
    def git_credentials(self) -> pkg_workspace.GitCredentialsAPI:
        """Registers personal access token for Databricks to do operations on behalf of the user."""
        return self._git_credentials

    @property
    def global_init_scripts(self) -> pkg_compute.GlobalInitScriptsAPI:
        """The Global Init Scripts API enables Workspace administrators to configure global initialization scripts for their workspace."""
        return self._global_init_scripts

    @property
    def grants(self) -> pkg_catalog.GrantsAPI:
        """In Unity Catalog, data is secure by default."""
        return self._grants

    @property
    def groups_v2(self) -> pkg_iam.GroupsV2API:
        """Groups simplify identity management, making it easier to assign access to Databricks workspace, data, and other securable objects."""
        return self._groups_v2

    @property
    def instance_pools(self) -> pkg_compute.InstancePoolsAPI:
        """Instance Pools API are used to create, edit, delete and list instance pools by using ready-to-use cloud instances which reduces a cluster start and auto-scaling times."""
        return self._instance_pools

    @property
    def instance_profiles(self) -> pkg_compute.InstanceProfilesAPI:
        """The Instance Profiles API allows admins to add, list, and remove instance profiles that users can launch clusters with."""
        return self._instance_profiles

    @property
    def ip_access_lists(self) -> pkg_settings.IpAccessListsAPI:
        """IP Access List enables admins to configure IP access lists."""
        return self._ip_access_lists

    @property
    def jobs(self) -> JobsExt:
        """The Jobs API allows you to create, edit, and delete jobs."""
        return self._jobs

    @property
    def lakeview(self) -> pkg_dashboards.LakeviewAPI:
        """These APIs provide specific management operations for Lakeview dashboards."""
        return self._lakeview

    @property
    def lakeview_embedded(self) -> pkg_dashboards.LakeviewEmbeddedAPI:
        """Token-based Lakeview APIs for embedding dashboards in external applications."""
        return self._lakeview_embedded

    @property
    def libraries(self) -> pkg_compute.LibrariesAPI:
        """The Libraries API allows you to install and uninstall libraries and get the status of libraries on a cluster."""
        return self._libraries

    @property
    def materialized_features(self) -> pkg_ml.MaterializedFeaturesAPI:
        """Materialized Features are columns in tables and views that can be directly used as features to train and serve ML models."""
        return self._materialized_features

    @property
    def metastores(self) -> pkg_catalog.MetastoresAPI:
        """A metastore is the top-level container of objects in Unity Catalog."""
        return self._metastores

    @property
    def model_registry(self) -> pkg_ml.ModelRegistryAPI:
        """Note: This API reference documents APIs for the Workspace Model Registry."""
        return self._model_registry

    @property
    def model_versions(self) -> pkg_catalog.ModelVersionsAPI:
        """Databricks provides a hosted version of MLflow Model Registry in Unity Catalog."""
        return self._model_versions

    @property
    def notification_destinations(self) -> pkg_settings.NotificationDestinationsAPI:
        """The notification destinations API lets you programmatically manage a workspace's notification destinations."""
        return self._notification_destinations

    @property
    def online_tables(self) -> pkg_catalog.OnlineTablesAPI:
        """Online tables provide lower latency and higher QPS access to data from Delta tables."""
        return self._online_tables

    @property
    def permission_migration(self) -> pkg_iam.PermissionMigrationAPI:
        """APIs for migrating acl permissions, used only by the ucx tool: https://github.com/databrickslabs/ucx."""
        return self._permission_migration

    @property
    def permissions(self) -> pkg_iam.PermissionsAPI:
        """Permissions API are used to create read, write, edit, update and manage access for various users on different objects and endpoints."""
        return self._permissions

    @property
    def pipelines(self) -> pkg_pipelines.PipelinesAPI:
        """The Lakeflow Spark Declarative Pipelines API allows you to create, edit, delete, start, and view details about pipelines."""
        return self._pipelines

    @property
    def policies(self) -> pkg_catalog.PoliciesAPI:
        """Attribute-Based Access Control (ABAC) provides high leverage governance for enforcing compliance policies in Unity Catalog."""
        return self._policies

    @property
    def policy_compliance_for_clusters(self) -> pkg_compute.PolicyComplianceForClustersAPI:
        """The policy compliance APIs allow you to view and manage the policy compliance status of clusters in your workspace."""
        return self._policy_compliance_for_clusters

    @property
    def policy_compliance_for_jobs(self) -> pkg_jobs.PolicyComplianceForJobsAPI:
        """The compliance APIs allow you to view and manage the policy compliance status of jobs in your workspace."""
        return self._policy_compliance_for_jobs

    @property
    def policy_families(self) -> pkg_compute.PolicyFamiliesAPI:
        """View available policy families."""
        return self._policy_families

    @property
    def postgres(self) -> pkg_postgres.PostgresAPI:
        """Use the Postgres API to create and manage Lakebase Autoscaling Postgres infrastructure, including projects, branches, compute endpoints, and roles."""
        return self._postgres

    @property
    def provider_exchange_filters(self) -> pkg_marketplace.ProviderExchangeFiltersAPI:
        """Marketplace exchanges filters curate which groups can access an exchange."""
        return self._provider_exchange_filters

    @property
    def provider_exchanges(self) -> pkg_marketplace.ProviderExchangesAPI:
        """Marketplace exchanges allow providers to share their listings with a curated set of customers."""
        return self._provider_exchanges

    @property
    def provider_files(self) -> pkg_marketplace.ProviderFilesAPI:
        """Marketplace offers a set of file APIs for various purposes such as preview notebooks and provider icons."""
        return self._provider_files

    @property
    def provider_listings(self) -> pkg_marketplace.ProviderListingsAPI:
        """Listings are the core entities in the Marketplace."""
        return self._provider_listings

    @property
    def provider_personalization_requests(self) -> pkg_marketplace.ProviderPersonalizationRequestsAPI:
        """Personalization requests are an alternate to instantly available listings."""
        return self._provider_personalization_requests

    @property
    def provider_provider_analytics_dashboards(self) -> pkg_marketplace.ProviderProviderAnalyticsDashboardsAPI:
        """Manage templated analytics solution for providers."""
        return self._provider_provider_analytics_dashboards

    @property
    def provider_providers(self) -> pkg_marketplace.ProviderProvidersAPI:
        """Providers are entities that manage assets in Marketplace."""
        return self._provider_providers

    @property
    def providers(self) -> pkg_sharing.ProvidersAPI:
        """A data provider is an object representing the organization in the real world who shares the data."""
        return self._providers

    @property
    def quality_monitor_v2(self) -> pkg_qualitymonitorv2.QualityMonitorV2API:
        """Deprecated: Please use the Data Quality Monitoring API instead (REST: /api/data-quality/v1/monitors)."""
        return self._quality_monitor_v2

    @property
    def quality_monitors(self) -> pkg_catalog.QualityMonitorsAPI:
        """Deprecated: Please use the Data Quality Monitors API instead (REST: /api/data-quality/v1/monitors), which manages both Data Profiling and Anomaly Detection."""
        return self._quality_monitors

    @property
    def queries(self) -> pkg_sql.QueriesAPI:
        """The queries API can be used to perform CRUD operations on queries."""
        return self._queries

    @property
    def queries_legacy(self) -> pkg_sql.QueriesLegacyAPI:
        """These endpoints are used for CRUD operations on query definitions."""
        return self._queries_legacy

    @property
    def query_history(self) -> pkg_sql.QueryHistoryAPI:
        """A service responsible for storing and retrieving the list of queries run against SQL endpoints and serverless compute."""
        return self._query_history

    @property
    def query_visualizations(self) -> pkg_sql.QueryVisualizationsAPI:
        """This is an evolving API that facilitates the addition and removal of visualizations from existing queries in the Databricks Workspace."""
        return self._query_visualizations

    @property
    def query_visualizations_legacy(self) -> pkg_sql.QueryVisualizationsLegacyAPI:
        """This is an evolving API that facilitates the addition and removal of vizualisations from existing queries within the Databricks Workspace."""
        return self._query_visualizations_legacy

    @property
    def recipient_activation(self) -> pkg_sharing.RecipientActivationAPI:
        """The Recipient Activation API is only applicable in the open sharing model where the recipient object has the authentication type of `TOKEN`."""
        return self._recipient_activation

    @property
    def recipient_federation_policies(self) -> pkg_sharing.RecipientFederationPoliciesAPI:
        """The Recipient Federation Policies APIs are only applicable in the open sharing model where the recipient object has the authentication type of `OIDC_RECIPIENT`, enabling data sharing from Databricks to non-Databricks recipients."""
        return self._recipient_federation_policies

    @property
    def recipients(self) -> pkg_sharing.RecipientsAPI:
        """A recipient is an object you create using :method:recipients/create to represent an organization which you want to allow access shares."""
        return self._recipients

    @property
    def redash_config(self) -> pkg_sql.RedashConfigAPI:
        """Redash V2 service for workspace configurations (internal)."""
        return self._redash_config

    @property
    def registered_models(self) -> pkg_catalog.RegisteredModelsAPI:
        """Databricks provides a hosted version of MLflow Model Registry in Unity Catalog."""
        return self._registered_models

    @property
    def repos(self) -> pkg_workspace.ReposAPI:
        """The Repos API allows users to manage their git repos."""
        return self._repos

    @property
    def resource_quotas(self) -> pkg_catalog.ResourceQuotasAPI:
        """Unity Catalog enforces resource quotas on all securable objects, which limits the number of resources that can be created."""
        return self._resource_quotas

    @property
    def rfa(self) -> pkg_catalog.RfaAPI:
        """Request for Access enables users to request access for Unity Catalog securables."""
        return self._rfa

    @property
    def schemas(self) -> pkg_catalog.SchemasAPI:
        """A schema (also called a database) is the second layer of Unity Catalog’s three-level namespace."""
        return self._schemas

    @property
    def secrets(self) -> pkg_workspace.SecretsAPI:
        """The Secrets API allows you to manage secrets, secret scopes, and access permissions."""
        return self._secrets

    @property
    def service_principal_secrets_proxy(self) -> pkg_oauth2.ServicePrincipalSecretsProxyAPI:
        """These APIs enable administrators to manage service principal secrets at the workspace level."""
        return self._service_principal_secrets_proxy

    @property
    def service_principals_v2(self) -> pkg_iam.ServicePrincipalsV2API:
        """Identities for use with jobs, automated tools, and systems such as scripts, apps, and CI/CD platforms."""
        return self._service_principals_v2

    @property
    def serving_endpoints(self) -> ServingEndpointsExt:
        """The Serving Endpoints API allows you to create, update, and delete model serving endpoints."""
        return self._serving_endpoints

    @property
    def serving_endpoints_data_plane(self) -> pkg_serving.ServingEndpointsDataPlaneAPI:
        """Serving endpoints DataPlane provides a set of operations to interact with data plane endpoints for Serving endpoints service."""
        return self._serving_endpoints_data_plane

    @property
    def settings(self) -> pkg_settings.SettingsAPI:
        """Workspace Settings API allows users to manage settings at the workspace level."""
        return self._settings

    @property
    def shares(self) -> pkg_sharing.SharesAPI:
        """A share is a container instantiated with :method:shares/create."""
        return self._shares

    @property
    def statement_execution(self) -> pkg_sql.StatementExecutionAPI:
        """The Databricks SQL Statement Execution API can be used to execute SQL statements on a SQL warehouse and fetch the result."""
        return self._statement_execution

    @property
    def storage_credentials(self) -> pkg_catalog.StorageCredentialsAPI:
        """A storage credential represents an authentication and authorization mechanism for accessing data stored on your cloud tenant."""
        return self._storage_credentials

    @property
    def system_schemas(self) -> pkg_catalog.SystemSchemasAPI:
        """A system schema is a schema that lives within the system catalog."""
        return self._system_schemas

    @property
    def table_constraints(self) -> pkg_catalog.TableConstraintsAPI:
        """Primary key and foreign key constraints encode relationships between fields in tables."""
        return self._table_constraints

    @property
    def tables(self) -> pkg_catalog.TablesAPI:
        """A table resides in the third layer of Unity Catalog’s three-level namespace."""
        return self._tables

    @property
    def tag_policies(self) -> pkg_tags.TagPoliciesAPI:
        """The Tag Policy API allows you to manage policies for governed tags in Databricks."""
        return self._tag_policies

    @property
    def temporary_path_credentials(self) -> pkg_catalog.TemporaryPathCredentialsAPI:
        """Temporary Path Credentials refer to short-lived, downscoped credentials used to access external cloud storage locations registered in Databricks."""
        return self._temporary_path_credentials

    @property
    def temporary_table_credentials(self) -> pkg_catalog.TemporaryTableCredentialsAPI:
        """Temporary Table Credentials refer to short-lived, downscoped credentials used to access cloud storage locations where table data is stored in Databricks."""
        return self._temporary_table_credentials

    @property
    def token_management(self) -> pkg_settings.TokenManagementAPI:
        """Enables administrators to get all tokens and delete tokens for other users."""
        return self._token_management

    @property
    def tokens(self) -> pkg_settings.TokensAPI:
        """The Token API allows you to create, list, and revoke tokens that can be used to authenticate and access Databricks REST APIs."""
        return self._tokens

    @property
    def users_v2(self) -> pkg_iam.UsersV2API:
        """User identities recognized by Databricks and represented by email addresses."""
        return self._users_v2

    @property
    def vector_search_endpoints(self) -> pkg_vectorsearch.VectorSearchEndpointsAPI:
        """**Endpoint**: Represents the compute resources to host vector search indexes."""
        return self._vector_search_endpoints

    @property
    def vector_search_indexes(self) -> pkg_vectorsearch.VectorSearchIndexesAPI:
        """**Index**: An efficient representation of your embedding vectors that supports real-time and efficient approximate nearest neighbor (ANN) search queries."""
        return self._vector_search_indexes

    @property
    def volumes(self) -> pkg_catalog.VolumesAPI:
        """Volumes are a Unity Catalog (UC) capability for accessing, storing, governing, organizing and processing files."""
        return self._volumes

    @property
    def warehouses(self) -> pkg_sql.WarehousesAPI:
        """A SQL warehouse is a compute resource that lets you run SQL commands on data objects within Databricks SQL."""
        return self._warehouses

    @property
    def workspace(self) -> WorkspaceExt:
        """The Workspace API allows you to list, import, export, and delete notebooks and folders."""
        return self._workspace

    @property
    def workspace_bindings(self) -> pkg_catalog.WorkspaceBindingsAPI:
        """A securable in Databricks can be configured as __OPEN__ or __ISOLATED__."""
        return self._workspace_bindings

    @property
    def workspace_conf(self) -> pkg_settings.WorkspaceConfAPI:
        """This API allows updating known workspace settings for advanced users."""
        return self._workspace_conf

    @property
    def workspace_entity_tag_assignments(self) -> pkg_tags.WorkspaceEntityTagAssignmentsAPI:
        """Manage tag assignments on workspace-scoped objects."""
        return self._workspace_entity_tag_assignments

    @property
    def workspace_iam_v2(self) -> pkg_iamv2.WorkspaceIamV2API:
        """These APIs are used to manage identities and the workspace access of these identities in <Databricks>."""
        return self._workspace_iam_v2

    @property
    def workspace_settings_v2(self) -> pkg_settingsv2.WorkspaceSettingsV2API:
        """APIs to manage workspace level settings."""
        return self._workspace_settings_v2

    @property
    def groups(self) -> pkg_iam.GroupsAPI:
        """Groups simplify identity management, making it easier to assign access to Databricks workspace, data, and other securable objects."""
        return self._groups

    @property
    def service_principals(self) -> pkg_iam.ServicePrincipalsAPI:
        """Identities for use with jobs, automated tools, and systems such as scripts, apps, and CI/CD platforms."""
        return self._service_principals

    @property
    def users(self) -> pkg_iam.UsersAPI:
        """User identities recognized by Databricks and represented by email addresses."""
        return self._users

    @property
    def files(self) -> FilesExt:
        """The Files API is a standard HTTP API that allows you to read, write, list, and delete files and directories by referring to their URI."""
        return self._files

    def get_workspace_id(self) -> int:
        """Get the workspace ID of the workspace that this client is connected to."""
        response = self._api_client.do("GET", "/api/2.0/preview/scim/v2/Me", response_headers=["X-Databricks-Org-Id"])
        return int(response["X-Databricks-Org-Id"])

    def __repr__(self):
        return f"WorkspaceClient(host='{self._config.host}', auth_type='{self._config.auth_type}', ...)"


class AccountClient:
    """
    The AccountClient is a client for the account-level Databricks REST API.
    """

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        account_id: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token: Optional[str] = None,
        profile: Optional[str] = None,
        config_file: Optional[str] = None,
        azure_workspace_resource_id: Optional[str] = None,
        azure_client_secret: Optional[str] = None,
        azure_client_id: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
        azure_environment: Optional[str] = None,
        auth_type: Optional[str] = None,
        cluster_id: Optional[str] = None,
        google_credentials: Optional[str] = None,
        google_service_account: Optional[str] = None,
        debug_truncate_bytes: Optional[int] = None,
        debug_headers: Optional[bool] = None,
        product="unknown",
        product_version="0.0.0",
        credentials_strategy: Optional[CredentialsStrategy] = None,
        credentials_provider: Optional[CredentialsStrategy] = None,
        token_audience: Optional[str] = None,
        config: Optional[client.Config] = None,
        custom_headers: Optional[dict] = None,
    ):
        if not config:
            config = client.Config(
                host=host,
                account_id=account_id,
                username=username,
                password=password,
                client_id=client_id,
                client_secret=client_secret,
                token=token,
                profile=profile,
                config_file=config_file,
                azure_workspace_resource_id=azure_workspace_resource_id,
                azure_client_secret=azure_client_secret,
                azure_client_id=azure_client_id,
                azure_tenant_id=azure_tenant_id,
                azure_environment=azure_environment,
                auth_type=auth_type,
                cluster_id=cluster_id,
                google_credentials=google_credentials,
                google_service_account=google_service_account,
                credentials_strategy=credentials_strategy,
                credentials_provider=credentials_provider,
                debug_truncate_bytes=debug_truncate_bytes,
                debug_headers=debug_headers,
                product=product,
                product_version=product_version,
                token_audience=token_audience,
                custom_headers=custom_headers,
            )
        self._config = config.copy()
        self._api_client = client.ApiClient(self._config)
        self._access_control = pkg_iam.AccountAccessControlAPI(self._api_client)
        self._billable_usage = pkg_billing.BillableUsageAPI(self._api_client)
        self._budget_policy = pkg_billing.BudgetPolicyAPI(self._api_client)
        self._budgets = pkg_billing.BudgetsAPI(self._api_client)
        self._credentials = pkg_provisioning.CredentialsAPI(self._api_client)
        self._custom_app_integration = pkg_oauth2.CustomAppIntegrationAPI(self._api_client)
        self._encryption_keys = pkg_provisioning.EncryptionKeysAPI(self._api_client)
        self._endpoints = pkg_networking.EndpointsAPI(self._api_client)
        self._federation_policy = pkg_oauth2.AccountFederationPolicyAPI(self._api_client)
        self._groups_v2 = pkg_iam.AccountGroupsV2API(self._api_client)
        self._iam_v2 = pkg_iamv2.AccountIamV2API(self._api_client)
        self._ip_access_lists = pkg_settings.AccountIpAccessListsAPI(self._api_client)
        self._log_delivery = pkg_billing.LogDeliveryAPI(self._api_client)
        self._metastore_assignments = pkg_catalog.AccountMetastoreAssignmentsAPI(self._api_client)
        self._metastores = pkg_catalog.AccountMetastoresAPI(self._api_client)
        self._network_connectivity = pkg_settings.NetworkConnectivityAPI(self._api_client)
        self._network_policies = pkg_settings.NetworkPoliciesAPI(self._api_client)
        self._networks = pkg_provisioning.NetworksAPI(self._api_client)
        self._o_auth_published_apps = pkg_oauth2.OAuthPublishedAppsAPI(self._api_client)
        self._private_access = pkg_provisioning.PrivateAccessAPI(self._api_client)
        self._published_app_integration = pkg_oauth2.PublishedAppIntegrationAPI(self._api_client)
        self._service_principal_federation_policy = pkg_oauth2.ServicePrincipalFederationPolicyAPI(self._api_client)
        self._service_principal_secrets = pkg_oauth2.ServicePrincipalSecretsAPI(self._api_client)
        self._service_principals_v2 = pkg_iam.AccountServicePrincipalsV2API(self._api_client)
        self._settings = pkg_settings.AccountSettingsAPI(self._api_client)
        self._settings_v2 = pkg_settingsv2.AccountSettingsV2API(self._api_client)
        self._storage = pkg_provisioning.StorageAPI(self._api_client)
        self._storage_credentials = pkg_catalog.AccountStorageCredentialsAPI(self._api_client)
        self._usage_dashboards = pkg_billing.UsageDashboardsAPI(self._api_client)
        self._users_v2 = pkg_iam.AccountUsersV2API(self._api_client)
        self._vpc_endpoints = pkg_provisioning.VpcEndpointsAPI(self._api_client)
        self._workspace_assignment = pkg_iam.WorkspaceAssignmentAPI(self._api_client)
        self._workspace_network_configuration = pkg_settings.WorkspaceNetworkConfigurationAPI(self._api_client)
        self._workspaces = pkg_provisioning.WorkspacesAPI(self._api_client)
        self._groups = pkg_iam.AccountGroupsAPI(self._api_client)
        self._service_principals = pkg_iam.AccountServicePrincipalsAPI(self._api_client)
        self._users = pkg_iam.AccountUsersAPI(self._api_client)

    @property
    def config(self) -> client.Config:
        return self._config

    @property
    def api_client(self) -> client.ApiClient:
        return self._api_client

    @property
    def access_control(self) -> pkg_iam.AccountAccessControlAPI:
        """These APIs manage access rules on resources in an account."""
        return self._access_control

    @property
    def billable_usage(self) -> pkg_billing.BillableUsageAPI:
        """This API allows you to download billable usage logs for the specified account and date range."""
        return self._billable_usage

    @property
    def budget_policy(self) -> pkg_billing.BudgetPolicyAPI:
        """A service serves REST API about Budget policies."""
        return self._budget_policy

    @property
    def budgets(self) -> pkg_billing.BudgetsAPI:
        """These APIs manage budget configurations for this account."""
        return self._budgets

    @property
    def credentials(self) -> pkg_provisioning.CredentialsAPI:
        """These APIs manage credential configurations for this workspace."""
        return self._credentials

    @property
    def custom_app_integration(self) -> pkg_oauth2.CustomAppIntegrationAPI:
        """These APIs enable administrators to manage custom OAuth app integrations, which is required for adding/using Custom OAuth App Integration like Tableau Cloud for Databricks in AWS cloud."""
        return self._custom_app_integration

    @property
    def encryption_keys(self) -> pkg_provisioning.EncryptionKeysAPI:
        """These APIs manage encryption key configurations for this workspace (optional)."""
        return self._encryption_keys

    @property
    def endpoints(self) -> pkg_networking.EndpointsAPI:
        """These APIs manage endpoint configurations for this account."""
        return self._endpoints

    @property
    def federation_policy(self) -> pkg_oauth2.AccountFederationPolicyAPI:
        """These APIs manage account federation policies."""
        return self._federation_policy

    @property
    def groups_v2(self) -> pkg_iam.AccountGroupsV2API:
        """Groups simplify identity management, making it easier to assign access to Databricks account, data, and other securable objects."""
        return self._groups_v2

    @property
    def iam_v2(self) -> pkg_iamv2.AccountIamV2API:
        """These APIs are used to manage identities and the workspace access of these identities in <Databricks>."""
        return self._iam_v2

    @property
    def ip_access_lists(self) -> pkg_settings.AccountIpAccessListsAPI:
        """The Accounts IP Access List API enables account admins to configure IP access lists for access to the account console."""
        return self._ip_access_lists

    @property
    def log_delivery(self) -> pkg_billing.LogDeliveryAPI:
        """These APIs manage log delivery configurations for this account."""
        return self._log_delivery

    @property
    def metastore_assignments(self) -> pkg_catalog.AccountMetastoreAssignmentsAPI:
        """These APIs manage metastore assignments to a workspace."""
        return self._metastore_assignments

    @property
    def metastores(self) -> pkg_catalog.AccountMetastoresAPI:
        """These APIs manage Unity Catalog metastores for an account."""
        return self._metastores

    @property
    def network_connectivity(self) -> pkg_settings.NetworkConnectivityAPI:
        """These APIs provide configurations for the network connectivity of your workspaces for serverless compute resources."""
        return self._network_connectivity

    @property
    def network_policies(self) -> pkg_settings.NetworkPoliciesAPI:
        """These APIs manage network policies for this account."""
        return self._network_policies

    @property
    def networks(self) -> pkg_provisioning.NetworksAPI:
        """These APIs manage network configurations for customer-managed VPCs (optional)."""
        return self._networks

    @property
    def o_auth_published_apps(self) -> pkg_oauth2.OAuthPublishedAppsAPI:
        """These APIs enable administrators to view all the available published OAuth applications in Databricks."""
        return self._o_auth_published_apps

    @property
    def private_access(self) -> pkg_provisioning.PrivateAccessAPI:
        """These APIs manage private access settings for this account."""
        return self._private_access

    @property
    def published_app_integration(self) -> pkg_oauth2.PublishedAppIntegrationAPI:
        """These APIs enable administrators to manage published OAuth app integrations, which is required for adding/using Published OAuth App Integration like Tableau Desktop for Databricks in AWS cloud."""
        return self._published_app_integration

    @property
    def service_principal_federation_policy(self) -> pkg_oauth2.ServicePrincipalFederationPolicyAPI:
        """These APIs manage service principal federation policies."""
        return self._service_principal_federation_policy

    @property
    def service_principal_secrets(self) -> pkg_oauth2.ServicePrincipalSecretsAPI:
        """These APIs enable administrators to manage service principal secrets."""
        return self._service_principal_secrets

    @property
    def service_principals_v2(self) -> pkg_iam.AccountServicePrincipalsV2API:
        """Identities for use with jobs, automated tools, and systems such as scripts, apps, and CI/CD platforms."""
        return self._service_principals_v2

    @property
    def settings(self) -> pkg_settings.AccountSettingsAPI:
        """Accounts Settings API allows users to manage settings at the account level."""
        return self._settings

    @property
    def settings_v2(self) -> pkg_settingsv2.AccountSettingsV2API:
        """APIs to manage account level settings."""
        return self._settings_v2

    @property
    def storage(self) -> pkg_provisioning.StorageAPI:
        """These APIs manage storage configurations for this workspace."""
        return self._storage

    @property
    def storage_credentials(self) -> pkg_catalog.AccountStorageCredentialsAPI:
        """These APIs manage storage credentials for a particular metastore."""
        return self._storage_credentials

    @property
    def usage_dashboards(self) -> pkg_billing.UsageDashboardsAPI:
        """These APIs manage usage dashboards for this account."""
        return self._usage_dashboards

    @property
    def users_v2(self) -> pkg_iam.AccountUsersV2API:
        """User identities recognized by Databricks and represented by email addresses."""
        return self._users_v2

    @property
    def vpc_endpoints(self) -> pkg_provisioning.VpcEndpointsAPI:
        """These APIs manage VPC endpoint configurations for this account."""
        return self._vpc_endpoints

    @property
    def workspace_assignment(self) -> pkg_iam.WorkspaceAssignmentAPI:
        """The Workspace Permission Assignment API allows you to manage workspace permissions for principals in your account."""
        return self._workspace_assignment

    @property
    def workspace_network_configuration(self) -> pkg_settings.WorkspaceNetworkConfigurationAPI:
        """These APIs allow configuration of network settings for Databricks workspaces by selecting which network policy to associate with the workspace."""
        return self._workspace_network_configuration

    @property
    def workspaces(self) -> pkg_provisioning.WorkspacesAPI:
        """These APIs manage workspaces for this account."""
        return self._workspaces

    @property
    def groups(self) -> pkg_iam.AccountGroupsAPI:
        """Groups simplify identity management, making it easier to assign access to Databricks account, data, and other securable objects."""
        return self._groups

    @property
    def service_principals(self) -> pkg_iam.AccountServicePrincipalsAPI:
        """Identities for use with jobs, automated tools, and systems such as scripts, apps, and CI/CD platforms."""
        return self._service_principals

    @property
    def users(self) -> pkg_iam.AccountUsersAPI:
        """User identities recognized by Databricks and represented by email addresses."""
        return self._users

    def get_workspace_client(self, workspace: Workspace) -> WorkspaceClient:
        """Constructs a ``WorkspaceClient`` for the given workspace.

        Returns a ``WorkspaceClient`` that is configured to use the same
        credentials as this ``AccountClient``. The underlying config is
        copied from this ``AccountClient``, but the ``host`` and
        ``azure_workspace_resource_id`` are overridden to match the
        given workspace, and the ``account_id`` field is cleared.

        Usage:

        .. code-block::

            wss = list(a.workspaces.list())
            if len(wss) == 0:
                pytest.skip("no workspaces")
            w = a.get_workspace_client(wss[0])
            assert w.current_user.me().active

        :param workspace: The workspace to construct a client for.
        :return: A ``WorkspaceClient`` for the given workspace.
        """
        config = self._config.deep_copy()
        config.host = config.environment.deployment_url(workspace.deployment_name)
        config.azure_workspace_resource_id = azure.get_azure_resource_id(workspace)
        config.account_id = None
        config.workspace_id = workspace.workspace_id
        config.init_auth()
        return WorkspaceClient(config=config)

    def __repr__(self):
        return f"AccountClient(account_id='{self._config.account_id}', auth_type='{self._config.auth_type}', ...)"
