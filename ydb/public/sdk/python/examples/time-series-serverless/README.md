# Prepare your cloud environment
## Create serverless DB

Go to [web console](https://console.cloud.yandex.ru/) and select Yandex Database, find your folder and create database via **Create database** button. Please select serverless option and name of your database.
Select your DB in the list of databases and note **Endpoint** and **Database** values, you will need them to connect to your DB from cloud function.  

## Create table for time series
Select **Navigation** in left pane and press **Create** in the top right corner, choose **Table** and fill table name, columns name and type.
You should select next values for this example.

| Column name | Type      | Key     |
| ----------- | :-------: | :-----: |
| timestamp   | Timestamp | Primary |
| value       | Double    |         |

Remember YDB endpoint, database and table name

## Create Service account
To create service account you need to go to cloud console to **Service accounts** in left pane and create account with **editor** and **viewer** roles.
Remember account id, it will be used later to allow access to serverless database.

# Create cloud function
Create cloud function from source code. You should archive source code directory to zip file and upload it into **Cloud functions** section. 
Please do not forget to add requirements.txt to this archive.

To upload function you may use CLI. You can refer to CLI documentation to understand 
[how to install it](https://cloud.yandex.com/en/docs/cli/quickstart#install "CLI installation") and 
[how to create profile](https://cloud.yandex.com/en-ru/docs/cli/operations/authentication/user "Get profile via CLI"). 
Please note that you should select proper profile type in left pane to get right instructions.

After successful installation of CLI and creating profile you should execute next command to create function:
```shell
yc serverless function create --name=time-series
```

Next you should upload code and create new version of function via following command:
```shell
yc sls fn version create --service-account-id=<service-account-id> --function-name=time-series --runtime python37 --entrypoint time_series.handler --memory 128m --execution-timeout 60s --source-path <path-to-archived-sources> --environment YDB_ENDPOINT=<db-endpoint>,YDB_DATABASE=<db-database>,YDB_TABLE=<db-table>,USE_METADATA_CREDENTIALS=1
```

The environment variables passed to function 
* YDB_ENDPOINT is the endpoint of your database
* YDB_DATABASE is the path to your database
* YDB_TABLE is the table name
* USE_METADATA_CREDENTIALS=1 means that your code automatically get [IAM token](https://cloud.yandex.com/en-ru/docs/iam/concepts/authorization/iam-token) for service account you specified 

After a while you receive message that function was created. Now you can test it in cloud console or in CLI.

# Test cloud function
## Testing via CLI
You should invoke function via next command
```shell
yc serverless function invoke <function-id> -d '{"queryStringParameters": {"start": "1615000000000", "end": "1615000010000", "interval": "1", "mean": "12.3", "sigma": "5"}}'
```

## Testing via CURL
Before you can access your function via CURL you should make function public. You can do it in web console at
funtion overview page.

Next you may call function with next command:
```shell
curl "https://functions.yandexcloud.net/<function-id>?start=1615000000000&end=1615000010000&interval=1&mean=12.3&sigma=5"
```