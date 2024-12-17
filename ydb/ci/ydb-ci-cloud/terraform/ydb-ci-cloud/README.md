```
export YC_TOKEN=$(yc --profile=ydbtech iam create-token)
./get-backend-configuration.sh
terraform init
```


terraform yandex provider limitations:
1. Make the serverless container public
2. Make a revision with 1 Always on prepared container