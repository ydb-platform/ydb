FROM quay.io/keycloak/keycloak:26.0.7@sha256:4388e2379b7e870a447adbe7b80bd61f5fbf04e925832b19669fda4957f05a81

COPY --chown=1000:0 keycloak.conf server.pem server.key /opt/keycloak/conf/
COPY --chown=1000:0 production-realm.json /opt/keycloak/data/import/production-realm.json
