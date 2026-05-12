ARG BASE_IMAGE=mirror.gcr.io/library/mysql@sha256:1579fe3a97a436cc10824fc771a07fcedc92213e7ab7604eb5d2976ca419abc8
FROM $BASE_IMAGE

COPY ./init/init_db.sql /docker-entrypoint-initdb.d/init_db.sql
COPY ./custom.cnf /etc/mysql/conf.d/custom.cnf

HEALTHCHECK --interval=5s --timeout=70s --retries=20 --start-period=20s \
  CMD /usr/bin/mysql -h 127.0.0.1 -uroot -ppassword -e 'show databases;' || exit 1

