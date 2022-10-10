ARG REPOSITORY=""

FROM ${REPOSITORY}/spark:3.3.2-SNAPSHOT

ARG project_version=1.0-SNAPSHOT
ARG project_name=spark-benchmark
ARG project_dir=/opt/spark/apps/$project_name

USER root

RUN apt-get install -y gcc make flex bison byacc git && \
	git clone --branch address-make-issues https://github.com/awdavidson/tpcds-kit.git /tmp/databricks--tpcds-kit && \
	cd /tmp/databricks--tpcds-kit/tools && \
	make OS=LINUX

RUN mkdir -p ${project_dir}/${project_version}

COPY target/scala-2.12/${project_name}-assembly-${project_version}.jar ${project_dir}/${project_version}/

RUN chmod 755 -R ${project_dir}/${project_version}

