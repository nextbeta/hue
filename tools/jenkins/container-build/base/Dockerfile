FROM ubuntu:16.04

LABEL description="Hue Project https://github.com/cloudera/hue"

RUN apt-get update -y && apt-get install -y \
    ant \
    asciidoc \
    build-essential \
    curl \
    git \
    libcurl4-openssl-dev \
    libffi-dev \
    libgmp-dev \
    libkrb5-dev \
    libldap2-dev \
    libmysqlclient-dev \
    libncurses5-dev \
    libreadline-dev \
    libsasl2-2 \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libsqlite3-dev \
    libssl-dev \
    libtidy-dev \
    libxml2-dev \
    libxslt1-dev \
    maven \
    netcat \
    postgresql-client \
    postgresql-common \
    python-dev \
    python-setuptools \
    software-properties-common \
    sudo \
    tar \
    vim \
    wget \
    xmlsec1 \
    zlib1g-dev \
     && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update -y
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java8-installer

RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y nodejs

RUN mkdir -p /home/hue
RUN useradd -ms /bin/bash hue && chown -R hue /home/hue

WORKDIR /home/hue
ENV PREFIX=/opt
