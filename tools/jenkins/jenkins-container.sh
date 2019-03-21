#!/bin/bash

set -e
set -x

BINDIR=$(dirname $(readlink -f $0))
ROOT=${BINDIR%/*/*/*}
BUILD_LOG=/var/log/hue-build.log

BASEOS="ubuntu1604"
BASEIMAGE="${BASEOS}:base"
DOCKER_REGISTRY=${DOCKER_REGISTRY:-"docker-registry.infra.cloudera.com/huecontainer"}
BASEDOCKER=${DOCKER_REGISTRY}/${BASEIMAGE}

docker pull $BASEDOCKER 1>$BUILD_LOG 2>&1

WEBAPP_DIR=$BINDIR/container-build/webapp
HUE_SRC=$ROOT/hue
HUE_BLD=$WEBAPP_DIR/hue_build
mkdir -p $HUE_BLD

docker pull $BASEDOCKER
docker tag $BASEDOCKER $BASEIMAGE

docker run -it -v $HUE_SRC:/hue -v $HUE_BLD:/opt $BASEDOCKER bash -c "cd /hue; make install"
docker run -it -v $HUE_SRC:/hue -v $HUE_BLD:/opt $BASEDOCKER bash -c "cd /opt/hue; /opt/hue/build/env/bin/pip install psycopg2-binary"

#docker run -it -v $HUE_SRC:/hue -v $HUE_BLD:/hue_build $BASEDOCKER bash -c "./build/env/bin/pip install psycopg2-binary"

cd $WEBAPP_DIR
mkdir -p hue-conf 
cp -a $HUE_SRC/desktop/conf.dist/* hue-conf
GBN=$(curl http://gbn.infra.cloudera.com/)
WEBAPPIMAGE="webapp:$GBN"
docker tag $DOCKER_REGISTRY/$BASEIMAGE $BASEIMAGE
docker build -f $WEBAPP_DIR/Dockerfile -t $WEBAPPIMAGE .
docker tag $WEBAPPIMAGE $DOCKER_REGISTRY/$WEBAPPIMAGE 
docker push $DOCKER_REGISTRY/$WEBAPPIMAGE
