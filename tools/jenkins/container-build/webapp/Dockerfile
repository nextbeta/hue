FROM ubuntu1604:base

LABEL description="Hue Project https://github.com/cloudera/hue"

ADD ./hue_build/hue /opt/hue
RUN mkdir -p /etc/hue/conf
ADD ./hue-conf/* /etc/hue/conf

EXPOSE 8888
CMD ["./startup.sh"]
