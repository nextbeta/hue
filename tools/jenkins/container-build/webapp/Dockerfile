FROM ubuntu1604:base

LABEL description="Hue Project https://github.com/cloudera/hue"

ADD ./hue_build/hue /opt

EXPOSE 8888
CMD ["./startup.sh"]
