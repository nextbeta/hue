FROM ubuntu1604:base

LABEL description="Hue Project https://github.com/cloudera/hue"

ADD ./hue_build/hue /opt/hue
RUN mkdir -p /etc/alternatives
ADD ./hue-conf /etc/alternatives/
RUN mkdir -p /etc/hue/
RUN ln -s /etc/alternatives/hue-conf /etc/hue/conf 

EXPOSE 8888
CMD ["./startup.sh"]
