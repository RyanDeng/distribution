FROM golang:1.4

RUN echo "deb http://mirrors.163.com/debian/ jessie main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb http://mirrors.163.com/debian/ jessie-updates main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb http://mirrors.163.com/debian/ jessie-backports main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian/ jessie main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian/ jessie-updates main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian/ jessie-backports main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb http://mirrors.163.com/debian-security/ jessie/updates main non-free contrib" >> /etc/apt/sources.list
RUN echo "deb-src http://mirrors.163.com/debian-security/ jessie/updates main non-free contrib" >> /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y librados-dev apache2-utils && \
    rm -rf /var/lib/apt/lists/*

ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV GOPATH $DISTRIBUTION_DIR/Godeps/_workspace:$GOPATH
#ENV DOCKER_BUILDTAGS include_rados include_oss include_gcs

WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR
COPY cmd/registry/config-dev.yml /etc/docker/registry/config.yml
RUN make PREFIX=/go clean binaries

VOLUME ["/var/lib/registry"]
EXPOSE 5000
ENTRYPOINT ["registry"]
CMD ["/etc/docker/registry/config.yml"]
