FROM alpine:edge as builder
RUN apk add --no-cache librdkafka-dev build-base git go
RUN mkdir /app
WORKDIR /app
COPY . .
RUN env GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -v -o build/route81

FROM alpine:edge
RUN apk add --no-cache librdkafka ca-certificates
ARG BUILD_DATE
ARG VCS_REF
ARG VSC_URL
ARG BUILD_VERSION
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url=$VSC_URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version=$BUILD_VERSION
ENTRYPOINT ["/bin/route81"]
COPY --from=builder /app/build/route81 /bin/route81
