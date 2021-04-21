#!/bin/bash

echo "building..."
rm -rf worker_binary
# workaround https://issues.apache.org/jira/browse/BEAM-12158
GOOS=linux GOARCH=amd64 go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn" -o worker_binary .

echo "deploying..."
GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn go run backfill.go \
  --runner dataflow \
  --project=your_gcp_project \
  --region=us-central1 \
  --job_name=backfill \
  --no_use_public_ips \
  --temp_location gs://your_staging_bucket/backfill-tmp/ \
  --staging_location gs://your_staging_bucket/backfill-binaries/ \
  --worker_harness_container_image=apache/beam_go_sdk:latest \
  --worker_binary worker_binary \
  --environment=development \
  --start_date=2020-11-01 \
  --end_date=2021-03-01
