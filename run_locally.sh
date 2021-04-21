#!/bin/bash

GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn go run backfill.go --runner=direct --project=your_gcp_project
