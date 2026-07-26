#!/usr/bin/env bash
# Usage: deploy.sh <integration-dir> <env: dev|prod> <build-sha> <gcp-project> <gcp-region> [overrides-json-path]
#
# Create-if-not-exists a Cloud Build GitHub trigger for one integration + env,
# run it against the given commit SHA, and poll until it finishes. Exits
# non-zero on any non-SUCCESS terminal build status so the calling workflow
# job fails accordingly.
#
# Trigger name and service account are looked up per-directory in the
# overrides JSON (falls back to "<dir with _ -> -->-<env>" / the default SA
# when a directory has no override entry).
set -euo pipefail

DIR="$1"
ENV="$2"
SHA="$3"
PROJECT="$4"
REGION="$5"
OVERRIDES="${6:-.github/integration-overrides.json}"

if [ "$ENV" != "dev" ] && [ "$ENV" != "prod" ]; then
  echo "Environment must be dev or prod (got '$ENV')" >&2
  exit 1
fi

case "$DIR" in
  /*|*..*) echo "Integration directory must be a repository-relative path without '..'" >&2; exit 1 ;;
esac

DEFAULT_BASE=$(basename "$DIR" | tr '_' '-')
DEFAULT_SA=$(jq -r '.defaults.service_account' "$OVERRIDES")

TRIGGER_BASE=$(jq -r --arg d "$DIR" '.overrides[$d].trigger_base_name // empty' "$OVERRIDES")
TRIGGER_BASE="${TRIGGER_BASE:-$DEFAULT_BASE}"

PROD_OVERRIDE=""
if [ "$ENV" = "prod" ]; then
  PROD_OVERRIDE=$(jq -r --arg d "$DIR" '.overrides[$d].prod_trigger_name // empty' "$OVERRIDES")
fi
TRIGGER_NAME="${PROD_OVERRIDE:-${TRIGGER_BASE}-${ENV}}"

SA=$(jq -r --arg d "$DIR" '.overrides[$d].service_account // empty' "$OVERRIDES")
SA="${SA:-$DEFAULT_SA}"
ENV_SUBSTITUTION=$(jq -r --arg d "$DIR" \
  '.overrides[$d].environment_substitution // .defaults.environment_substitution // "_ENV"' \
  "$OVERRIDES")

if [[ ! "$ENV_SUBSTITUTION" =~ ^_[A-Z0-9_]+$ ]]; then
  echo "Invalid environment substitution '$ENV_SUBSTITUTION' for $DIR" >&2
  exit 1
fi

if [ ! -f "$DIR/prefect.yaml" ] || [ ! -f "$DIR/cloudbuild.yaml" ]; then
  echo "$DIR must contain both prefect.yaml and cloudbuild.yaml" >&2
  exit 1
fi

echo "== ${TRIGGER_NAME} (dir=${DIR}, env=${ENV}, sha=${SHA}, sa=${SA}) =="

if ! gcloud builds triggers describe "$TRIGGER_NAME" \
      --project="$PROJECT" --region="$REGION" >/dev/null 2>&1; then
  echo "Trigger $TRIGGER_NAME does not exist — creating it"
  TMP_YAML=$(mktemp)
  {
    echo "name: ${TRIGGER_NAME}"
    echo "description: \"Auto-created by GitHub Actions CI for ${DIR} [${ENV}]\""
    echo "serviceAccount: projects/${PROJECT}/serviceAccounts/${SA}"
    echo "github:"
    echo "  owner: Advisa"
    echo "  name: de-ingestion-orchestration"
    echo "  push:"
    if [ "$ENV" = "dev" ]; then
      echo "    branch: ^develop\$"
    else
      echo "    branch: ^main\$"
    fi
    echo "includedFiles:"
    echo "- ${DIR}/**"
    echo "filename: ${DIR}/cloudbuild.yaml"
    echo "substitutions:"
    echo "  ${ENV_SUBSTITUTION}: ${ENV}"
    echo "  _PROJECT: ${PROJECT}"
  } > "$TMP_YAML"
  gcloud builds triggers import \
    --source="$TMP_YAML" \
    --project="$PROJECT" --region="$REGION"
  rm -f "$TMP_YAML"
else
  echo "Trigger $TRIGGER_NAME already exists — reusing it"
fi

echo "Running $TRIGGER_NAME against commit $SHA"
BUILD_ID=$(gcloud builds triggers run "$TRIGGER_NAME" \
  --sha="$SHA" \
  --project="$PROJECT" --region="$REGION" \
  --format='value(metadata.build.id)')

if [ -z "$BUILD_ID" ]; then
  echo "Could not extract a build ID from 'gcloud builds triggers run' output — check the gcloud version/output shape." >&2
  exit 1
fi

LOG_URL="https://console.cloud.google.com/cloud-build/builds/${REGION}/${BUILD_ID}?project=${PROJECT}"
echo "Build ID: $BUILD_ID — polling for completion"
echo "Logs: $LOG_URL"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "build_id=$BUILD_ID"
    echo "log_url=$LOG_URL"
  } >> "$GITHUB_OUTPUT"
fi

STATUS="QUEUED"
while [[ "$STATUS" =~ ^(QUEUED|WORKING|PENDING)$ ]]; do
  sleep 10
  STATUS=$(gcloud builds describe "$BUILD_ID" \
    --project="$PROJECT" --region="$REGION" \
    --format='value(status)')
  echo "  status: $STATUS"
done

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  echo "status=$STATUS" >> "$GITHUB_OUTPUT"
fi

if [ "$STATUS" != "SUCCESS" ]; then
  echo "Build $BUILD_ID for $TRIGGER_NAME finished with status $STATUS"
  exit 1
fi

echo "$TRIGGER_NAME deployed successfully (build $BUILD_ID)"
