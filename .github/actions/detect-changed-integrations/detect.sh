#!/usr/bin/env bash
# Usage: detect.sh <before-sha-or-ref> <after-sha-or-ref>
#
# Prints one changed integration directory per line — a directory is an
# "integration" if it (or an ancestor, for nested folders like
# reversed_etl/facebook_offline_conversions) contains a prefect.yaml.
set -euo pipefail

before="$1"
after="$2"

git diff --name-only "$before" "$after" | while IFS= read -r f; do
  # Archived integrations are retained for reference and must never deploy.
  case "$f" in
    archive/*) continue ;;
  esac
  d=$(dirname "$f")
  while [ "$d" != "." ] && [ "$d" != "/" ]; do
    if [ -f "$d/prefect.yaml" ] && [ -f "$d/cloudbuild.yaml" ]; then
      echo "$d"
      break
    fi
    d=$(dirname "$d")
  done
done | sort -u
