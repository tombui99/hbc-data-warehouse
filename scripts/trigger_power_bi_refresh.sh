#!/usr/bin/env bash

set -euo pipefail

required_variables=(
  PBI_TENANT_ID
  PBI_CLIENT_ID
  PBI_CLIENT_SECRET
  PBI_WORKSPACE_ID
  PBI_DATASET_ID
)

for variable_name in "${required_variables[@]}"; do
  if [[ -z "${!variable_name:-}" ]]; then
    echo "Missing required GitHub Actions secret: ${variable_name}" >&2
    exit 1
  fi
done

response_file=$(mktemp)
trap 'rm -f "${response_file}"' EXIT

http_status=$(curl --silent --show-error \
  --output "${response_file}" \
  --write-out "%{http_code}" \
  -X POST "https://login.microsoftonline.com/${PBI_TENANT_ID}/oauth2/v2.0/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "client_id=${PBI_CLIENT_ID}" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "scope=https://analysis.windows.net/powerbi/api/.default" \
  --data-urlencode "client_secret=${PBI_CLIENT_SECRET}")

if [[ "${http_status}" != "200" ]]; then
  error_code=$(jq -r '.error // "unknown_error"' "${response_file}")
  error_description=$(jq -r '.error_description // "No error description returned by Azure."' "${response_file}")
  echo "Azure token request failed with HTTP ${http_status}." >&2
  echo "${error_code}: ${error_description}" >&2
  echo "Check PBI_TENANT_ID, PBI_CLIENT_ID, and PBI_CLIENT_SECRET in GitHub Actions secrets." >&2
  exit 1
fi

access_token=$(jq -er '.access_token' "${response_file}")

curl --fail-with-body --silent --show-error \
  -X POST "https://api.powerbi.com/v1.0/myorg/groups/${PBI_WORKSPACE_ID}/datasets/${PBI_DATASET_ID}/refreshes" \
  -H "Authorization: Bearer ${access_token}" \
  -H "Content-Length: 0"

echo "Power BI dataset refresh accepted."
